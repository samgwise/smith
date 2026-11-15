//! Integration tests for the axum-on-hyper serve api (the migration target
//! of the hand-rolled HTTP layer). Phase 1 covers the skeleton: health,
//! auth, connection cap, idle timeouts, and TLS.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt as _;

use smith_core::{Agent, ApprovalGate, LlmProvider, Message, ProviderEvent, ToolDef, ToolExecutor};
use smith_protocol::ServeClient;
use tempfile::TempDir;

const TOKEN: &str = "test-token";

// ---- fakes (minimal: the api skeleton needs a Controller to exist) ----

struct NullFactory;

#[async_trait::async_trait]
impl smith_server::SessionFactory for NullFactory {
    async fn create(
        &self,
        _workspace: &std::path::Path,
        _provider: Option<&str>,
        _model: Option<&str>,
        gate: Arc<dyn ApprovalGate>,
    ) -> anyhow::Result<smith_server::SessionParts> {
        let provider_impl: Arc<dyn LlmProvider> = Arc::new(EmptyProvider);
        let executor = Arc::new(smith_core::testing::RecordingExecutor::new());
        let agent = Agent::new(
            provider_impl,
            executor.clone(),
            smith_core::builtin_tools(),
            gate,
            None,
        );
        Ok(smith_server::SessionParts {
            agent,
            executor,
            sandbox: None,
            container_label: Some("fake".into()),
            mcp_status: vec![],
            provider_name: "scripted".into(),
            model_name: "scripted-m".into(),
            workspace_name: "ws".into(),
        })
    }

    fn build_provider(
        &self,
        _provider: Option<&str>,
        _model: Option<&str>,
    ) -> anyhow::Result<Arc<dyn LlmProvider>> {
        Ok(Arc::new(EmptyProvider))
    }

    fn mcp_config(&self) -> smith_mcp::McpConfig {
        smith_mcp::McpConfig::default()
    }
}

struct EmptyProvider;

#[async_trait::async_trait]
impl LlmProvider for EmptyProvider {
    fn model(&self) -> &str {
        "empty"
    }

    async fn chat_stream(
        &self,
        _history: &[Message],
        _tools: &[ToolDef],
    ) -> anyhow::Result<smith_core::ProviderStream> {
        Ok(Box::pin(futures::stream::pending::<
            anyhow::Result<ProviderEvent>,
        >()))
    }
}

// ---- harness -----------------------------------------------------------

fn api_controller(ws: &std::path::Path) -> Arc<smith_server::Controller> {
    let metrics = Arc::new(smith_protocol::metrics::Metrics::default());
    let fleet = Arc::new(smith_server::Fleet::new(ws.join("workers.json"), metrics.clone()));
    smith_server::Controller::with_parts(
        Arc::new(NullFactory),
        Duration::from_secs(60),
        fleet,
        metrics,
        smith_server::WorkerDefaults::default(),
        smith_server::OrchestrateSection::default(),
        None,
        None,
    )
}

/// Start the api server; returns its port.
async fn api_harness(mut options: smith_server::api::ApiOptions) -> (u16, TempDir) {
    let ws = tempfile::tempdir().unwrap();
    let controller = api_controller(ws.path());
    let listener = smith_server::api::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    options.shutdown = tokio_util::sync::CancellationToken::new();
    tokio::spawn(smith_server::api::accept_loop(
        listener,
        controller,
        TOKEN.to_string(),
        options,
    ));
    (port, ws)
}

// ---- tests -------------------------------------------------------------

#[tokio::test]
async fn health_round_trips_and_requires_token() {
    let (port, _ws) = api_harness(Default::default()).await;
    let client = ServeClient::new("127.0.0.1", port, TOKEN.to_string());
    client.health().await.expect("health with the right token");

    let wrong = ServeClient::new("127.0.0.1", port, "nope".into());
    let err = wrong.health().await.unwrap_err().to_string();
    assert!(err.contains("401"), "{err}");
}

/// S-04 part 1: a connection beyond the cap is closed promptly, not queued.
#[tokio::test]
async fn connection_cap_closes_extra_connections() {
    let (port, _ws) = api_harness(smith_server::api::ApiOptions {
        max_connections: 4,
        header_read_timeout: Duration::from_secs(60), // isolates the cap
        ..Default::default()
    })
    .await;

    // Four connections fill the cap; the fifth must be closed at once.
    let mut conns = Vec::new();
    for _ in 0..5 {
        conns.push(tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap());
    }
    let mut probe = Vec::new();
    let closed = tokio::time::timeout(Duration::from_secs(5), conns[4].read_to_end(&mut probe)).await;
    assert!(
        matches!(closed, Ok(Ok(0))),
        "a connection beyond the cap stayed open — the semaphore is not enforced"
    );
}

/// S-04 part 2: the header-read timeout bounds both a silent connection and
/// a keep-alive connection left idle after a completed request.
#[tokio::test]
async fn idle_connections_close_within_the_header_timeout() {
    let (port, _ws) = api_harness(smith_server::api::ApiOptions {
        header_read_timeout: Duration::from_millis(500),
        ..Default::default()
    })
    .await;

    // (a) A client that never sends a head.
    let mut silent = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut buf = Vec::new();
    let closed = tokio::time::timeout(Duration::from_secs(5), silent.read_to_end(&mut buf)).await;
    assert!(
        matches!(closed, Ok(Ok(0))),
        "a silent connection stayed open for 5s with a 500ms header timeout"
    );

    // (b) A keep-alive connection idle after one completed request.
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    s.write_all(b"GET /v1/health HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer test-token\r\n\r\n")
        .await
        .unwrap();
    // Read the complete content-length-framed response.
    let mut resp: Vec<u8> = Vec::new();
    loop {
        let mut chunk = [0u8; 1024];
        let n = s.read(&mut chunk).await.unwrap();
        assert!(n > 0, "server closed before answering");
        resp.extend_from_slice(&chunk[..n]);
        let text = String::from_utf8_lossy(&resp);
        if let Some(head_end) = text.find("\r\n\r\n") {
            let len: usize = text
                .lines()
                .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
                .and_then(|l| l.split(':').nth(1).and_then(|v| v.trim().parse().ok()))
                .unwrap_or(0);
            if resp.len() >= head_end + 4 + len {
                break;
            }
        }
    }
    assert!(text_contains(&resp, "\"ok\":true"), "{}", String::from_utf8_lossy(&resp));
    // Now idle: the server must close the connection on its own.
    let mut tail = Vec::new();
    let closed = tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut tail)).await;
    assert!(
        matches!(closed, Ok(Ok(0))),
        "an idle keep-alive connection stayed open for 5s with a 500ms header timeout"
    );
}

fn text_contains(bytes: &[u8], needle: &str) -> bool {
    String::from_utf8_lossy(bytes).contains(needle)
}

/// S-05 / D-2: the api serves TLS from a self-signed pair, and a client that
/// pins exactly that certificate connects while a mismatched pin is refused.
#[tokio::test]
async fn tls_serves_health_with_a_pinned_certificate() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("leaf.pem");
    let key_path = dir.path().join("leaf.key");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();
    let acceptor = smith_server::api::load_tls_acceptor(&cert_path, &key_path).unwrap();

    let (port, _ws) = api_harness(smith_server::api::ApiOptions {
        tls: Some(acceptor),
        ..Default::default()
    })
    .await;

    // A client pinning the leaf: connects and round-trips over TLS.
    let pinned = pinned_client(cert.cert.pem().as_bytes()).unwrap();
    let name = rustls::pki_types::ServerName::try_from("localhost".to_string()).unwrap();
    let mut stream = pinned
        .connect(name, tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap())
        .await
        .expect("TLS handshake with the pinned leaf");
    let (status, body) = smith_protocol::http::raw_request(&mut stream, "GET", "/v1/health", TOKEN, None)
        .await
        .expect("health over TLS");
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"ok\":true"), "{body}");

    // A client pinning a DIFFERENT certificate: refused.
    let decoy = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let wrong = pinned_client(decoy.cert.pem().as_bytes()).unwrap();
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let name = rustls::pki_types::ServerName::try_from("localhost".to_string()).unwrap();
    let refused = wrong.connect(name, stream).await.is_err();
    assert!(refused, "a mismatched certificate pin must be refused");
}

fn pinned_client(pem: &[u8]) -> anyhow::Result<tokio_rustls::TlsConnector> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::CertificateDer;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from_pem_slice(pem)?)?;
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

/// The executor stub is unused by the api tests but required by SessionParts.
#[allow(dead_code)]
fn _assert_executor_is_send_sync(_: Arc<dyn ToolExecutor>) {}
