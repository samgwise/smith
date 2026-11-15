//! Integration tests for the axum-on-hyper serve api (the migration target
//! of the hand-rolled HTTP layer). Phase 1 covered the skeleton: health,
//! auth, connection cap, idle timeouts, and TLS. Phase 2 covers the
//! non-streaming routes: the auth matrix, body limits (413), and the session
//! and worker flows.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt as _;

use smith_core::{Agent, ApprovalDecision, ApprovalGate, LlmProvider};
use smith_protocol::{
    Capability, CheckpointRequest, EnrolRequest, ExecEvent, ExecEventKind, ExecEventsRequest,
    HeartbeatRequest, Load, OpResult, OpResultRequest, ServeClient,
};
use tempfile::TempDir;

const TOKEN: &str = "test-token";

/// Deterministic api server options: no comment heartbeats, so byte-level
/// assertions never race a keep-alive. (The dedicated heartbeat test turns
/// them on deliberately.)
fn api_options() -> smith_server::api::ApiOptions {
    smith_server::api::ApiOptions {
        sse_heartbeat: None,
        ..Default::default()
    }
}

// ---- fakes (a scripted provider so turns complete) ---------------------

struct ScriptedFactory {
    script: Vec<smith_core::testing::MockResponse>,
}

#[async_trait::async_trait]
impl smith_server::SessionFactory for ScriptedFactory {
    async fn create(
        &self,
        _workspace: &std::path::Path,
        _provider: Option<&str>,
        _model: Option<&str>,
        gate: Arc<dyn ApprovalGate>,
    ) -> anyhow::Result<smith_server::SessionParts> {
        use smith_core::testing::{RecordingExecutor, ScriptedProvider};
        let provider_impl: Arc<dyn LlmProvider> = Arc::new(ScriptedProvider::new(self.script.clone()));
        let executor = Arc::new(RecordingExecutor::new());
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
        use smith_core::testing::ScriptedProvider;
        Ok(Arc::new(ScriptedProvider::new(self.script.clone())))
    }

    fn mcp_config(&self) -> smith_mcp::McpConfig {
        smith_mcp::McpConfig::default()
    }
}

// ---- harness -----------------------------------------------------------

fn api_controller(ws: &std::path::Path, script: Vec<smith_core::testing::MockResponse>) -> Arc<smith_server::Controller> {
    let metrics = Arc::new(smith_protocol::metrics::Metrics::default());
    let fleet = Arc::new(smith_server::Fleet::new(ws.join("workers.json"), metrics.clone()));
    smith_server::Controller::with_parts(
        Arc::new(ScriptedFactory { script }),
        Duration::from_secs(60),
        fleet,
        metrics,
        smith_server::WorkerDefaults::default(),
        smith_server::OrchestrateSection::default(),
        None,
        None,
    )
}

/// Start the api server; returns its port and the controller (for direct
/// registry access in tests).
async fn api_harness(
    options: smith_server::api::ApiOptions,
    script: Vec<smith_core::testing::MockResponse>,
) -> (u16, TempDir, Arc<smith_server::Controller>) {
    let ws = tempfile::tempdir().unwrap();
    let controller = api_controller(ws.path(), script);
    let listener = smith_server::api::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(smith_server::api::accept_loop(
        listener,
        controller.clone(),
        TOKEN.to_string(),
        options,
    ));
    (port, ws, controller)
}

// ---- Phase 1 (skeleton) ------------------------------------------------

#[tokio::test]
async fn health_round_trips_and_requires_token() {
    let (port, _ws, _c) = api_harness(Default::default(), vec![]).await;
    let client = ServeClient::new("127.0.0.1", port, TOKEN.to_string());
    client.health().await.expect("health with the right token");

    let wrong = ServeClient::new("127.0.0.1", port, "nope".into());
    let err = wrong.health().await.unwrap_err().to_string();
    assert!(err.contains("401"), "{err}");
}

/// S-04 part 1: a connection beyond the cap is closed promptly, not queued.
#[tokio::test]
async fn connection_cap_closes_extra_connections() {
    let (port, _ws, _c) = api_harness(
        smith_server::api::ApiOptions {
            max_connections: 4,
            header_read_timeout: Duration::from_secs(60), // isolates the cap
            ..Default::default()
        },
        vec![],
    )
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
    let (port, _ws, _c) = api_harness(
        smith_server::api::ApiOptions {
            header_read_timeout: Duration::from_millis(500),
            ..Default::default()
        },
        vec![],
    )
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
    use tokio::io::AsyncWriteExt;
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
    assert!(String::from_utf8_lossy(&resp).contains("\"ok\":true"));
    // Now idle: the server must close the connection on its own.
    let mut tail = Vec::new();
    let closed = tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut tail)).await;
    assert!(
        matches!(closed, Ok(Ok(0))),
        "an idle keep-alive connection stayed open for 5s with a 500ms header timeout"
    );
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

    let (port, _ws, _c) = api_harness(
        smith_server::api::ApiOptions {
            tls: Some(acceptor),
            ..Default::default()
        },
        vec![],
    )
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
    let stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
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

// ---- Phase 2 (routes) --------------------------------------------------

/// Route-by-auth matrix: daemon routes take the daemon token (worker and
/// anonymous are 401), worker routes take the worker credential (daemon and
/// anonymous are 401), enrol is open (its one-time token is in the body),
/// and unported routes 404.
#[tokio::test]
async fn auth_matrix_routes_by_group() {
    let (port, _ws, controller) = api_harness(Default::default(), vec![]).await;

    // A live worker credential for the worker rows.
    let enrolment = controller.fleet.registry.issue_enrolment("matrix-worker").unwrap();
    let (_wid, worker_token) = controller
        .fleet
        .registry
        .enrol("matrix-worker", &enrolment, &Capability::default())
        .unwrap();

    let heartbeat = serde_json::to_string(&HeartbeatRequest {
        worker_id: "matrix-worker".into(),
        live_leases: vec![],
        capability: Capability::default(),
        load: Load::default(),
    })
    .unwrap();
    let op_result = serde_json::to_string(&OpResultRequest {
        session_id: "sess".into(),
        exec_id: "exec".into(),
        result: OpResult::Done { output: "x".into() },
    })
    .unwrap();
    let bogus_enrol = serde_json::to_string(&EnrolRequest {
        name: "nope".into(),
        enrolment_token: "bogus".into(),
        capability: Capability::default(),
    })
    .unwrap();

    let daemon = TOKEN;
    let worker = worker_token.as_str();
    let none = "";
    let rows: &[(&str, &str, Option<String>, &str, u16)] = &[
        // daemon group
        ("GET", "/v1/sessions", None, daemon, 200),
        ("GET", "/v1/sessions", None, worker, 401),
        ("GET", "/v1/sessions", None, none, 401),
        ("GET", "/v1/workers", None, daemon, 200),
        ("GET", "/v1/workers", None, worker, 401),
        ("GET", "/v1/repos", None, daemon, 200),
        ("GET", "/v1/repos", None, none, 401),
        ("GET", "/v1/metrics", None, daemon, 200),
        ("GET", "/v1/metrics", None, worker, 401),
        // worker group
        ("POST", "/v1/worker/heartbeat", Some(heartbeat.clone()), worker, 200),
        ("POST", "/v1/worker/heartbeat", Some(heartbeat.clone()), daemon, 401),
        ("POST", "/v1/worker/heartbeat", Some(heartbeat), none, 401),
        ("POST", "/v1/worker/op-result", Some(op_result.clone()), worker, 200),
        ("POST", "/v1/worker/op-result", Some(op_result), none, 401),
        // enrol is OPEN: no bearer is fine; the body token decides (403 for a
        // bogus one) — the point is that this is never a bearer 401.
        ("POST", "/v1/worker/enrol", Some(bogus_enrol), none, 403),
        // not yet ported (SSE is Phase 3, git Phase 4): the fallback.
        ("GET", "/v1/sessions/whatever/events", None, daemon, 404),
        ("GET", "/v1/nope", None, daemon, 404),
    ];

    for (method, path, body, token, expected) in rows {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let (status, resp) = smith_protocol::http::raw_request(&mut stream, method, path, token, body.as_deref())
            .await
            .unwrap_or_else(|e| panic!("{method} {path} (token {token:?}) transport error: {e}"));
        assert_eq!(status, *expected, "{method} {path} (token {token:?}): {resp}");
    }
}

/// Over-limit bodies are rejected with 413 — not dropped, not buffered
/// without bound (D-5).
#[tokio::test]
async fn oversized_bodies_get_413() {
    let (port, _ws, controller) = api_harness(Default::default(), vec![]).await;
    let client = ServeClient::new("127.0.0.1", port, TOKEN.to_string());

    // A heartbeat (64 KiB cap) carrying a 100 KiB name. Worker credential:
    // the daemon token is 401 on this route before the limit applies.
    let enrolment = controller.fleet.registry.issue_enrolment("limit-worker").unwrap();
    let (_wid, worker_token) = controller
        .fleet
        .registry
        .enrol("limit-worker", &enrolment, &Capability::default())
        .unwrap();
    let worker_client = ServeClient::new("127.0.0.1", port, worker_token);
    let big = "x".repeat(100 * 1024);
    let err = worker_client
        .post_body(
            "/v1/worker/heartbeat",
            serde_json::to_string(&HeartbeatRequest {
                worker_id: big,
                live_leases: vec![],
                capability: Capability::default(),
                load: Load::default(),
            })
            .unwrap(),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("413"), "{err}");

    // A turn prompt (1 MiB cap) carrying 2 MiB.
    let id = client.create_session("C:\\", None, None).await.unwrap();
    let err = client
        .start_turn(&id, &"x".repeat(2 * 1024 * 1024))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("413"), "{err}");
}

/// The session routes end to end: create, turn (202), status, diff, and the
/// approvals route's no-pending 409.
#[tokio::test]
async fn session_flow_over_the_api() {
    use smith_core::testing::{MockResponse, tool_named};
    let script = vec![MockResponse {
        text: "done".into(),
        calls: vec![tool_named("c1", "list_dir", serde_json::json!({ "path": "." }))],
    }];
    let (port, ws, _c) = api_harness(Default::default(), script).await;
    let client = ServeClient::new("127.0.0.1", port, TOKEN.to_string());

    let id = client
        .create_session(ws.path().to_str().unwrap(), None, None)
        .await
        .unwrap();

    // A turn is accepted; its completion lands in status.
    client.start_turn(&id, "list the files").await.unwrap();
    let mut status = client.status(&id).await.unwrap();
    let mut waited = 0;
    while status.busy && waited < 50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        waited += 1;
        status = client.status(&id).await.unwrap();
    }
    assert!(!status.busy, "the scripted turn must finish");
    assert_eq!(status.provider, "scripted");

    // Diff runs (the fake executor returns an empty diff).
    let diff = client.diff(&id).await.unwrap();
    assert!(diff.contains("git diff"), "{diff}");

    // No approval is pending after the turn: 409.
    let err = client
        .approval(&id, None, ApprovalDecision::Deny)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("409"), "{err}");

    // Cancel on an idle session is still a 202.
    client.cancel(&id).await.unwrap();
}

/// GREEN (C-01, Phase 2): the worker flow over the api, including a 2 MiB
/// op-result — the size the legacy server dropped.
#[tokio::test]
async fn worker_flow_and_two_mib_op_result() {
    let (port, _ws, _c) = api_harness(Default::default(), vec![]).await;
    let daemon_client = ServeClient::new("127.0.0.1", port, TOKEN.to_string());

    // Issue (daemon token) → enrol (body-carried one-time token).
    let enrolment = daemon_client.worker_issue("api-worker").await.unwrap();
    let cred = daemon_client
        .worker_enrol(&EnrolRequest {
            name: "api-worker".into(),
            enrolment_token: enrolment,
            capability: Capability::default(),
        })
        .await
        .unwrap();
    let worker_client = ServeClient::new("127.0.0.1", port, cred.worker_token.clone());

    // Heartbeat.
    worker_client
        .post_body(
            "/v1/worker/heartbeat",
            serde_json::to_string(&HeartbeatRequest {
                worker_id: cred.worker_id.clone(),
                live_leases: vec![],
                capability: Capability::default(),
                load: Load::default(),
            })
            .unwrap(),
        )
        .await
        .unwrap();

    // GREEN (C-01): a 2 MiB op-result is accepted (32 MiB route limit).
    let big = "x".repeat(2 * 1024 * 1024);
    worker_client
        .post_body(
            "/v1/worker/op-result",
            serde_json::to_string(&OpResultRequest {
                session_id: "sess".into(),
                exec_id: "exec".into(),
                result: OpResult::Done { output: big },
            })
            .unwrap(),
        )
        .await
        .unwrap();

    // Checkpoint and exec-events round-trip.
    worker_client
        .post_body(
            "/v1/worker/checkpoint",
            serde_json::to_string(&CheckpointRequest {
                session_id: "sess".into(),
                commit: "abcdef1234567890abcdef".into(),
                pushed: true,
            })
            .unwrap(),
        )
        .await
        .unwrap();
    worker_client
        .post_body(
            "/v1/worker/exec-events",
            serde_json::to_string(&ExecEventsRequest {
                events: vec![ExecEvent {
                    lease_id: "sess".into(),
                    exec_id: "exec".into(),
                    seq: 1,
                    kind: ExecEventKind::Started,
                }],
            })
            .unwrap(),
        )
        .await
        .unwrap();

    // The registry lists the worker.
    let workers = daemon_client.workers().await.unwrap();
    assert!(workers.iter().any(|w| w.name == "api-worker"), "{workers:?}");
}

/// GREEN (S-03, Phase 2): an unauthenticated request is refused before its
/// body is read. The head declares a 256 MiB body that never arrives; the
/// 401 must come back anyway.
#[tokio::test]
async fn s03_unauthenticated_body_is_not_read() {
    let (port, _ws, _c) = api_harness(Default::default(), vec![]).await;

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let head = "POST /v1/sessions HTTP/1.1\r\n\
                Host: localhost\r\n\
                Authorization: Bearer wrong\r\n\
                Content-Length: 268435456\r\n\
                Connection: close\r\n\r\n";
    use tokio::io::AsyncWriteExt;
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    // Deliberately send NO body. Auth must not wait for it.

    let mut buf = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut buf)).await;
    let text = match read {
        Ok(Ok(_)) => String::from_utf8_lossy(&buf).into_owned(),
        Ok(Err(e)) => panic!("connection error before a response: {e}"),
        Err(_) => panic!(
            "no response within 3s to an unauthenticated request whose body never arrived — \
             the server is buffering the declared body before checking auth (S-03)"
        ),
    };
    assert!(text.starts_with("HTTP/1.1 401"), "{text}");
}

// ---- Phase 3 (SSE) -----------------------------------------------------

use smith_protocol::Envelope;

/// The golden-bytes guarantee: both servers serve the same journal, and the
/// SSE payloads are identical envelope JSON, replay and live. (Chunk framing
/// differs by transport and is not part of the frozen contract.)
#[tokio::test]
async fn sse_payloads_are_identical_across_legacy_and_api() {
    let ws = tempfile::tempdir().unwrap();
    let metrics = Arc::new(smith_protocol::metrics::Metrics::default());
    let fleet = Arc::new(smith_server::Fleet::new(ws.path().join("workers.json"), metrics.clone()));
    let controller = smith_server::Controller::with_parts(
        Arc::new(ScriptedFactory { script: vec![] }),
        Duration::from_secs(60),
        fleet,
        metrics,
        smith_server::WorkerDefaults::default(),
        smith_server::OrchestrateSection::default(),
        None,
        None,
    );

    // The legacy server on one port, the api server on another — one journal.
    let legacy_listener = smith_server::api::bind("127.0.0.1:0").await.unwrap();
    let legacy_port = legacy_listener.local_addr().unwrap().port();
    tokio::spawn(smith_server::http::accept_loop(
        legacy_listener,
        controller.clone(),
        TOKEN.to_string(),
    ));
    let api_listener = smith_server::api::bind("127.0.0.1:0").await.unwrap();
    let api_port = api_listener.local_addr().unwrap().port();
    tokio::spawn(smith_server::api::accept_loop(
        api_listener,
        controller.clone(),
        TOKEN.to_string(),
        api_options(),
    ));

    let legacy = ServeClient::new("127.0.0.1", legacy_port, TOKEN.to_string());
    let api = ServeClient::new("127.0.0.1", api_port, TOKEN.to_string());
    let id = legacy
        .create_session(ws.path().to_str().unwrap(), None, None)
        .await
        .unwrap();
    let handle = controller.get(&id).unwrap();

    // Replay path: two notices journalled before anyone connects.
    handle
        .journal
        .publish_core(smith_core::CoreEvent::Notice { text: "one".into() });
    handle
        .journal
        .publish_core(smith_core::CoreEvent::Notice { text: "two".into() });

    let mut legacy_stream = legacy.events(&id, 0).await.unwrap();
    let mut api_stream = api.events(&id, 0).await.unwrap();

    // Live path: more events while both streams are attached.
    handle
        .journal
        .publish_core(smith_core::CoreEvent::Notice { text: "live-1".into() });
    handle
        .journal
        .publish_core(smith_core::CoreEvent::Notice { text: "live-2".into() });

    let mut legacy_json = Vec::new();
    let mut api_json = Vec::new();
    for _ in 0..5 {
        // Status(create) + notice1 + notice2 (replay) + live-1 + live-2.
        let l: Envelope = tokio::time::timeout(Duration::from_secs(5), legacy_stream.next())
            .await
            .expect("legacy stream event")
            .expect("legacy stream error")
            .expect("legacy stream ended");
        let a: Envelope = tokio::time::timeout(Duration::from_secs(5), api_stream.next())
            .await
            .expect("api stream event")
            .expect("api stream error")
            .expect("api stream ended");
        legacy_json.push(serde_json::to_string(&l).unwrap());
        api_json.push(serde_json::to_string(&a).unwrap());
    }
    assert_eq!(legacy_json, api_json, "the two servers must emit identical bytes");
    assert!(legacy_json.iter().any(|j| j.contains("\"one\"")), "{legacy_json:?}");
    assert!(legacy_json.iter().any(|j| j.contains("\"live-2\"")), "{legacy_json:?}");
}

/// Heartbeats: comment frames appear on an idle stream (raw bytes) and are
/// skipped by the client-side SSE parser (no phantom payloads).
#[tokio::test]
async fn sse_heartbeats_are_comments_the_client_ignores() {
    let (port, ws, _controller) = api_harness(
        smith_server::api::ApiOptions {
            sse_heartbeat: Some(Duration::from_millis(100)),
            ..Default::default()
        },
        vec![],
    )
    .await;
    let client = ServeClient::new("127.0.0.1", port, TOKEN.to_string());
    let id = client
        .create_session(ws.path().to_str().unwrap(), None, None)
        .await
        .unwrap();

    // (a) Raw socket: the comment frames are on the wire while idle.
    let mut raw_sock = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    use tokio::io::AsyncWriteExt;
    raw_sock
        .write_all(
            format!(
                "GET /v1/sessions/{id}/events HTTP/1.1\r\nHost: localhost\r\n\
                 Authorization: Bearer {TOKEN}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut raw = Vec::new();
    let _ = tokio::time::timeout(Duration::from_millis(600), raw_sock.read_to_end(&mut raw)).await;
    let raw_text = String::from_utf8_lossy(&raw);
    assert!(raw_text.contains("text/event-stream"), "{raw_text}");
    assert!(
        raw_text.contains(": keep-alive\n\n"),
        "expected comment heartbeats on the wire: {raw_text}"
    );

    // (b) The client parser sees envelopes only — comments never surface as
    // payloads. A fresh connection from since=1 has an empty replay and
    // nothing live: only heartbeats flow, which are not payloads.
    let mut quiet_stream = client.events(&id, 1).await.unwrap();
    let quiet = tokio::time::timeout(Duration::from_millis(400), quiet_stream.next_raw()).await;
    match quiet {
        Err(_) => {} // quiet window: no payload surfaced from the comments
        Ok(Ok(None)) => {} // stream ended: also fine
        Ok(Ok(Some(payload))) => panic!("a comment leaked as a payload: {payload}"),
        Ok(Err(e)) => panic!("stream error: {e}"),
    }

    // And the normal client path still parses the heartbeating stream: the
    // status replay arrives as the first envelope.
    let mut stream = client.events(&id, 0).await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("status envelope")
        .expect("stream error")
        .expect("stream ended");
    assert_eq!(first.seq, 1);
}

/// A subscriber that overflows its broadcast queue (4096) is refilled from
/// the journal ring: every envelope arrives, exactly once, in order.
#[tokio::test]
async fn lagged_sse_subscribers_refill_from_the_ring() {
    let (port, _ws, controller) = api_harness(
        smith_server::api::ApiOptions {
            sse_heartbeat: None,
            ..Default::default()
        },
        vec![],
    )
    .await;
    let client = ServeClient::new("127.0.0.1", port, TOKEN.to_string());
    let id = client
        .create_session(
            tempfile::tempdir()
                .unwrap()
                .path()
                .to_str()
                .unwrap(),
            None,
            None,
        )
        .await
        .unwrap();
    let handle = controller.get(&id).unwrap();

    let mut stream = client.events(&id, 0).await.unwrap();
    // The broadcast queue (4096) overflows while nothing polls the stream.
    for i in 0..4200 {
        handle
            .journal
            .publish_core(smith_core::CoreEvent::Notice { text: format!("n{i}") });
    }

    // Everything: status(seq 1) + 4200 notices, in order, no dups.
    let mut last = 0;
    for _ in 0..4201 {
        let env = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("timed out waiting for an event")
            .expect("stream error")
            .expect("stream ended early");
        assert!(env.seq > last, "sequence went backwards: {} after {}", env.seq, last);
        last = env.seq;
    }
    assert_eq!(last, 4201);
}
