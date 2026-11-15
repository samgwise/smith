//! The axum-on-hyper serve API (migration target of the hand-rolled
//! `http` module — see docs/SECURITY_REVIEW.md S-03/S-04/S-05).
//!
//! Phase 1 skeleton: accept loop with a connection cap, hyper connection
//! settings (header-read timeout doubles as the idle keep-alive bound —
//! verified in the Phase 0 spike), optional TLS, graceful shutdown, and the
//! `/v1/health` route behind constant-time bearer auth. Routes move here
//! phase by phase; the legacy loop keeps serving until the cutover.

use std::net::ToSocketAddrs as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use smith_protocol::PROTOCOL_VERSION;

use crate::controller::Controller;

/// Request head budget (D-5). hyper errors out oversized heads with 431.
const MAX_HEADER_BYTES: usize = 16 * 1024;
/// Idle bound for connections: hyper applies the header-read timeout to the
/// initial head AND to each keep-alive request's head (Phase 0 spike), so
/// this is also the idle keep-alive lifetime. Clients reconnect transparently.
pub const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Concurrent connections; extras are refused immediately (D-5).
pub const MAX_CONNECTIONS: usize = 1024;

/// Anything we can serve a connection over: plain TCP or a TLS stream.
enum ConnIo {
    Plain(TcpStream),
    Tls(tokio_rustls::server::TlsStream<TcpStream>),
}

impl tokio::io::AsyncRead for ConnIo {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnIo::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            ConnIo::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for ConnIo {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            ConnIo::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            ConnIo::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnIo::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            ConnIo::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnIo::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            ConnIo::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Runtime knobs for [`accept_loop`]. Defaults are the D-5 production values;
/// tests override the timeouts to stay fast.
#[derive(Clone)]
pub struct ApiOptions {
    /// Serve TLS when set (from `[serve].tls_cert` + `tls_key`).
    pub tls: Option<tokio_rustls::TlsAcceptor>,
    /// Idle bound per request head (see [`HEADER_READ_TIMEOUT`]).
    pub header_read_timeout: Duration,
    /// Concurrent connection cap; extras are closed on accept.
    pub max_connections: usize,
    /// Cancel to stop accepting and let in-flight connections finish.
    pub shutdown: CancellationToken,
}

impl Default for ApiOptions {
    fn default() -> Self {
        Self {
            tls: None,
            header_read_timeout: HEADER_READ_TIMEOUT,
            max_connections: MAX_CONNECTIONS,
            shutdown: CancellationToken::new(),
        }
    }
}

/// Shared router state.
#[derive(Clone)]
pub(crate) struct ApiState {
    /// Used by the routes moving in during Phases 2-4; unused while the api
    /// serves health only.
    #[allow(dead_code)]
    pub controller: Arc<Controller>,
    pub token: Arc<String>,
}

/// Bind the serve API (split from [`accept_loop`] so tests can bind port 0
/// and learn the port).
pub async fn bind(addr: &str) -> anyhow::Result<TcpListener> {
    TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding the serve API on {addr}"))
}

/// The accept loop: one task per connection, bounded by a semaphore —
/// connections beyond the cap are closed immediately rather than queued.
/// Cancelling `options.shutdown` stops accepting; in-flight connections
/// finish naturally under their own timeouts (the daemon stays up, so
/// nothing is force-closed underneath a running turn).
pub async fn accept_loop(
    listener: TcpListener,
    controller: Arc<Controller>,
    token: String,
    options: ApiOptions,
) -> anyhow::Result<()> {
    let state = ApiState {
        controller,
        token: Arc::new(token),
    };
    let router = Arc::new(build_router(state));
    let semaphore = Arc::new(tokio::sync::Semaphore::new(options.max_connections));
    let tls = options.tls;
    let header_timeout = options.header_read_timeout;

    loop {
        let accepted = tokio::select! {
            _ = options.shutdown.cancelled() => break,
            accepted = listener.accept() => accepted,
        };
        let (stream, _peer) = accepted?;
        // Over cap: close at once. Queuing would let an unauthenticated peer
        // pin a slot just as well as a real client (S-04).
        let Ok(permit) = semaphore.clone().try_acquire_owned() else {
            drop(stream);
            continue;
        };
        let tls = tls.clone();
        let router = Arc::clone(&router);
        tokio::spawn(async move {
            let io: ConnIo = match &tls {
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(tls_stream) => ConnIo::Tls(tls_stream),
                    Err(e) => {
                        tracing::debug!("TLS accept failed: {e}");
                        return;
                    }
                },
                None => ConnIo::Plain(stream),
            };
            let mut builder = hyper::server::conn::http1::Builder::new();
            builder
                .timer(TokioTimer::new())
                .header_read_timeout(header_timeout)
                .max_header_size(MAX_HEADER_BYTES);
            if let Err(e) = builder.serve_connection(
                TokioIo::new(io),
                TowerToHyperService::new((*router).clone()),
            ).await {
                tracing::debug!("serve connection ended: {e}");
            }
            drop(permit);
        });
    }
    Ok(())
}

/// The serve API router. Auth middleware wraps everything: it runs before
/// any body extractor, so an unauthenticated request never gets its body
/// buffered (S-03).
pub(crate) fn build_router(state: ApiState) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth,
        ))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    json_response(
        StatusCode::OK,
        &serde_json::json!({
            "ok": true,
            "version": env!("CARGO_PKG_VERSION"),
            "protocol": PROTOCOL_VERSION,
        }),
    )
}

/// Bearer-token auth. The comparison is constant time over SHA-256 digests
/// so token equality doesn't leak through timing (S-18).
pub(crate) async fn auth(
    State(state): State<ApiState>,
    req: Request,
    next: Next,
) -> Response {
    let provided = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);
    match provided {
        Some(t) if tokens_match(t, &state.token) => next.run(req).await,
        _ => error_response(StatusCode::UNAUTHORIZED, "unauthorised"),
    }
}

pub(crate) fn tokens_match(provided: &str, expected: &str) -> bool {
    use sha2::{Digest, Sha256};
    use subtle::ConstantTimeEq;
    let provided = Sha256::digest(provided.as_bytes());
    let expected = Sha256::digest(expected.as_bytes());
    provided.ct_eq(&expected).into()
}

/// Every API error is a serde-built `{"error": ...}` envelope (S-24) — no
/// string-formatted JSON anywhere.
pub(crate) fn error_response(status: StatusCode, message: &str) -> Response {
    json_response(status, &serde_json::json!({ "error": message }))
}

pub(crate) fn json_response(status: StatusCode, value: &serde_json::Value) -> Response {
    let body = value.to_string();
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

/// Load the TLS acceptor from `[serve].tls_cert` + `tls_key` (PEM).
pub fn load_tls_acceptor(cert_path: &Path, key_path: &Path) -> anyhow::Result<tokio_rustls::TlsAcceptor> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    // Fail soft if another test/server already installed a provider.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)
        .with_context(|| format!("reading TLS certificate {}", cert_path.display()))?
        .collect::<Result<_, _>>()
        .with_context(|| format!("parsing TLS certificate {}", cert_path.display()))?;
    anyhow::ensure!(!certs.is_empty(), "TLS certificate file is empty: {}", cert_path.display());
    let key = PrivateKeyDer::from_pem_file(key_path)
        .with_context(|| format!("parsing TLS key {}", key_path.display()))?;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("building the TLS server configuration")?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// Bind-address policy (S-05): a non-loopback bind needs TLS actually being
/// served, or an explicit `allow_plaintext_remote = true`. Callers pass
/// `tls_served` truthfully — configuring TLS is not the same as serving it.
pub fn ensure_bind_policy(
    addr: &str,
    tls_served: bool,
    allow_plaintext_remote: bool,
) -> anyhow::Result<()> {
    let ips: Vec<std::net::IpAddr> = addr
        .to_socket_addrs()
        .with_context(|| format!("resolving the bind address {addr}"))?
        .map(|sock| sock.ip())
        .collect();
    anyhow::ensure!(!ips.is_empty(), "bind address {addr} resolved to nothing");
    let all_loopback = ips.iter().all(|ip| ip.is_loopback());
    if !all_loopback && !tls_served && !allow_plaintext_remote {
        anyhow::bail!(
            "refusing to bind smith serve on a non-loopback address ({addr}) without TLS: \
             set [serve].tls_cert and [serve].tls_key, or explicitly opt in with \
             [serve].allow_plaintext_remote = true"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_guard_allows_loopback_and_refuses_remote_plaintext() {
        assert!(ensure_bind_policy("127.0.0.1:9240", false, false).is_ok());
        assert!(ensure_bind_policy("localhost:9240", false, false).is_ok());
        assert!(ensure_bind_policy("[::1]:9240", false, false).is_ok());
        // Remote plaintext: refused unless explicitly allowed.
        assert!(ensure_bind_policy("0.0.0.0:9240", false, false).is_err());
        assert!(ensure_bind_policy("192.168.1.5:9240", false, false).is_err());
        assert!(ensure_bind_policy("0.0.0.0:9240", false, true).is_ok());
        // TLS being served unlocks remote binds.
        assert!(ensure_bind_policy("0.0.0.0:9240", true, false).is_ok());
    }

    #[test]
    fn token_comparison_is_digest_based() {
        assert!(tokens_match("tok", "tok"));
        assert!(!tokens_match("tok", "tok "));
        assert!(!tokens_match("tok", "different"));
        assert!(!tokens_match("", "tok"));
    }
}
