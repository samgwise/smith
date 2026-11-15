//! The axum-on-hyper serve API (migration target of the hand-rolled
//! `http` module — see docs/SECURITY_REVIEW.md S-03/S-04/S-05).
//!
//! Phase 1 skeleton: accept loop with a connection cap, hyper connection
//! settings (header-read timeout doubles as the idle keep-alive bound —
//! verified in the Phase 0 spike), optional TLS, graceful shutdown, and the
//! `/v1/health` route behind constant-time bearer auth. Routes move here
//! phase by phase; the legacy loop keeps serving until the cutover.

use std::net::ToSocketAddrs as _;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Extension, Path, Request, State};
use axum::handler::Handler;
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::Router;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use futures::Stream;
use smith_core::CoreEvent;
use smith_protocol::{
    ApprovalPost, CheckpointRequest, ClearancePost, CreateSession, EnrolReply, EnrolRequest,
    Envelope, ExecEventKind, ExecEventsRequest, HeartbeatRequest, OpResultRequest, ProviderPost,
    ResumePost, StartTurn, WireEvent, WorkerCommand, WorkerSummary, PROTOCOL_VERSION,
};

use crate::controller::{ApiError, Controller};
use crate::journal::Journal;
use crate::workers::WorkerStreams;

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
    /// SSE comment heartbeats on idle streams (keeps NAT/proxy state alive
    /// and detects dead peers). Comments are skipped by every SSE parser.
    pub sse_heartbeat: Option<Duration>,
    /// Cancel to stop accepting and let in-flight connections finish.
    pub shutdown: CancellationToken,
}

impl Default for ApiOptions {
    fn default() -> Self {
        Self {
            tls: None,
            header_read_timeout: HEADER_READ_TIMEOUT,
            max_connections: MAX_CONNECTIONS,
            sse_heartbeat: Some(Duration::from_secs(15)),
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
    /// SSE comment heartbeat cadence (None = none; tests use None for
    /// exact-byte checks).
    pub sse_heartbeat: Option<Duration>,
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
        sse_heartbeat: options.sse_heartbeat,
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

/// Body budgets per route group (D-5). The legacy server capped everything
/// at 1 MiB; the metadata posts are tightened and op-result widened so the
/// worker's capped results (D-6) fit after JSON escaping.
const JSON_BODY_LIMIT: usize = 1024 * 1024;
const META_BODY_LIMIT: usize = 64 * 1024;
const EXEC_EVENTS_LIMIT: usize = 1024 * 1024;
const OP_RESULT_LIMIT: usize = 32 * 1024 * 1024;

/// The authenticated worker's registry name (inserted by [`worker_auth`]).
#[derive(Clone)]
pub(crate) struct WorkerIdentity(pub String);

/// The serve API router. Auth middleware wraps everything: it runs before
/// any body extractor, so an unauthenticated request never gets its body
/// buffered (S-03). Three groups: daemon-token routes, worker-credential
/// routes, and the open enrol route (which authenticates with the one-time
/// token carried in its body).
pub(crate) fn build_router(state: ApiState) -> Router {
    let daemon = Router::new()
        .route("/v1/health", get(health))
        .route(
            "/v1/sessions",
            get(sessions_list)
                .post(session_create.layer(DefaultBodyLimit::max(JSON_BODY_LIMIT))),
        )
        .route(
            "/v1/sessions/{id}/turns",
            post(session_turns.layer(DefaultBodyLimit::max(JSON_BODY_LIMIT))),
        )
        .route("/v1/sessions/{id}/cancel", post(session_cancel))
        .route(
            "/v1/sessions/{id}/approvals",
            post(session_approvals.layer(DefaultBodyLimit::max(META_BODY_LIMIT))),
        )
        .route(
            "/v1/sessions/{id}/provider",
            post(session_provider.layer(DefaultBodyLimit::max(META_BODY_LIMIT))),
        )
        .route(
            "/v1/sessions/{id}/clearance",
            post(session_clearance.layer(DefaultBodyLimit::max(META_BODY_LIMIT))),
        )
        .route(
            "/v1/sessions/{id}/resume",
            post(session_resume.layer(DefaultBodyLimit::max(META_BODY_LIMIT))),
        )
        .route("/v1/sessions/{id}/diff", get(session_diff))
        .route("/v1/sessions/{id}/status", get(session_status))
        .route("/v1/sessions/{id}/events", get(session_events))
        .route(
            "/v1/workers",
            get(workers_list)
                .post(worker_issue.layer(DefaultBodyLimit::max(META_BODY_LIMIT))),
        )
        .route("/v1/workers/{name}", delete(worker_revoke))
        .route("/v1/repos", get(repos_list))
        .route("/v1/metrics", get(metrics))
        .layer(
            axum::middleware::from_fn_with_state(state.clone(), daemon_auth),
        );

    let worker = Router::new()
        .route(
            "/v1/worker/heartbeat",
            post(worker_heartbeat).layer(DefaultBodyLimit::max(META_BODY_LIMIT)),
        )
        .route(
            "/v1/worker/op-result",
            post(worker_op_result).layer(DefaultBodyLimit::max(OP_RESULT_LIMIT)),
        )
        .route(
            "/v1/worker/exec-events",
            post(worker_exec_events).layer(DefaultBodyLimit::max(EXEC_EVENTS_LIMIT)),
        )
        .route(
            "/v1/worker/checkpoint",
            post(worker_checkpoint).layer(DefaultBodyLimit::max(META_BODY_LIMIT)),
        )
        .route("/v1/worker/events", get(worker_events))
        .layer(
            axum::middleware::from_fn_with_state(state.clone(), worker_auth),
        );

    // Enrolment authenticates with the one-time token in its body; no
    // bearer applies (the worker has no credential yet).
    let open = Router::new().route(
        "/v1/worker/enrol",
        post(worker_enrol).layer(DefaultBodyLimit::max(META_BODY_LIMIT)),
    );

    daemon
        .merge(worker)
        .merge(open)
        .fallback(not_found)
        .with_state(state)
}

async fn not_found() -> Response {
    error_response(StatusCode::NOT_FOUND, "unknown route")
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

/// Bearer-token auth for the daemon routes. The comparison is constant time
/// over SHA-256 digests so token equality doesn't leak through timing (S-18).
pub(crate) async fn daemon_auth(
    State(state): State<ApiState>,
    req: Request,
    next: Next,
) -> Response {
    if bearer(req.headers()).is_some_and(|t| tokens_match(t, &state.token)) {
        next.run(req).await
    } else {
        error_response(StatusCode::UNAUTHORIZED, "unauthorised")
    }
}

/// Worker-credential auth: resolves the worker's registry name and hands it
/// to the handler via request extensions.
pub(crate) async fn worker_auth(
    State(state): State<ApiState>,
    mut req: Request,
    next: Next,
) -> Response {
    let Some(worker) = bearer(req.headers())
        .and_then(|t| state.controller.fleet.registry.authenticate(t))
    else {
        return error_response(StatusCode::UNAUTHORIZED, "unauthorised");
    };
    req.extensions_mut().insert(WorkerIdentity(worker));
    next.run(req).await
}

fn bearer(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
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

/// Map the controller's [`ApiError`] onto a response.
fn api_error(e: &ApiError) -> Response {
    let status = StatusCode::from_u16(e.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    error_response(status, &e.message)
}

/// The `202 {"accepted": true}` envelope for acked operations.
fn accepted() -> Response {
    json_response(StatusCode::ACCEPTED, &serde_json::json!({ "accepted": true }))
}

/// Parse a JSON body; the legacy error shape is kept byte-identical.
fn parse_body<T: serde::de::DeserializeOwned>(body: &Bytes) -> Result<T, Response> {
    serde_json::from_slice(body)
        .map_err(|e| error_response(StatusCode::BAD_REQUEST, &format!("bad request body: {e}")))
}

pub(crate) fn json_response<T: serde::Serialize>(status: StatusCode, value: &T) -> Response {
    match serde_json::to_string(value) {
        Ok(body) => (
            status,
            [(header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [(header::CONTENT_TYPE, "application/json")],
            serde_json::json!({ "error": e.to_string() }).to_string(),
        )
            .into_response(),
    }
}

// ---- daemon routes -----------------------------------------------------

async fn sessions_list(State(state): State<ApiState>) -> Response {
    json_response(StatusCode::OK, &state.controller.sessions())
}

async fn session_create(
    State(state): State<ApiState>,
    body: Bytes,
) -> Response {
    let Ok(req) = parse_body::<CreateSession>(&body) else {
        return error_response(StatusCode::BAD_REQUEST, "bad request body");
    };
    match state.controller.create_session(req).await {
        Ok(id) => json_response(StatusCode::OK, &serde_json::json!({ "session_id": id })),
        Err(e) => api_error(&e),
    }
}

async fn session_turns(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    let Ok(req) = parse_body::<StartTurn>(&body) else {
        return error_response(StatusCode::BAD_REQUEST, "bad request body");
    };
    match state.controller.start_turn(&id, &req.input).await {
        Ok(()) => accepted(),
        Err(e) => api_error(&e),
    }
}

async fn session_cancel(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Response {
    match state.controller.cancel_turn(&id) {
        Ok(()) => accepted(),
        Err(e) => api_error(&e),
    }
}

async fn session_approvals(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    let Ok(req) = parse_body::<ApprovalPost>(&body) else {
        return error_response(StatusCode::BAD_REQUEST, "bad request body");
    };
    match state
        .controller
        .deliver_approval(&id, req.call_id.as_deref(), req.decision)
        .await
    {
        Ok(()) => accepted(),
        Err(e) => api_error(&e),
    }
}

async fn session_provider(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    let Ok(req) = parse_body::<ProviderPost>(&body) else {
        return error_response(StatusCode::BAD_REQUEST, "bad request body");
    };
    match state
        .controller
        .set_provider(&id, req.provider.as_deref(), req.model.as_deref())
        .await
    {
        Ok(()) => accepted(),
        Err(e) => api_error(&e),
    }
}

async fn session_clearance(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    let Ok(req) = parse_body::<ClearancePost>(&body) else {
        return error_response(StatusCode::BAD_REQUEST, "bad request body");
    };
    match state
        .controller
        .forward_clearance(&id, req.id, req.decision)
        .await
    {
        Ok(()) => accepted(),
        Err(e) => api_error(&e),
    }
}

async fn session_resume(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    let Ok(req) = parse_body::<ResumePost>(&body) else {
        return error_response(StatusCode::BAD_REQUEST, "bad request body");
    };
    match state.controller.resume(&id, &req.prefix).await {
        Ok(reply) => json_response(StatusCode::OK, &reply),
        Err(e) => api_error(&e),
    }
}

async fn session_diff(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    match state.controller.diff(&id).await {
        Ok(text) => json_response(StatusCode::OK, &serde_json::json!({ "text": text })),
        Err(e) => api_error(&e),
    }
}

async fn session_status(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    match state.controller.get(&id) {
        Ok(handle) => json_response(StatusCode::OK, &handle.status().await),
        Err(e) => api_error(&e),
    }
}

async fn workers_list(State(state): State<ApiState>) -> Response {
    let list: Vec<WorkerSummary> = state
        .controller
        .fleet
        .registry
        .list()
        .into_iter()
        .map(|w| WorkerSummary {
            name: w.name,
            id: w.id,
            status: w.status,
            capability: w.capability,
            load: w.load,
            last_seen_ms: w.last_seen_ms,
        })
        .collect();
    json_response(StatusCode::OK, &list)
}

async fn worker_issue(
    State(state): State<ApiState>,
    body: Bytes,
) -> Response {
    let parsed: Result<serde_json::Value, _> = serde_json::from_slice(&body);
    let Ok(v) = parsed else {
        return error_response(StatusCode::BAD_REQUEST, "bad body");
    };
    let Some(name) = v.get("name").and_then(serde_json::Value::as_str) else {
        return error_response(StatusCode::BAD_REQUEST, "name is required");
    };
    match state.controller.fleet.registry.issue_enrolment(name) {
        Ok(token) => json_response(
            StatusCode::OK,
            &serde_json::json!({ "enrolment_token": token }),
        ),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e:#}")),
    }
}

async fn worker_revoke(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Response {
    match state.controller.fleet.registry.revoke(&name) {
        Ok(true) => {
            let lost = state.controller.fleet.board.sessions_of(&name);
            state.controller.fleet.streams.disconnect(&name);
            state.controller.handle_lease_loss(&name, &lost);
            json_response(StatusCode::OK, &serde_json::json!({ "revoked": true }))
        }
        Ok(false) => error_response(StatusCode::NOT_FOUND, "no such worker"),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e:#}")),
    }
}

async fn repos_list(State(state): State<ApiState>) -> Response {
    let repos = state
        .controller
        .forge
        .as_ref()
        .map(|f| f.list_repos())
        .unwrap_or_default();
    json_response(StatusCode::OK, &repos)
}

async fn metrics(State(state): State<ApiState>) -> Response {
    let text = state.controller.metrics.render();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        text,
    )
        .into_response()
}

// ---- worker routes -----------------------------------------------------

async fn worker_enrol(State(state): State<ApiState>, body: Bytes) -> Response {
    let Ok(req) = parse_body::<EnrolRequest>(&body) else {
        return error_response(StatusCode::BAD_REQUEST, "bad request body");
    };
    match state
        .controller
        .fleet
        .registry
        .enrol(&req.name, &req.enrolment_token, &req.capability)
    {
        Ok((worker_id, worker_token)) => json_response(
            StatusCode::OK,
            &EnrolReply {
                worker_id,
                worker_token,
            },
        ),
        Err(e) => error_response(StatusCode::FORBIDDEN, &format!("{e:#}")),
    }
}

async fn worker_heartbeat(
    State(state): State<ApiState>,
    Extension(worker): Extension<WorkerIdentity>,
    body: Bytes,
) -> Response {
    let Ok(req) = parse_body::<HeartbeatRequest>(&body) else {
        return error_response(StatusCode::BAD_REQUEST, "bad request body");
    };
    state
        .controller
        .fleet
        .metrics
        .worker_heartbeats_total
        .fetch_add(1, Ordering::Relaxed);
    state
        .controller
        .fleet
        .registry
        .record_heartbeat(&worker.0, &req.capability, &req.load);
    // Reconcile: the worker's live-lease list wins; the daemon re-learns
    // leases it forgot across a restart (M16).
    state.controller.relearn_leases(&worker.0, &req.live_leases);
    let lost = state.controller.fleet.reconcile(&worker.0, &req.live_leases);
    if !lost.is_empty() {
        state.controller.handle_lease_loss(&worker.0, &lost);
    }
    json_response(StatusCode::OK, &serde_json::json!({ "ok": true }))
}

async fn worker_op_result(
    State(state): State<ApiState>,
    Extension(_worker): Extension<WorkerIdentity>,
    body: Bytes,
) -> Response {
    let Ok(req) = parse_body::<OpResultRequest>(&body) else {
        return error_response(StatusCode::BAD_REQUEST, "bad request body");
    };
    let delivered = state
        .controller
        .fleet
        .streams
        .deliver_op_result(&req.exec_id, req.result);
    json_response(StatusCode::OK, &serde_json::json!({ "delivered": delivered }))
}

async fn worker_exec_events(
    State(state): State<ApiState>,
    Extension(_worker): Extension<WorkerIdentity>,
    body: Bytes,
) -> Response {
    let Ok(req) = parse_body::<ExecEventsRequest>(&body) else {
        return error_response(StatusCode::BAD_REQUEST, "bad request body");
    };
    for event in req.events {
        if let ExecEventKind::Finished { duration_ms, .. } = &event.kind {
            state
                .controller
                .fleet
                .metrics
                .exec_duration_seconds
                .observe_ms(*duration_ms);
        }
        if let ExecEventKind::StdoutDelta { text } | ExecEventKind::StderrDelta { text } =
            &event.kind
        {
            let is_stderr = matches!(event.kind, ExecEventKind::StderrDelta { .. });
            if let Ok(handle) = state.controller.get(&event.lease_id) {
                handle.journal.publish(WireEvent::ExecOutput {
                    exec_id: event.exec_id.clone(),
                    is_stderr,
                    text: text.clone(),
                });
            }
        }
    }
    json_response(StatusCode::OK, &serde_json::json!({ "ok": true }))
}

async fn worker_checkpoint(
    State(state): State<ApiState>,
    Extension(_worker): Extension<WorkerIdentity>,
    body: Bytes,
) -> Response {
    let Ok(req) = parse_body::<CheckpointRequest>(&body) else {
        return error_response(StatusCode::BAD_REQUEST, "bad request body");
    };
    if let Ok(handle) = state.controller.get(&req.session_id) {
        // Byte-slice safely: worker-supplied strings must not panic the
        // daemon on a multi-byte boundary (S-24).
        let commit = req.commit.get(..12).unwrap_or(&req.commit).to_string();
        let session = req.session_id.get(..8).unwrap_or(&req.session_id).to_string();
        handle.journal.publish_core(CoreEvent::Notice {
            text: format!(
                "[checkpoint {commit} on smith/{session}{}]",
                if req.pushed { " (pushed)" } else { " (local only)" }
            ),
        });
    }
    json_response(StatusCode::OK, &serde_json::json!({ "ok": true }))
}

// ---- SSE (Phase 3) ------------------------------------------------------
//
// The framing is written by hand rather than via axum's Sse type so the
// bytes stay exactly what the frozen contract (and the current client
// parser) expect: one `data: <json>\n\n` frame per envelope. hyper adds the
// chunked transfer encoding on top; chunk boundaries are not part of the
// contract and clients parse the decoded body.

/// One envelope's exact wire frame.
fn sse_frame_of(env: &Envelope) -> Bytes {
    let json = serde_json::to_string(env).expect("envelope serialisation");
    Bytes::from(format!("data: {json}\n\n"))
}

/// An SSE comment frame (heartbeat); every SSE parser skips comment lines.
fn sse_comment_frame(text: &str) -> Bytes {
    Bytes::from(format!(": {text}\n\n"))
}

fn sse_response<S>(stream: S) -> Response
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
{
    axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(axum::body::Body::from_stream(stream))
        .expect("static SSE response parts are valid")
}

fn heartbeat_interval(state: &ApiState) -> Option<tokio::time::Interval> {
    // The first tick of `interval` is immediate; delay it by one period so a
    // fresh stream starts silent.
    state.sse_heartbeat.map(|period| {
        tokio::time::interval_at(tokio::time::Instant::now() + period, period)
    })
}

/// `?since=` from the raw query (legacy semantics: absent or unparseable → 0;
/// no URL decoding — smith's params are numbers).
fn since_param(query: Option<&str>) -> u64 {
    query
        .unwrap_or("")
        .split('&')
        .find_map(|pair| pair.strip_prefix("since="))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// `GET /v1/sessions/{id}/events`: replay after `since`, then live, with
/// dedupe by sequence and lag recovery from the ring.
async fn session_events(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
) -> Response {
    let handle = match state.controller.get(&id) {
        Ok(h) => h,
        Err(e) => return api_error(&e),
    };
    let since = since_param(query.as_deref());
    sse_response(SessionEvents::new(
        Arc::clone(&handle.journal),
        since,
        state.sse_heartbeat,
    ))
}

/// `GET /v1/worker/events`: the controller→worker command stream. The worker
/// is marked connected for as long as the stream lives; the stream's end
/// unregisters it — but never a reconnect replacement (generations).
async fn worker_events(
    State(state): State<ApiState>,
    Extension(worker): Extension<WorkerIdentity>,
) -> Response {
    let (rx, generation) = state.controller.fleet.streams.connect(&worker.0);
    let stream = WorkerCommands {
        streams: Arc::clone(&state.controller.fleet.streams),
        worker: worker.0,
        rx,
        generation,
        heartbeat: heartbeat_interval(&state),
    };
    sse_response(stream)
}

/// The session's event stream: journal replay, then the live broadcast,
/// deduplicating by sequence and refilling from the ring on lag.
///
/// Built on `unfold` on purpose: the in-flight `recv()` future lives inside
/// the unfold state across polls, so the broadcast sender's waker stays
/// registered while the stream idles. (Polling a fresh `recv()` per poll and
/// dropping it on `Pending` loses the waker — the stream never wakes again.)
struct SessionEvents {
    inner: std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
}

impl SessionEvents {
    fn new(journal: Arc<Journal>, since: u64, heartbeat: Option<Duration>) -> Self {
        // Subscribe BEFORE snapshotting the replay so nothing is missed; the
        // dedupe by sequence covers the overlap (frozen semantics).
        let rx = journal.subscribe();
        let replay = journal.replay(since);
        let state = SessionEventState {
            journal,
            rx,
            replay: replay.into_iter(),
            replaying: true,
            last_sent: since,
            heartbeat: heartbeat
                .map(|period| tokio::time::interval_at(tokio::time::Instant::now() + period, period)),
        };
            Self {
            inner: Box::pin(futures::stream::unfold(state, |mut s| async move {
                loop {
                    // Replay first (the subscription predates the snapshot).
                    if s.replaying {
                        match s.replay.next() {
                            Some(env) => {
                                s.last_sent = s.last_sent.max(env.seq);
                                return Some((Ok(sse_frame_of(&env)), s));
                            }
                            None => s.replaying = false,
                        }
                    }
                    tokio::select! {
                        res = s.rx.recv() => match res {
                            Ok(env) => {
                                // A duplicate (subscribed-before-snapshot
                                // overlap) is skipped.
                                if env.seq > s.last_sent {
                                    s.last_sent = env.seq;
                                    return Some((Ok(sse_frame_of(&env)), s));
                                }
                            }
                            // Overflowed queue: refill from the ring (it is
                            // larger).
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                s.replay = s.journal.replay(s.last_sent).into_iter();
                                s.replaying = true;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                        },
                        // Idle long enough: a comment heartbeat. (Both arms
                        // are cancel-safe; recv is re-created by the select.)
                        _ = heartbeat_tick(&mut s.heartbeat) => {
                            return Some((Ok(sse_comment_frame("keep-alive")), s));
                        }
                    }
                }
            })),
        }
    }
}

/// Await the next heartbeat tick; with no heartbeat configured, never
/// resolves (the select arm stays permanently out of the race).
async fn heartbeat_tick(interval: &mut Option<tokio::time::Interval>) {
    match interval.as_mut() {
        Some(iv) => {
            iv.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

impl Stream for SessionEvents {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

struct SessionEventState {
    journal: Arc<Journal>,
    rx: tokio::sync::broadcast::Receiver<Envelope>,
    replay: std::vec::IntoIter<Envelope>,
    replaying: bool,
    last_sent: u64,
    heartbeat: Option<tokio::time::Interval>,
}

/// The worker's command stream: the registry's queue, framed as SSE. Ending
/// the stream (worker disconnected or replaced) unregisters the connection
/// for this generation only.
struct WorkerCommands {
    streams: Arc<WorkerStreams>,
    worker: String,
    rx: tokio::sync::mpsc::UnboundedReceiver<WorkerCommand>,
    generation: u64,
    heartbeat: Option<tokio::time::Interval>,
}

impl Stream for WorkerCommands {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match self.rx.poll_recv(cx) {
                std::task::Poll::Ready(Some(cmd)) => {
                    let json = serde_json::to_string(&cmd).expect("command serialisation");
                    return std::task::Poll::Ready(Some(Ok(Bytes::from(format!(
                        "data: {json}\n\n"
                    )))));
                }
                // The registry dropped this stream's sender (worker replaced
                // or revoked): the stream ends.
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                std::task::Poll::Pending => {
                    if let Some(interval) = self.heartbeat.as_mut() {
                        if interval.poll_tick(cx).is_ready() {
                            return std::task::Poll::Ready(Some(Ok(sse_comment_frame(
                                "keep-alive",
                            ))));
                        }
                    }
                    return std::task::Poll::Pending;
                }
            }
        }
    }
}

impl Drop for WorkerCommands {
    fn drop(&mut self) {
        self.streams
            .disconnect_generation(&self.worker, self.generation);
    }
}

/// Load the TLS acceptor from `[serve].tls_cert` + `tls_key` (PEM).
pub fn load_tls_acceptor(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> anyhow::Result<tokio_rustls::TlsAcceptor> {
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
