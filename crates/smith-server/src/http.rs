//! The daemon's HTTP surface: a tokio accept loop over the hand-rolled
//! protocol plumbing (same house pattern as the egress sidecar's control
//! API). Bearer-token auth on every route; the event stream is SSE over
//! chunked transfer with journal replay.

use std::future::Future;
use std::sync::Arc;

use anyhow::Context;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::error::RecvError;

use smith_protocol::http::{
    read_request, write_json_response, write_sse_end, write_sse_event, write_sse_head,
};
use smith_protocol::{
    ApprovalPost, ClearancePost, CreateSession, PROTOCOL_VERSION, ProviderPost, ResumePost,
    StartTurn,
};

use crate::controller::{ApiError, Controller};

/// Bind the daemon's listener (split from [`accept_loop`] so tests can bind
/// port 0 and learn the port).
pub async fn bind(addr: &str) -> anyhow::Result<TcpListener> {
    TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding the serve API on {addr}"))
}

/// Accept loop: one task per connection, one request per connection
/// (the SSE stream keeps its own connection open until the client leaves).
pub async fn accept_loop(
    listener: TcpListener,
    controller: Arc<Controller>,
    token: String,
) -> anyhow::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let controller = Arc::clone(&controller);
        let token = token.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, controller, token).await {
                tracing::debug!("serve connection ended: {e:#}");
            }
        });
    }
}

async fn handle_conn(
    mut stream: TcpStream,
    controller: Arc<Controller>,
    token: String,
) -> anyhow::Result<()> {
    let req = read_request(&mut stream).await?;
    // Every route requires the token — there are no public endpoints.
    if req.bearer_token() != Some(token.as_str()) {
        return write_json_response(&mut stream, 401, "{\"error\":\"unauthorised\"}").await;
    }
    route(&controller, &req, &mut stream).await
}

async fn route(
    controller: &Arc<Controller>,
    req: &smith_protocol::http::Request,
    stream: &mut TcpStream,
) -> anyhow::Result<()> {
    let segments: Vec<&str> = req.path.split('/').filter(|s| !s.is_empty()).collect();
    let method = req.method.as_str();

    match (method, segments.as_slice()) {
        ("GET", ["v1", "health"]) => {
            reply(
                stream,
                200,
                format!(
                    "{{\"ok\":true,\"version\":\"{}\",\"protocol\":{PROTOCOL_VERSION}}}",
                    env!("CARGO_PKG_VERSION")
                ),
            )
            .await
        }
        ("GET", ["v1", "sessions"]) => {
            let body = serde_json::to_string(&controller.sessions())?;
            reply(stream, 200, body).await
        }
        ("POST", ["v1", "sessions"]) => {
            let Some(body) = parse_or_reply::<CreateSession>(req, stream).await else {
                return Ok(());
            };
            match controller
                .create_session(
                    &body.workspace,
                    body.provider.as_deref(),
                    body.model.as_deref(),
                )
                .await
            {
                Ok(id) => {
                    reply(
                        stream,
                        200,
                        serde_json::json!({ "session_id": id }).to_string(),
                    )
                    .await
                }
                Err(e) => reply(stream, e.status, error_json(&e)).await,
            }
        }
        (_, ["v1", "sessions", id, rest @ ..]) => match (method, rest.first()) {
            ("POST", Some(&"turns")) => {
                let Some(body) = parse_or_reply::<StartTurn>(req, stream).await else {
                    return Ok(());
                };
                ack(stream, controller.start_turn(id, &body.input).await).await
            }
            ("POST", Some(&"cancel")) => ack(stream, controller.cancel_turn(id)).await,
            ("POST", Some(&"approvals")) => {
                let Some(body) = parse_or_reply::<ApprovalPost>(req, stream).await else {
                    return Ok(());
                };
                ack(
                    stream,
                    controller
                        .deliver_approval(id, body.call_id.as_deref(), body.decision)
                        .await,
                )
                .await
            }
            ("POST", Some(&"provider")) => {
                let Some(body) = parse_or_reply::<ProviderPost>(req, stream).await else {
                    return Ok(());
                };
                ack(
                    stream,
                    controller
                        .set_provider(id, body.provider.as_deref(), body.model.as_deref())
                        .await,
                )
                .await
            }
            ("POST", Some(&"clearance")) => {
                let Some(body) = parse_or_reply::<ClearancePost>(req, stream).await else {
                    return Ok(());
                };
                ack(
                    stream,
                    controller
                        .forward_clearance(id, body.id, body.decision)
                        .await,
                )
                .await
            }
            ("POST", Some(&"resume")) => {
                let Some(body) = parse_or_reply::<ResumePost>(req, stream).await else {
                    return Ok(());
                };
                match controller.resume(id, &body.prefix).await {
                    Ok(r) => reply(stream, 200, serde_json::to_string(&r)?).await,
                    Err(e) => reply(stream, e.status, error_json(&e)).await,
                }
            }
            ("GET", Some(&"diff")) => match controller.diff(id).await {
                Ok(text) => {
                    reply(stream, 200, serde_json::json!({ "text": text }).to_string()).await
                }
                Err(e) => reply(stream, e.status, error_json(&e)).await,
            },
            ("GET", Some(&"status")) => match controller.get(id) {
                Ok(handle) => {
                    let status = handle.status().await;
                    reply(stream, 200, serde_json::to_string(&status)?).await
                }
                Err(e) => reply(stream, e.status, error_json(&e)).await,
            },
            ("GET", Some(&"events")) => stream_events(controller, id, req, stream).await,
            _ => not_found(stream).await,
        },
        _ => not_found(stream).await,
    }
}

/// SSE event stream: journal replay after `?since=`, then live broadcast.
/// Lagged subscribers recover from the ring before continuing live.
async fn stream_events(
    controller: &Arc<Controller>,
    id: &str,
    req: &smith_protocol::http::Request,
    stream: &mut TcpStream,
) -> anyhow::Result<()> {
    let handle = match controller.get(id) {
        Ok(h) => h,
        Err(e) => return reply(stream, e.status, error_json(&e)).await,
    };
    let since = req
        .query_param("since")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    write_sse_head(stream).await.context("sse head")?;
    // Subscribe BEFORE snapshotting the replay so nothing is missed; the
    // live loop deduplicates by sequence.
    let mut rx = handle.journal.subscribe();
    let mut last_sent = since;
    for env in handle.journal.replay(since) {
        write_sse_event(stream, &serde_json::to_string(&env)?)
            .await
            .context("sse replay")?;
        last_sent = last_sent.max(env.seq);
    }

    loop {
        match rx.recv().await {
            Ok(env) => {
                if env.seq > last_sent {
                    write_sse_event(stream, &serde_json::to_string(&env)?)
                        .await
                        .context("sse event")?;
                    last_sent = env.seq;
                }
            }
            // Overflowed queue: refill from the ring (it is larger).
            Err(RecvError::Lagged(_)) => {
                for env in handle.journal.replay(last_sent) {
                    write_sse_event(stream, &serde_json::to_string(&env)?)
                        .await
                        .context("sse lag recovery")?;
                    last_sent = last_sent.max(env.seq);
                }
            }
            Err(RecvError::Closed) => break,
        }
    }
    write_sse_end(stream).await.context("sse end")
}

async fn reply(stream: &mut TcpStream, status: u16, body: String) -> anyhow::Result<()> {
    write_json_response(stream, status, &body).await
}

async fn ack(stream: &mut TcpStream, result: Result<(), ApiError>) -> anyhow::Result<()> {
    match result {
        Ok(()) => reply(stream, 202, "{\"accepted\":true}".to_string()).await,
        Err(e) => reply(stream, e.status, error_json(&e)).await,
    }
}

async fn not_found(stream: &mut TcpStream) -> anyhow::Result<()> {
    let e = ApiError::not_found("unknown route");
    reply(stream, e.status, error_json(&e)).await
}

fn parse_or_reply<'a, T: serde::de::DeserializeOwned + 'a>(
    req: &'a smith_protocol::http::Request,
    stream: &'a mut TcpStream,
) -> impl Future<Output = Option<T>> + 'a {
    let parsed: Result<T, ApiError> = serde_json::from_slice(&req.body)
        .map_err(|e| ApiError::bad_request(format!("bad request body: {e}")));
    async move {
        match parsed {
            Ok(v) => Some(v),
            Err(e) => {
                let _ = reply(stream, e.status, error_json(&e)).await;
                None
            }
        }
    }
}

fn error_json(e: &ApiError) -> String {
    serde_json::json!({ "error": e.message }).to_string()
}
