//! The daemon's HTTP surface: a tokio accept loop over the hand-rolled
//! protocol plumbing (same house pattern as the egress sidecar's control
//! API). Bearer-token auth on every route; the event stream is SSE over
//! chunked transfer with journal replay.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::Context;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::error::RecvError;

use smith_core::CoreEvent;
use smith_protocol::http::{
    complete_request, head_path, read_head, write_json_response, write_sse_end, write_sse_event,
    write_sse_head,
};
use smith_protocol::{
    ApprovalPost, ClearancePost, CreateSession, PROTOCOL_VERSION, ProviderPost, ResumePost,
    StartTurn, WireEvent,
};

use crate::controller::{ApiError, Controller};
use crate::forge::{Forge, run_git_svc};

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
    // Read the head first so the body cap can be picked per path: git
    // routes carry pack bodies (up to MAX_GIT_BODY_BYTES); everything else
    // stays at the 1 MiB RPC cap.
    let (head, leftover) = read_head(&mut stream).await?;
    let is_git = head_path(&head).is_some_and(|p| p.starts_with("/git/"));
    let cap = if is_git {
        smith_protocol::http::MAX_GIT_BODY_BYTES
    } else {
        smith_protocol::http::MAX_BODY_BYTES
    };
    let req = complete_request(&mut stream, &head, leftover, cap).await?;
    // There are no public endpoints. Worker routes authenticate with the
    // worker credential (enrol carries its body-carried one-time token);
    // git routes do their own token auth (Basic for git clients, scoped
    // per session); everything else — including the fleet admin routes
    // and metrics — needs the daemon token.
    let git_route = req.path.starts_with("/git/");
    let worker_route = req.path.starts_with("/v1/worker/");
    if !worker_route && !git_route && req.bearer_token() != Some(token.as_str()) {
        return write_json_response(&mut stream, 401, "{\"error\":\"unauthorised\"}").await;
    }
    route(&controller, &req, &mut stream, &token).await
}

/// Authenticate a worker credential → worker name, or None (401).
fn worker_auth(controller: &Controller, req: &smith_protocol::http::Request) -> Option<String> {
    controller.fleet.registry.authenticate(req.bearer_token()?)
}

async fn route(
    controller: &Arc<Controller>,
    req: &smith_protocol::http::Request,
    stream: &mut TcpStream,
    daemon_token: &str,
) -> anyhow::Result<()> {
    let segments: Vec<&str> = req.path.split('/').filter(|s| !s.is_empty()).collect();
    let method = req.method.as_str();

    // ---- the forge (M17): git over the house HTTP ----
    match (method, segments.as_slice()) {
        ("GET", ["git", repo, "info", "refs"]) => {
            return git_service_info(controller, req, stream, repo, daemon_token).await;
        }
        ("POST", ["git", repo, "git-upload-pack"]) => {
            return git_rpc(controller, req, stream, repo, daemon_token, "upload-pack").await;
        }
        ("POST", ["git", repo, "git-receive-pack"]) => {
            return git_rpc(controller, req, stream, repo, daemon_token, "receive-pack").await;
        }
        _ => {}
    }

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
            match controller.create_session(body).await {
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
        // ---- fleet admin (daemon token) ----
        ("POST", ["v1", "workers"]) => {
            let body: serde_json::Value = match serde_json::from_slice(&req.body) {
                Ok(v) => v,
                Err(e) => {
                    return reply(
                        stream,
                        400,
                        serde_json::json!({ "error": format!("bad body: {e}") }).to_string(),
                    )
                    .await;
                }
            };
            let Some(name) = body.get("name").and_then(serde_json::Value::as_str) else {
                return reply(stream, 400, "{\"error\":\"name is required\"}".into()).await;
            };
            match controller.fleet.registry.issue_enrolment(name) {
                Ok(token) => {
                    reply(
                        stream,
                        200,
                        serde_json::json!({ "enrolment_token": token }).to_string(),
                    )
                    .await
                }
                Err(e) => {
                    reply(
                        stream,
                        500,
                        serde_json::json!({ "error": format!("{e:#}") }).to_string(),
                    )
                    .await
                }
            }
        }
        ("GET", ["v1", "workers"]) => {
            let list: Vec<smith_protocol::WorkerSummary> = controller
                .fleet
                .registry
                .list()
                .into_iter()
                .map(|w| smith_protocol::WorkerSummary {
                    name: w.name,
                    id: w.id,
                    status: w.status,
                    capability: w.capability,
                    load: w.load,
                    last_seen_ms: w.last_seen_ms,
                })
                .collect();
            reply(stream, 200, serde_json::to_string(&list)?).await
        }
        ("DELETE", ["v1", "workers", name]) => match controller.fleet.registry.revoke(name) {
            Ok(true) => {
                let lost = controller.fleet.board.sessions_of(name);
                controller.fleet.streams.disconnect(name);
                controller.handle_lease_loss(name, &lost);
                reply(stream, 200, "{\"revoked\":true}".into()).await
            }
            Ok(false) => reply(stream, 404, "{\"error\":\"no such worker\"}".into()).await,
            Err(e) => {
                reply(
                    stream,
                    500,
                    serde_json::json!({ "error": format!("{e:#}") }).to_string(),
                )
                .await
            }
        },
        ("GET", ["v1", "repos"]) => {
            let repos = controller
                .forge
                .as_ref()
                .map(|f| f.list_repos())
                .unwrap_or_default();
            reply(stream, 200, serde_json::to_string(&repos)?).await
        }
        ("GET", ["v1", "metrics"]) => {
            let text = controller.metrics.render();
            write_plain_text(stream, &text).await
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
        // ---- worker mesh (worker credential; enrol carries its own token) ----
        ("POST", ["v1", "worker", "enrol"]) => {
            let Some(body) = parse_or_reply::<smith_protocol::EnrolRequest>(req, stream).await
            else {
                return Ok(());
            };
            match controller.fleet.registry.enrol(
                &body.name,
                &body.enrolment_token,
                &body.capability,
            ) {
                Ok((worker_id, worker_token)) => {
                    reply(
                        stream,
                        200,
                        serde_json::to_string(&smith_protocol::EnrolReply {
                            worker_id,
                            worker_token,
                        })?,
                    )
                    .await
                }
                Err(e) => {
                    reply(
                        stream,
                        403,
                        serde_json::json!({ "error": format!("{e:#}") }).to_string(),
                    )
                    .await
                }
            }
        }
        ("GET", ["v1", "worker", "events"]) => match worker_auth(controller, req) {
            Some(worker) => stream_worker_commands(controller, &worker, stream).await,
            None => reply(stream, 401, "{\"error\":\"unauthorised\"}".into()).await,
        },
        ("POST", ["v1", "worker", "heartbeat"]) => match worker_auth(controller, req) {
            Some(worker) => {
                let Some(body) =
                    parse_or_reply::<smith_protocol::HeartbeatRequest>(req, stream).await
                else {
                    return Ok(());
                };
                controller
                    .fleet
                    .metrics
                    .worker_heartbeats_total
                    .fetch_add(1, Ordering::Relaxed);
                controller
                    .fleet
                    .registry
                    .record_heartbeat(&worker, &body.capability, &body.load);
                // Reconcile: the worker's live-lease list wins. The M16
                // add-direction re-learns leases the DAEMON forgot (restart).
                controller.relearn_leases(&worker, &body.live_leases);
                let lost = controller.fleet.reconcile(&worker, &body.live_leases);
                if !lost.is_empty() {
                    controller.handle_lease_loss(&worker, &lost);
                }
                reply(stream, 200, "{\"ok\":true}".into()).await
            }
            None => reply(stream, 401, "{\"error\":\"unauthorised\"}".into()).await,
        },
        ("POST", ["v1", "worker", "op-result"]) => match worker_auth(controller, req) {
            Some(_) => {
                let Some(body) =
                    parse_or_reply::<smith_protocol::OpResultRequest>(req, stream).await
                else {
                    return Ok(());
                };
                let delivered = controller
                    .fleet
                    .streams
                    .deliver_op_result(&body.exec_id, body.result);
                reply(
                    stream,
                    200,
                    serde_json::json!({ "delivered": delivered }).to_string(),
                )
                .await
            }
            None => reply(stream, 401, "{\"error\":\"unauthorised\"}".into()).await,
        },
        ("POST", ["v1", "worker", "exec-events"]) => match worker_auth(controller, req) {
            Some(_) => {
                let Some(body) =
                    parse_or_reply::<smith_protocol::ExecEventsRequest>(req, stream).await
                else {
                    return Ok(());
                };
                for event in body.events {
                    if let smith_protocol::ExecEventKind::Finished { duration_ms, .. } = &event.kind
                    {
                        controller
                            .fleet
                            .metrics
                            .exec_duration_seconds
                            .observe_ms(*duration_ms);
                    }
                    if let smith_protocol::ExecEventKind::StdoutDelta { text }
                    | smith_protocol::ExecEventKind::StderrDelta { text } = &event.kind
                    {
                        let is_stderr = matches!(
                            event.kind,
                            smith_protocol::ExecEventKind::StderrDelta { .. }
                        );
                        if let Ok(handle) = controller.get(&event.lease_id) {
                            handle.journal.publish(WireEvent::ExecOutput {
                                exec_id: event.exec_id,
                                is_stderr,
                                text: text.clone(),
                            });
                        }
                    }
                }
                reply(stream, 200, "{\"ok\":true}".into()).await
            }
            None => reply(stream, 401, "{\"error\":\"unauthorised\"}".into()).await,
        },
        ("POST", ["v1", "worker", "checkpoint"]) => match worker_auth(controller, req) {
            Some(_) => {
                let Some(body) =
                    parse_or_reply::<smith_protocol::CheckpointRequest>(req, stream).await
                else {
                    return Ok(());
                };
                if let Ok(handle) = controller.get(&body.session_id) {
                    handle.journal.publish_core(CoreEvent::Notice {
                        text: format!(
                            "[checkpoint {} on smith/{}{}]",
                            &body.commit[..12.min(body.commit.len())],
                            &body.session_id[..8.min(body.session_id.len())],
                            if body.pushed {
                                " (pushed)"
                            } else {
                                " (local only)"
                            }
                        ),
                    });
                }
                reply(stream, 200, "{\"ok\":true}".into()).await
            }
            None => reply(stream, 401, "{\"error\":\"unauthorised\"}".into()).await,
        },
        _ => not_found(stream).await,
    }
}

// ------------------------------------------------- the forge (M17) ----

/// One git request's authorisation: a session token scoped to `repo`, or
/// the daemon token (full access). `None` → 401.
fn forge_grant(
    controller: &Arc<Controller>,
    req: &smith_protocol::http::Request,
    repo: &str,
    daemon_token: &str,
) -> Option<crate::forge::GitCred> {
    let token = Forge::token_from_request(req)?;
    if token == daemon_token {
        return Some(crate::forge::GitCred {
            session_id: "daemon".into(),
            token_hash: String::new(),
            repo: repo.to_string(),
            read: true,
            write: true,
        });
    }
    let grant = controller.forge.as_ref()?.authenticate(&token)?;
    (grant.repo == repo).then_some(grant)
}

async fn git_unauthorised(stream: &mut TcpStream) -> anyhow::Result<()> {
    // The WWW-Authenticate header is what makes git's client send creds.
    stream
        .write_all(
            b"HTTP/1.1 401 Unauthorized\r\n\
              WWW-Authenticate: Basic realm=\"smith forge\"\r\n\
              Content-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .await?;
    stream.flush().await?;
    Ok(())
}

async fn git_forbidden(stream: &mut TcpStream) -> anyhow::Result<()> {
    write_json_response(stream, 403, "{\"error\":\"token lacks this scope\"}").await
}

async fn write_git_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> anyhow::Result<()> {
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\n\
         Cache-Control: no-cache\r\nContent-Length: {len}\r\n\
         Connection: close\r\n\r\n",
        reason = smith_protocol::http::reason(status),
        len = body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}

/// `GET /git/<repo>/info/refs?service=git-upload-pack|git-receive-pack` —
/// the smart-protocol advertisement.
async fn git_service_info(
    controller: &Arc<Controller>,
    req: &smith_protocol::http::Request,
    stream: &mut TcpStream,
    repo: &str,
    daemon_token: &str,
) -> anyhow::Result<()> {
    // git appends `.git` to clone/push URLs; grants are keyed on the bare
    // name.
    let repo = repo.strip_suffix(".git").unwrap_or(repo);
    let grant = forge_grant(controller, req, repo, daemon_token);
    let Some(grant) = grant else {
        return git_unauthorised(stream).await;
    };
    let service = req.query_param("service").unwrap_or_default();
    let (svc, needs_write) = match service.as_str() {
        "git-upload-pack" => ("upload-pack", false),
        "git-receive-pack" => ("receive-pack", true),
        other => {
            return write_json_response(
                stream,
                400,
                &format!("{{\"error\":\"bad service {other:?}\"}}"),
            )
            .await;
        }
    };
    if needs_write && !grant.write {
        return git_forbidden(stream).await;
    }
    let Some(forge) = &controller.forge else {
        return write_json_response(stream, 404, "{\"error\":\"no forge\"}").await;
    };
    // Push-to-create fires at the advertisement: git's push sequence starts
    // here, and the auth already checked out.
    let repo_path = if needs_write {
        match forge.ensure_repo(repo).await {
            Ok(p) => p,
            Err(e) => {
                return write_json_response(
                    stream,
                    400,
                    &format!("{{\"error\":\"{}\"}}", e).replace('"', "'"),
                )
                .await;
            }
        }
    } else {
        match forge.repo_path(repo) {
            Some(p) if p.exists() => p,
            _ => {
                return write_json_response(stream, 404, "{\"error\":\"no such repo\"}").await;
            }
        }
    };
    let advertisement = match run_git_svc(&repo_path, svc, true, vec![]).await {
        Ok(b) => b,
        Err(e) => {
            return write_json_response(
                stream,
                500,
                &format!("{{\"error\":\"{}\"}}", e).replace('"', "'"),
            )
            .await;
        }
    };
    // The smart-HTTP advertisement leads with the service announcement
    // pkt-line + flush; `git --advertise-refs` alone prints only the refs.
    let mut body = pkt_line(&format!("# service=git-{svc}\n"));
    body.extend_from_slice(b"0000");
    body.extend_from_slice(&advertisement);
    write_git_response(
        stream,
        200,
        &format!("application/x-git-{svc}-advertisement"),
        &body,
    )
    .await
}

/// One pkt-line: 4-hex length (including itself) + payload.
fn pkt_line(data: &str) -> Vec<u8> {
    let mut out = format!("{:04x}", data.len() + 4).into_bytes();
    out.extend_from_slice(data.as_bytes());
    out
}

/// `POST /git/<repo>/git-upload-pack` / `git-receive-pack` — the RPC half
/// of the smart protocol: request body → the subprocess's stdin, its
/// stdout → the response.
async fn git_rpc(
    controller: &Arc<Controller>,
    req: &smith_protocol::http::Request,
    stream: &mut TcpStream,
    repo: &str,
    daemon_token: &str,
    svc: &str,
) -> anyhow::Result<()> {
    let repo = repo.strip_suffix(".git").unwrap_or(repo);
    let grant = forge_grant(controller, req, repo, daemon_token);
    let Some(grant) = grant else {
        return git_unauthorised(stream).await;
    };
    let needs_write = svc == "receive-pack";
    if needs_write && !grant.write {
        return git_forbidden(stream).await;
    }
    if !needs_write && !grant.read {
        return git_forbidden(stream).await;
    }
    let Some(forge) = &controller.forge else {
        return write_json_response(stream, 404, "{\"error\":\"no forge\"}").await;
    };
    let repo_path = if needs_write {
        forge.ensure_repo(repo).await?
    } else {
        match forge.repo_path(repo) {
            Some(p) if p.exists() => p,
            _ => {
                return write_json_response(stream, 404, "{\"error\":\"no such repo\"}").await;
            }
        }
    };
    match run_git_svc(&repo_path, svc, false, req.body.clone()).await {
        Ok(body) => {
            write_git_response(
                stream,
                200,
                &format!("application/x-git-{svc}-result"),
                &body,
            )
            .await
        }
        Err(e) => {
            // A failed receive-pack (non-fast-forward etc.) — surface the
            // protocol-level failure to the client.
            write_json_response(
                stream,
                500,
                &format!("{{\"error\":\"{}\"}}", e).replace('"', "'"),
            )
            .await
        }
    }
}

/// The controller→worker command stream: SSE over the worker's own
/// connection; commands queue in the registry while the worker reconnects.
async fn stream_worker_commands(
    controller: &Arc<Controller>,
    worker: &str,
    stream: &mut TcpStream,
) -> anyhow::Result<()> {
    let mut rx = controller.fleet.streams.connect(worker);
    write_sse_head(stream).await.context("worker sse head")?;
    while let Some(cmd) = rx.recv().await {
        write_sse_event(stream, &serde_json::to_string(&cmd)?)
            .await
            .context("worker sse command")?;
    }
    controller.fleet.streams.disconnect(worker);
    write_sse_end(stream).await.context("worker sse end")
}

/// A `text/plain` body (metrics) — same one-request-per-connection framing.
async fn write_plain_text(stream: &mut TcpStream, text: &str) -> anyhow::Result<()> {
    smith_protocol::http::write_plain_text(stream, text).await
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
