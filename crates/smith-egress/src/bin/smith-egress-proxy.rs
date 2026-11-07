//! smith-egress-proxy: an allowlist-filtering HTTP proxy with per-destination
//! clearance, running as a sidecar container inside the podman machine.
//!
//! Data plane (port 3128):
//! - `CONNECT host:port` (HTTPS): the hostname is checked against the
//!   allowlist; if permitted the tunnel is opened and traffic passes through
//!   end-to-end encrypted. No certificate interception anywhere.
//! - Plain HTTP: the target is taken from the absolute URI / `Host` header,
//!   filtered, then forwarded.
//!
//! Clearance (control API, port 9239, token-authenticated): a destination
//! outside the allowlist can be held and asked — `GET /pending`, `POST
//! /decide`, `GET|POST /mode`. Decisions: allow once, allow always
//! (session-scoped runtime allowlist), deny.

use anyhow::Context;
use smith_egress::Allowlist;
use smith_egress::clearance::{Clearance, ClearanceMode, Decision};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const PORT: u16 = 3128;
const CONTROL_PORT: u16 = 9239;
/// Head bytes buffered before deciding (requests smaller than this only).
const HEAD_LIMIT: usize = 16 * 1024;
/// How long a held CONNECT waits for a clearance decision.
const CLEARANCE_WAIT: Duration = Duration::from_secs(120);

struct ProxyState {
    allowlist: Allowlist,
    clearance: Clearance,
    /// Bearer token for the control API; `None` disables the API.
    token: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let allowlist = match std::env::var("EGRESS_ALLOWLIST").as_deref() {
        // "none" = empty allowlist (tests): every destination needs clearance.
        Ok("none") => Allowlist::default(),
        Ok(value) if !value.trim().is_empty() => Allowlist::parse(value),
        _ => Allowlist::defaults(),
    };
    let mode = std::env::var("EGRESS_CLEARANCE")
        .ok()
        .and_then(|v| ClearanceMode::parse(&v))
        .unwrap_or(ClearanceMode::Ask);
    let token = std::env::var("CLEARANCE_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());

    let state = Arc::new(ProxyState {
        allowlist,
        clearance: Clearance::new(mode),
        token,
    });
    tracing::info!(
        "egress proxy listening on :{PORT} (clearance mode {})",
        state.clearance.mode().as_str()
    );

    // Reap expired pending requests periodically.
    let reaper_state = Arc::clone(&state);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            reaper_state.clearance.reap_expired(CLEARANCE_WAIT);
        }
    });

    let control_state = Arc::clone(&state);
    tokio::spawn(async move {
        if let Err(e) = control_server(control_state).await {
            tracing::error!("clearance control API failed: {e:#}");
        }
    });

    let port: u16 = std::env::var("EGRESS_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(PORT);
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!("data plane on :{port}");
    loop {
        let (client, peer) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(client, state).await {
                tracing::debug!("connection from {peer} ended: {e:#}");
            }
        });
    }
}

async fn handle_connection(client: TcpStream, state: Arc<ProxyState>) -> anyhow::Result<()> {
    let mut client = client;
    let (head, leftover) = read_head(&mut client).await?;

    let request_line = head.lines().next().context("empty request")?.to_string();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().context("no method")?;
    let target = parts.next().context("no target")?;

    if method.eq_ignore_ascii_case("CONNECT") {
        let host = target
            .rsplit_once(':')
            .map(|(h, _)| h.to_string())
            .unwrap_or_else(|| target.to_string());
        if decide_destination(&state, &host).await != Decision::Deny {
            tracing::info!("tunnel allowed: {host}");
            client
                .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .await?;
            let mut upstream = connect_upstream(target).await?;
            tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
            Ok(())
        } else {
            tracing::info!("tunnel denied: {host}");
            client
                .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                .await?;
            anyhow::bail!("destination not cleared: {host}")
        }
    } else {
        // Plain HTTP: absolute-URI request; target host from the URI, falling
        // back to the Host header.
        let host = plain_http_host(target, &head).context("no resolvable host")?;
        if decide_destination(&state, &host).await == Decision::Deny {
            tracing::info!("http denied: {host}");
            client
                .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                .await?;
            anyhow::bail!("destination not cleared: {host}")
        }
        tracing::info!("http allowed: {host}");
        let port = target
            .rsplit_once(':')
            .and_then(|(_, p)| p.parse::<u16>().ok())
            .unwrap_or(80);
        let mut upstream = connect_upstream(&format!("{host}:{port}")).await?;
        // Rewrite the absolute URI to a path-form request, replay the head
        // plus whatever body bytes were already buffered, then pipe.
        let rewritten = rewrite_request_line(&head, target)?;
        upstream.write_all(rewritten.as_bytes()).await?;
        upstream.write_all(&leftover).await?;
        tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
        Ok(())
    }
}

/// The clearance decision for one destination: static allowlist, mode, or a
/// held pending request answered by the controller.
async fn decide_destination(state: &ProxyState, host: &str) -> Decision {
    if state.clearance.is_allowed(&state.allowlist, host) {
        return Decision::AllowOnce;
    }
    match state.clearance.mode() {
        ClearanceMode::Allow => {
            tracing::info!("clearance auto-allow: {host}");
            Decision::AllowOnce
        }
        ClearanceMode::Deny => {
            tracing::info!("clearance auto-deny: {host}");
            Decision::Deny
        }
        ClearanceMode::Ask => {
            let (id, rx) = state.clearance.create_pending(host);
            tracing::info!("clearance requested: {host} (request {id})");
            match tokio::time::timeout(CLEARANCE_WAIT, rx).await {
                Ok(Ok(decision)) => decision,
                Ok(Err(_)) | Err(_) => {
                    tracing::info!("clearance timed out or abandoned: {host}");
                    Decision::Deny
                }
            }
        }
    }
}

// ------------------------------------------------------- control server --

async fn control_server(state: Arc<ProxyState>) -> anyhow::Result<()> {
    let port: u16 = std::env::var("EGRESS_CONTROL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(CONTROL_PORT);
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!("clearance control API on :{port}");
    loop {
        let (stream, _) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle_control(stream, state).await {
                tracing::debug!("control connection ended: {e:#}");
            }
        });
    }
}

async fn handle_control(stream: TcpStream, state: Arc<ProxyState>) -> anyhow::Result<()> {
    let mut stream = stream;
    let (head, leftover) = read_head(&mut stream).await?;

    // Token auth: every control route requires it.
    let expected = state
        .token
        .clone()
        .context("control API disabled (no token)")?;
    let authorised = head.lines().skip(1).any(|line| {
        line.split_once(':').is_some_and(|(k, v)| {
            k.trim().eq_ignore_ascii_case("authorization")
                && v.trim().trim_start_matches("Bearer ") == expected
        })
    });
    if !authorised {
        return http_reply(&mut stream, 401, "{\"error\":\"unauthorised\"}").await;
    }

    let request_line = head.lines().next().context("empty request")?.to_string();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().context("no method")?.to_ascii_lowercase();
    let path = parts
        .next()
        .unwrap_or("")
        .split('?')
        .next()
        .unwrap_or("")
        .to_string();

    match (method.as_str(), path.as_str()) {
        ("get", "/pending") => {
            let list: Vec<serde_json::Value> = state
                .clearance
                .pending_list()
                .into_iter()
                .map(|(id, host)| serde_json::json!({ "id": id, "host": host }))
                .collect();
            http_reply(&mut stream, 200, &serde_json::to_string(&list)?).await
        }
        ("post", "/decide") => {
            let body = body_bytes(&mut stream, &head, leftover).await?;
            let v: serde_json::Value = serde_json::from_slice(&body).context("bad JSON body")?;
            let id = v
                .get("id")
                .and_then(serde_json::Value::as_u64)
                .context("missing id")?;
            let decision = v
                .get("decision")
                .and_then(serde_json::Value::as_str)
                .context("missing decision")?;
            let decision = match decision {
                "allow" => Decision::AllowOnce,
                "always" => Decision::AllowAlways,
                "deny" => Decision::Deny,
                other => anyhow::bail!("unknown decision `{other}`"),
            };
            let applied = state.clearance.decide(id, decision);
            http_reply(
                &mut stream,
                200,
                &serde_json::json!({ "applied": applied }).to_string(),
            )
            .await
        }
        ("post", "/mode") => {
            let body = body_bytes(&mut stream, &head, leftover).await?;
            let v: serde_json::Value = serde_json::from_slice(&body).context("bad JSON body")?;
            let mode = v
                .get("mode")
                .and_then(serde_json::Value::as_str)
                .context("missing mode")?;
            let mode = ClearanceMode::parse(mode).context("unknown mode")?;
            state.clearance.set_mode(mode);
            http_reply(
                &mut stream,
                200,
                &serde_json::json!({ "mode": mode.as_str() }).to_string(),
            )
            .await
        }
        ("get", "/mode") => {
            http_reply(
                &mut stream,
                200,
                &serde_json::json!({ "mode": state.clearance.mode().as_str() }).to_string(),
            )
            .await
        }
        _ => http_reply(&mut stream, 404, "{\"error\":\"not found\"}").await,
    }
}

async fn http_reply(stream: &mut TcpStream, status: u16, body: &str) -> anyhow::Result<()> {
    let reason = match status {
        200 => "OK",
        401 => "Unauthorized",
        404 => "Not Found",
        _ => "Bad Request",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    Ok(())
}

/// Read the request body (Content-Length bytes) beyond the already-buffered
/// leftover.
async fn body_bytes(
    stream: &mut TcpStream,
    head: &str,
    mut leftover: Vec<u8>,
) -> anyhow::Result<Vec<u8>> {
    let content_length: usize = head
        .lines()
        .skip(1)
        .find_map(|line| {
            line.split_once(':')
                .filter(|(k, _)| k.trim().eq_ignore_ascii_case("content-length"))
                .and_then(|(_, v)| v.trim().parse().ok())
        })
        .unwrap_or(0);
    let mut body = std::mem::take(&mut leftover);
    while body.len() < content_length {
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            anyhow::bail!("connection closed mid-body");
        }
        body.extend_from_slice(&buf[..n]);
    }
    body.truncate(content_length);
    Ok(body)
}

// ------------------------------------------------------------ shared io --

/// Read until the end of the HTTP head (\r\n\r\n), returning the head string
/// plus any body bytes already buffered.
async fn read_head(stream: &mut TcpStream) -> anyhow::Result<(String, Vec<u8>)> {
    let mut buf = [0u8; HEAD_LIMIT];
    let mut filled = 0usize;
    loop {
        if filled == buf.len() {
            anyhow::bail!("request head exceeds {HEAD_LIMIT} bytes");
        }
        let n = stream.read(&mut buf[filled..]).await?;
        if n == 0 {
            anyhow::bail!("connection closed before a full request head arrived");
        }
        filled += n;
        if let Some(pos) = find_head_end(&buf[..filled]) {
            let head = String::from_utf8_lossy(&buf[..pos]).into_owned();
            let leftover = buf[pos + 4..filled].to_vec();
            return Ok((head, leftover));
        }
    }
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Connect to `host:port`, trying IPv4 before IPv6 with a short per-address
/// timeout — many podman machines advertise IPv6 DNS but have no IPv6 egress,
/// so naive connects black-hole on the first AAAA address.
async fn connect_upstream(target: &str) -> anyhow::Result<TcpStream> {
    const PER_ADDR_TIMEOUT: Duration = Duration::from_secs(5);
    let addrs: Vec<_> = tokio::net::lookup_host(target)
        .await
        .context("DNS resolution failed")?
        .collect();
    let (v4, v6): (Vec<_>, Vec<_>) = addrs.into_iter().partition(|a| a.is_ipv4());
    let mut last = None;
    for addr in v4.into_iter().chain(v6) {
        match tokio::time::timeout(PER_ADDR_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(e)) => last = Some(anyhow::Error::new(e)),
            Err(_) => last = Some(anyhow::anyhow!("connect to {addr} timed out")),
        }
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("no addresses for {target}")))
}

/// Extract the destination host for a plain-HTTP request: from the absolute
/// URI (`GET http://host/path`), else from the `Host:` header.
fn plain_http_host(target: &str, head: &str) -> Option<String> {
    if let Some(rest) = target
        .strip_prefix("http://")
        .map(|r| r.split(['/', '?']).next().unwrap_or(r).to_string())
        .filter(|h| !h.is_empty())
    {
        return Some(rest);
    }
    for line in head.lines().skip(1) {
        if let Some(value) = line
            .split_once(':')
            .filter(|(k, _)| k.eq_ignore_ascii_case("host"))
        {
            return Some(value.1.trim().to_string());
        }
    }
    None
}

/// Replace the absolute URI in the request line with path-or-origin form.
fn rewrite_request_line(head: &str, target: &str) -> anyhow::Result<String> {
    let (first_line, rest) = head.split_once("\r\n").context("malformed head")?;
    let path = if let Some(rest_uri) = target.strip_prefix("http://") {
        match rest_uri.find('/') {
            Some(i) => &rest_uri[i..],
            None => "/",
        }
    } else {
        target
    };
    let mut pieces = first_line.splitn(3, ' ');
    let method = pieces.next().context("no method")?;
    let version = pieces.nth(1).context("no version")?;
    // `rest` holds the header block without its terminating blank line — the
    // upstream request is not complete until we append it.
    Ok(format!("{method} {path} {version}\r\n{rest}\r\n\r\n"))
}
