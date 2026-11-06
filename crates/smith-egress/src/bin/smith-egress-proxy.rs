//! smith-egress-proxy: an allowlist-filtering HTTP proxy that runs as a
//! sidecar container inside the podman machine.
//!
//! - `CONNECT host:port` (HTTPS): the hostname is checked against the
//!   allowlist; if permitted the tunnel is opened and traffic passes through
//!   end-to-end encrypted. No certificate interception anywhere.
//! - Plain HTTP: the target is taken from the absolute URI / `Host` header,
//!   filtered, then forwarded.
//! - The allowlist arrives as `EGRESS_ALLOWLIST=host1,host2,...`; an empty
//!   value uses the built-in defaults. Empty allowlist entries never permit
//!   anything.

use anyhow::Context;
use smith_egress::Allowlist;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const PORT: u16 = 3128;
/// Head bytes buffered before deciding (requests smaller than this only).
const HEAD_LIMIT: usize = 16 * 1024;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let allowlist = match std::env::var("EGRESS_ALLOWLIST") {
        Ok(value) if !value.trim().is_empty() => Allowlist::parse(&value),
        _ => Allowlist::defaults(),
    };
    tracing::info!("egress proxy listening on :{PORT}");

    let listener = TcpListener::bind(("0.0.0.0", PORT)).await?;
    loop {
        let (client, peer) = listener.accept().await?;
        let allowlist = allowlist.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(client, allowlist).await {
                tracing::debug!("connection from {peer} ended: {e:#}");
            }
        });
    }
}

async fn handle_connection(client: TcpStream, allowlist: Allowlist) -> anyhow::Result<()> {
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
        if allowlist.allows(&host) {
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
            anyhow::bail!("destination not on the allowlist: {host}")
        }
    } else {
        // Plain HTTP: absolute-URI request; target host from the URI, falling
        // back to the Host header.
        let host = plain_http_host(target, &head).context("no resolvable host")?;
        if !allowlist.allows(&host) {
            tracing::info!("http denied: {host}");
            client
                .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                .await?;
            anyhow::bail!("destination not on the allowlist: {host}")
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
