//! Minimal HTTP/1.1 plumbing shared by the `smith serve` daemon and its
//! clients — the same framework-free approach as the egress sidecar's
//! control API, with one addition: chunked transfer for the SSE event
//! stream.
//!
//! Deliberately minimal: one request per connection for RPC-style calls,
//! bearer-token auth at the route layer, `Content-Length` bodies only
//! (requests), and a strict size cap. SSE responses use chunked framing so
//! they survive reverse proxies later.

use std::time::Duration;

use anyhow::{Context, bail};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Requests larger than this are refused (prompts and config are small).
pub const MAX_BODY_BYTES: usize = 1 << 20;

/// Budget for reading a request head (and body) before giving up.
pub const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// A parsed HTTP/1.1 request (head plus `Content-Length` body).
#[derive(Clone, Debug)]
pub struct Request {
    pub method: String,
    /// Path without the query string.
    pub path: String,
    /// Raw query string (`""` when absent).
    pub query: String,
    /// Header names lowercased.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }

    /// First value of a query parameter (no URL decoding; smith's params are
    /// ids and numbers).
    pub fn query_param(&self, name: &str) -> Option<String> {
        self.query.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            (k == name).then(|| v.to_string())
        })
    }

    pub fn bearer_token(&self) -> Option<&str> {
        self.header("authorization")?
            .strip_prefix("Bearer ")
            .map(str::trim)
    }
}

/// Read until the end of an HTTP head (`\r\n\r\n`), returning the head
/// string plus any body bytes already buffered.
pub async fn read_head<S: AsyncRead + Unpin>(stream: &mut S) -> anyhow::Result<(String, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        if let Some(pos) = find_head_end(&buf) {
            let head = String::from_utf8_lossy(&buf[..pos]).into_owned();
            // Everything after the \r\n\r\n separator — not the separator itself.
            let leftover = buf[pos + 4..].to_vec();
            return Ok((head, leftover));
        }
        if buf.len() > MAX_BODY_BYTES {
            bail!("HTTP head exceeds the size cap");
        }
        let n = tokio::time::timeout(READ_TIMEOUT, stream.read(&mut chunk))
            .await
            .context("timed out reading the HTTP head")??;
        if n == 0 {
            bail!("connection closed before the HTTP head ended");
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Read one request: head plus a `Content-Length` body. One request per
/// connection — callers reply and close.
pub async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> anyhow::Result<Request> {
    let (head, mut leftover) = tokio::time::timeout(READ_TIMEOUT, read_head(stream))
        .await
        .context("timed out waiting for a request")??;

    let mut lines = head.lines();
    let request_line = lines.next().context("empty request")?.to_string();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().context("no method")?.to_ascii_uppercase();
    let target = parts.next().context("no path")?.to_string();
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target, String::new()),
    };

    let mut headers = Vec::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
        }
    }
    let header = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };

    let content_length: usize = header("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    anyhow::ensure!(
        content_length <= MAX_BODY_BYTES,
        "request body exceeds the size cap"
    );

    let mut body = std::mem::take(&mut leftover);
    body.truncate(content_length);
    while body.len() < content_length {
        let mut chunk = [0u8; 8192];
        let n = tokio::time::timeout(READ_TIMEOUT, stream.read(&mut chunk))
            .await
            .context("timed out reading the request body")??;
        if n == 0 {
            bail!("connection closed mid-body");
        }
        let take = n.min(content_length - body.len());
        body.extend_from_slice(&chunk[..take]);
    }
    body.truncate(content_length);

    Ok(Request {
        method,
        path,
        query,
        headers,
        body,
    })
}

/// Reason phrase for the statuses smith replies with.
fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        500 => "Internal Server Error",
        _ => "OK",
    }
}

/// Send a JSON response with `Content-Length`; the caller then closes the
/// connection (one request per connection).
pub async fn write_json_response<S: AsyncWrite + Unpin>(
    stream: &mut S,
    status: u16,
    body: &str,
) -> anyhow::Result<()> {
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
        reason = reason(status)
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

/// Start an SSE response: chunked `text/event-stream`.
pub async fn write_sse_head<S: AsyncWrite + Unpin>(stream: &mut S) -> anyhow::Result<()> {
    let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\
                Cache-Control: no-cache\r\nTransfer-Encoding: chunked\r\n\r\n";
    stream.write_all(head.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

/// The chunked frame for one SSE event (a single `data:` line).
pub fn sse_frame(json: &str) -> Vec<u8> {
    let payload = format!("data: {json}\n\n");
    let mut frame = format!("{:x}\r\n", payload.len()).into_bytes();
    frame.extend_from_slice(payload.as_bytes());
    frame.extend_from_slice(b"\r\n");
    frame
}

/// Write one SSE event as a chunked frame.
pub async fn write_sse_event<S: AsyncWrite + Unpin>(
    stream: &mut S,
    json: &str,
) -> anyhow::Result<()> {
    stream.write_all(&sse_frame(json)).await?;
    stream.flush().await?;
    Ok(())
}

/// End an SSE stream (terminal chunk).
pub async fn write_sse_end<S: AsyncWrite + Unpin>(stream: &mut S) -> anyhow::Result<()> {
    stream.write_all(b"0\r\n\r\n").await?;
    stream.flush().await?;
    Ok(())
}

// --------------------------------------------------------- client side --

/// Issue one RPC-style request and read the full response (the server closes
/// after replying). Returns (status, body).
pub async fn raw_request<S: AsyncWrite + AsyncRead + Unpin>(
    stream: &mut S,
    method: &str,
    path: &str,
    token: &str,
    body: Option<&str>,
) -> anyhow::Result<(u16, String)> {
    let auth = format!("Authorization: Bearer {token}\r\n");
    let length = body
        .map(|b| format!("Content-Length: {}\r\n", b.len()))
        .unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\n{auth}{length}Connection: close\r\n\r\n{}",
        body.unwrap_or("")
    );
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status: u16 = text
        .lines()
        .next()
        .and_then(|l| l.split(' ').nth(1))
        .and_then(|s| s.parse().ok())
        .context("malformed HTTP response")?;
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    Ok((status, body))
}

/// Issue a streaming-request and return the stream positioned after the
/// response head, wrapped for chunked decoding. The caller owns the stream
/// until the body ends.
pub async fn open_chunked_response<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    method: &str,
    path: &str,
    token: &str,
) -> anyhow::Result<ChunkedReader<S>> {
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\n\
         Authorization: Bearer {token}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;

    let (head, leftover) = read_head(&mut stream).await?;
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split(' ').nth(1))
        .and_then(|s| s.parse().ok())
        .context("malformed streaming response")?;
    anyhow::ensure!(status == 200, "event stream returned {status}");
    Ok(ChunkedReader::new(stream, leftover))
}

/// Incremental decoder for a chunked response body (client side of the SSE
/// stream). Yields decoded body bytes; `None` at the terminal chunk.
pub struct ChunkedReader<S> {
    stream: S,
    buf: Vec<u8>,
    /// Bytes left in the current chunk; `None` when the next thing to read
    /// is a chunk-size line (or the terminal chunk).
    remaining: Option<usize>,
    /// A `\r\n` chunk terminator is due before the next size line.
    expect_terminator: bool,
    done: bool,
}

impl<S: AsyncRead + Unpin> ChunkedReader<S> {
    pub fn new(stream: S, leftover: Vec<u8>) -> Self {
        Self {
            stream,
            buf: leftover,
            remaining: None,
            expect_terminator: false,
            done: false,
        }
    }

    /// Next decoded body bytes (at most `max`); `None` at end of body.
    pub async fn next_bytes(&mut self, max: usize) -> anyhow::Result<Option<Vec<u8>>> {
        loop {
            if self.done {
                return Ok(None);
            }
            // Serve buffered chunk data first.
            if let Some(remaining) = self.remaining {
                let take = remaining.min(max).min(self.buf.len());
                if take > 0 {
                    let out: Vec<u8> = self.buf.drain(..take).collect();
                    let left = remaining - take;
                    self.remaining = Some(left);
                    if left == 0 {
                        self.remaining = None;
                        self.expect_terminator = true;
                    }
                    return Ok(Some(out));
                }
            } else if self.expect_terminator {
                if self.buf.starts_with(b"\r\n") {
                    self.buf.drain(..2);
                    self.expect_terminator = false;
                    continue;
                }
                if self.buf.len() >= 2 {
                    bail!("malformed chunk terminator");
                }
            } else if let Some(pos) = find_crlf(&self.buf) {
                let line: String = String::from_utf8_lossy(&self.buf[..pos]).into_owned();
                self.buf.drain(..pos + 2);
                let size = usize::from_str_radix(line.trim().split(';').next().unwrap_or(""), 16)
                    .map_err(|_| anyhow::anyhow!("malformed chunk size `{line}`"))?;
                if size == 0 {
                    self.done = true;
                    return Ok(None);
                }
                self.remaining = Some(size);
                continue;
            }

            // Need more bytes from the wire.
            let mut chunk = [0u8; 8192];
            let n = self.stream.read(&mut chunk).await?;
            if n == 0 {
                bail!("connection closed mid-chunk");
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

/// Parse complete SSE data payloads out of a growing buffer. Server events
/// are `data: <json>\n\n`; returns the payloads and the unconsumed tail.
pub fn parse_sse_data(buf: &[u8]) -> (Vec<String>, Vec<u8>) {
    let mut payloads = Vec::new();
    let mut consumed = 0;
    while let Some(pos) = find_double_newline(&buf[consumed..]) {
        let segment = &buf[consumed..consumed + pos];
        consumed += pos + 2;
        let data: Vec<&[u8]> = segment
            .split(|b| *b == b'\n')
            .filter_map(|line| {
                let line = if line.ends_with(b"\r") {
                    &line[..line.len() - 1]
                } else {
                    line
                };
                line.strip_prefix(b"data:")
                    .map(|rest| rest.strip_prefix(b" ").unwrap_or(rest))
                    .filter(|rest| !rest.is_empty())
            })
            .collect();
        if !data.is_empty() {
            payloads.push(String::from_utf8_lossy(&data.join(&b'\n')).into_owned());
        }
    }
    (payloads, buf[consumed..].to_vec())
}

fn find_double_newline(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\n\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::Envelope;
    use std::io::Cursor;

    fn request_bytes(head: &str, body: &str) -> Cursor<Vec<u8>> {
        Cursor::new(format!("{head}\r\n{body}").into_bytes())
    }

    #[tokio::test]
    async fn read_request_parses_method_path_query_headers_body() {
        let mut stream = request_bytes(
            "POST /v1/sessions/abc/turns?since=3 HTTP/1.1\r\n\
             Host: localhost\r\n\
             Authorization: Bearer tok\r\n\
             Content-Length: 14\r\n",
            r#"{"input":"hi"}"#,
        );
        let req = read_request(&mut stream).await.unwrap();
        assert_eq!(req.method, "POST");
        assert_eq!(req.path, "/v1/sessions/abc/turns");
        assert_eq!(req.query_param("since"), Some("3".into()));
        assert_eq!(req.bearer_token(), Some("tok"));
        assert_eq!(req.body, br#"{"input":"hi"}"#);
    }

    #[tokio::test]
    async fn read_request_rejects_oversized_and_truncated_bodies() {
        let mut stream = request_bytes("POST /x HTTP/1.1\r\nContent-Length: 99999999\r\n", "");
        assert!(read_request(&mut stream).await.is_err());

        let mut stream =
            Cursor::new(b"POST /x HTTP/1.1\r\nContent-Length: 50\r\n\r\nshort".to_vec());
        assert!(read_request(&mut stream).await.is_err());
    }

    #[tokio::test]
    async fn json_response_round_trip_through_raw_request() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let server_task = tokio::spawn(async move {
            let req = read_request(&mut server).await.unwrap();
            assert_eq!(req.path, "/v1/health");
            write_json_response(&mut server, 200, r#"{"ok":true}"#).await
        });
        let (status, body) = raw_request(&mut client, "GET", "/v1/health", "t", None)
            .await
            .unwrap();
        // The task's inner Result (the write) must be consumed too.
        let _ = server_task.await.unwrap();
        assert_eq!(status, 200);
        assert_eq!(body, r#"{"ok":true}"#);
    }

    #[tokio::test]
    async fn sse_stream_round_trip_including_unicode() {
        // Buffer must hold the request + head without both sides blocked in
        // write_all (a tiny duplex deadlocks by construction).
        let (client, mut server) = tokio::io::duplex(4096);
        let server_task = tokio::spawn(async move {
            write_sse_head(&mut server).await.unwrap();
            write_sse_event(
                &mut server,
                r#"{"v":1,"seq":1,"event":{"type":"core","core":{"type":"turn_completed"}}}"#,
            )
            .await
            .unwrap();
            // Force multiple chunks: a long event plus multi-byte characters.
            let long = format!(
                r#"{{"v":1,"seq":2,"event":{{"type":"core","core":{{"type":"user_message","text":"{}"}}}}}}"#,
                "é".repeat(300)
            );
            write_sse_event(&mut server, &long).await.unwrap();
            write_sse_end(&mut server).await.unwrap();
        });

        let mut reader = open_chunked_response(client, "GET", "/v1/s/x/events", "t")
            .await
            .unwrap();
        let mut seen = Vec::new();
        while let Some(bytes) = reader.next_bytes(1024).await.unwrap() {
            seen.extend_from_slice(&bytes);
        }
        server_task.await.unwrap();
        let (payloads, rest) = parse_sse_data(&seen);
        assert!(rest.is_empty());
        assert_eq!(payloads.len(), 2);
        let first: Envelope = serde_json::from_str(&payloads[0]).unwrap();
        assert_eq!(first.seq, 1);
        let second: Envelope = serde_json::from_str(&payloads[1]).unwrap();
        assert_eq!(second.seq, 2);
    }

    #[tokio::test]
    async fn chunked_reader_handles_split_frames() {
        // Frame the payload one byte at a time across chunk boundaries.
        let body = "0123456789".repeat(50);
        let mut raw = format!(
            "{:x}\r\n{}\r\n{:x}\r\n{}\r\n0\r\n\r\n",
            10,
            &body[..10],
            490,
            &body[10..]
        )
        .into_bytes();
        raw.extend_from_slice(b"trailing"); // bytes after the terminal chunk
        let mut reader = ChunkedReader::new(Cursor::new(raw), vec![]);
        let mut out = Vec::new();
        while let Some(bytes) = reader.next_bytes(7).await.unwrap() {
            out.extend_from_slice(&bytes);
        }
        assert_eq!(out, body.as_bytes());
    }

    #[test]
    fn sse_parse_splits_complete_events_and_keeps_the_tail() {
        let raw = b"data: {a}\n\ndata: {b}\n\ndata: {par".to_vec();
        let (payloads, rest) = parse_sse_data(&raw);
        assert_eq!(payloads, vec!["{a}", "{b}"]);
        assert_eq!(rest, b"data: {par");
    }

    #[tokio::test]
    async fn read_head_fails_cleanly_without_a_peer() {
        // With the peer dropped, the head read must error (EOF) rather than
        // hang or misparse.
        let (mut client, server) = tokio::io::duplex(16);
        drop(server);
        let result = read_head(&mut client).await;
        assert!(result.is_err());
    }
}
