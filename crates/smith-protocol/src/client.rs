//! Client for the `smith serve` control API: raw HTTP over loopback TCP
//! (matching the daemon's hand-rolled server), plus the SSE [`EventStream`]
//! that decodes envelopes.

use std::collections::VecDeque;
use std::time::Duration;

use anyhow::Context;
use tokio::net::TcpStream;

use crate::events::{
    ApprovalPost, ClearancePost, CreateReply, CreateSession, DiffReply, Envelope, ProviderPost,
    ResumePost, ResumeReply, SessionStatus, SessionSummary, StartTurn,
};
use crate::http::{open_chunked_response, raw_request};

/// Connect timeout for RPC-style calls.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Standard request timeout. Creating a session is exempt (sandbox cold
/// start, possibly an image build).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// Timeout for `create_session` (image builds on a cold machine).
const CREATE_TIMEOUT: Duration = Duration::from_secs(600);

#[derive(Clone)]
pub struct ServeClient {
    host: String,
    port: u16,
    token: String,
}

impl ServeClient {
    /// Client for an explicit target.
    pub fn new(host: &str, port: u16, token: String) -> Self {
        Self {
            host: host.to_string(),
            port,
            token,
        }
    }

    /// Resolve the target from the environment: `SMITH_SERVE_URL`
    /// (`host` or `host:port`) and `SMITH_SERVE_TOKEN`, falling back to the
    /// default port and the daemon's token file.
    pub fn from_env() -> anyhow::Result<Self> {
        let url = std::env::var("SMITH_SERVE_URL").unwrap_or_default();
        let (host, port) = match url.rsplit_once(':') {
            Some((h, p)) if !h.is_empty() => (
                h.trim_matches(|c| c == '[' || c == ']'),
                p.parse().context("SMITH_SERVE_URL port")?,
            ),
            _ => (
                url.trim_matches(|c| c == '[' || c == ']'),
                crate::events::DEFAULT_SERVE_PORT,
            ),
        };
        let host = if host.is_empty() { "127.0.0.1" } else { host };
        let token = match std::env::var("SMITH_SERVE_TOKEN") {
            Ok(t) if !t.trim().is_empty() => t.trim().to_string(),
            _ => read_token_file()
                .context("no serve token: SMITH_SERVE_TOKEN or the daemon's token file")?,
        };
        Ok(Self::new(host, port, token))
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    async fn connect(&self) -> anyhow::Result<TcpStream> {
        tokio::time::timeout(
            CONNECT_TIMEOUT,
            TcpStream::connect((self.host.as_str(), self.port)),
        )
        .await
        .context("timed out connecting to the daemon")?
        .with_context(|| format!("connecting to {}:{}", self.host, self.port))
    }

    async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
        budget: Duration,
    ) -> anyhow::Result<String> {
        let mut stream = self.connect().await?;
        let (status, body) = tokio::time::timeout(
            budget,
            raw_request(&mut stream, method, path, &self.token, body),
        )
        .await
        .context("timed out waiting for the daemon")??;
        anyhow::ensure!(
            status == 200 || status == 202,
            "daemon returned {status}: {}",
            body.chars().take(300).collect::<String>()
        );
        Ok(body)
    }

    async fn call_json<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        body: Option<String>,
        budget: Duration,
    ) -> anyhow::Result<T> {
        let raw = self.call(method, path, body.as_deref(), budget).await?;
        serde_json::from_str(&raw).with_context(|| format!("parsing {path} response"))
    }

    pub async fn health(&self) -> anyhow::Result<()> {
        self.call("GET", "/v1/health", None, REQUEST_TIMEOUT)
            .await?;
        Ok(())
    }

    pub async fn sessions(&self) -> anyhow::Result<Vec<SessionSummary>> {
        self.call_json("GET", "/v1/sessions", None, REQUEST_TIMEOUT)
            .await
    }

    /// Create a session for a workspace path (daemon-side). Long call.
    pub async fn create_session(
        &self,
        workspace: &str,
        provider: Option<&str>,
        model: Option<&str>,
    ) -> anyhow::Result<String> {
        self.create_session_req(&CreateSession {
            workspace: workspace.to_string(),
            provider: provider.map(str::to_string),
            model: model.map(str::to_string),
            ..Default::default()
        })
        .await
    }

    /// Create with the full M14 surface (see `CreateSession`): approval mode
    /// and worker-mode execution (worker name prefix, repo URL, git ref).
    pub async fn create_session_req(&self, req: &CreateSession) -> anyhow::Result<String> {
        let body = serde_json::to_string(req)?;
        let reply: CreateReply = self
            .call_json("POST", "/v1/sessions", Some(body), CREATE_TIMEOUT)
            .await?;
        Ok(reply.session_id)
    }

    pub async fn start_turn(&self, id: &str, input: &str) -> anyhow::Result<()> {
        let body = serde_json::to_string(&StartTurn {
            input: input.to_string(),
        })?;
        self.call(
            "POST",
            &format!("/v1/sessions/{id}/turns"),
            Some(&body),
            REQUEST_TIMEOUT,
        )
        .await?;
        Ok(())
    }

    pub async fn cancel(&self, id: &str) -> anyhow::Result<()> {
        self.call(
            "POST",
            &format!("/v1/sessions/{id}/cancel"),
            None,
            REQUEST_TIMEOUT,
        )
        .await?;
        Ok(())
    }

    pub async fn approval(
        &self,
        id: &str,
        call_id: Option<&str>,
        decision: smith_core::ApprovalDecision,
    ) -> anyhow::Result<()> {
        let body = serde_json::to_string(&ApprovalPost {
            decision,
            call_id: call_id.map(str::to_string),
        })?;
        self.call(
            "POST",
            &format!("/v1/sessions/{id}/approvals"),
            Some(&body),
            REQUEST_TIMEOUT,
        )
        .await?;
        Ok(())
    }

    pub async fn clearance(
        &self,
        id: &str,
        clearance_id: u64,
        decision: smith_core::ApprovalDecision,
    ) -> anyhow::Result<()> {
        let body = serde_json::to_string(&ClearancePost {
            id: clearance_id,
            decision,
        })?;
        self.call(
            "POST",
            &format!("/v1/sessions/{id}/clearance"),
            Some(&body),
            REQUEST_TIMEOUT,
        )
        .await?;
        Ok(())
    }

    pub async fn set_provider(
        &self,
        id: &str,
        provider: Option<&str>,
        model: Option<&str>,
    ) -> anyhow::Result<()> {
        let body = serde_json::to_string(&ProviderPost {
            provider: provider.map(str::to_string),
            model: model.map(str::to_string),
        })?;
        self.call(
            "POST",
            &format!("/v1/sessions/{id}/provider"),
            Some(&body),
            REQUEST_TIMEOUT,
        )
        .await?;
        Ok(())
    }

    pub async fn diff(&self, id: &str) -> anyhow::Result<String> {
        let reply: DiffReply = self
            .call_json(
                "GET",
                &format!("/v1/sessions/{id}/diff"),
                None,
                REQUEST_TIMEOUT,
            )
            .await?;
        Ok(reply.text)
    }

    pub async fn status(&self, id: &str) -> anyhow::Result<SessionStatus> {
        self.call_json(
            "GET",
            &format!("/v1/sessions/{id}/status"),
            None,
            REQUEST_TIMEOUT,
        )
        .await
    }

    pub async fn resume(&self, id: &str, prefix: &str) -> anyhow::Result<ResumeReply> {
        let body = serde_json::to_string(&ResumePost {
            prefix: prefix.to_string(),
        })?;
        self.call_json(
            "POST",
            &format!("/v1/sessions/{id}/resume"),
            Some(body),
            REQUEST_TIMEOUT,
        )
        .await
    }

    /// Subscribe to a session's event stream, replaying everything after
    /// `since` first.
    pub async fn events(&self, id: &str, since: u64) -> anyhow::Result<EventStream> {
        self.stream(&format!("/v1/sessions/{id}/events?since={since}"))
            .await
    }

    /// Open an SSE stream on an arbitrary path (worker mesh command streams).
    pub async fn stream(&self, path: &str) -> anyhow::Result<EventStream> {
        let stream = self.connect().await?;
        let reader = open_chunked_response(stream, "GET", path, &self.token).await?;
        Ok(EventStream::new(reader))
    }

    /// POST a body to an arbitrary path (worker mesh routes), requiring
    /// 200/202.
    pub async fn post_body(&self, path: &str, body: String) -> anyhow::Result<()> {
        self.call("POST", path, Some(&body), REQUEST_TIMEOUT)
            .await
            .map(|_| ())
    }

    /// `POST /v1/worker/enrol` — exchange a one-time enrolment token for a
    /// worker credential (the bearer here is the enrolment token).
    pub async fn worker_enrol(
        &self,
        req: &crate::workers::EnrolRequest,
    ) -> anyhow::Result<crate::workers::EnrolReply> {
        let body = serde_json::to_string(req)?;
        let raw = self
            .call("POST", "/v1/worker/enrol", Some(&body), REQUEST_TIMEOUT)
            .await?;
        serde_json::from_str(&raw).context("parsing enrolment reply")
    }

    // ---------------------------------------------------- fleet admin --

    /// Mint a single-use enrolment token for a worker name (daemon token).
    pub async fn worker_issue(&self, name: &str) -> anyhow::Result<String> {
        let raw = self
            .call(
                "POST",
                "/v1/workers",
                Some(&serde_json::json!({ "name": name }).to_string()),
                REQUEST_TIMEOUT,
            )
            .await?;
        let v: serde_json::Value = serde_json::from_str(&raw).context("parsing issue reply")?;
        Ok(v.get("enrolment_token")
            .and_then(serde_json::Value::as_str)
            .context("no enrolment_token in reply")?
            .to_string())
    }

    /// List registered workers (daemon token).
    pub async fn workers(&self) -> anyhow::Result<Vec<crate::workers::WorkerSummary>> {
        self.call_json("GET", "/v1/workers", None, REQUEST_TIMEOUT)
            .await
    }

    /// Revoke a worker (daemon token). Its leases are lost.
    pub async fn worker_revoke(&self, name: &str) -> anyhow::Result<()> {
        self.call(
            "DELETE",
            &format!("/v1/workers/{name}"),
            None,
            REQUEST_TIMEOUT,
        )
        .await
        .map(|_| ())
    }

    /// Scrape the daemon's metrics text.
    pub async fn metrics(&self) -> anyhow::Result<String> {
        self.call("GET", "/v1/metrics", None, REQUEST_TIMEOUT).await
    }
}

/// The daemon's token file: `~/.smith/serve-token`.
pub fn token_file() -> anyhow::Result<std::path::PathBuf> {
    Ok(smith_core::session::smith_dir()?.join("serve-token"))
}

/// Read the daemon's token, if it has been started before.
pub fn read_token_file() -> Option<String> {
    std::fs::read_to_string(token_file().ok()?)
        .ok()
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
}

/// Decoded stream over the daemon's SSE response: envelopes for client
/// event streams, raw payloads for worker command streams.
pub struct EventStream {
    chunks: crate::http::ChunkedReader<TcpStream>,
    pending: VecDeque<Envelope>,
    pending_raw: VecDeque<String>,
    raw: Vec<u8>,
    done: bool,
}

impl EventStream {
    fn new(chunks: crate::http::ChunkedReader<TcpStream>) -> Self {
        Self {
            chunks,
            pending: VecDeque::new(),
            pending_raw: VecDeque::new(),
            raw: Vec::new(),
            done: false,
        }
    }

    /// Next raw SSE data payload; `None` when the daemon ends the stream.
    /// Used by the worker command stream (whose payloads are commands, not
    /// envelopes).
    pub async fn next_raw(&mut self) -> anyhow::Result<Option<String>> {
        loop {
            if let Some(payload) = self.pending_raw.pop_front() {
                return Ok(Some(payload));
            }
            if self.done {
                return Ok(None);
            }
            let bytes = self.chunks.next_bytes(8192).await?;
            match bytes {
                None => self.done = true,
                Some(b) => {
                    self.raw.extend_from_slice(&b);
                    let (payloads, rest) = crate::http::parse_sse_data(&self.raw);
                    self.raw = rest;
                    self.pending_raw.extend(payloads);
                }
            }
        }
    }

    /// Next envelope; `None` when the daemon ends the stream.
    pub async fn next(&mut self) -> anyhow::Result<Option<Envelope>> {
        loop {
            if let Some(env) = self.pending.pop_front() {
                return Ok(Some(env));
            }
            if self.done {
                return Ok(None);
            }
            let bytes = self.chunks.next_bytes(8192).await?;
            match bytes {
                None => self.done = true,
                Some(b) => {
                    self.raw.extend_from_slice(&b);
                    let (payloads, rest) = crate::http::parse_sse_data(&self.raw);
                    self.raw = rest;
                    self.pending_raw.extend(payloads.iter().cloned());
                    for payload in payloads {
                        let env: Envelope = serde_json::from_str(&payload)
                            .context("malformed event in the daemon stream")?;
                        self.pending.push_back(env);
                    }
                }
            }
        }
    }
}

/// Map an `ApprovalDecision` to the sidecar's decision word
/// (`allow` / `always` / `deny`), shared by the daemon and local clients.
pub fn decision_word(decision: smith_core::ApprovalDecision) -> &'static str {
    match decision {
        smith_core::ApprovalDecision::AllowOnce => "allow",
        smith_core::ApprovalDecision::AllowAlways => "always",
        smith_core::ApprovalDecision::Deny => "deny",
    }
}

/// Connection-refused style errors mean the daemon is not running; surface
/// that hint once rather than a raw socket error.
pub fn is_connection_error(err: &anyhow::Error) -> bool {
    err.chain()
        .filter_map(|c| c.downcast_ref::<std::io::Error>())
        .any(|io| {
            matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::NotFound
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_url_parsing() {
        // Covered end-to-end in smith-server's integration tests; here just
        // the decision word mapping (part of the frozen surface).
        assert_eq!(
            decision_word(smith_core::ApprovalDecision::AllowOnce),
            "allow"
        );
        assert_eq!(
            decision_word(smith_core::ApprovalDecision::AllowAlways),
            "always"
        );
        assert_eq!(decision_word(smith_core::ApprovalDecision::Deny), "deny");
    }
}
