//! `smith attach`: the prompting-plane thin client. Attaches the TUI to a
//! session hosted by a `smith serve` daemon — the agent loop, history,
//! approvals and model keys all live daemon-side.

use anyhow::Context;
use smith_core::session::workspace_identity;
use smith_protocol::ServeClient;
use smith_tui::{App, AppParams, RemoteLink};

/// Default daemon address when neither `--url` nor `SMITH_SERVE_URL` is set.
const DEFAULT_URL: &str = "127.0.0.1:9240";

/// Resolve the client from explicit flags, falling back to the environment
/// and finally the defaults.
pub fn resolve_client(url: Option<&str>, token: Option<&str>) -> anyhow::Result<ServeClient> {
    let url = url
        .map(str::to_string)
        .or_else(|| std::env::var("SMITH_SERVE_URL").ok())
        .unwrap_or_else(|| DEFAULT_URL.to_string());
    let token = token
        .map(str::to_string)
        .or_else(|| std::env::var("SMITH_SERVE_TOKEN").ok())
        .map(Ok)
        .unwrap_or_else(|| {
            smith_protocol::client::read_token_file().context(
                "no serve token: pass --token, set SMITH_SERVE_TOKEN, or start `smith serve` once",
            )
        })?;
    let (host, port) = parse_url(&url)?;
    Ok(ServeClient::new(&host, port, token))
}

/// Accept `host:port`, bare `host`, or an `http://` prefix (no TLS at this
/// layer; a reverse proxy terminates it in front of the daemon).
fn parse_url(url: &str) -> anyhow::Result<(String, u16)> {
    let url = url.trim().trim_start_matches("http://");
    let url = url.trim_end_matches('/');
    match url.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() => Ok((
            host.trim_matches(|c| c == '[' || c == ']').to_string(),
            port.parse().context("port in the serve URL")?,
        )),
        Some(_) => Ok(("127.0.0.1".into(), smith_protocol::DEFAULT_SERVE_PORT)),
        None => Ok((url.to_string(), smith_protocol::DEFAULT_SERVE_PORT)),
    }
}

/// Run the attach flow: health check, find-or-create the session, then the
/// TUI over a `RemoteLink`. Worker-mode flags (`--worker/--repo/--ref`)
/// lease a fleet worker for the session.
pub async fn run_attach(
    url: Option<&str>,
    token: Option<&str>,
    id: Option<&str>,
    worker: Option<&str>,
    repo: Option<&str>,
    git_ref: Option<&str>,
    orchestrate: bool,
) -> anyhow::Result<()> {
    let client = resolve_client(url, token)?;
    client
        .health()
        .await
        .context("daemon unreachable — is `smith serve` running?")?;
    let worker_mode = worker.is_some() || repo.is_some();

    let cwd = std::env::current_dir().context("cannot resolve working directory")?;
    let canon = dunce::canonicalize(&cwd)?;
    let identity = workspace_identity(&canon);

    let session_id = match id {
        Some(prefix) => {
            let sessions = client.sessions().await?;
            let matches: Vec<_> = sessions
                .iter()
                .filter(|s| s.id.starts_with(prefix))
                .collect();
            match matches.len() {
                0 => anyhow::bail!("no daemon session matches `{prefix}`"),
                1 => matches[0].id.clone(),
                n => anyhow::bail!("{n} sessions match `{prefix}`; be more specific"),
            }
        }
        None => {
            let existing = client
                .sessions()
                .await?
                .into_iter()
                .filter(|s| s.workspace_id == identity)
                .max_by_key(|s| s.created_ms);
            match existing {
                Some(s) if !worker_mode => s.id,
                // Worker mode always creates a fresh lease (a new task, a
                // fresh worktree); a path-based reuse makes no sense.
                _ => {
                    println!(
                        "starting a session for {} ...",
                        repo.unwrap_or(&canon.display().to_string())
                    );
                    client
                        .create_session_req(&smith_protocol::CreateSession {
                            workspace: canon.to_str().context("cwd path")?.to_string(),
                            provider: None,
                            model: None,
                            approval_mode: smith_protocol::ApprovalMode::Ask,
                            worker: worker.map(str::to_string),
                            repo: repo.map(str::to_string),
                            git_ref: git_ref.map(str::to_string),
                            orchestrate,
                        })
                        .await?
                }
            }
        }
    };

    let status = client.status(&session_id).await?;
    println!(
        "attached to session {} ({}:{}) — the daemon owns the harness",
        &session_id[..8.min(session_id.len())],
        status.provider,
        status.model
    );

    let link = RemoteLink::spawn(client, session_id.clone());
    let params = AppParams {
        link: Box::new(link),
        user_echo: true,
        workspace_label: status.workspace,
        container_label: status.container.unwrap_or_else(|| "remote".to_string()),
        session_label: session_id.chars().take(8).collect(),
        provider_name: status.provider,
        model_name: status.model,
        mcp_status: status.mcp_status,
    };
    App::new(params).run().await
}

// ------------------------------------------------------------ fleet admin --

/// Mint a single-use enrolment token for a worker (daemon token).
pub async fn worker_issue(
    url: Option<&str>,
    token: Option<&str>,
    name: &str,
) -> anyhow::Result<()> {
    let client = resolve_client(url, token)?;
    let enrolment = client.worker_issue(name).await?;
    println!("enrolment token for `{name}` (single use, hand to the worker):\n{enrolment}");
    Ok(())
}

/// List registered workers.
pub async fn workers_list(url: Option<&str>, token: Option<&str>) -> anyhow::Result<()> {
    let client = resolve_client(url, token)?;
    let workers = client.workers().await?;
    if workers.is_empty() {
        println!("no workers enrolled (smith worker issue <name>)");
        return Ok(());
    }
    for w in workers {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let seen = if w.last_seen_ms == 0 {
            "never".to_string()
        } else {
            format!("{:>3}s ago", (now_ms.saturating_sub(w.last_seen_ms)) / 1000)
        };
        println!(
            "{}  {}  {:?}  leases {}  cpus {}  {}  seen {}",
            w.name,
            &w.id[..8.min(w.id.len())],
            w.status,
            w.load.leases,
            w.capability.cpus,
            w.capability.os,
            seen
        );
    }
    Ok(())
}

/// Revoke a worker; its leases are lost (checkpoint branches hold the work).
pub async fn worker_revoke(
    url: Option<&str>,
    token: Option<&str>,
    name: &str,
) -> anyhow::Result<()> {
    let client = resolve_client(url, token)?;
    client.worker_revoke(name).await?;
    println!("worker `{name}` revoked");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_serve_urls() {
        assert_eq!(
            parse_url("127.0.0.1:9240").unwrap(),
            ("127.0.0.1".into(), 9240)
        );
        assert_eq!(
            parse_url("http://10.0.0.5:9250").unwrap(),
            ("10.0.0.5".into(), 9250)
        );
        assert_eq!(
            parse_url("gpu-box").unwrap(),
            ("gpu-box".into(), smith_protocol::DEFAULT_SERVE_PORT)
        );
        assert_eq!(
            parse_url("localhost:9999").unwrap(),
            ("localhost".into(), 9999)
        );
    }
}
