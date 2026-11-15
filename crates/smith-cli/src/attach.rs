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
/// and finally the defaults. `cert` (a PEM file, mirroring
/// `SMITH_SERVE_CERT`) pins the controller's certificate for https targets.
pub fn resolve_client(
    url: Option<&str>,
    token: Option<&str>,
    cert: Option<&str>,
) -> anyhow::Result<ServeClient> {
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
    let cert_pem = cert
        .map(str::to_string)
        .or_else(|| std::env::var("SMITH_SERVE_CERT").ok())
        .filter(|p| !p.trim().is_empty())
        .map(|p| {
            std::fs::read_to_string(&p)
                .with_context(|| format!("reading the controller certificate {p}"))
        })
        .transpose()?;
    ServeClient::from_target(&url, token, cert_pem)
}

/// The attach flow's options (from the CLI flags).
pub struct AttachOpts<'a> {
    pub url: Option<&'a str>,
    pub token: Option<&'a str>,
    /// PEM file pinning the daemon's certificate (https targets).
    pub cert: Option<&'a str>,
    /// Session id (or unique prefix); newest for this workspace when None.
    pub id: Option<&'a str>,
    pub worker: Option<&'a str>,
    pub repo: Option<&'a str>,
    pub git_ref: Option<&'a str>,
    pub orchestrate: bool,
}

/// Run the attach flow: health check, find-or-create the session, then the
/// TUI over a `RemoteLink`. Worker-mode flags (`--worker/--repo/--ref`)
/// lease a fleet worker for the session.
pub async fn run_attach(opts: AttachOpts<'_>) -> anyhow::Result<()> {
    let client = resolve_client(opts.url, opts.token, opts.cert)?;
    client
        .health()
        .await
        .context("daemon unreachable — is `smith serve` running?")?;
    let worker_mode = opts.worker.is_some() || opts.repo.is_some();

    let cwd = std::env::current_dir().context("cannot resolve working directory")?;
    let canon = dunce::canonicalize(&cwd)?;
    let identity = workspace_identity(&canon);

    let session_id = match opts.id {
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
                        opts.repo.unwrap_or(&canon.display().to_string())
                    );
                    client
                        .create_session_req(&smith_protocol::CreateSession {
                            workspace: canon.to_str().context("cwd path")?.to_string(),
                            provider: None,
                            model: None,
                            approval_mode: smith_protocol::ApprovalMode::Ask,
                            worker: opts.worker.map(str::to_string),
                            repo: opts.repo.map(str::to_string),
                            git_ref: opts.git_ref.map(str::to_string),
                            orchestrate: opts.orchestrate,
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
    cert: Option<&str>,
    name: &str,
) -> anyhow::Result<()> {
    let client = resolve_client(url, token, cert)?;
    let enrolment = client.worker_issue(name).await?;
    println!("enrolment token for `{name}` (single use, hand to the worker):\n{enrolment}");
    Ok(())
}

/// List registered workers.
pub async fn workers_list(
    url: Option<&str>,
    token: Option<&str>,
    cert: Option<&str>,
) -> anyhow::Result<()> {
    let client = resolve_client(url, token, cert)?;
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
    cert: Option<&str>,
    name: &str,
) -> anyhow::Result<()> {
    let client = resolve_client(url, token, cert)?;
    client.worker_revoke(name).await?;
    println!("worker `{name}` revoked");
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn parses_serve_urls() {
        // The URL parser lives in smith-protocol (shared with the worker);
        // the CLI's contract is that its targets pass through it.
        assert_eq!(
            smith_protocol::parse_serve_url("127.0.0.1:9240").unwrap(),
            (false, "127.0.0.1".into(), 9240)
        );
        assert_eq!(
            smith_protocol::parse_serve_url("http://10.0.0.5:9250").unwrap(),
            (false, "10.0.0.5".into(), 9250)
        );
        assert_eq!(
            smith_protocol::parse_serve_url("gpu-box").unwrap(),
            (false, "gpu-box".into(), smith_protocol::DEFAULT_SERVE_PORT)
        );
        assert_eq!(
            smith_protocol::parse_serve_url("localhost:9999").unwrap(),
            (false, "localhost".into(), 9999)
        );
    }
}
