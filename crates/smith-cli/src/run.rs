//! Headless one-shot runs (`smith run`) and session resume (`smith resume`).
//!
//! Everything is auto-approved here (there is no TUI to ask), which is only
//! safe because execution stays inside the podman sandbox.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use smith_core::{
    Agent, ApprovalDecision, ApprovalGate, ApprovalRequest, CoreEvent, Message, SessionLogger,
    ToolExecutor, clamp_output,
};
use smith_sandbox::{Sandbox, Session};
use tokio_util::sync::CancellationToken;

use crate::config::{Config, VerifyConfig};
use crate::executor::LocalExecutor;

struct AutoAllowGate;

#[async_trait::async_trait]
impl ApprovalGate for AutoAllowGate {
    async fn decide(&self, _request: ApprovalRequest) -> ApprovalDecision {
        ApprovalDecision::AllowOnce
    }
}

pub struct RunOutcome {
    pub session_path: PathBuf,
    pub container: String,
}

/// Build the provider for the configured backend.
pub fn build_provider(config: &Config) -> anyhow::Result<Arc<dyn smith_core::LlmProvider>> {
    match config.provider.as_str() {
        "ollama" => Ok(Arc::new(smith_providers::OllamaProvider::new(
            config.ollama.base_url.clone(),
            config.model.clone(),
        ))),
        "openai" => {
            let key = std::env::var("OPENAI_API_KEY").context(
                "OPENAI_API_KEY environment variable is not set (required for the openai provider)",
            )?;
            Ok(Arc::new(smith_providers::OpenAiProvider::new(
                config.openai.base_url.clone(),
                key,
                config.model.clone(),
            )))
        }
        other => anyhow::bail!("unknown provider `{other}`; expected `ollama` or `openai`"),
    }
}

/// Start a sandbox session and executor for a workspace.
pub async fn start_executor(
    config: &Config,
    workspace: &Path,
) -> anyhow::Result<(Arc<Sandbox>, LocalExecutor)> {
    let sandbox = Arc::new(Sandbox::new(config.sandbox_config()?));
    sandbox.ensure_ready().await?;
    sandbox.reap_stopped_sessions().await?;
    let session = sandbox.start_session(workspace).await?;
    let executor = LocalExecutor::new(Arc::clone(&sandbox), session);
    Ok((sandbox, executor))
}

/// Run one task headlessly, printing events as they arrive.
pub async fn run_headless(
    config: &Config,
    workspace: &Path,
    task: &str,
    keep_container: bool,
) -> anyhow::Result<RunOutcome> {
    let (sandbox, executor) = start_executor(config, workspace).await?;
    let container = executor.session().name.clone();
    set_clearance_mode(config, false);

    let logger = SessionLogger::new(
        &executor.session().workspace_host,
        &config.provider,
        &config.model,
    )?;
    let session_path = logger.path.clone();
    println!("session: {}", session_path.display());

    let workspace_canon = executor.session().workspace_host.clone();
    let mut mcp_session = connect_mcp(config, &workspace_canon).await;
    let mut tools = smith_core::builtin_tools();
    tools.extend(mcp_session.take_tools());

    let provider = build_provider(config)?;
    let executor: Arc<dyn ToolExecutor> = Arc::new(executor);
    let mut agent = Agent::new(
        provider,
        Arc::clone(&executor),
        tools,
        Arc::new(AutoAllowGate),
        Some(Arc::new(logger)),
    );
    agent.project_instructions =
        smith_core::instructions::load_project_instructions(&workspace_canon);
    agent.environment_notes = Some(environment_notes(config));

    let mut history = vec![Message::system(agent.system_prompt().await)];
    let cancel = CancellationToken::new();

    // Ctrl+C cancels the current turn (and any gate rounds).
    let cancel_for_signal = cancel.clone();
    let sig_task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            cancel_for_signal.cancel();
        }
    });

    run_turn_with_printer(&agent, &mut history, task, &cancel).await?;
    if !cancel.is_cancelled() {
        verify_gate(&agent, &mut history, &executor, &config.verify, &cancel).await?;
    }
    sig_task.abort();
    mcp_session.shutdown().await;

    if keep_container {
        println!("keeping container {container} (remove later with `smith clean`)");
    } else {
        let session = Session {
            name: container.clone(),
            workspace_host: workspace.to_path_buf(),
            workspace_container: "/workspace",
        };
        sandbox.stop_session(&session).await?;
    }

    Ok(RunOutcome {
        session_path,
        container,
    })
}

/// Continue a saved session: fresh container, same workspace, same log file.
/// Without `task`, prints a recap of where the session left off.
pub async fn resume_headless(
    config: &Config,
    workspace: &Path,
    id: Option<&str>,
    task: Option<&str>,
    force: bool,
    keep: bool,
) -> anyhow::Result<RunOutcome> {
    let canon = dunce::canonicalize(workspace)?;
    let sessions = smith_core::list_sessions()?;
    anyhow::ensure!(
        !sessions.is_empty(),
        "no saved sessions (see `smith sessions`)"
    );

    let (path, meta) = match id {
        Some(id) => {
            let matches: Vec<_> = sessions
                .iter()
                .filter(|(_, m)| m.id.starts_with(id))
                .collect();
            match matches.len() {
                0 => anyhow::bail!("no session matches `{id}` (see `smith sessions`)"),
                1 => matches[0].clone(),
                n => anyhow::bail!("{n} sessions match `{id}`; be more specific"),
            }
        }
        None => sessions[0].clone(),
    };

    let current = smith_core::session::workspace_identity(&canon);
    if meta.workspace_id != current && !force {
        anyhow::bail!(
            "session {} belongs to workspace `{}`; run from that workspace or pass --force",
            &meta.id[..8],
            meta.workspace_name
        );
    }

    let (sandbox, executor) = start_executor(config, workspace).await?;
    let container = executor.session().name.clone();
    set_clearance_mode(config, false);

    let logger = smith_core::SessionLogger::open_for_append(&path)?;
    println!("resumed session {} ({})", meta.id, path.display());

    let workspace_canon = executor.session().workspace_host.clone();
    let mut mcp_session = connect_mcp(config, &workspace_canon).await;
    let mut tools = smith_core::builtin_tools();
    tools.extend(mcp_session.take_tools());

    let provider = build_provider(config)?;
    let executor: Arc<dyn ToolExecutor> = Arc::new(executor);
    let mut agent = Agent::new(
        provider,
        Arc::clone(&executor),
        tools,
        Arc::new(AutoAllowGate),
        Some(Arc::new(logger)),
    );
    agent.project_instructions =
        smith_core::instructions::load_project_instructions(&workspace_canon);
    agent.environment_notes = Some(environment_notes(config));
    let (_, mut history) = smith_core::load_history(&path)?;
    if !history
        .first()
        .is_some_and(|m| m.role == smith_core::Role::System)
    {
        history.insert(0, Message::system(agent.system_prompt().await));
    }

    let cancel = CancellationToken::new();
    match task {
        Some(task) => {
            run_turn_with_printer(&agent, &mut history, task, &cancel).await?;
            if !cancel.is_cancelled() {
                verify_gate(&agent, &mut history, &executor, &config.verify, &cancel).await?;
            }
        }
        None => {
            let recap = history
                .iter()
                .rev()
                .find(|m| m.role == smith_core::Role::Assistant)
                .map(|m| m.content.clone())
                .unwrap_or_else(|| "(no assistant messages yet)".into());
            println!("recap: {}", recap.trim());
        }
    }

    if keep {
        println!("keeping container {container} (remove later with `smith clean`)");
    } else {
        let session = Session {
            name: container.clone(),
            workspace_host: canon,
            workspace_container: "/workspace",
        };
        sandbox.stop_session(&session).await?;
    }
    mcp_session.shutdown().await;

    Ok(RunOutcome {
        session_path: path,
        container,
    })
}

/// One-line environment summary injected into the system prompt (shared with
/// the TUI path in main.rs).
fn environment_notes(config: &Config) -> String {
    let egress = match config
        .sandbox_config()
        .map(|s| (s.network, s.egress_enforce))
    {
        Ok((smith_sandbox::NetworkMode::Proxy, true)) => {
            "egress: allowlist proxy, packet-enforced".to_string()
        }
        Ok((smith_sandbox::NetworkMode::Proxy, false)) => {
            "egress: allowlist proxy (cooperative only)".to_string()
        }
        Ok((smith_sandbox::NetworkMode::Disabled, _)) => "network: disabled".to_string(),
        Ok((smith_sandbox::NetworkMode::Open, _)) => "network: unrestricted".to_string(),
        Err(e) => return format!("network: unknown ({e:#})"),
    };
    format!("Network mode: {egress}")
}

/// Set the sidecar clearance mode for a headless run: `ask` degrades to
/// `deny` (nothing is attached to answer the modal); `allow` is honoured.
/// Best-effort: a missing sidecar/token just keeps the sidecar default.
pub fn set_clearance_mode(config: &Config, interactive: bool) {
    let effective = match config.sandbox.egress_clearance.as_str() {
        "allow" => "allow",
        "deny" => "deny",
        _ if interactive => "ask",
        _ => {
            println!("[clearance] ask degraded to deny for this headless run");
            "deny"
        }
    };
    if let Some(token) = smith_egress::client::read_token()
        && let Err(e) = smith_egress::client::set_mode(
            smith_egress::client::CLEARANCE_CONTROL_PORT,
            &token,
            effective,
        )
    {
        tracing::debug!("clearance mode not set: {e:#}");
    }
}

/// Load layered MCP config (user + workspace) and connect; never fatal.
async fn connect_mcp(config: &Config, workspace_canon: &Path) -> smith_mcp::McpSession {
    let mut mcp_config = config.mcp.clone();
    match smith_mcp::McpConfig::load_workspace(workspace_canon) {
        Ok(workspace_config) => mcp_config = mcp_config.merged(workspace_config),
        Err(e) => tracing::warn!("could not load workspace MCP config: {e:#}"),
    }
    smith_mcp::McpSession::connect(&mcp_config).await
}

/// Run one turn with the live event printer attached.
async fn run_turn_with_printer(
    agent: &Agent,
    history: &mut Vec<Message>,
    task: &str,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let printer = tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            print_event(&ev);
        }
    });
    let result = agent.run_turn(history, task, tx, cancel.clone()).await;
    printer.await.ok();
    result
}

/// Headless verify gate: run the configured command after the agent's turn;
/// feed failures back as fixing turns, up to `max_rounds` total runs.
async fn verify_gate(
    agent: &Agent,
    history: &mut Vec<Message>,
    executor: &Arc<dyn ToolExecutor>,
    verify: &VerifyConfig,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let command = verify.command.trim().to_string();
    if command.is_empty() {
        return Ok(());
    }
    let rounds = verify.max_rounds.max(1);
    for round in 0..rounds {
        println!("[verify] round {}: running `{command}`", round + 1);
        let res = executor
            .exec_shell(&command, Duration::from_secs(600))
            .await?;
        if res.exit_code == 0 {
            println!("[verify] passed");
            return Ok(());
        }
        if round + 1 >= rounds {
            println!("[verify] still failing after {rounds} round(s)");
            return Ok(());
        }
        println!(
            "[verify] failed (exit {}); asking the agent to fix it",
            res.exit_code
        );
        run_turn_with_printer(
            agent,
            history,
            &format!(
                "The verification command `{command}` failed with exit code {}:\n{}\n\n\
                 Fix the issue so the verification command passes.",
                res.exit_code,
                clamp_output(res.render(), 6000)
            ),
            cancel,
        )
        .await?;
    }
    Ok(())
}

fn print_event(ev: &CoreEvent) {
    match ev {
        CoreEvent::AssistantDelta { text } => {
            print!("{text}");
            let _ = std::io::stdout().flush();
        }
        CoreEvent::AssistantMessage { .. } => println!(),
        CoreEvent::ToolStarted { name, detail, .. } => {
            println!("\n[tool] {name}: {detail}");
        }
        CoreEvent::ToolFinished { ok, summary, .. } => {
            let marker = if *ok { "ok" } else { "FAILED" };
            println!("[tool] {marker} {}", summary.replace('\n', " | "));
        }
        CoreEvent::Usage(u) => {
            println!("[usage] in: {}, out: {}", u.input_tokens, u.output_tokens);
        }
        CoreEvent::Compacted { messages, .. } => {
            println!("[compacted {messages} messages]");
        }
        CoreEvent::Notice { text } => {
            println!("{text}");
        }
        CoreEvent::TurnCompleted => println!("\n[done]"),
        CoreEvent::TurnCancelled => println!("\n[cancelled]"),
        CoreEvent::Error { message } => println!("[error] {message}"),
        CoreEvent::ApprovalRequested { .. } => {}
    }
}
