//! Headless one-shot runs (`smith run`) and session resume (`smith resume`).
//!
//! Everything is auto-approved here (there is no TUI to ask), which is only
//! safe because execution stays inside the podman sandbox.

use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use smith_core::{
    Agent, ApprovalDecision, ApprovalGate, ApprovalRequest, CoreEvent, Message, SessionLogger,
    ToolExecutor, clamp_output,
};
// Harness assembly lives in smith-server; re-exported for main.rs.
use smith_sandbox::Session;
pub use smith_server::{
    build_provider, connect_mcp, environment_notes, set_clearance_mode, start_executor,
};
use tokio_util::sync::CancellationToken;

use crate::config::{Config, VerifyConfig};

struct AutoAllowGate;

#[async_trait::async_trait]
impl ApprovalGate for AutoAllowGate {
    async fn decide(&self, _request: ApprovalRequest) -> ApprovalDecision {
        ApprovalDecision::AllowOnce
    }
}

pub struct RunOutcome {
    pub session_path: std::path::PathBuf,
    pub container: String,
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
    let mut mcp_session = connect_mcp(config, Some(&workspace_canon)).await;
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
    let mut mcp_session = connect_mcp(config, Some(&workspace_canon)).await;
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

/// The CI issuer's submission: a task for a daemon-hosted session.
pub struct ServeRun<'a> {
    pub url: Option<&'a str>,
    pub token: Option<&'a str>,
    pub task: &'a str,
    pub worker: Option<&'a str>,
    pub repo: Option<&'a str>,
    pub git_ref: Option<&'a str>,
    pub provider: Option<&'a str>,
    pub model: Option<&'a str>,
}

/// Submit a task to a `smith serve` daemon and print the streamed events —
/// the CI issuer shape of the prompting plane. The session runs on the
/// daemon (auto-approved, like local headless runs), optionally leased to a
/// fleet worker with a git-materialised workspace. Returns the exit code.
pub async fn run_via_serve(opts: ServeRun<'_>) -> anyhow::Result<i32> {
    let client = crate::attach::resolve_client(opts.url, opts.token)?;
    client
        .health()
        .await
        .context("daemon unreachable — is `smith serve` running?")?;

    let workspace = std::env::current_dir().context("cannot resolve working directory")?;
    let canon = dunce::canonicalize(&workspace)?;
    let session_id = client
        .create_session_req(&smith_protocol::CreateSession {
            workspace: canon.to_str().context("cwd path")?.to_string(),
            provider: opts.provider.map(str::to_string),
            model: opts.model.map(str::to_string),
            approval_mode: smith_protocol::ApprovalMode::Auto,
            worker: opts.worker.map(str::to_string),
            repo: opts.repo.map(str::to_string),
            git_ref: opts.git_ref.map(str::to_string),
        })
        .await?;
    println!("session: {session_id}");

    // Subscribe first, then submit: the turn streams live.
    let mut events = client.events(&session_id, 0).await?;
    client.start_turn(&session_id, opts.task).await?;

    let mut exit = 0i32;
    loop {
        let env = tokio::time::timeout(Duration::from_secs(1800), events.next())
            .await
            .context("timed out waiting for the turn (30m)")??;
        let Some(env) = env else { break };
        let stop = match &env.event {
            smith_protocol::WireEvent::Core { core } => match core {
                // The task line is already visible to the issuer.
                CoreEvent::UserMessage { .. } => false,
                CoreEvent::TurnCompleted => {
                    println!("\n[done]");
                    true
                }
                CoreEvent::TurnCancelled => {
                    println!("\n[cancelled]");
                    true
                }
                CoreEvent::Error { .. } => {
                    exit = 1;
                    print_event(core);
                    true
                }
                _ => {
                    print_event(core);
                    false
                }
            },
            smith_protocol::WireEvent::ExecOutput {
                is_stderr, text, ..
            } => {
                // Live remote exec output; stderr tagged.
                let tag = if *is_stderr { "!" } else { "" };
                print!("{tag}{text}");
                let _ = std::io::stdout().flush();
                false
            }
            _ => false,
        };
        if stop {
            break;
        }
    }
    Ok(exit)
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
        // The headless printer shows the task line itself; and approvals are
        // auto-allowed here.
        CoreEvent::UserMessage { .. } => {}
        CoreEvent::ApprovalRequested { .. } => {}
    }
}
