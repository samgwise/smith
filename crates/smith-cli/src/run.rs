//! Headless one-shot runs (`smith run`) and session resume (`smith resume`).
//!
//! Everything is auto-approved here (there is no TUI to ask), which is only
//! safe because execution stays inside the podman sandbox.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use smith_core::{
    Agent, ApprovalDecision, ApprovalGate, ApprovalRequest, CoreEvent, Message, SessionLogger,
};
use smith_sandbox::{Sandbox, Session};
use tokio_util::sync::CancellationToken;

use crate::config::Config;
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

    let logger = SessionLogger::new(
        &executor.session().workspace_host,
        &config.provider,
        &config.model,
    )?;
    let session_path = logger.path.clone();
    println!("session: {}", session_path.display());

    let provider = build_provider(config)?;
    let agent = Agent::new(
        provider,
        Arc::new(executor),
        smith_core::builtin_tools(),
        Arc::new(AutoAllowGate),
        Some(Arc::new(logger)),
    );

    let mut history = vec![Message::system(agent.system_prompt())];
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let cancel = CancellationToken::new();

    // Live event printer.
    let printer = tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            print_event(&ev);
        }
    });

    // Ctrl+C cancels the current turn.
    let cancel_for_signal = cancel.clone();
    let sig_task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            cancel_for_signal.cancel();
        }
    });

    let turn = agent.run_turn(&mut history, task, tx, cancel);
    let turn = tokio::select! {
        _ = sig_task => anyhow::bail!("interrupted"),
        result = turn => result,
    };
    turn?;
    printer.await.ok();

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

    let logger = smith_core::SessionLogger::open_for_append(&path)?;
    println!("resumed session {} ({})", meta.id, path.display());

    let provider = build_provider(config)?;
    let agent = Agent::new(
        provider,
        Arc::new(executor),
        smith_core::builtin_tools(),
        Arc::new(AutoAllowGate),
        Some(Arc::new(logger)),
    );
    let (_, mut history) = smith_core::load_history(&path)?;
    if !history
        .first()
        .is_some_and(|m| m.role == smith_core::Role::System)
    {
        history.insert(0, Message::system(agent.system_prompt()));
    }

    let cancel = CancellationToken::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let printer = tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            print_event(&ev);
        }
    });

    match task {
        Some(task) => {
            agent.run_turn(&mut history, task, tx, cancel).await?;
            printer.await.ok();
        }
        None => {
            let recap = history
                .iter()
                .rev()
                .find(|m| m.role == smith_core::Role::Assistant)
                .map(|m| m.content.clone())
                .unwrap_or_else(|| "(no assistant messages yet)".into());
            println!("recap: {}", recap.trim());
            let _ = tx; // closes the printer on drop
            printer.await.ok();
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

    Ok(RunOutcome {
        session_path: path,
        container,
    })
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
