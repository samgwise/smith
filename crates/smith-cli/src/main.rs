mod config;
mod doctor;
mod executor;
mod run;

use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use smith_core::{Agent, Message, SessionLogger};
use smith_sandbox::Sandbox;

use crate::config::Config;

/// Interactive TUI session: sandbox up for the whole session, approval modal
/// in the UI, container left running on exit (reaped later by `smith clean`).
async fn run_tui(config: &Config, workspace: &std::path::Path) -> anyhow::Result<()> {
    let (gate_tx, gate_rx) = smith_tui::approval_channel();

    let (_sandbox, executor) = run::start_executor(config, workspace).await?;
    let container = executor.session().name.clone();
    let workspace_canon = executor.session().workspace_host.clone();

    let logger = SessionLogger::new(&workspace_canon, &config.provider, &config.model)?;
    println!("session: {}", logger.path.display());
    let session_label = logger.meta.id.chars().take(8).collect::<String>();

    // MCP servers: connected for the whole TUI session, tools added to the
    // registry once (cache-stable), shut down when the session ends.
    let mut mcp_session = {
        let mut mcp_config = config.mcp.clone();
        match smith_mcp::McpConfig::load_workspace(&workspace_canon) {
            Ok(ws) => mcp_config = mcp_config.merged(ws),
            Err(e) => tracing::warn!("could not load workspace MCP config: {e:#}"),
        }
        smith_mcp::McpSession::connect(&mcp_config).await
    };
    let mcp_status = mcp_session.status_lines();
    let mut tools = smith_core::builtin_tools();
    tools.extend(mcp_session.take_tools());

    let provider = run::build_provider(config)?;
    let mut agent = Agent::new(
        provider,
        std::sync::Arc::new(executor),
        tools,
        std::sync::Arc::new(smith_tui::TuiGate::new(gate_tx)),
        Some(std::sync::Arc::new(logger)),
    );
    agent.project_instructions =
        smith_core::instructions::load_project_instructions(&workspace_canon);
    agent.environment_notes = Some(environment_notes(config));
    let system_prompt = agent.system_prompt().await;

    let provider_name = config.provider.clone();
    let model_name = config.model.clone();
    let container_label = container
        .trim_start_matches("smith-")
        .chars()
        .take(12)
        .collect::<String>();

    let cfg_for_factory = config.clone();
    let build_provider: smith_tui::app::ProviderFactory =
        std::sync::Arc::new(move |provider: &str, model: &str| {
            let mut cfg = cfg_for_factory.clone();
            cfg.provider = provider.to_string();
            cfg.model = model.to_string();
            run::build_provider(&cfg)
        });

    let params = smith_tui::AppParams {
        agent: std::sync::Arc::new(agent),
        history: vec![Message::system(system_prompt)],
        gate_rx,
        workspace: workspace_canon.clone(),
        workspace_label: workspace_canon
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| workspace.display().to_string()),
        container_label,
        session_label,
        provider_name,
        model_name,
        build_provider,
        mcp_status,
    };

    let result = smith_tui::App::new(params).run().await;
    mcp_session.shutdown().await;

    println!("\nsession container {container} is still running; remove it with `smith clean`");
    result
}

#[derive(Parser)]
#[command(name = "smith", about = "Podman-sandboxed coding agent", version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run one task headlessly (auto-approved, sandboxed).
    Run {
        /// The task for the agent.
        task: String,
        /// Workspace directory (defaults to the current directory).
        #[arg(long)]
        workspace: Option<PathBuf>,
        /// Keep the session container after the run.
        #[arg(long)]
        keep: bool,
        /// Override the configured provider (`ollama` or `openai`).
        #[arg(long)]
        provider: Option<String>,
        /// Override the configured model.
        #[arg(long)]
        model: Option<String>,
        /// Headless verify command (overrides config; empty disables).
        #[arg(long)]
        verify: Option<String>,
    },
    /// Continue a previous session.
    Resume {
        /// Session id (or unique prefix); newest if omitted.
        id: Option<String>,
        /// Follow-up task to run in the resumed session.
        #[arg(long)]
        task: Option<String>,
        /// Resume even though the workspace differs from the session's.
        #[arg(long)]
        force: bool,
        /// Keep the session container after the run.
        #[arg(long)]
        keep: bool,
        /// Workspace directory (defaults to the current directory).
        #[arg(long)]
        workspace: Option<PathBuf>,
        /// Override the configured provider.
        #[arg(long)]
        provider: Option<String>,
        /// Override the configured model.
        #[arg(long)]
        model: Option<String>,
        /// Headless verify command (overrides config; empty disables).
        #[arg(long)]
        verify: Option<String>,
    },
    /// Diagnose the environment (read-only; --fix repairs, --deep runs round trips).
    Doctor {
        /// Emit machine-readable JSON instead of a table.
        #[arg(long)]
        json: bool,
        /// Include deep checks (real round trips).
        #[arg(long)]
        deep: bool,
        /// Remediate what can be fixed (start machine, pull/build images).
        #[arg(long)]
        fix: bool,
    },
    /// Run the full conformance suite (deep checks as a command).
    Conformance {
        #[arg(long)]
        json: bool,
        /// Network mode override for the run: none | proxy | open.
        #[arg(long)]
        network: Option<String>,
    },
    /// List recent sessions.
    Sessions,
    /// Sandbox utilities.
    Sandbox {
        #[command(subcommand)]
        cmd: SandboxCmd,
    },
    /// Remove every smith session container.
    Clean,
}

#[derive(Subcommand)]
enum SandboxCmd {
    /// Start a session container against the given workspace and run a command.
    Exec {
        /// Workspace directory to mount (defaults to the current directory).
        #[arg(long)]
        workspace: Option<PathBuf>,
        /// Network mode override: none | proxy | open.
        #[arg(long)]
        network: Option<String>,
        /// User to exec as (default: the non-root `dev` user).
        #[arg(long)]
        user: Option<String>,
        /// Command to run inside the container.
        #[arg(last = true)]
        command: Vec<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();

    let cli = Cli::parse();
    let config = Config::load()?;
    config::ensure_example_config()?;

    match cli.command {
        None => {
            let workspace = std::env::current_dir().context("cannot resolve working directory")?;
            run_tui(&config, &workspace).await?;
        }
        Some(Command::Run {
            task,
            workspace,
            keep,
            provider,
            model,
            verify,
        }) => {
            let mut config = config;
            if let Some(provider) = provider {
                config.provider = provider;
            }
            if let Some(model) = model {
                config.model = model;
            }
            if let Some(verify) = verify {
                config.verify.command = verify;
            }
            let workspace = workspace.unwrap_or_else(|| std::env::current_dir().expect("cwd"));
            let outcome = run::run_headless(&config, &workspace, &task, keep).await?;
            println!(
                "container {} removed; session log: {}",
                outcome.container,
                outcome.session_path.display()
            );
        }
        Some(Command::Resume {
            id,
            task,
            force,
            keep,
            workspace,
            provider,
            model,
            verify,
        }) => {
            let mut config = config;
            if let Some(provider) = provider {
                config.provider = provider;
            }
            if let Some(model) = model {
                config.model = model;
            }
            if let Some(verify) = verify {
                config.verify.command = verify;
            }
            let workspace = workspace.unwrap_or_else(|| std::env::current_dir().expect("cwd"));
            let outcome = run::resume_headless(
                &config,
                &workspace,
                id.as_deref(),
                task.as_deref(),
                force,
                keep,
            )
            .await?;
            println!("session log: {}", outcome.session_path.display());
        }
        Some(Command::Doctor { json, deep, fix }) => {
            let workspace = std::env::current_dir().context("cannot resolve working directory")?;
            let code = doctor::run_and_print(&config, workspace, deep, fix, json).await;
            if code != 0 {
                std::process::exit(code);
            }
        }
        Some(Command::Conformance { json, network }) => {
            let mut config = config;
            if let Some(network) = network {
                config.sandbox.network = network;
            }
            let workspace = std::env::current_dir().context("cannot resolve working directory")?;
            let code = doctor::run_and_print(&config, workspace, true, true, json).await;
            if code != 0 {
                std::process::exit(code);
            }
        }
        Some(Command::Sessions) => {
            for (path, meta) in smith_core::list_sessions()? {
                println!(
                    "{}  {}  {}:{}  {}",
                    meta.id,
                    chrono_like(meta.created_ms),
                    meta.provider,
                    meta.model,
                    path.display()
                );
            }
        }
        Some(Command::Sandbox {
            cmd:
                SandboxCmd::Exec {
                    workspace,
                    network,
                    user,
                    command,
                },
        }) => {
            let mut sandbox_config = config.sandbox_config()?;
            if let Some(network) = network {
                sandbox_config.network = config::parse_network_mode(&network)?;
            }
            let sandbox = Sandbox::new(sandbox_config);
            let workspace = workspace.unwrap_or_else(|| std::env::current_dir().expect("cwd"));
            let command = if command.is_empty() {
                "echo hello".to_string()
            } else {
                command.join(" ")
            };

            sandbox.ensure_ready().await?;
            sandbox.reap_stopped_sessions().await?;
            let session = sandbox.start_session(&workspace).await?;
            println!("session container: {}", session.name);

            let outcome = sandbox
                .exec_as(&session, user.as_deref(), &command, None)
                .await?;
            println!("--- stdout ---\n{}", outcome.stdout.trim_end());
            println!(
                "exit={} timed_out={} duration={:?}",
                outcome.exit_code, outcome.timed_out, outcome.duration
            );

            sandbox.stop_session(&session).await?;
            println!("session container removed");
        }
        Some(Command::Clean) => {
            let sandbox = Sandbox::new(config.sandbox_config()?);
            sandbox.ensure_ready().await?;
            let n = sandbox.clean_all().await?;
            println!("removed {n} container(s)");
        }
    }
    Ok(())
}

/// One-line environment summary injected into the system prompt.
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

/// Minimal ms-since-epoch formatter (avoids a chrono dependency).
fn chrono_like(ms: u64) -> String {
    let secs = ms / 1000;
    let days = secs / 86_400;
    // Days since 1970-01-01 -> (y, m, d) via civil-from-days algorithm.
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    let s_of_day = secs % 86_400;
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        s_of_day / 3600,
        (s_of_day % 3600) / 60,
        s_of_day % 60
    )
}
