mod attach;
mod config;
mod doctor;
mod run;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::{Parser, Subcommand};
use smith_core::{Agent, Message, SessionLogger};
use smith_sandbox::Sandbox;
use smith_tui::{LocalLink, TuiGate, approval_channel};

use crate::config::Config;

/// Interactive TUI session: sandbox up for the whole session, approval modal
/// in the UI, container left running on exit (reaped later by `smith clean`).
/// The harness runs in-process (LocalLink); `smith attach` is the remote
/// counterpart.
async fn run_tui(config: &Config, workspace: &std::path::Path) -> anyhow::Result<()> {
    let (gate_tx, gate_rx) = approval_channel();

    let (_sandbox, executor) = run::start_executor(config, workspace).await?;
    let container = executor.session().name.clone();
    let workspace_canon = executor.session().workspace_host.clone();

    let logger = SessionLogger::new(&workspace_canon, &config.provider, &config.model)?;
    println!("session: {}", logger.path.display());
    let session_label = logger.meta.id.chars().take(8).collect::<String>();
    smith_server::set_clearance_mode(config, true);

    // MCP servers: connected for the whole TUI session, tools added to the
    // registry once (cache-stable), shut down when the session ends.
    let mut mcp_session = smith_server::connect_mcp(config, &workspace_canon).await;
    let mcp_status = mcp_session.status_lines();
    let mut tools = smith_core::builtin_tools();
    tools.extend(mcp_session.take_tools());

    let provider = run::build_provider(config)?;
    let mut agent = Agent::new(
        provider,
        Arc::new(executor),
        tools,
        Arc::new(TuiGate::new(gate_tx)),
        Some(Arc::new(logger)),
    );
    agent.project_instructions =
        smith_core::instructions::load_project_instructions(&workspace_canon);
    agent.environment_notes = Some(smith_server::environment_notes(config));
    let system_prompt = agent.system_prompt().await;

    let provider_name = config.provider.clone();
    let model_name = config.model.clone();
    let container_label = container
        .trim_start_matches("smith-")
        .chars()
        .take(12)
        .collect::<String>();

    let cfg_for_factory = config.clone();
    let build_provider: smith_tui::ProviderFactory =
        Arc::new(move |provider: &str, model: &str| {
            let mut cfg = cfg_for_factory.clone();
            cfg.provider = provider.to_string();
            cfg.model = model.to_string();
            run::build_provider(&cfg)
        });

    let link = LocalLink::new(
        Arc::new(agent),
        vec![Message::system(system_prompt)],
        gate_rx,
        build_provider,
        workspace_canon.clone(),
    );

    let params = smith_tui::AppParams {
        link: Box::new(link),
        user_echo: false,
        workspace_label: workspace_canon
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| workspace.display().to_string()),
        container_label,
        session_label,
        provider_name,
        model_name,
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
    /// Network mode override for the TUI session: none | proxy | open.
    #[arg(long)]
    network: Option<String>,
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
        /// Podman system connection for remote execution.
        #[arg(long)]
        connection: Option<String>,
        /// Workspace path on the remote host.
        #[arg(long)]
        remote_workspace: Option<String>,
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
        /// Podman system connection for remote execution.
        #[arg(long)]
        connection: Option<String>,
        /// Workspace path on the remote host.
        #[arg(long)]
        remote_workspace: Option<String>,
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
        /// Podman system connection for remote execution.
        #[arg(long)]
        connection: Option<String>,
        /// Workspace path on the remote host.
        #[arg(long)]
        remote_workspace: Option<String>,
    },
    /// Run the harness daemon: hosted sessions, approvals, model keys.
    /// Thin clients attach with `smith attach`.
    Serve {
        /// Bind address override (default: [serve].bind, 127.0.0.1:9240).
        #[arg(long)]
        bind: Option<String>,
    },
    /// Attach the TUI to a session hosted by a `smith serve` daemon.
    Attach {
        /// Session id (or unique prefix); newest for this workspace if omitted.
        id: Option<String>,
        /// Daemon address, e.g. 127.0.0.1:9240 (env SMITH_SERVE_URL).
        #[arg(long)]
        url: Option<String>,
        /// Daemon token (env SMITH_SERVE_TOKEN; default: the daemon's token file).
        #[arg(long)]
        token: Option<String>,
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
    let mut config = Config::load()?;
    if let Some(network) = &cli.network {
        config.sandbox.network = network.clone();
    }
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
            connection,
            remote_workspace,
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
            if let Some(connection) = connection {
                config.sandbox.podman_connection = Some(connection);
            }
            if let Some(remote_workspace) = remote_workspace {
                config.sandbox.remote_workspace = Some(remote_workspace);
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
            connection,
            remote_workspace,
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
            if let Some(connection) = connection {
                config.sandbox.podman_connection = Some(connection);
            }
            if let Some(remote_workspace) = remote_workspace {
                config.sandbox.remote_workspace = Some(remote_workspace);
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
        Some(Command::Conformance {
            json,
            network,
            connection,
            remote_workspace,
        }) => {
            let mut config = config;
            if let Some(network) = network {
                config.sandbox.network = network;
            }
            if let Some(connection) = connection {
                config.sandbox.podman_connection = Some(connection);
            }
            if let Some(remote_workspace) = remote_workspace {
                config.sandbox.remote_workspace = Some(remote_workspace);
            }
            let workspace = std::env::current_dir().context("cannot resolve working directory")?;
            let code = doctor::run_and_print(&config, workspace, true, true, json).await;
            if code != 0 {
                std::process::exit(code);
            }
        }
        Some(Command::Serve { bind }) => {
            smith_server::serve(&config, bind).await?;
        }
        Some(Command::Attach { id, url, token }) => {
            attach::run_attach(url.as_deref(), token.as_deref(), id.as_deref()).await?;
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

/// Minimal ms-since-epoch formatter (avoids a chrono dependency).
fn chrono_like(ms: u64) -> String {
    let secs = ms / 1000;
    let days = secs / 86_400;
    // Days since 1970-01-01 -> (y, m, d) via civil-from-days algorithm.
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
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
