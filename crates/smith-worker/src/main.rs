//! `smith-worker`: the execution plane daemon.
//!
//! Dial-out only: the worker holds the controller's command stream (SSE)
//! and POSTs results back — both connections are worker-initiated, so the
//! worker machine needs no inbound ports (NAT-safe by construction). It
//! executes executor ops against its own podman sandbox stack and
//! materialises workspaces from git; nothing durable lives here.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use smith_protocol::{
    Capability, CheckpointRequest, EnrolRequest, ExecEvent, ExecEventKind, ExecEventsRequest,
    ExecOp, HeartbeatRequest, Load, OpResult, WorkerCommand,
};

mod materialise;

/// The worker credential, minted by enrolment.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Credential {
    controller_url: String,
    worker_id: String,
    worker_token: String,
}

fn credential_path() -> anyhow::Result<PathBuf> {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .context("cannot resolve home directory")?;
    let dir = std::path::Path::new(&home).join(".smith-worker");
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    Ok(dir.join("credential.json"))
}

fn load_credential() -> anyhow::Result<Credential> {
    let path = credential_path()?;
    let raw = std::fs::read_to_string(&path).with_context(|| {
        format!(
            "reading {} (enrol first: smith-worker enrol)",
            path.display()
        )
    })?;
    serde_json::from_str(&raw).with_context(|| format!("parsing {}", path.display()))
}

fn controller_client(cred: &Credential) -> smith_protocol::ServeClient {
    let url = cred.controller_url.trim_start_matches("http://");
    let (host, port) = match url.rsplit_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse().unwrap_or(smith_protocol::DEFAULT_SERVE_PORT),
        ),
        _ => (url.to_string(), smith_protocol::DEFAULT_SERVE_PORT),
    };
    smith_protocol::ServeClient::new(&host, port, cred.worker_token.clone())
}

#[derive(Parser)]
#[command(
    name = "smith-worker",
    about = "Smith execution-plane worker: leases sessions from a smith serve controller",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Enrol with a controller using a one-time token (minted with
    /// `smith worker issue <name>`).
    Enrol {
        /// Controller address, e.g. 127.0.0.1:9240 (TLS via reverse proxy
        /// when remote; the server's certificate is the worker's trust anchor).
        #[arg(long)]
        url: String,
        /// The one-time enrolment token.
        #[arg(long)]
        token: String,
        /// This worker's name.
        #[arg(long)]
        name: String,
    },
    /// Run the worker: command stream, heartbeats, op execution.
    Run {
        /// Loopback port for the metrics endpoint.
        #[arg(long, default_value_t = 9241)]
        metrics_port: u16,
        /// Sandbox image reported in capability heartbeats (and used for
        /// sessions). Defaults to the smith sandbox default.
        #[arg(long)]
        image: Option<String>,
    },
}

/// A live leased session on this worker.
struct WorkerSession {
    sandbox: Arc<smith_sandbox::Sandbox>,
    session: smith_sandbox::Session,
    worktree: PathBuf,
    source: Option<smith_protocol::WorkspaceSource>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Cmd::Enrol { url, token, name } => {
            let url = url.trim_start_matches("http://").to_string();
            // The one-time token authenticates (bearer and body); the reply
            // holds the long-lived worker credential.
            let client =
                smith_protocol::ServeClient::new(&url_host(&url), url_port(&url), token.clone());
            let reply = client
                .worker_enrol(&EnrolRequest {
                    name: name.clone(),
                    enrolment_token: token,
                    capability: probe_capability(None),
                })
                .await?;
            let cred = Credential {
                controller_url: url,
                worker_id: reply.worker_id,
                worker_token: reply.worker_token,
            };
            let path = credential_path()?;
            std::fs::write(&path, serde_json::to_string_pretty(&cred)?)
                .with_context(|| format!("writing {}", path.display()))?;
            println!(
                "worker `{name}` enrolled (id {}); credential written to {}",
                &cred.worker_id[..8.min(cred.worker_id.len())],
                path.display()
            );
            Ok(())
        }
        Cmd::Run {
            metrics_port,
            image,
        } => {
            let cred = load_credential()?;
            let state = Arc::new(WorkerState {
                credential: Mutex::new(cred.clone()),
                sessions: Mutex::new(HashMap::new()),
                image: image.unwrap_or_else(|| "docker.io/library/ubuntu:24.04".into()),
                metrics: Arc::new(smith_protocol::metrics::Metrics::default()),
            });

            // Best-effort sandbox readiness; the machine may still be
            // starting — start_session surfaces a clear error otherwise.
            let probe = smith_sandbox::Sandbox::new(sandbox_config(&state));
            if probe.ensure_ready().await.is_ok() {
                tracing::info!("podman ready");
            } else {
                tracing::warn!("podman not ready yet; session starts will retry");
            }

            let metrics_task = tokio::spawn(metrics_server(metrics_port, state.clone()));
            let heartbeat_task = tokio::spawn(heartbeat_loop(state.clone()));
            run_command_stream(state).await?;
            heartbeat_task.abort();
            metrics_task.abort();
            Ok(())
        }
    }
}

fn url_host(url: &str) -> String {
    match url.rsplit_once(':') {
        Some((h, _)) if !h.is_empty() => h.to_string(),
        _ => url.to_string(),
    }
}

fn url_port(url: &str) -> u16 {
    url.rsplit_once(':')
        .and_then(|(_, p)| p.parse().ok())
        .unwrap_or(smith_protocol::DEFAULT_SERVE_PORT)
}

/// Worker state shared across the mesh tasks.
struct WorkerState {
    credential: Mutex<Credential>,
    /// session id → its sandbox + worktree.
    sessions: Mutex<HashMap<String, WorkerSession>>,
    image: String,
    metrics: Arc<smith_protocol::metrics::Metrics>,
}

fn sandbox_config(state: &WorkerState) -> smith_sandbox::SandboxConfig {
    smith_sandbox::SandboxConfig {
        image: state.image.clone(),
        // A Linux worker enforces natively (root_shell auto-detects).
        ..Default::default()
    }
}

/// Capability probe: cached by the caller (60s) — toolchain versions cost a
/// few process spawns.
fn probe_capability(image: Option<&str>) -> Capability {
    let version = |cmd: &str, args: &[&str]| -> Option<String> {
        let out = std::process::Command::new(cmd).args(args).output().ok()?;
        let text = String::from_utf8_lossy(&out.stdout);
        Some(text.lines().next()?.trim().to_string())
    };
    let mut toolchains = vec![];
    for (cmd, args, name) in [
        ("cargo", vec!["--version"], "cargo"),
        ("node", vec!["--version"], "node"),
        ("python3", vec!["--version"], "python3"),
        ("go", vec!["version"], "go"),
    ] {
        if let Some(v) = version(cmd, &args) {
            toolchains.push(format!("{name} {v}"));
        }
    }
    let mem_mb = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|m| {
            m.lines().next().and_then(|l| {
                l.split_whitespace()
                    .nth(1)
                    .and_then(|kb| kb.parse::<u64>().ok())
            })
        })
        .map_or(0, |kb| kb / 1024);
    Capability {
        image: image.unwrap_or("").to_string(),
        toolchains,
        cpus: std::thread::available_parallelism().map_or(0, |n| n.get() as u32),
        mem_mb,
        os: std::env::consts::OS.to_string(),
    }
}

/// The command stream: reconnect with backoff forever; commands are
/// dispatched as they arrive.
async fn run_command_stream(state: Arc<WorkerState>) -> anyhow::Result<()> {
    loop {
        let client = controller_client(&state.credential.lock().await.clone());
        match client.stream("/v1/worker/events").await {
            Ok(mut stream) => {
                tracing::info!("command stream connected");
                loop {
                    match stream.next_raw().await {
                        Ok(Some(payload)) => {
                            let cmd: WorkerCommand = match serde_json::from_str(&payload) {
                                Ok(c) => c,
                                Err(e) => {
                                    tracing::warn!("bad command from controller: {e}");
                                    continue;
                                }
                            };
                            let state = state.clone();
                            tokio::spawn(async move {
                                handle_command(state, cmd).await;
                            });
                        }
                        Ok(None) => break,
                        Err(e) => {
                            tracing::warn!("command stream error: {e:#}");
                            break;
                        }
                    }
                }
            }
            Err(e) => tracing::warn!("controller unreachable: {e:#}"),
        }
        tracing::info!("reconnecting in 2s");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn handle_command(state: Arc<WorkerState>, cmd: WorkerCommand) {
    match cmd {
        WorkerCommand::StartSession {
            session_id,
            exec_id,
            source,
        } => {
            let result = start_session(&state, &session_id, source.as_ref()).await;
            let reply = match &result {
                Ok(()) => OpResult::WriteFile,
                Err(e) => {
                    tracing::error!("start_session {session_id}: {e:#}");
                    OpResult::Failed {
                        message: format!("{e:#}"),
                    }
                }
            };
            let cred = state.credential.lock().await.clone();
            let _ = controller_client(&cred)
                .post_body(
                    "/v1/worker/op-result",
                    serde_json::to_string(&smith_protocol::OpResultRequest {
                        session_id: session_id.clone(),
                        exec_id,
                        result: reply,
                    })
                    .unwrap_or_else(|_| "{\"result\":\"failed\"}".into()),
                )
                .await;
        }
        WorkerCommand::Exec {
            session_id,
            exec_id,
            op,
        } => {
            execute_op(&state, &session_id, &exec_id, op).await;
        }
        WorkerCommand::StopSession { session_id } => {
            let mut sessions = state.sessions.lock().await;
            if let Some(worker_session) = sessions.remove(&session_id) {
                let _ = worker_session
                    .sandbox
                    .stop_session(&worker_session.session)
                    .await;
                tracing::info!("session {session_id} stopped");
            }
        }
        WorkerCommand::MergeGate {
            session_id,
            exec_id,
            integration_ref,
            gate_command,
        } => {
            let result = run_merge_gate(&state, &session_id, &integration_ref, &gate_command).await;
            let reply = match result {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("merge_gate {session_id}: {e:#}");
                    OpResult::Failed {
                        message: format!("{e:#}"),
                    }
                }
            };
            let cred = state.credential.lock().await.clone();
            let _ = controller_client(&cred)
                .post_body(
                    "/v1/worker/op-result",
                    serde_json::to_string(&smith_protocol::OpResultRequest {
                        session_id: session_id.clone(),
                        exec_id,
                        result: reply,
                    })
                    .unwrap_or_else(|_| "{\"result\":\"failed\"}".into()),
                )
                .await;
        }
        WorkerCommand::PushBranch {
            session_id,
            exec_id,
            from_ref,
            to_ref,
        } => {
            let record = state
                .sessions
                .lock()
                .await
                .get(&session_id)
                .map(|s| s.source.clone());
            let reply = match record {
                Some(source) => {
                    let result = tokio::task::spawn_blocking(move || {
                        materialise::push_branch(&source, &from_ref, &to_ref)
                    })
                    .await
                    .unwrap_or_else(|_| Err(anyhow::anyhow!("push_branch join failed")));
                    match result {
                        Ok(output) => OpResult::Done { output },
                        Err(e) => OpResult::Failed {
                            message: format!("{e:#}"),
                        },
                    }
                }
                None => OpResult::Failed {
                    message: "session not found on this worker".into(),
                },
            };
            let cred = state.credential.lock().await.clone();
            let _ = controller_client(&cred)
                .post_body(
                    "/v1/worker/op-result",
                    serde_json::to_string(&smith_protocol::OpResultRequest {
                        session_id,
                        exec_id,
                        result: reply,
                    })
                    .unwrap_or_else(|_| "{\"result\":\"failed\"}".into()),
                )
                .await;
        }
        WorkerCommand::Checkpoint { session_id } => {
            let record = state
                .sessions
                .lock()
                .await
                .get(&session_id)
                .map(|s| (s.worktree.clone(), s.source.clone()));
            let Some((worktree, source)) = record else {
                return;
            };
            let ack_session_id = session_id.clone();
            // Blocking git on the blocking pool.
            let result = tokio::task::spawn_blocking(move || {
                materialise::checkpoint(&worktree, &source, &session_id)
            })
            .await
            .unwrap_or(Err(anyhow::anyhow!("checkpoint join failed")));
            let (commit, pushed) = match &result {
                Ok((commit, pushed)) => (commit.clone(), *pushed),
                Err(e) => {
                    tracing::warn!("checkpoint failed: {e:#}");
                    ("none".into(), false)
                }
            };
            let cred = state.credential.lock().await.clone();
            let _ = controller_client(&cred)
                .post_body(
                    "/v1/worker/checkpoint",
                    serde_json::to_string(&CheckpointRequest {
                        session_id: ack_session_id,
                        commit,
                        pushed,
                    })
                    .unwrap_or_else(|_| "{\"error\":true}".into()),
                )
                .await;
        }
    }
}

async fn start_session(
    state: &Arc<WorkerState>,
    session_id: &str,
    source: Option<&smith_protocol::WorkspaceSource>,
) -> anyhow::Result<()> {
    let session_id = session_id.to_string();
    let source = source.cloned();
    let sid_for_task = session_id.clone();
    let source_for_task = source.clone();
    let worktree = tokio::task::spawn_blocking(move || {
        materialise::materialise(&sid_for_task, &source_for_task)
    })
    .await
    .context("materialisation join failed")??;

    let sandbox = Arc::new(smith_sandbox::Sandbox::new(sandbox_config(state)));
    sandbox.ensure_ready().await?;
    sandbox.reap_stopped_sessions().await?;
    let session = sandbox.start_session(&worktree).await?;
    tracing::info!(
        "session {session_id} started in {} (container {})",
        worktree.display(),
        session.name
    );
    state.sessions.lock().await.insert(
        session_id.to_string(),
        WorkerSession {
            sandbox,
            session,
            worktree,
            source,
        },
    );
    Ok(())
}

/// Merge gate (M15): git half on the blocking pool, then the optional
/// gate command in a fresh sandbox over the merged tree. The result is
/// always planner-actionable — `{ok, conflict, output}` — and nothing is
/// destructive: a failed gate leaves the integration branch untouched.
async fn run_merge_gate(
    state: &Arc<WorkerState>,
    session_id: &str,
    integration_ref: &str,
    gate_command: &str,
) -> anyhow::Result<OpResult> {
    let (_worktree, source) = state
        .sessions
        .lock()
        .await
        .get(session_id)
        .map(|s| (s.worktree.clone(), s.source.clone()))
        .context("session not found on this worker")?;

    // Git half: fetch, seed, worktree, merge --no-ff.
    let source_for_task = source.clone();
    let sid = session_id.to_string();
    let iref = integration_ref.to_string();
    let step = tokio::task::spawn_blocking(move || {
        materialise::merge_for_gate(&source_for_task, &sid, &iref)
    })
    .await
    .context("merge gate join failed")??;
    let step = match step {
        materialise::GateStep::Failed { conflict, output } => {
            return Ok(OpResult::MergeGate {
                ok: false,
                conflict,
                output,
            });
        }
        step => step,
    };
    let materialise::GateStep::Merged(gate) = step else {
        anyhow::bail!("merge gate reported no outcome");
    };
    let merge_output = gate.output.clone();

    let command = gate_command.trim().to_string();
    if command.is_empty() {
        // Merge-only gate: the merge result IS the outcome.
        let source_for_task = source.clone();
        let iref = integration_ref.to_string();
        let pushed = tokio::task::spawn_blocking(move || {
            materialise::commit_gate(&source_for_task, &gate, &iref)
        })
        .await
        .context("merge gate join failed")??;
        return Ok(OpResult::MergeGate {
            ok: true,
            conflict: false,
            output: merge_output + &push_note(pushed),
        });
    }

    // Verification gate: a fresh disposable sandbox over the merged tree
    // (the child's own container still holds its session worktree).
    let sandbox = Arc::new(smith_sandbox::Sandbox::new(sandbox_config(state)));
    sandbox.ensure_ready().await?;
    sandbox.reap_stopped_sessions().await?;
    let gate_session = sandbox.start_session(&gate.worktree).await?;
    let outcome = sandbox
        .exec(&gate_session, &command, Some(Duration::from_secs(600)))
        .await;
    let _ = sandbox.stop_session(&gate_session).await;

    let (source_for_task, iref) = (source.clone(), integration_ref.to_string());
    match outcome {
        Ok(out) if out.exit_code == 0 => {
            let pushed = tokio::task::spawn_blocking(move || {
                materialise::commit_gate(&source_for_task, &gate, &iref)
            })
            .await
            .context("merge gate join failed")??;
            Ok(OpResult::MergeGate {
                ok: true,
                conflict: false,
                output: clamp_gate_output(&format!(
                    "gate `{command}` passed (exit 0):\n{}",
                    out.stdout + &out.stderr
                )) + &push_note(pushed),
            })
        }
        Ok(out) => {
            tokio::task::spawn_blocking(move || materialise::abort_gate(&source_for_task, &gate))
                .await
                .context("merge gate join failed")??;
            Ok(OpResult::MergeGate {
                ok: false,
                conflict: false,
                output: clamp_gate_output(&format!(
                    "gate `{command}` failed (exit {}):\n{}",
                    out.exit_code,
                    out.stdout + &out.stderr
                )),
            })
        }
        Err(e) => {
            tokio::task::spawn_blocking(move || materialise::abort_gate(&source_for_task, &gate))
                .await
                .context("merge gate join failed")??;
            Ok(OpResult::MergeGate {
                ok: false,
                conflict: false,
                output: clamp_gate_output(&format!("gate `{command}` could not run: {e:#}")),
            })
        }
    }
}

fn push_note(pushed: bool) -> String {
    if pushed {
        " (integration branch pushed)".to_string()
    } else {
        " (integration branch advanced locally only)".to_string()
    }
}

/// Tool-output budget for the gate result the planner sees.
fn clamp_gate_output(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= 6000 {
        return text.to_string();
    }
    let start = chars.len() - 6000;
    format!("…{}", chars[start..].iter().collect::<String>())
}

/// Execute one op against the leased sandbox; report via op-result (+ exec
/// events for shell).
async fn execute_op(state: &Arc<WorkerState>, session_id: &str, exec_id: &str, op: ExecOp) {
    let result = run_op(state, session_id, exec_id, &op).await;
    let reply = match result {
        Ok(r) => r,
        Err(e) => OpResult::Failed {
            message: format!("{e:#}"),
        },
    };
    let cred = state.credential.lock().await.clone();
    let _ = controller_client(&cred)
        .post_body(
            "/v1/worker/op-result",
            serde_json::to_string(&smith_protocol::OpResultRequest {
                session_id: session_id.to_string(),
                exec_id: exec_id.to_string(),
                result: reply,
            })
            .unwrap_or_else(|_| "{\"result\":\"failed\"}".into()),
        )
        .await;
}

async fn run_op(
    state: &Arc<WorkerState>,
    session_id: &str,
    exec_id: &str,
    op: &ExecOp,
) -> anyhow::Result<OpResult> {
    let record = state
        .sessions
        .lock()
        .await
        .get(session_id)
        .map(|s| (s.sandbox.clone(), s.session.clone()))
        .context("session not found on this worker")?;

    match op {
        ExecOp::ExecShell {
            command,
            timeout_secs,
        } => {
            let (tx, mut rx) = mpsc::unbounded_channel();
            // Event pump: started + output deltas (telemetry); the Finished
            // event goes out after the exec with the real numbers.
            let event_state = state.clone();
            let pump_exec_id = exec_id.to_string();
            let pump_session_id = session_id.to_string();
            let seq = Arc::new(AtomicU64::new(1));
            let pump_seq = seq.clone();
            let started = std::time::Instant::now();
            let pump = tokio::spawn(async move {
                let next = |lease: &str, exec: &str, kind: ExecEventKind| ExecEvent {
                    lease_id: lease.to_string(),
                    exec_id: exec.to_string(),
                    seq: pump_seq.fetch_add(1, Ordering::Relaxed),
                    kind,
                };
                let cred = event_state.credential.lock().await.clone();
                let _ = controller_client(&cred)
                    .post_body(
                        "/v1/worker/exec-events",
                        serde_json::to_string(&ExecEventsRequest {
                            events: vec![next(
                                &pump_session_id,
                                &pump_exec_id,
                                ExecEventKind::Started,
                            )],
                        })
                        .expect("exec event json"),
                    )
                    .await;
                while let Some((is_stderr, text)) = rx.recv().await {
                    let ev = next(
                        &pump_session_id,
                        &pump_exec_id,
                        if is_stderr {
                            ExecEventKind::StderrDelta { text }
                        } else {
                            ExecEventKind::StdoutDelta { text }
                        },
                    );
                    let cred = event_state.credential.lock().await.clone();
                    let _ = controller_client(&cred)
                        .post_body(
                            "/v1/worker/exec-events",
                            serde_json::to_string(&ExecEventsRequest { events: vec![ev] })
                                .expect("exec event json"),
                        )
                        .await;
                }
            });

            let outcome = record
                .0
                .exec_streaming(
                    &record.1,
                    command,
                    Some(Duration::from_secs(*timeout_secs)),
                    tx,
                )
                .await;
            let _ = pump.await;
            let duration_ms = started.elapsed().as_millis() as u64;
            state.metrics.exec_duration_seconds.observe_ms(duration_ms);
            // Closing telemetry event with the real outcome.
            let finished = ExecEvent {
                lease_id: session_id.to_string(),
                exec_id: exec_id.to_string(),
                seq: seq.fetch_add(1, Ordering::Relaxed),
                kind: match &outcome {
                    Ok(o) => ExecEventKind::Finished {
                        exit_code: o.exit_code,
                        timed_out: o.timed_out,
                        duration_ms,
                    },
                    Err(_) => ExecEventKind::Finished {
                        exit_code: -1,
                        timed_out: false,
                        duration_ms,
                    },
                },
            };
            let cred = state.credential.lock().await.clone();
            let _ = controller_client(&cred)
                .post_body(
                    "/v1/worker/exec-events",
                    serde_json::to_string(&ExecEventsRequest {
                        events: vec![finished],
                    })
                    .unwrap_or_else(|_| "{\"events\":[]}".into()),
                )
                .await;
            let outcome = outcome?;
            Ok(OpResult::ExecShell(smith_protocol::ExecResultDto {
                exit_code: outcome.exit_code,
                stdout: outcome.stdout,
                stderr: outcome.stderr,
                timed_out: outcome.timed_out,
                duration_ms: outcome.duration.as_millis() as u64,
            }))
        }
        ExecOp::ReadFile {
            path,
            offset,
            limit,
        } => {
            let exec: &dyn smith_core::ToolExecutor = &LocalExecShim(record.0, record.1);
            Ok(OpResult::ReadFile(
                exec.read_file(path, *offset, *limit).await?,
            ))
        }
        ExecOp::WriteFile { path, contents } => {
            let exec: &dyn smith_core::ToolExecutor = &LocalExecShim(record.0, record.1);
            exec.write_file(path, contents).await?;
            Ok(OpResult::WriteFile)
        }
        ExecOp::ListDir { path } => {
            let exec: &dyn smith_core::ToolExecutor = &LocalExecShim(record.0, record.1);
            Ok(OpResult::ListDir {
                entries: exec.list_dir(path).await?,
            })
        }
    }
}

/// Local executor shim over a sandbox + session pair (file ops + the
/// `ToolExecutor` impl used by `run_op`).
struct LocalExecShim(Arc<smith_sandbox::Sandbox>, smith_sandbox::Session);

#[async_trait::async_trait]
impl smith_core::ToolExecutor for LocalExecShim {
    async fn exec_shell(
        &self,
        command: &str,
        timeout: Duration,
    ) -> anyhow::Result<smith_core::ExecResult> {
        let out = self.0.exec(&self.1, command, Some(timeout)).await?;
        Ok(smith_core::ExecResult {
            exit_code: out.exit_code,
            stdout: out.stdout,
            stderr: out.stderr,
            timed_out: out.timed_out,
            duration: out.duration,
        })
    }

    async fn read_file(
        &self,
        path: &str,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> anyhow::Result<smith_core::FileContents> {
        // Route through the shared LocalExecutor for paging/locking parity.
        let executor = smith_sandbox::executor::LocalExecutor::new(self.0.clone(), self.1.clone());
        executor.read_file(path, offset, limit).await
    }

    async fn write_file(&self, path: &str, contents: &str) -> anyhow::Result<()> {
        let executor = smith_sandbox::executor::LocalExecutor::new(self.0.clone(), self.1.clone());
        executor.write_file(path, contents).await
    }

    async fn list_dir(&self, path: &str) -> anyhow::Result<Vec<String>> {
        let executor = smith_sandbox::executor::LocalExecutor::new(self.0.clone(), self.1.clone());
        executor.list_dir(path).await
    }

    fn workspace_root(&self) -> &str {
        self.1.workspace_container
    }
}

/// Heartbeat loop: capability + load + live leases (reconciles the board).
/// Cadence is 10s — three missed beats mark the worker stale controller-side.
async fn heartbeat_loop(state: Arc<WorkerState>) {
    loop {
        let capability = probe_capability(Some(&state.image));
        let cred = state.credential.lock().await.clone();
        let live_leases: Vec<String> = state.sessions.lock().await.keys().cloned().collect();
        let req = HeartbeatRequest {
            worker_id: cred.worker_id.clone(),
            live_leases: live_leases.clone(),
            capability,
            load: Load {
                leases: live_leases.len() as u32,
                cpu_percent: 0,
            },
        };
        let client = controller_client(&cred);
        if let Err(e) = client
            .post_body(
                "/v1/worker/heartbeat",
                serde_json::to_string(&req).expect("heartbeat json"),
            )
            .await
        {
            tracing::debug!("heartbeat failed: {e:#}");
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

/// Loopback metrics endpoint (`/metrics`).
async fn metrics_server(port: u16, state: Arc<WorkerState>) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .context("binding the worker metrics port")?;
    tracing::info!("worker metrics on 127.0.0.1:{port}/metrics");
    loop {
        let (mut stream, _) = listener.accept().await?;
        let metrics = state.metrics.clone();
        tokio::spawn(async move {
            let Ok(req) = smith_protocol::http::read_request(&mut stream).await else {
                return;
            };
            let _ = req; // no auth: loopback only
            let text = metrics.render();
            let _ = smith_protocol::http::write_plain_text(&mut stream, &text).await;
        });
    }
}
