//! Podman-backed sandbox for agent command execution.
//!
//! Security model: the host process only ever *starts* containers and *executes*
//! commands inside them via `podman exec`. Arbitrary agent-generated code runs in
//! the container, never on the host. Secrets are never injected into the container
//! environment.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;
use tokio::time::timeout;

pub mod enforce;
pub mod executor;

/// How the container is allowed to talk to the network.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkMode {
    /// Default podman networking (containers can reach the internet).
    Open,
    /// All traffic routed through the allowlist-filtering egress proxy
    /// sidecar on the `smith-egress` network.
    Proxy,
    /// `--network=none`: no network interfaces at all.
    Disabled,
}

/// Name of the dedicated podman network and proxy sidecar.
pub const EGRESS_NETWORK: &str = "smith-egress";
pub const EGRESS_SIDECAR: &str = "smith-egress-proxy";
pub const EGRESS_IMAGE: &str = "localhost/smith-egress:0.1";
pub const PROXY_PORT: u16 = 3128;
/// Clearance control API port (published on host loopback only).
pub const CLEARANCE_PORT: u16 = 9239;

impl NetworkMode {
    fn podman_args(&self) -> &'static [&'static str] {
        match self {
            NetworkMode::Open => &[],
            NetworkMode::Disabled => &["--network", "none"],
            // Proxy mode builds its args in start_session (network + env).
            NetworkMode::Proxy => &[],
        }
    }
}

/// Sandbox configuration, resolved from the user config file.
#[derive(Clone, Debug)]
pub struct SandboxConfig {
    /// Pinned base image for session containers.
    pub image: String,
    pub network: NetworkMode,
    /// Memory limit, e.g. `"4g"`.
    pub memory: String,
    /// CPU limit, e.g. `"2"`.
    pub cpus: String,
    /// Process-count limit inside the container.
    pub pids_limit: u32,
    /// Wall-clock limit applied to each `exec`.
    pub exec_timeout: Duration,
    /// Translate Windows paths to `/mnt/<drive>/...` before mounting.
    /// Leave `false` unless direct Windows-path mounts fail on your setup.
    pub translate_paths: bool,
    /// Destination allowlist for proxy mode; empty = built-in defaults.
    pub egress_allowlist: Vec<String>,
    /// Enforce the allowlist with nftables inside the podman machine
    /// (fail closed when installation fails). Proxy mode only.
    pub egress_enforce: bool,
    /// Clearance policy for non-allowlisted destinations in proxy mode:
    /// `ask` | `deny` | `allow`.
    pub egress_clearance: String,
    /// Podman system connection name (remote execution). `None` = local.
    pub podman_connection: Option<String>,
    /// Workspace path on the remote host (required when connected).
    pub remote_workspace: Option<String>,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            image: "docker.io/library/ubuntu:24.04".to_string(),
            network: NetworkMode::Proxy,
            memory: "4g".to_string(),
            cpus: "2".to_string(),
            pids_limit: 256,
            exec_timeout: Duration::from_secs(120),
            translate_paths: false,
            egress_allowlist: vec![],
            egress_enforce: true,
            egress_clearance: "ask".to_string(),
            podman_connection: None,
            remote_workspace: None,
        }
    }
}

/// A running sandbox container bound to one agent session.
#[derive(Clone, Debug)]
pub struct Session {
    /// Container name (also used as a stable handle for `podman exec`).
    pub name: String,
    /// Canonicalised host-side workspace directory.
    pub workspace_host: PathBuf,
    /// Where the workspace is mounted inside the container.
    pub workspace_container: &'static str,
}

/// Outcome of one command executed in the container.
#[derive(Clone, Debug)]
pub struct ExecOutcome {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
    pub duration: Duration,
}

/// Entry point for controlling the podman sandbox.
pub struct Sandbox {
    pub config: SandboxConfig,
    podman: String,
}

impl Sandbox {
    pub fn new(config: SandboxConfig) -> Self {
        Self {
            config,
            podman: "podman".to_string(),
        }
    }

    /// Run the podman CLI and capture output (exposed for diagnostics).
    /// Prepends `--connection <name>` when a remote connection is set.
    pub async fn run_podman(&self, args: &[&str]) -> anyhow::Result<std::process::Output> {
        let mut full: Vec<&str> = vec![];
        if let Some(connection) = &self.config.podman_connection {
            full.push("--connection");
            full.push(connection);
        }
        full.extend_from_slice(args);
        Command::new(&self.podman)
            .args(full)
            .output()
            .await
            .context("failed to spawn the podman CLI (is it on PATH?)")
    }

    /// Make sure podman itself is usable, starting the podman machine on Windows
    /// if it exists but is stopped.
    pub async fn ensure_ready(&self) -> anyhow::Result<()> {
        let probe = Command::new(&self.podman).arg("info").output().await;
        match probe {
            Ok(out) if out.status.success() => return Ok(()),
            _ => {}
        }

        if !cfg!(windows) {
            bail!("podman is installed but `podman info` failed; check your podman setup");
        }

        // On Windows, the podman machine (WSL VM) may exist but not be running.
        // podman 5.x list reporters have no State field; LastUp reads
        // "Currently running" when the machine is up.
        let machines = self.machine_listing().await?;
        if machines.iter().any(|(_, running)| *running) {
            // Machine is running; the earlier info failure was something else.
            anyhow::bail!(
                "podman machine is running but `podman info` failed; check your podman setup"
            );
        }

        if machines.is_empty() {
            bail!("no podman machine exists; run `podman machine init` first");
        }

        tracing::info!("starting podman machine");
        let start = self.run_podman(&["machine", "start"]).await?;
        if !start.status.success() {
            anyhow::bail!(
                "`podman machine start` failed: {}",
                String::from_utf8_lossy(&start.stderr)
            );
        }

        // Give the API inside the VM a moment, then verify.
        for _ in 0..15 {
            if let Ok(out) = Command::new(&self.podman).arg("info").output().await
                && out.status.success()
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        bail!("podman machine started but `podman info` still failing")
    }

    /// Remove containers left behind by previous smith sessions that are no
    /// longer running. Called at session start so debris does not accumulate.
    pub async fn reap_stopped_sessions(&self) -> anyhow::Result<usize> {
        let out = self
            .run_podman(&[
                "ps",
                "-a",
                "--filter",
                "name=smith-",
                "--filter",
                "status=exited",
                "--format",
                "{{.Names}}",
            ])
            .await?;
        let names = String::from_utf8_lossy(&out.stdout);
        let mut count = 0;
        for name in names.lines().filter(|l| !l.trim().is_empty()) {
            let rm = self.run_podman(&["rm", "-f", name.trim()]).await?;
            if rm.status.success() {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Start a session container with the workspace bind-mounted.
    pub async fn start_session(&self, workspace: &Path) -> anyhow::Result<Session> {
        // Remote mode: the workspace path is a path on the remote host —
        // no local canonicalisation, and only sanity checks apply.
        let remote = self.config.podman_connection.is_some();
        let canon = if remote {
            let remote_path = self
                .config
                .remote_workspace
                .clone()
                .context("a remote connection needs sandbox.remote_workspace set")?;
            if !remote_path.starts_with('/') {
                bail!("remote_workspace must be an absolute path on the remote host");
            }
            PathBuf::from(remote_path)
        } else {
            let canon = dunce::canonicalize(workspace)
                .with_context(|| format!("canonicalising workspace {}", workspace.display()))?;
            self.refuse_sensitive_workspace(&canon)?;
            canon
        };

        // Ensure the image is available before creating the container.
        let exists = self
            .run_podman(&["image", "exists", &self.config.image])
            .await?;
        if !exists.status.success() {
            tracing::info!("pulling sandbox image {}", self.config.image);
            let pull = self.run_podman(&["pull", &self.config.image]).await?;
            if !pull.status.success() {
                anyhow::bail!(
                    "failed to pull image {}: {}",
                    self.config.image,
                    String::from_utf8_lossy(&pull.stderr)
                );
            }
        }

        // Proxy mode: ensure the egress network, image and sidecar exist
        // before the agent container starts.
        let mut proxy_env: Vec<(String, String)> = vec![];
        if self.config.network == NetworkMode::Proxy {
            self.ensure_egress_network().await?;
            self.ensure_sidecar().await?;
            if self.config.egress_enforce {
                self.enforce_install().await?;
            } else {
                tracing::warn!("egress enforcement is disabled; the allowlist is cooperative only");
            }
            let proxy_url = format!("http://{EGRESS_SIDECAR}:{PROXY_PORT}");
            // Both cases: curl honours either, apt honours lowercase.
            for name in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"] {
                proxy_env.push((name.into(), proxy_url.clone()));
            }
            for name in ["NO_PROXY", "no_proxy"] {
                proxy_env.push((name.into(), "localhost,127.0.0.1".into()));
            }
        }

        let name = format!("smith-{}", uuid::Uuid::new_v4().simple());
        let pids_limit = self.config.pids_limit.to_string();
        let host_mount = if remote {
            // A path on the remote host: already POSIX, no translation.
            canon.display().to_string()
        } else if self.config.translate_paths {
            to_wsl_path(&canon)?
        } else {
            // Use the plain (non-UNC) Windows path; podman handles drive-letter mounts.
            dunce::simplified(&canon).display().to_string()
        };
        let mount = format!("{host_mount}:/workspace");

        // Create the non-root `dev` user, then idle forever. In the
        // background (as root, via the allowlisted ubuntu mirrors) install
        // curl + git + CAs — the base image has none of them and the agent
        // (non-root) cannot install anything itself.
        let boot = "useradd -m -s /bin/bash dev >/dev/null 2>&1 || true; \
            (apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
             curl git ca-certificates) >/dev/null 2>&1 & \
            exec sleep infinity";
        let mut args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--name".into(),
            name.clone(),
            "-v".into(),
            mount,
            "--memory".into(),
            self.config.memory.clone(),
            "--cpus".into(),
            self.config.cpus.clone(),
            "--pids-limit".into(),
            pids_limit,
        ];
        match self.config.network {
            NetworkMode::Proxy => {
                args.push("--network".into());
                args.push(EGRESS_NETWORK.into());
                for (k, v) in &proxy_env {
                    args.push("-e".into());
                    args.push(format!("{k}={v}"));
                }
            }
            other => {
                for arg in other.podman_args() {
                    args.push((*arg).to_string());
                }
            }
        }
        args.extend_from_slice(&[
            self.config.image.clone(),
            "/bin/sh".into(),
            "-c".into(),
            boot.into(),
        ]);

        let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let out = self.run_podman(&arg_refs).await?;
        if !out.status.success() {
            anyhow::bail!(
                "failed to start sandbox container: {}{}",
                String::from_utf8_lossy(&out.stderr),
                String::from_utf8_lossy(&out.stdout)
            );
        }

        Ok(Session {
            name,
            workspace_host: canon,
            workspace_container: "/workspace",
        })
    }

    /// Create the dedicated egress network with the pinned subnet if it does
    /// not exist yet; recreate it if the subnet drifted from the pinned one.
    async fn ensure_egress_network(&self) -> anyhow::Result<()> {
        let exists = self
            .run_podman(&["network", "exists", EGRESS_NETWORK])
            .await?;
        if exists.status.success() {
            let current = self
                .run_podman(&[
                    "network",
                    "inspect",
                    "-f",
                    "{{(index .Subnets 0).Subnet}}",
                    EGRESS_NETWORK,
                ])
                .await?;
            if String::from_utf8_lossy(&current.stdout).trim() == enforce::EGRESS_SUBNET {
                return Ok(());
            }
            // Wrong subnet (e.g. created by an older smith): recreate.
            tracing::info!("egress network subnet mismatch; recreating");
            self.stop_egress_sidecar().await?;
            let _ = self
                .run_podman(&["network", "rm", "-f", EGRESS_NETWORK])
                .await?;
        }
        tracing::info!("creating podman network {EGRESS_NETWORK}");
        let out = self
            .run_podman(&[
                "network",
                "create",
                "--subnet",
                enforce::EGRESS_SUBNET,
                EGRESS_NETWORK,
            ])
            .await?;
        if !out.status.success() {
            anyhow::bail!(
                "failed to create network {EGRESS_NETWORK}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(())
    }

    /// Build the egress proxy image once, then keep the sidecar running.
    /// The sidecar survives across sessions; `smith clean` removes it.
    /// Exposed for `doctor --fix`.
    pub async fn ensure_sidecar(&self) -> anyhow::Result<()> {
        let token = write_clearance_token()?;
        let clearance = self.config.egress_clearance.clone();
        let image_exists = self.run_podman(&["image", "exists", EGRESS_IMAGE]).await?;
        if !image_exists.status.success() {
            tracing::info!("building egress proxy image (one-time, a few minutes)");
            let context = smith_egress::container_context()?;
            let out = self
                .run_podman(&[
                    "build",
                    "-t",
                    EGRESS_IMAGE,
                    "-f",
                    smith_egress::containerfile_path()?
                        .to_str()
                        .context("containerfile path")?,
                    context.to_str().context("egress build context")?,
                ])
                .await?;
            if !out.status.success() {
                anyhow::bail!(
                    "failed to build the egress proxy image: {}{}",
                    String::from_utf8_lossy(&out.stderr),
                    String::from_utf8_lossy(&out.stdout)
                );
            }
        }

        let running = self
            .run_podman(&["container", "exists", EGRESS_SIDECAR])
            .await?;
        if running.status.success() {
            let state = self
                .run_podman(&["inspect", "-f", "{{.State.Running}}", EGRESS_SIDECAR])
                .await?;
            if String::from_utf8_lossy(&state.stdout).trim() == "true" {
                // Mode changes need a sidecar restart (the env is fixed at
                // container start).
                let mode_env = self
                    .run_podman(&[
                        "inspect",
                        "-f",
                        "{{range .Config.Env}}{{println .}}{{end}}",
                        EGRESS_SIDECAR,
                    ])
                    .await?;
                let env_lines: Vec<String> = String::from_utf8_lossy(&mode_env.stdout)
                    .lines()
                    .map(str::to_string)
                    .collect();
                let current_mode = env_lines
                    .iter()
                    .find_map(|l| l.strip_prefix("EGRESS_CLEARANCE="))
                    .unwrap_or("ask")
                    .to_string();
                // Pre-clearance sidecars have no token env and no published
                // control port; they must be recreated, not reused.
                let has_token = env_lines.iter().any(|l| l.starts_with("CLEARANCE_TOKEN="));
                if current_mode == clearance && has_token {
                    return Ok(());
                }
                tracing::info!(
                    "clearance mode changed {current_mode} -> {clearance}; restarting sidecar"
                );
                let _ = self.run_podman(&["rm", "-f", EGRESS_SIDECAR]).await?;
            } else {
                let _ = self.run_podman(&["rm", "-f", EGRESS_SIDECAR]).await?;
            }
        }

        let allowlist = if self.config.egress_allowlist.is_empty() {
            smith_egress::DEFAULT_ALLOWLIST.join(",")
        } else {
            self.config.egress_allowlist.join(",")
        };
        tracing::info!("starting egress proxy sidecar");
        let out = self
            .run_podman(&[
                "run",
                "-d",
                "--name",
                EGRESS_SIDECAR,
                "--network",
                EGRESS_NETWORK,
                "--ip",
                enforce::EGRESS_PROXY_IP,
                "-p",
                &format!("127.0.0.1:{PROXY_PORT}:{CLEARANCE_PORT}"),
                "-e",
                &format!("EGRESS_ALLOWLIST={allowlist}"),
                "-e",
                &format!("EGRESS_CLEARANCE={clearance}"),
                "-e",
                &format!("CLEARANCE_TOKEN={token}"),
                EGRESS_IMAGE,
            ])
            .await?;
        if !out.status.success() {
            anyhow::bail!(
                "failed to start the egress proxy sidecar: {}{}",
                String::from_utf8_lossy(&out.stderr),
                String::from_utf8_lossy(&out.stdout)
            );
        }
        Ok(())
    }

    /// Execute a shell command inside the session container as the non-root user.
    /// Both an in-container `timeout` and a host-side wall-clock guard apply.
    pub async fn exec(
        &self,
        session: &Session,
        command: &str,
        timeout_dur: Option<Duration>,
    ) -> anyhow::Result<ExecOutcome> {
        self.exec_as(session, None, command, timeout_dur).await
    }

    /// Execute with stdin fed to the command (remote file writes use this).
    pub async fn exec_stdin_as(
        &self,
        session: &Session,
        user: Option<&str>,
        command: &str,
        stdin: &str,
        timeout_dur: Option<Duration>,
    ) -> anyhow::Result<ExecOutcome> {
        self.exec_inner(session, user, command, timeout_dur, Some(stdin.as_bytes()))
            .await
    }

    /// Execute as an explicit user (`None` = the non-root `dev` user).
    pub async fn exec_as(
        &self,
        session: &Session,
        user: Option<&str>,
        command: &str,
        timeout_dur: Option<Duration>,
    ) -> anyhow::Result<ExecOutcome> {
        self.exec_inner(session, user, command, timeout_dur, None)
            .await
    }

    async fn exec_inner(
        &self,
        session: &Session,
        user: Option<&str>,
        command: &str,
        timeout_dur: Option<Duration>,
        stdin: Option<&[u8]>,
    ) -> anyhow::Result<ExecOutcome> {
        let timeout_dur = timeout_dur.unwrap_or(self.config.exec_timeout);
        // Slightly generous outer guard: the in-container `timeout` should fire first.
        let outer = timeout_dur + Duration::from_secs(10);

        let secs = timeout_dur.as_secs().max(1).to_string();
        let mut args: Vec<&str> = vec![];
        if let Some(connection) = &self.config.podman_connection {
            args.push("--connection");
            args.push(connection);
        }
        args.extend_from_slice(&[
            "exec",
            // -i: keep stdin attached — required for exec_stdin_as.
            "-i",
            "--user",
            user.unwrap_or("dev"),
            "--workdir",
            session.workspace_container,
            &session.name,
            "timeout",
            &secs,
            "sh",
            "-c",
            command,
        ]);

        let started = Instant::now();
        let mut child = Command::new(&self.podman)
            .args(&args)
            .kill_on_drop(true)
            .stdin(if stdin.is_some() {
                std::process::Stdio::piped()
            } else {
                std::process::Stdio::null()
            })
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("failed to spawn `podman exec`")?;

        // Feed stdin from its own task: a child that never reads must not
        // deadlock the wait, and dropping the pipe on exit closes cleanly.
        let writer_task = child.stdin.take().map(|mut pipe| {
            let data = stdin.map(|d| d.to_vec());
            tokio::spawn(async move {
                if let Some(data) = data {
                    let _ = pipe.write_all(&data).await;
                    let _ = pipe.shutdown().await;
                }
            })
        });

        let outcome = match timeout(outer, child.wait_with_output()).await {
            Ok(Ok(output)) => {
                let code = output.status.code().unwrap_or(-1);
                // 124 = in-container `timeout` fired; 137 = SIGKILL.
                let timed_out = code == 124 || code == 137;
                Ok(ExecOutcome {
                    exit_code: code,
                    stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                    timed_out,
                    duration: started.elapsed(),
                })
            }
            Ok(Err(e)) => Err(anyhow::anyhow!(
                "failed while waiting for `podman exec`: {e}"
            )),
            // Outer wall-clock guard fired: the child future was dropped and
            // kill_on_drop already killed it.
            Err(_) => Ok(ExecOutcome {
                exit_code: -1,
                stdout: String::new(),
                stderr: String::new(),
                timed_out: true,
                duration: started.elapsed(),
            }),
        };
        if let Some(task) = writer_task {
            let _ = task.await;
        }
        outcome
    }

    /// Remove a session container.
    pub async fn stop_session(&self, session: &Session) -> anyhow::Result<()> {
        let out = self.run_podman(&["rm", "-f", &session.name]).await?;
        if !out.status.success() {
            tracing::warn!(
                "rm {}: {}",
                session.name,
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(())
    }

    /// Installed podman client version via the client-only `--version`
    /// (works without a running machine).
    pub async fn podman_version(&self) -> anyhow::Result<String> {
        let out = self.run_podman(&["--version"]).await?;
        let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
        Ok(text.rsplit(' ').next().unwrap_or("unknown").to_string())
    }

    /// Whether the podman server (machine) answers. `podman info` prints
    /// client details on stderr and a connection error when it is down.
    pub async fn podman_reachable(&self) -> bool {
        self.run_podman(&["info", "--format", "ok"])
            .await
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// Whether any podman machine is currently running (read-only).
    pub async fn machine_running(&self) -> anyhow::Result<bool> {
        Ok(self
            .machine_listing()
            .await?
            .iter()
            .any(|(_, running)| *running))
    }

    /// Whether the sandbox base image exists locally (read-only).
    pub async fn sandbox_image_exists(&self) -> bool {
        self.run_podman(&["image", "exists", &self.config.image])
            .await
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// `(network_exists, subnet_matches_pinned)` for the egress network.
    pub async fn egress_network_state(&self) -> anyhow::Result<(bool, bool)> {
        let exists = self
            .run_podman(&["network", "exists", EGRESS_NETWORK])
            .await?;
        if !exists.status.success() {
            return Ok((false, false));
        }
        let subnet = self
            .run_podman(&[
                "network",
                "inspect",
                "-f",
                "{{(index .Subnets 0).Subnet}}",
                EGRESS_NETWORK,
            ])
            .await?;
        Ok((
            true,
            String::from_utf8_lossy(&subnet.stdout).trim() == enforce::EGRESS_SUBNET,
        ))
    }

    /// Whether the egress sidecar container exists and is running.
    pub async fn egress_sidecar_running(&self) -> bool {
        let exists = self
            .run_podman(&["container", "exists", EGRESS_SIDECAR])
            .await
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !exists {
            return false;
        }
        self.run_podman(&["inspect", "-f", "{{.State.Running}}", EGRESS_SIDECAR])
            .await
            .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "true")
            .unwrap_or(false)
    }

    /// Whether the enforcement table is installed where the podman server
    /// lives (errors count as not installed).
    pub async fn egress_table_installed(&self) -> bool {
        self.root_shell(&format!(
            "nft list table inet {} >/dev/null 2>&1",
            enforce::EGRESS_TABLE
        ))
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
    }

    /// Remove the egress proxy sidecar and the enforcement table.
    pub async fn stop_egress_sidecar(&self) -> anyhow::Result<()> {
        let exists = self
            .run_podman(&["container", "exists", EGRESS_SIDECAR])
            .await?;
        if exists.status.success() {
            let _ = self.run_podman(&["rm", "-f", EGRESS_SIDECAR]).await?;
        }
        self.enforce_remove().await;
        Ok(())
    }

    /// `(name, running?)` for every podman machine. The list reporter has no
    /// State field in podman 5.x; LastUp reads "Currently running" when up.
    async fn machine_listing(&self) -> anyhow::Result<Vec<(String, bool)>> {
        let list = self
            .run_podman(&["machine", "list", "--format", "{{.Name}}|{{.LastUp}}"])
            .await?;
        let text = String::from_utf8_lossy(&list.stdout).into_owned();
        Ok(text
            .lines()
            .filter_map(|l| l.trim().split_once('|'))
            .map(|(name, last_up)| (name.to_string(), last_up.contains("Currently running")))
            .collect())
    }

    /// Name of the first Running podman machine, if any.
    async fn running_machine(&self) -> anyhow::Result<String> {
        self.machine_listing()
            .await?
            .into_iter()
            .find(|(_, running)| *running)
            .map(|(name, _)| name)
            .context("no running podman machine")
    }

    /// Run a shell command as root where the podman server lives: inside the
    /// podman machine (local setups) or over SSH on the remote host
    /// (connection mode).
    ///
    /// Both transports join their arguments and run the result through a
    /// remote shell, so pipes inside `command` are protected by quoting the
    /// whole thing as one remote argument.
    async fn root_shell(&self, command: &str) -> anyhow::Result<std::process::Output> {
        let remote_shell = format!("sudo sh -c \"{command}\"");
        if let Some(connection) = &self.config.podman_connection {
            let (user, host, port, identity) = self.connection_ssh_target(connection).await?;
            let mut cmd = Command::new("ssh");
            if let Some(identity) = identity {
                cmd.arg("-i").arg(&identity);
            }
            cmd.arg("-p")
                .arg(port.to_string())
                .arg(format!("{user}@{host}"))
                .arg(remote_shell);
            return cmd
                .output()
                .await
                .context("failed to run ssh to the remote podman host");
        }
        let machine = self.running_machine().await?;
        Command::new(&self.podman)
            .args(["machine", "ssh", &machine, "--", &remote_shell])
            .output()
            .await
            .context("failed to run `podman machine ssh`")
    }

    /// `(user, host, port, identity)` from a podman system connection.
    async fn connection_ssh_target(
        &self,
        connection: &str,
    ) -> anyhow::Result<(String, String, u16, Option<String>)> {
        let out = self
            .run_podman(&[
                "system",
                "connection",
                "inspect",
                "-f",
                "{{.URI}}|{{.Identity}}",
                connection,
            ])
            .await?;
        anyhow::ensure!(
            out.status.success(),
            "unknown podman connection `{connection}`: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let text = String::from_utf8_lossy(&out.stdout);
        let (uri, identity) = text
            .trim()
            .split_once('|')
            .map(|(u, i)| {
                (
                    u.to_string(),
                    Some(i.trim().to_string()).filter(|s| !s.is_empty()),
                )
            })
            .unwrap_or_else(|| (text.trim().to_string(), None));
        let (user, host, port) = enforce::ssh_target_from_uri(&uri)?;
        Ok((user, host, port, identity))
    }

    /// Install the enforcement ruleset into the machine VM. Fails closed:
    /// a failed install is a hard error for proxy-mode session start.
    async fn enforce_install(&self) -> anyhow::Result<()> {
        let bridge = self
            .run_podman(&[
                "network",
                "inspect",
                "-f",
                "{{.NetworkInterface}}",
                EGRESS_NETWORK,
            ])
            .await?;
        let bridge = String::from_utf8_lossy(&bridge.stdout).trim().to_string();
        if bridge.is_empty() {
            bail!("could not resolve the bridge interface for {EGRESS_NETWORK}");
        }
        let ruleset = enforce::ruleset(&bridge, enforce::EGRESS_PROXY_IP);
        let payload = enforce::encode_payload(&ruleset);
        let shell = format!("echo {payload} | base64 -d | nft -f /dev/stdin",);
        let out = self.root_shell(&shell).await?;
        if !out.status.success() {
            bail!(
                "failed to install egress enforcement (nft) in the podman machine: {}{}",
                String::from_utf8_lossy(&out.stderr),
                String::from_utf8_lossy(&out.stdout)
            );
        }
        // Verify the table actually exists — a silent no-op (e.g. empty
        // ruleset input) must fail closed too.
        let verify = self
            .root_shell(&format!("nft list table inet {}", enforce::EGRESS_TABLE))
            .await?;
        if !verify.status.success() {
            bail!("egress enforcement table missing after install");
        }
        tracing::info!("egress enforcement installed on bridge {bridge}");
        Ok(())
    }

    /// Best-effort removal of the enforcement table.
    async fn enforce_remove(&self) {
        let out = self.root_shell(&enforce::delete_command()).await;
        match out {
            Ok(out) if out.status.success() => {}
            Ok(out) => tracing::debug!(
                "enforcement table removal: {}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ),
            Err(e) => tracing::debug!("enforcement table removal unavailable: {e:#}"),
        }
    }

    /// Remove every container from every smith session (running or not),
    /// plus the proxy sidecar and its enforcement table.
    pub async fn clean_all(&self) -> anyhow::Result<usize> {
        let out = self
            .run_podman(&[
                "ps",
                "-a",
                "--filter",
                "name=smith-",
                "--format",
                "{{.Names}}",
            ])
            .await?;
        let names = String::from_utf8_lossy(&out.stdout);
        let mut count = 0;
        for name in names.lines().filter(|l| !l.trim().is_empty()) {
            let rm = self.run_podman(&["rm", "-f", name.trim()]).await?;
            if rm.status.success() {
                count += 1;
            }
        }
        self.stop_egress_sidecar().await?;
        Ok(count)
    }

    /// Validate a workspace path (canonicalises, refuses home/sensitive).
    /// Read-only; used by doctor and session start.
    pub async fn check_workspace(&self, workspace: &Path) -> anyhow::Result<PathBuf> {
        let canon = dunce::canonicalize(workspace)
            .with_context(|| format!("canonicalising workspace {}", workspace.display()))?;
        self.refuse_sensitive_workspace(&canon)?;
        Ok(canon)
    }

    /// Refuse workspaces that are the home directory or well-known sensitive paths.
    fn refuse_sensitive_workspace(&self, canon: &Path) -> anyhow::Result<()> {
        if let Some(home) = dirs::home_dir()
            && canon == home
        {
            bail!("refusing to mount the home directory as a workspace");
        }
        const SENSITIVE: [&str; 5] = [".ssh", ".aws", ".gnupg", ".kube", ".docker"];
        for part in canon.components() {
            if let std::path::Component::Normal(os) = part
                && let Some(name) = os.to_str()
                && SENSITIVE.contains(&name)
            {
                bail!("refusing to use a sensitive path as the workspace: {name}");
            }
        }
        Ok(())
    }
}

/// Generate (or reuse) the clearance token and write it to
/// `~/.smith/egress-token`. All local smith sessions read this to talk to
/// the sidecar's control API.
fn write_clearance_token() -> anyhow::Result<String> {
    let dir = dirs::home_dir()
        .context("cannot resolve home directory")?
        .join(".smith");
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let path = dir.join("egress-token");
    let token = match std::fs::read_to_string(&path) {
        Ok(existing) if !existing.trim().is_empty() => existing.trim().to_string(),
        _ => {
            let token = uuid::Uuid::new_v4().simple().to_string();
            std::fs::write(&path, &token).with_context(|| format!("writing {}", path.display()))?;
            token
        }
    };
    Ok(token)
}

/// Convert a Windows path to its WSL form, e.g. `C:\\a\\b` -> `/mnt/c/a/b`.
/// Non-Windows paths are returned unchanged.
pub fn to_wsl_path(path: &Path) -> anyhow::Result<String> {
    let text = path.display().to_string();
    let bytes = text.as_bytes();
    if bytes.len() >= 2 && bytes[1] == b':' && bytes[0].is_ascii_alphabetic() {
        let drive = (bytes[0] as char).to_ascii_lowercase();
        let rest = text[2..].replace('\\', "/");
        Ok(format!("/mnt/{drive}{rest}"))
    } else {
        Ok(text.replace('\\', "/"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wsl_path_translation() {
        assert_eq!(
            to_wsl_path(Path::new(r"C:\Users\samgw\projects")).unwrap(),
            "/mnt/c/Users/samgw/projects"
        );
        assert_eq!(to_wsl_path(Path::new("/usr/local")).unwrap(), "/usr/local");
    }

    #[test]
    fn network_mode_args() {
        assert_eq!(NetworkMode::Disabled.podman_args(), &["--network", "none"]);
        assert!(NetworkMode::Open.podman_args().is_empty());
    }

    /// Live egress test: allowed host tunnels, non-allowlisted host is
    /// denied by the proxy. Requires podman; run with `cargo test --ignored`.
    #[tokio::test]
    #[ignore = "requires podman and network; run with --ignored"]
    async fn egress_proxy_allows_and_denies() {
        let sandbox = Sandbox::new(SandboxConfig {
            network: NetworkMode::Proxy,
            ..Default::default()
        });
        sandbox.ensure_ready().await.unwrap();
        let ws = tempfile::tempdir().unwrap();
        let session = sandbox.start_session(ws.path()).await.unwrap();

        // curl is not in the ubuntu base image; install it through the proxy
        // (archive.ubuntu.com is on the default allowlist — itself a check).
        let install = sandbox
            .exec_as(
                &session,
                Some("root"),
                "apt-get update -qq && apt-get install -y -qq curl",
                Some(Duration::from_secs(300)),
            )
            .await
            .unwrap();
        assert_eq!(
            install.exit_code, 0,
            "apt through the proxy: {}",
            install.stdout
        );

        let allowed = sandbox
            .exec_as(
                &session,
                Some("root"),
                "curl -sSf --max-time 20 https://registry.npmjs.org -o /dev/null && echo ALLOWED",
                Some(Duration::from_secs(60)),
            )
            .await
            .unwrap();
        assert!(allowed.stdout.contains("ALLOWED"), "{}", allowed.stdout);

        let denied = sandbox
            .exec_as(
                &session,
                Some("root"),
                "curl -sSf --max-time 20 https://example.com -o /dev/null; echo exit=$?",
                Some(Duration::from_secs(60)),
            )
            .await
            .unwrap();
        // curl exit 22 = HTTP error: the proxy answered the CONNECT with 403.
        assert!(denied.stdout.contains("exit=22"), "{}", denied.stdout);

        sandbox.stop_session(&session).await.unwrap();
        sandbox.stop_egress_sidecar().await.unwrap();
    }
}
