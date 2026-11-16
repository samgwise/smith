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
// The clearance control API port lives in one place:
// `smith_egress::client::CLEARANCE_CONTROL_PORT` (9239) — used both by the
// sidecar's publisher and the host-side client (S-09).

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
    /// Enforcement-shell transport override: `machine` | `ssh` | `local`.
    /// `None` auto-detects: connection → ssh, running machine → machine,
    /// otherwise native sudo (a Linux worker's own podman).
    pub enforce_transport: Option<String>,
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
            enforce_transport: None,
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

    /// `podman ps` filter arguments matching smith session containers. The
    /// name filter is anchored and the `smith.session` label (set at
    /// `podman run`) is required, so a container named `legacy-smith-dev` is
    /// never swept (S-16).
    fn session_container_filters(exited_only: bool) -> Vec<String> {
        let mut filters = vec![
            "name=^smith-".to_string(),
            "label=smith.session=1".to_string(),
        ];
        if exited_only {
            filters.push("status=exited".to_string());
        }
        filters
    }

    /// Remove containers left behind by previous smith sessions that are no
    /// longer running. Called at session start so debris does not accumulate.
    pub async fn reap_stopped_sessions(&self) -> anyhow::Result<usize> {
        let mut ps_args: Vec<String> = vec![
            "ps".into(),
            "-a".into(),
            "--format".into(),
            "{{.Names}}".into(),
        ];
        for filter in Self::session_container_filters(true) {
            ps_args.push("--filter".into());
            ps_args.push(filter);
        }
        let out = self
            .run_podman(&ps_args.iter().map(String::as_str).collect::<Vec<_>>())
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
        let host_mount = if remote {
            // A path on the remote host: already POSIX, no translation.
            canon.display().to_string()
        } else if self.config.translate_paths {
            to_wsl_path(&canon)?
        } else {
            // Use the plain (non-UNC) Windows path; podman handles drive-letter mounts.
            dunce::simplified(&canon).display().to_string()
        };
        let mount = Self::build_mount(&host_mount)?;

        let args = self.run_args(&name, &mount, &proxy_env);
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

    /// The `-v` mount specification for the workspace. A colon in the source
    /// would change the mount's meaning, so it is refused — except the
    /// Windows drive-letter prefix, which podman expects in the source (S-34).
    fn build_mount(host_mount: &str) -> anyhow::Result<String> {
        let bytes = host_mount.as_bytes();
        let body = if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
            &host_mount[2..]
        } else {
            host_mount
        };
        anyhow::ensure!(
            !body.contains(':'),
            "refusing a workspace path containing `:`: {host_mount}"
        );
        Ok(format!("{host_mount}:/workspace"))
    }

    /// The `podman run` argument list for a session container. `proxy_env`
    /// carries the proxy-mode environment pairs (empty otherwise).
    fn run_args(&self, name: &str, mount: &str, proxy_env: &[(String, String)]) -> Vec<String> {
        // Create the non-root `dev` user, then idle forever. In the
        // background (as root, via the allowlisted ubuntu mirrors) install
        // curl + git + CAs — the base image has none of them and the agent
        // (non-root) cannot install anything itself.
        let boot = "useradd -m -s /bin/bash dev >/dev/null 2>&1 || true; \
            (apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
             curl git ca-certificates) >/dev/null 2>&1 & \
            exec sleep infinity";
        let pids_limit = self.config.pids_limit.to_string();
        let mut args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--name".into(),
            name.into(),
            "-v".into(),
            mount.into(),
            "--memory".into(),
            self.config.memory.clone(),
            "--cpus".into(),
            self.config.cpus.clone(),
            "--pids-limit".into(),
            pids_limit,
            // Hardening (S-27): drop every capability, then give back only
            // what the root boot step needs (useradd + apt);
            // no-new-privileges stops the agent user regaining any of it.
            "--cap-drop".into(),
            "ALL".into(),
        ];
        for cap in ["CHOWN", "DAC_OVERRIDE", "FOWNER", "SETGID", "SETUID"] {
            args.push("--cap-add".into());
            args.push(cap.into());
        }
        args.extend([
            "--security-opt".into(),
            "no-new-privileges".into(),
            // The session label backs the anchored sweep filters (S-16).
            "--label".into(),
            "smith.session=1".into(),
        ]);
        match self.config.network {
            NetworkMode::Proxy => {
                args.push("--network".into());
                args.push(EGRESS_NETWORK.into());
                for (k, v) in proxy_env {
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
        args
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
        // The clearance token must not appear in the process argument list
        // (S-30): the environment rides in a private env file (0600, the
        // current user's) — `podman run --env-file` is read by the podman
        // CLIENT, so the file must be readable by whoever runs podman. It is
        // deleted once the sidecar is up.
        let env_file = std::env::temp_dir().join(format!(
            "smith-egress-env-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        smith_core::secret::write_private(
            &env_file,
            format!(
                "EGRESS_ALLOWLIST={allowlist}\n\
                 EGRESS_CLEARANCE={clearance}\n\
                 CLEARANCE_TOKEN={token}\n"
            )
            .as_bytes(),
        )
        .context("writing the sidecar env file failed")?;

        let args = self.sidecar_run_args(&env_file.display().to_string());
        let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let out = self.run_podman(&arg_refs).await?;
        // Best-effort: the env file holds the clearance token.
        let _ = std::fs::remove_file(&env_file);
        if !out.status.success() {
            anyhow::bail!(
                "failed to start the egress proxy sidecar: {}{}",
                String::from_utf8_lossy(&out.stderr),
                String::from_utf8_lossy(&out.stdout)
            );
        }
        Ok(())
    }

    /// The `podman run` argument list for the egress proxy sidecar. The
    /// clearance control API is published on host loopback only, and the
    /// environment (including the clearance token) rides a private env file
    /// read by the podman client — never the argv (S-30).
    fn sidecar_run_args(&self, env_file: &str) -> Vec<String> {
        use smith_egress::client::CLEARANCE_CONTROL_PORT;
        vec![
            "run".into(),
            "-d".into(),
            "--name".into(),
            EGRESS_SIDECAR.into(),
            "--network".into(),
            EGRESS_NETWORK.into(),
            "--ip".into(),
            enforce::EGRESS_PROXY_IP.into(),
            "-p".into(),
            // The clearance control port, published on host loopback: the
            // host client dials the same port the sidecar listens on (S-09).
            // Defined in one place: smith_egress.
            format!("127.0.0.1:{CLEARANCE_CONTROL_PORT}:{CLEARANCE_CONTROL_PORT}"),
            "--env-file".into(),
            env_file.into(),
            EGRESS_IMAGE.into(),
        ]
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

    /// The `podman exec` argument list shared by the buffered and streaming
    /// paths.
    fn exec_args(
        &self,
        session: &Session,
        user: Option<&str>,
        command: &str,
        timeout_secs: u64,
    ) -> Vec<String> {
        let secs = timeout_secs.max(1).to_string();
        let mut args: Vec<String> = vec![];
        if let Some(connection) = &self.config.podman_connection {
            args.push("--connection".into());
            args.push(connection.clone());
        }
        args.extend([
            "exec".into(),
            // -i: keep stdin attached — required for exec_stdin_as.
            "-i".into(),
            "--user".into(),
            user.unwrap_or("dev").into(),
            "--workdir".into(),
            session.workspace_container.into(),
            session.name.clone(),
            "timeout".into(),
            secs,
            "sh".into(),
            "-c".into(),
            command.into(),
        ]);
        args
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

        let args = self.exec_args(session, user, command, timeout_dur.as_secs());

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

        // Drain each stream through the capped reader: a noisy command must
        // not grow host or worker memory without limit (S-13, D-6).
        let stdout_pipe = child.stdout.take().context("exec stdout pipe")?;
        let stderr_pipe = child.stderr.take().context("exec stderr pipe")?;
        let out_task = tokio::spawn(read_capped(stdout_pipe, MAX_STREAM_BYTES, None));
        let err_task = tokio::spawn(read_capped(stderr_pipe, MAX_STREAM_BYTES, None));

        let outcome = match timeout(outer, child.wait()).await {
            Ok(Ok(status)) => {
                let code = status.code().unwrap_or(-1);
                // 124 = in-container `timeout` fired; 137 = SIGKILL.
                let timed_out = code == 124 || code == 137;
                Ok((code, timed_out))
            }
            Ok(Err(e)) => Err(anyhow::anyhow!(
                "failed while waiting for `podman exec`: {e}"
            )),
            // Outer wall-clock guard fired: dropping the child kills it
            // (kill_on_drop) and the readers reach EOF on their own.
            Err(_) => {
                drop(child);
                Ok((-1, true))
            }
        };
        let (exit_code, timed_out) = outcome?;
        let (stdout, _) = out_task.await.context("exec stdout reader")?;
        let (stderr, _) = err_task.await.context("exec stderr reader")?;
        if let Some(task) = writer_task {
            let _ = task.await;
        }
        Ok(ExecOutcome {
            exit_code,
            stdout,
            stderr,
            timed_out,
            duration: started.elapsed(),
        })
    }

    /// Execute a shell command, streaming stdout/stderr deltas as they
    /// arrive (`(is_stderr, text)`) while returning the same buffered
    /// [`ExecOutcome`] as [`exec`](Self::exec) — identical in-container
    /// `timeout`, wall-clock guard and kill semantics. The worker mesh uses
    /// this so the controller sees live output.
    pub async fn exec_streaming(
        &self,
        session: &Session,
        command: &str,
        timeout_dur: Option<Duration>,
        deltas: tokio::sync::mpsc::UnboundedSender<(bool, String)>,
    ) -> anyhow::Result<ExecOutcome> {
        let timeout_dur = timeout_dur.unwrap_or(self.config.exec_timeout);
        let outer = timeout_dur + Duration::from_secs(10);
        let args = self.exec_args(session, None, command, timeout_dur.as_secs());

        let started = Instant::now();
        let mut child = Command::new(&self.podman)
            .args(&args)
            .kill_on_drop(true)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("failed to spawn `podman exec`")?;

        let stdout_pipe = child.stdout.take().context("exec stdout pipe")?;
        let stderr_pipe = child.stderr.take().context("exec stderr pipe")?;
        let out_task = tokio::spawn(read_piped(stdout_pipe, false, deltas.clone()));
        let err_task = tokio::spawn(read_piped(stderr_pipe, true, deltas));

        let (exit_code, timed_out) = match timeout(outer, child.wait()).await {
            Ok(Ok(status)) => {
                let code = status.code().unwrap_or(-1);
                // 124 = in-container `timeout` fired; 137 = SIGKILL.
                (code, code == 124 || code == 137)
            }
            Ok(Err(e)) => anyhow::bail!("failed while waiting for `podman exec`: {e}"),
            // Guard fired: dropping the child kills it (kill_on_drop); the
            // readers then reach EOF on their own.
            Err(_) => {
                drop(child);
                (-1, true)
            }
        };
        let stdout = out_task.await.context("exec stdout reader")?;
        let stderr = err_task.await.context("exec stderr reader")?;
        Ok(ExecOutcome {
            exit_code,
            stdout,
            stderr,
            timed_out,
            duration: started.elapsed(),
        })
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

    /// Run a shell command as root where the podman server lives: natively
    /// with sudo (a Linux worker's own podman), inside the podman machine
    /// (local setups), or over SSH on the remote host (connection mode).
    ///
    /// The machine/ssh transports join their arguments and run the result
    /// through a remote shell, so pipes inside `command` are protected by
    /// quoting the whole thing as one remote argument.
    async fn root_shell(&self, command: &str) -> anyhow::Result<std::process::Output> {
        let remote_shell = format!("sudo sh -c \"{command}\"");
        // Explicit override first, then auto-detect.
        match self.config.enforce_transport.as_deref() {
            Some("ssh") => {
                let connection = self
                    .config
                    .podman_connection
                    .clone()
                    .context("enforce_transport = ssh needs sandbox.podman_connection")?;
                return self.root_shell_ssh(&connection, &remote_shell).await;
            }
            Some("machine") => return self.root_shell_machine(&remote_shell).await,
            Some("local") => return Self::root_shell_local(command).await,
            Some(other) => {
                bail!("unknown enforce_transport `{other}` (expected machine | ssh | local)")
            }
            None => {}
        }
        if let Some(connection) = &self.config.podman_connection {
            return self.root_shell_ssh(connection, &remote_shell).await;
        }
        if self.machine_running().await.unwrap_or(false) {
            return self.root_shell_machine(&remote_shell).await;
        }
        Self::root_shell_local(command).await
    }

    /// Native sudo — the Linux worker transport (no podman machine).
    async fn root_shell_local(command: &str) -> anyhow::Result<std::process::Output> {
        anyhow::ensure!(
            cfg!(target_os = "linux"),
            "no podman connection/machine and not a Linux host; cannot run the enforcement shell locally"
        );
        Command::new("sudo")
            .arg("sh")
            .arg("-c")
            .arg(command)
            .output()
            .await
            .context("failed to run the local enforcement shell (sudo)")
    }

    async fn root_shell_machine(&self, remote_shell: &str) -> anyhow::Result<std::process::Output> {
        let machine = self.running_machine().await?;
        Command::new(&self.podman)
            .args(["machine", "ssh", &machine, "--", remote_shell])
            .output()
            .await
            .context("failed to run `podman machine ssh`")
    }

    async fn root_shell_ssh(
        &self,
        connection: &str,
        remote_shell: &str,
    ) -> anyhow::Result<std::process::Output> {
        let (user, host, port, identity) = self.connection_ssh_target(connection).await?;
        let mut cmd = Command::new("ssh");
        if let Some(identity) = identity {
            cmd.arg("-i").arg(&identity);
        }
        cmd.arg("-p")
            .arg(port.to_string())
            .arg(format!("{user}@{host}"))
            .arg(remote_shell);
        cmd.output()
            .await
            .context("failed to run ssh to the remote podman host")
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
        let mut ps_args: Vec<String> = vec![
            "ps".into(),
            "-a".into(),
            "--format".into(),
            "{{.Names}}".into(),
        ];
        for filter in Self::session_container_filters(false) {
            ps_args.push("--filter".into());
            ps_args.push(filter);
        }
        let out = self
            .run_podman(&ps_args.iter().map(String::as_str).collect::<Vec<_>>())
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

    /// Refuse workspaces that are the home directory (or any of its
    /// ancestors), or well-known sensitive paths.
    fn refuse_sensitive_workspace(&self, canon: &Path) -> anyhow::Result<()> {
        if let Some(home) = dirs::home_dir()
            && (canon == home || home.starts_with(canon))
        {
            bail!("refusing to mount the home directory (or its ancestor) as a workspace");
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

/// Per-stream cap on exec output (decision D-6, S-13): a stream larger than
/// this keeps only its tail, with a truncation notice. Bounds host and
/// worker memory for noisy commands.
const MAX_STREAM_BYTES: usize = 2 * 1024 * 1024;

/// Incrementally read one exec pipe, forwarding `(is_stderr, text)` deltas
/// and accumulating the full buffer for the buffered result.
async fn read_piped(
    pipe: impl tokio::io::AsyncRead + Unpin,
    is_stderr: bool,
    deltas: tokio::sync::mpsc::UnboundedSender<(bool, String)>,
) -> String {
    read_capped(pipe, MAX_STREAM_BYTES, Some((&deltas, is_stderr)))
        .await
        .0
}

/// Read one pipe to EOF, forwarding optional `(text)` deltas and keeping at
/// most `cap` bytes (the tail). Returns the text — prefixed with a
/// truncation notice when the cap was hit — and whether it was.
async fn read_capped(
    mut pipe: impl tokio::io::AsyncRead + Unpin,
    cap: usize,
    deltas: Option<(&tokio::sync::mpsc::UnboundedSender<(bool, String)>, bool)>,
) -> (String, bool) {
    use tokio::io::AsyncReadExt;
    let mut tail: Vec<u8> = vec![];
    let mut chunk = [0u8; 4096];
    let mut truncated = false;
    loop {
        match pipe.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if let Some((tx, is_stderr)) = deltas.as_ref() {
                    let _ = tx.send((
                        *is_stderr,
                        String::from_utf8_lossy(&chunk[..n]).into_owned(),
                    ));
                }
                tail.extend_from_slice(&chunk[..n]);
                if tail.len() > cap {
                    truncated = true;
                    let overflow = tail.len() - cap;
                    tail.drain(..overflow);
                }
            }
        }
    }
    let mut out = String::new();
    if truncated {
        out.push_str(&format!(
            "[smith: output truncated — more than {} MiB produced; showing the last {} MiB]\n",
            cap / (1024 * 1024),
            cap / (1024 * 1024)
        ));
    }
    out.push_str(&String::from_utf8_lossy(&tail));
    (out, truncated)
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
            smith_core::secret::write_private(&path, token.as_bytes())
                .with_context(|| format!("writing {}", path.display()))?;
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

    use anyhow::Context as _;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// Feeds `total` bytes of filler with a recognisable tail, without
    /// holding the whole stream in memory.
    struct HugeStream {
        total: u64,
    }
    impl tokio::io::AsyncRead for HugeStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            const MARKER: &[u8] = b"SMITH-TAIL-MARKER";
            let this = self.get_mut();
            if this.total == 0 {
                return Poll::Ready(Ok(()));
            }
            let room = buf.remaining().min(64 * 1024) as u64;
            if this.total <= MARKER.len() as u64 {
                let n = this.total.min(room) as usize;
                buf.put_slice(&MARKER[..n]);
                this.total -= n as u64;
            } else {
                // Filler, but never consume into the marker's reservation.
                let n = room.min(this.total - MARKER.len() as u64) as usize;
                buf.put_slice(&vec![b'a'; n]);
                this.total -= n as u64;
            }
            Poll::Ready(Ok(()))
        }
    }

    /// S-13 / D-6 red: a 200 MB stream must be capped to a 2 MiB tail with
    /// a truncation notice, not buffered in full.
    #[tokio::test]
    async fn read_piped_caps_a_huge_stream_keeping_the_tail() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let out = read_piped(HugeStream { total: 200_000_000 }, false, tx).await;
        assert!(
            out.len() < 3 * 1024 * 1024,
            "stream was not capped: {} bytes",
            out.len()
        );
        assert!(out.contains("truncated"), "truncation notice missing");
        assert!(out.contains("SMITH-TAIL-MARKER"), "tail not kept");
    }

    /// S-16 red: the sweep filters must anchor the name and require a smith
    /// label, so a container named `legacy-smith-dev` survives `smith clean`.
    #[test]
    fn session_container_filters_are_anchored_and_labelled() {
        let filters = Sandbox::session_container_filters(false);
        assert!(
            filters.iter().any(|f| f == "name=^smith-"),
            "the name filter must be anchored: {filters:?}"
        );
        assert!(
            filters
                .iter()
                .any(|f| f.starts_with("label=smith.session=")),
            "the smith.session label must be required: {filters:?}"
        );
    }

    /// S-17 red: any ancestor of the home directory is refused, not just the
    /// home directory itself (`C:\Users` exposes `samgw\.ssh`).
    #[test]
    fn workspace_refusal_refuses_ancestors_of_the_home_directory() {
        let sandbox = Sandbox::new(Default::default());
        let home = dirs::home_dir().expect("home directory");
        assert!(sandbox.refuse_sensitive_workspace(&home).is_err());
        if let Some(parent) = home.parent() {
            assert!(
                sandbox.refuse_sensitive_workspace(parent).is_err(),
                "home ancestor `{}` must be refused",
                parent.display()
            );
        }
        // A normal workspace below the home stays allowed.
        assert!(
            sandbox
                .refuse_sensitive_workspace(&home.join("projects"))
                .is_ok()
        );
    }

    /// S-27 red: session containers must drop every capability, keep only
    /// what the root boot step needs, and set no-new-privileges.
    #[test]
    fn run_args_harden_the_container() {
        let sandbox = Sandbox::new(SandboxConfig {
            network: NetworkMode::Disabled,
            ..Default::default()
        });
        let args = sandbox.run_args("smith-probe", "/host/ws", &[]);
        let has = |flag: &str, value: &str| args.windows(2).any(|w| w[0] == flag && w[1] == value);
        assert!(has("--cap-drop", "ALL"), "caps must be dropped: {args:?}");
        assert!(
            has("--security-opt", "no-new-privileges"),
            "no-new-privileges must be set: {args:?}"
        );
        // The root boot step keeps exactly the caps it needs.
        for cap in ["CHOWN", "DAC_OVERRIDE", "FOWNER", "SETGID", "SETUID"] {
            assert!(has("--cap-add", cap), "boot cap {cap} missing: {args:?}");
        }
        assert!(
            has("--label", "smith.session=1"),
            "the session label backs the sweep filters: {args:?}"
        );
    }

    /// S-30: the clearance token must never appear in the process argument
    /// list — it reaches the sidecar through a server-side env file.
    #[test]
    fn sidecar_args_never_carry_the_token() {
        let sandbox = Sandbox::new(Default::default());
        let args = sandbox.sidecar_run_args("/tmp/smith-egress-env-test");
        assert!(
            !args.iter().any(|a| a.contains("sekrit-token-xyz")),
            "no token may ride the argv: {args:?}"
        );
        assert!(
            args.windows(2).any(|w| w[0] == "--env-file"),
            "the environment must ride an env file: {args:?}"
        );
        assert!(
            !args.iter().any(|a| a.starts_with("EGRESS_")),
            "the env file replaces the -e arguments: {args:?}"
        );
    }

    /// S-09: the sidecar must publish the clearance control port as
    /// `9239:9239` — the host client dials 9239.
    #[test]
    fn sidecar_publishes_the_clearance_control_port() {
        let sandbox = Sandbox::new(Default::default());
        let args = sandbox.sidecar_run_args("/tmp/smith-egress-env-test");
        let published = args
            .windows(2)
            .find(|w| w[0] == "-p")
            .map(|w| w[1].clone())
            .expect("a published port");
        assert_eq!(
            published, "127.0.0.1:9239:9239",
            "the clearance control port must be published on 9239 (S-09)"
        );
    }

    /// S-34 red: a colon in the source changes the mount's meaning; only the
    /// Windows drive-letter prefix may contain one.
    #[test]
    fn mount_spec_refuses_colons_in_the_source() {
        assert_eq!(
            Sandbox::build_mount(r"C:\Users\samgw\proj").unwrap(),
            r"C:\Users\samgw\proj:/workspace",
            "the drive-letter colon is expected"
        );
        assert_eq!(
            Sandbox::build_mount("/home/samgw/proj").unwrap(),
            "/home/samgw/proj:/workspace"
        );
        assert!(
            Sandbox::build_mount(r"C:\weird:dir").is_err(),
            "a second colon changes the mount meaning"
        );
        assert!(Sandbox::build_mount("/home/sa:gw").is_err());
    }

    /// Live egress test: allowed host tunnels, non-allowlisted host is
    /// denied by the proxy. Requires podman; run with `cargo test --ignored`.
    #[tokio::test]
    #[ignore = "requires podman and network; run with --ignored"]
    async fn egress_proxy_allows_and_denies() -> anyhow::Result<()> {
        // The deny path must be deterministic: run the sidecar in `deny`
        // mode (in `ask` mode the connection is HELD for a decision no test
        // answers, and curl times out instead of seeing the proxy's 403).
        // ensure_sidecar recreates a mismatched sidecar with this mode.
        let sandbox = Sandbox::new(SandboxConfig {
            network: NetworkMode::Proxy,
            egress_clearance: "deny".to_string(),
            ..Default::default()
        });
        sandbox.ensure_ready().await.unwrap();
        let ws = tempfile::tempdir().unwrap();
        let session = sandbox.start_session(ws.path()).await.unwrap();

        // curl is not in the ubuntu base image; the boot script installs it
        // in the background through the proxy (archive.ubuntu.com is on the
        // default allowlist — itself a check, and unaffected by the mode).
        // Wait for that install rather than racing it for apt's lock; only
        // if it never lands, install through the proxy explicitly.
        let install = sandbox
            .exec_as(
                &session,
                Some("root"),
                "for i in $(seq 1 60); do command -v curl >/dev/null && exit 0; sleep 5; done; \
                  apt-get update -qq && apt-get install -y -qq curl",
                Some(Duration::from_secs(420)),
            )
            .await
            .unwrap();
        assert_eq!(
            install.exit_code, 0,
            "curl install through the proxy: {}{}",
            install.stdout, install.stderr
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
        Ok(())
    }

    /// Live streaming-exec test: deltas arrive while the buffered result is
    /// intact. Requires podman; run with `cargo test --ignored`.
    #[tokio::test]
    #[ignore = "requires podman; run with --ignored"]
    async fn exec_streaming_emits_deltas_and_buffers() {
        let sandbox = Sandbox::new(Default::default());
        sandbox.ensure_ready().await.unwrap();
        let ws = tempfile::tempdir().unwrap();
        let session = sandbox.start_session(ws.path()).await.unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let outcome = sandbox
            .exec_streaming(
                &session,
                "echo one; echo two; echo err >&2",
                Some(Duration::from_secs(30)),
                tx,
            )
            .await
            .unwrap();
        assert_eq!(outcome.exit_code, 0);
        assert!(outcome.stdout.contains("one") && outcome.stdout.contains("two"));
        assert!(outcome.stderr.contains("err"));

        let mut deltas = vec![];
        while let Ok(d) = rx.try_recv() {
            deltas.push(d);
        }
        assert!(deltas.iter().any(|(err, t)| !err && t.contains("one")));
        assert!(deltas.iter().any(|(err, t)| *err && t.contains("err")));

        sandbox.stop_session(&session).await.unwrap();
    }

    /// Live enforcement test: re-applying the ruleset is idempotent (S-28)
    /// and the input chain scopes the gateway to DNS (S-08). Requires a
    /// LINUX podman server with nft (the podman machine or a Linux host);
    /// run with `cargo test --ignored`.
    #[tokio::test]
    #[ignore = "requires podman and a Linux podman server (nft); run with --ignored"]
    async fn egress_enforcement_is_idempotent_and_the_gateway_is_dns_only() {
        let sandbox = Sandbox::new(SandboxConfig {
            network: NetworkMode::Proxy,
            ..Default::default()
        });
        sandbox.ensure_ready().await.unwrap();

        // First session start installs the ruleset.
        let ws1 = tempfile::tempdir().unwrap();
        let s1 = sandbox.start_session(ws1.path()).await.unwrap();
        let list = format!("nft list table inet {}", enforce::EGRESS_TABLE);
        let first = sandbox.root_shell(&list).await.unwrap();
        assert!(
            first.status.success(),
            "nft list failed: {}",
            String::from_utf8_lossy(&first.stderr)
        );
        let rule_count = |text: &str| text.matches("iifname").count();
        let first_count = rule_count(&String::from_utf8_lossy(&first.stdout));
        assert!(
            first_count >= 6,
            "expected forward+input rules: {first_count}"
        );

        // A second session start re-applies the ruleset: no rule may
        // duplicate (S-28).
        let ws2 = tempfile::tempdir().unwrap();
        let s2 = sandbox.start_session(ws2.path()).await.unwrap();
        let second = sandbox.root_shell(&list).await.unwrap();
        assert_eq!(
            rule_count(&String::from_utf8_lossy(&second.stdout)),
            first_count,
            "rules duplicated on re-apply"
        );

        // S-08: from a container, the gateway answers DNS only — a dial to
        // any other port must not connect. On a rootful bridge the nft DROP
        // hangs the connect until the timeout kills it (exit=124); on a
        // rootless runner the traffic path differs and the refusal is fast
        // (exit=1). Either way the port is unreachable — never exit=0.
        let dial = sandbox
            .exec_as(
                &s1,
                None,
                "timeout 5 bash -c 'exec 3<>/dev/tcp/10.89.100.1/9999'; echo exit=$?",
                Some(Duration::from_secs(30)),
            )
            .await
            .unwrap();
        assert!(
            dial.stdout.contains("exit=124") || dial.stdout.contains("exit=1"),
            "a gateway dial on a non-DNS port must not connect: {}",
            dial.stdout
        );

        // DNS through the gateway still works (the documented residual
        // channel).
        let dns = sandbox
            .exec_as(
                &s1,
                None,
                "getent hosts registry.npmjs.org",
                Some(Duration::from_secs(30)),
            )
            .await
            .unwrap();
        assert_eq!(dns.exit_code, 0, "DNS must still resolve: {}", dns.stderr);

        sandbox.stop_session(&s1).await.unwrap();
        sandbox.stop_session(&s2).await.unwrap();
        sandbox.stop_egress_sidecar().await.unwrap();
    }
}
