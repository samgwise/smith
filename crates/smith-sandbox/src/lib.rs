//! Podman-backed sandbox for agent command execution.
//!
//! Security model: the host process only ever *starts* containers and *executes*
//! commands inside them via `podman exec`. Arbitrary agent-generated code runs in
//! the container, never on the host. Secrets are never injected into the container
//! environment.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use tokio::process::Command;
use tokio::time::timeout;

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

    async fn run_podman(&self, args: &[&str]) -> anyhow::Result<std::process::Output> {
        Command::new(&self.podman)
            .args(args)
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
        let list = self
            .run_podman(&["machine", "list", "--format", "{{.Name}} {{.State}}"])
            .await?;
        let listing = String::from_utf8_lossy(&list.stdout);
        tracing::info!("podman machine list: {}", listing.trim());

        if listing.contains("Running") {
            // Machine is running; the earlier info failure was something else.
            anyhow::bail!(
                "podman machine is running but `podman info` failed: {}",
                String::from_utf8_lossy(&list.stderr)
            );
        }

        if listing.trim().is_empty() {
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
        let canon = dunce::canonicalize(workspace)
            .with_context(|| format!("canonicalising workspace {}", workspace.display()))?;
        self.refuse_sensitive_workspace(&canon)?;

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
        let host_mount = if self.config.translate_paths {
            to_wsl_path(&canon)?
        } else {
            // Use the plain (non-UNC) Windows path; podman handles drive-letter mounts.
            dunce::simplified(&canon).display().to_string()
        };
        let mount = format!("{host_mount}:/workspace");

        // Create the non-root `dev` user on first boot, then idle forever.
        let boot = "useradd -m -s /bin/bash dev >/dev/null 2>&1 || true; exec sleep infinity";
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

    /// Create the dedicated egress network if it does not exist yet.
    async fn ensure_egress_network(&self) -> anyhow::Result<()> {
        let exists = self
            .run_podman(&["network", "exists", EGRESS_NETWORK])
            .await?;
        if !exists.status.success() {
            tracing::info!("creating podman network {EGRESS_NETWORK}");
            let out = self
                .run_podman(&["network", "create", EGRESS_NETWORK])
                .await?;
            if !out.status.success() {
                anyhow::bail!(
                    "failed to create network {EGRESS_NETWORK}: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
            }
        }
        Ok(())
    }

    /// Build the egress proxy image once, then keep the sidecar running.
    /// The sidecar survives across sessions; `smith clean` removes it.
    async fn ensure_sidecar(&self) -> anyhow::Result<()> {
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
                return Ok(());
            }
            let _ = self.run_podman(&["rm", "-f", EGRESS_SIDECAR]).await?;
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
                "-e",
                &format!("EGRESS_ALLOWLIST={allowlist}"),
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

    /// Execute as an explicit user (`None` = the non-root `dev` user).
    pub async fn exec_as(
        &self,
        session: &Session,
        user: Option<&str>,
        command: &str,
        timeout_dur: Option<Duration>,
    ) -> anyhow::Result<ExecOutcome> {
        let timeout_dur = timeout_dur.unwrap_or(self.config.exec_timeout);
        // Slightly generous outer guard: the in-container `timeout` should fire first.
        let outer = timeout_dur + Duration::from_secs(10);

        let secs = timeout_dur.as_secs().max(1).to_string();
        let args = [
            "exec",
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
        ];

        let started = Instant::now();
        let mut child = Command::new(&self.podman)
            .args(args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("failed to spawn `podman exec`")?;

        let timed_out = match timeout(outer, child.wait()).await {
            Ok(Ok(status)) => {
                let code = status.code().unwrap_or(-1);
                // 124 = in-container `timeout` fired; 137 = SIGKILL.
                let timed_out = code == 124 || code == 137;
                let stdout = drain_piped(&mut child).await;
                return Ok(ExecOutcome {
                    exit_code: code,
                    stdout,
                    stderr: String::new(),
                    timed_out,
                    duration: started.elapsed(),
                });
            }
            Ok(Err(e)) => {
                return Err(anyhow::anyhow!(
                    "failed while waiting for `podman exec`: {e}"
                ));
            }
            Err(_) => true, // outer wall-clock guard fired
        };

        let _ = child.kill().await;
        let stdout = drain_piped(&mut child).await;
        Ok(ExecOutcome {
            exit_code: -1,
            stdout,
            stderr: String::new(),
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

    /// Remove the egress proxy sidecar (kept across sessions otherwise).
    pub async fn stop_egress_sidecar(&self) -> anyhow::Result<()> {
        let exists = self
            .run_podman(&["container", "exists", EGRESS_SIDECAR])
            .await?;
        if exists.status.success() {
            let _ = self.run_podman(&["rm", "-f", EGRESS_SIDECAR]).await?;
        }
        Ok(())
    }

    /// Remove every container from every smith session (running or not).
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
        Ok(count)
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

/// Drain piped stdout of a spawned child into a string. Used after the child has
/// (almost) finished; small outputs only.
async fn drain_piped(child: &mut tokio::process::Child) -> String {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::new();
    if let Some(stdout) = child.stdout.as_mut() {
        let _ = stdout.read_to_end(&mut buf).await;
    }
    String::from_utf8_lossy(&buf).into_owned()
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
        let session = sandbox.start_session(&ws.path()).await.unwrap();

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
