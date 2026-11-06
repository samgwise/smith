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
    Enabled,
    /// `--network=none`: no network interfaces at all.
    Disabled,
}

impl NetworkMode {
    fn podman_args(&self) -> &'static [&'static str] {
        match self {
            NetworkMode::Enabled => &[],
            NetworkMode::Disabled => &["--network", "none"],
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
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            image: "docker.io/library/ubuntu:24.04".to_string(),
            network: NetworkMode::Enabled,
            memory: "4g".to_string(),
            cpus: "2".to_string(),
            pids_limit: 256,
            exec_timeout: Duration::from_secs(120),
            translate_paths: false,
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
        let mut args: Vec<&str> = vec![
            "run",
            "-d",
            "--name",
            &name,
            "-v",
            &mount,
            "--memory",
            &self.config.memory,
            "--cpus",
            &self.config.cpus,
            "--pids-limit",
            &pids_limit,
        ];
        args.extend_from_slice(self.config.network.podman_args());
        args.extend_from_slice(&[&self.config.image, "/bin/sh", "-c", boot]);

        let out = self.run_podman(&args).await?;
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

    /// Execute a shell command inside the session container as the non-root user.
    /// Both an in-container `timeout` and a host-side wall-clock guard apply.
    pub async fn exec(
        &self,
        session: &Session,
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
            "dev",
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
        assert!(NetworkMode::Enabled.podman_args().is_empty());
    }
}
