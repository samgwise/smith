//! The v1 local `ToolExecutor`: file IO host-side inside the workspace, shell
//! inside the session container. Both live behind the same seam so a remote
//! executor can replace this later.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use async_trait::async_trait;
use smith_core::{ExecResult, ToolExecutor};
use smith_sandbox::{Sandbox, Session};

/// Files larger than this are refused by `read_file` (use shell + grep).
const MAX_READ_BYTES: u64 = 1024 * 1024;

pub struct LocalExecutor {
    sandbox: Arc<Sandbox>,
    session: Session,
    /// Canonicalised host workspace root.
    root: PathBuf,
}

impl LocalExecutor {
    pub fn new(sandbox: Arc<Sandbox>, session: Session) -> Self {
        let root = session.workspace_host.clone();
        Self {
            sandbox,
            session,
            root,
        }
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Resolve a workspace-relative (or `/workspace`-rooted) path to a host
    /// path, refusing anything that escapes the workspace.
    fn resolve(&self, raw: &str) -> anyhow::Result<PathBuf> {
        let rel = raw.trim();
        if rel.is_empty() {
            bail!("empty path");
        }

        // Accept container-style absolute paths under the workspace mount.
        let stripped = rel
            .strip_prefix("/workspace")
            .map(|rest| rest.trim_start_matches(['/']));
        let rel = match stripped {
            Some(rest) if !rest.is_empty() => rest,
            Some(_) => ".",
            None => rel,
        };

        let candidate = Path::new(rel);
        if candidate.is_absolute() {
            bail!("paths must be relative to the workspace root (or /workspace/...): {raw}");
        }

        let joined = self.root.join(candidate);
        // For not-yet-existing files, canonicalise the parent.
        let resolved = if joined.exists() {
            dunce::canonicalize(&joined)?
        } else {
            let parent = joined.parent().unwrap_or(&self.root);
            if parent.exists() {
                dunce::canonicalize(parent)?
                    .join(candidate.file_name().unwrap_or(candidate.as_os_str()))
            } else {
                bail!("no such path: {raw}");
            }
        };

        let root_canon =
            dunce::canonicalize(&self.root).context("canonicalising workspace root")?;
        if !resolved.starts_with(&root_canon) {
            bail!("path escapes the workspace: {raw}");
        }
        Ok(resolved)
    }
}

#[async_trait]
impl ToolExecutor for LocalExecutor {
    async fn exec_shell(&self, command: &str, timeout: Duration) -> anyhow::Result<ExecResult> {
        let out = self
            .sandbox
            .exec(&self.session, command, Some(timeout))
            .await?;
        Ok(ExecResult {
            exit_code: out.exit_code,
            stdout: out.stdout,
            stderr: out.stderr,
            timed_out: out.timed_out,
            duration: out.duration,
        })
    }

    async fn read_file(&self, path: &str) -> anyhow::Result<String> {
        let host = self.resolve(path)?;
        let meta = std::fs::metadata(&host).with_context(|| format!("stat {path}"))?;
        if meta.is_dir() {
            bail!("{path} is a directory; use list_dir");
        }
        if meta.len() > MAX_READ_BYTES {
            bail!("{path} is larger than 1 MiB; use the shell tool (grep/sed) instead");
        }
        std::fs::read_to_string(&host).with_context(|| format!("reading {path}"))
    }

    async fn write_file(&self, path: &str, contents: &str) -> anyhow::Result<()> {
        let host = self.resolve(path)?;
        if let Some(parent) = host.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(&host, contents).with_context(|| format!("writing {path}"))
    }

    async fn list_dir(&self, path: &str) -> anyhow::Result<Vec<String>> {
        let host = self.resolve(path)?;
        let meta = std::fs::metadata(&host).with_context(|| format!("stat {path}"))?;
        if !meta.is_dir() {
            bail!("{path} is not a directory");
        }
        let mut entries: Vec<String> = std::fs::read_dir(&host)?
            .filter_map(|e| e.ok())
            .map(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    format!("{name}/")
                } else {
                    name
                }
            })
            .collect();
        entries.sort();
        Ok(entries)
    }

    fn workspace_root(&self) -> &str {
        self.session.workspace_container
    }
}
