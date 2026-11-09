//! The podman-backed `ToolExecutor`: file IO host-side inside the workspace
//! (local mode) or over the exec channel (remote mode), shell always inside
//! the session container. This is the single execution seam the agent loop
//! uses; the serve daemon reuses it unchanged.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use async_trait::async_trait;
use smith_core::{ExecResult, FileContents, ToolExecutor};

use crate::{Sandbox, Session};

/// Files larger than this are refused by `read_file` (pathological sizes;
/// normal large files are paged instead).
const MAX_READ_BYTES: u64 = 16 * 1024 * 1024;

/// Single-quote a path for safe embedding in a shell command.
fn shell_quote(path: &str) -> String {
    format!("'{}'", path.replace('\'', "'\\''"))
}
/// Lines returned per page when `limit` is not given.
const DEFAULT_PAGE_LINES: u64 = 2000;

pub struct LocalExecutor {
    sandbox: Arc<Sandbox>,
    session: Session,
    /// Canonicalised host workspace root.
    root: PathBuf,
    /// Remote mode: file ops go through exec pipes instead of the local
    /// filesystem (the workspace lives on the remote host).
    remote: bool,
}

impl LocalExecutor {
    pub fn new(sandbox: Arc<Sandbox>, session: Session) -> Self {
        let remote = sandbox.config.podman_connection.is_some();
        let root = session.workspace_host.clone();
        Self {
            sandbox,
            session,
            root,
            remote,
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

    async fn read_file(
        &self,
        path: &str,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> anyhow::Result<FileContents> {
        use std::io::BufRead;
        if self.remote {
            return self.read_file_remote(path, offset, limit).await;
        }
        let host = self.resolve(path)?;
        let meta = std::fs::metadata(&host).with_context(|| format!("stat {path}"))?;
        if meta.is_dir() {
            bail!("{path} is a directory; use list_dir");
        }
        if meta.len() > MAX_READ_BYTES {
            bail!("{path} is larger than 16 MiB; use the shell tool (grep/sed) instead");
        }

        let file = std::fs::File::open(&host).with_context(|| format!("opening {path}"))?;
        let start = offset.unwrap_or(1).max(1);
        let page = limit.unwrap_or(DEFAULT_PAGE_LINES).max(1);

        let mut total_lines = 0u64;
        let mut page_lines: Vec<String> = vec![];
        for (idx, line) in std::io::BufReader::new(file).lines().enumerate() {
            let n = (idx + 1) as u64; // 1-based
            total_lines = n;
            if n >= start && n < start + page {
                page_lines.push(line.with_context(|| format!("reading {path} line {n}"))?);
            }
        }

        let truncated = start + page_lines.len() as u64 <= total_lines;
        Ok(FileContents {
            text: page_lines.join("\n"),
            start_line: start,
            total_lines,
            truncated,
        })
    }

    async fn write_file(&self, path: &str, contents: &str) -> anyhow::Result<()> {
        if self.remote {
            return self.write_file_remote(path, contents).await;
        }
        let host = self.resolve(path)?;
        if let Some(parent) = host.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(&host, contents).with_context(|| format!("writing {path}"))
    }

    async fn list_dir(&self, path: &str) -> anyhow::Result<Vec<String>> {
        if self.remote {
            return self.list_dir_remote(path).await;
        }
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

// ------------------------------------------------------- remote mode --

impl LocalExecutor {
    /// Container-side path for a workspace-relative request (remote mode:
    /// the local `resolve` cannot validate a path that lives on the remote
    /// host, so path safety = the container's own mount + relative-only).
    fn container_path(&self, raw: &str) -> anyhow::Result<String> {
        let rel = raw.trim();
        anyhow::ensure!(!rel.is_empty(), "empty path");
        // Accept /workspace-rooted absolute paths; reject everything else.
        let rel = rel
            .strip_prefix("/workspace")
            .map(|rest| rest.trim_start_matches(['/']))
            .unwrap_or(rel);
        anyhow::ensure!(
            !rel.split(['/', '\\']).any(|seg| seg == ".."),
            "path must not traverse outside the workspace: {raw}"
        );
        Ok(if rel.is_empty() || rel == "." {
            "/workspace".to_string()
        } else {
            format!("/workspace/{rel}")
        })
    }

    /// `cat` the file over the exec channel, then page locally. (Remote
    /// `podman cp` is server-side and cannot reach the client machine.)
    async fn read_file_remote(
        &self,
        path: &str,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> anyhow::Result<FileContents> {
        let container_path = self.container_path(path)?;
        let stat = self
            .sandbox
            .exec_as(
                &self.session,
                Some("root"),
                &format!("stat -c %s '{container_path}'"),
                None,
            )
            .await?;
        let size: u64 = stat.stdout.trim().parse().unwrap_or(u64::MAX);
        if size > MAX_READ_BYTES {
            bail!("{path} is larger than 16 MiB; use the shell tool (grep/sed) instead");
        }

        let content = self
            .sandbox
            .exec_as(
                &self.session,
                None,
                &format!("cat '{container_path}'"),
                None,
            )
            .await?;
        anyhow::ensure!(
            content.exit_code == 0,
            "reading {path} failed: {}",
            content.stderr
        );

        let start = offset.unwrap_or(1).max(1);
        let page = limit.unwrap_or(DEFAULT_PAGE_LINES).max(1);
        let total_lines = content.stdout.lines().count() as u64;
        let page_lines: Vec<&str> = content
            .stdout
            .lines()
            .skip((start - 1) as usize)
            .take(page as usize)
            .collect();
        let truncated = start + page_lines.len() as u64 <= total_lines;
        Ok(FileContents {
            text: page_lines.join("\n"),
            start_line: start,
            total_lines,
            truncated,
        })
    }

    /// Feed the content via stdin: `cat > path`, then fix ownership.
    async fn write_file_remote(&self, path: &str, contents: &str) -> anyhow::Result<()> {
        let container_path = self.container_path(path)?;
        let parent = container_path
            .rsplit_once('/')
            .map(|(p, _)| p.to_string())
            .unwrap_or_else(|| "/".to_string());
        let quoted_parent = shell_quote(&parent);
        let mkdir = self
            .sandbox
            .exec_as(
                &self.session,
                Some("root"),
                &format!("mkdir -p {quoted_parent}"),
                None,
            )
            .await?;
        anyhow::ensure!(
            mkdir.exit_code == 0,
            "creating parent dir for {path} failed: {}",
            mkdir.stderr
        );

        let write = self
            .sandbox
            .exec_stdin_as(
                &self.session,
                Some("root"),
                &format!("cat > {}", shell_quote(&container_path)),
                contents,
                None,
            )
            .await?;
        anyhow::ensure!(
            write.exit_code == 0,
            "writing {path} failed: {}",
            write.stderr
        );
        // Keep the file editable by the non-root agent user.
        let _ = self
            .sandbox
            .exec_as(
                &self.session,
                Some("root"),
                &format!("chown dev:dev {}", shell_quote(&container_path)),
                None,
            )
            .await;
        Ok(())
    }

    /// `ls -1ap` inside the container: names, `/`-suffixed directories.
    async fn list_dir_remote(&self, path: &str) -> anyhow::Result<Vec<String>> {
        let container_path = self.container_path(path)?;
        let out = self
            .sandbox
            .exec_as(
                &self.session,
                None,
                &format!("ls -1ap -- {}", shell_quote(&container_path)),
                None,
            )
            .await?;
        anyhow::ensure!(out.exit_code == 0, "ls {path} failed: {}", out.stderr);
        Ok(out
            .stdout
            .lines()
            .filter(|l| !l.is_empty() && *l != "./" && *l != "../")
            .map(str::to_string)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn executor_for(tmp: &std::path::Path) -> LocalExecutor {
        let session = Session {
            name: "test".into(),
            workspace_host: tmp.to_path_buf(),
            workspace_container: "/workspace",
        };
        LocalExecutor::new(Arc::new(Sandbox::new(Default::default())), session)
    }

    #[tokio::test]
    async fn read_file_pages_with_offset_and_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let body: String = (1..=50).map(|i| format!("line {i}\n")).collect();
        std::fs::write(tmp.path().join("fixture.txt"), body).unwrap();
        let exec = executor_for(tmp.path());
        let exec: &dyn ToolExecutor = &exec;

        // Default page: everything (file smaller than the default).
        let all = exec.read_file("fixture.txt", None, None).await.unwrap();
        assert_eq!(all.total_lines, 50);
        assert_eq!(all.start_line, 1);
        assert!(!all.truncated);
        assert!(all.text.starts_with("line 1"));
        assert!(all.text.ends_with("line 50"));

        // Offset into the middle.
        let page = exec
            .read_file("fixture.txt", Some(10), Some(3))
            .await
            .unwrap();
        assert_eq!(page.start_line, 10);
        assert_eq!(page.total_lines, 50);
        assert!(page.truncated);
        assert_eq!(page.text, "line 10\nline 11\nline 12");

        // Last page: not truncated.
        let tail = exec
            .read_file("fixture.txt", Some(48), Some(10))
            .await
            .unwrap();
        assert!(!tail.truncated);
        assert_eq!(tail.text, "line 48\nline 49\nline 50");

        // Offset past the end: empty page.
        let past = exec
            .read_file("fixture.txt", Some(999), Some(10))
            .await
            .unwrap();
        assert!(past.text.is_empty());
        assert!(!past.truncated);
    }

    #[tokio::test]
    async fn read_file_refuses_paths_outside_the_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let exec = executor_for(tmp.path());
        let exec: &dyn ToolExecutor = &exec;
        assert!(exec.read_file("../outside.txt", None, None).await.is_err());
        assert!(exec.read_file("/etc/hosts", None, None).await.is_err());
    }
}
