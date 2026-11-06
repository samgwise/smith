//! The executor seam: every tool call crosses this boundary, so a future
//! remote executor can be swapped in without touching the agent loop.

use std::time::Duration;

use async_trait::async_trait;

/// Result of one shell command executed by the backend.
#[derive(Clone, Debug)]
pub struct ExecResult {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
    pub duration: Duration,
}

impl ExecResult {
    /// Compact rendering used as the tool result fed back to the model.
    pub fn render(&self) -> String {
        let mut out = String::new();
        if !self.stdout.trim().is_empty() {
            out.push_str(self.stdout.trim_end());
            out.push('\n');
        }
        if !self.stderr.trim().is_empty() {
            out.push_str("--- stderr ---\n");
            out.push_str(self.stderr.trim_end());
            out.push('\n');
        }
        if self.timed_out {
            out.push_str("--- command timed out and was killed ---\n");
        }
        out.push_str(&format!("[exit: {}]", self.exit_code));
        out
    }
}

/// Paged file content returned by `read_file`.
#[derive(Clone, Debug)]
pub struct FileContents {
    /// The requested page of the file, lines joined with `\n`.
    pub text: String,
    /// 1-based line number of the first returned line.
    pub start_line: u64,
    /// Total number of lines in the file.
    pub total_lines: u64,
    /// Whether lines were cut off after the returned page.
    pub truncated: bool,
}

/// Backend that tools run against: a local sandbox, or (later) a remote one.
#[async_trait]
pub trait ToolExecutor: Send + Sync {
    /// Run a shell command in the workspace. Relative to the workspace root.
    async fn exec_shell(&self, command: &str, timeout: Duration) -> anyhow::Result<ExecResult>;

    /// Read a page of a workspace-relative file. `offset` is 1-based;
    /// `None`/`1` starts at the first line. `limit` caps the number of lines
    /// returned (implementations apply their own default when absent).
    async fn read_file(
        &self,
        path: &str,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> anyhow::Result<FileContents>;

    /// Write (create or overwrite) a workspace-relative file.
    async fn write_file(&self, path: &str, contents: &str) -> anyhow::Result<()>;

    /// List a workspace-relative directory; entries are names, `/`-suffixed
    /// when they are directories.
    async fn list_dir(&self, path: &str) -> anyhow::Result<Vec<String>>;

    /// Workspace root for display (logical, not necessarily a host path).
    fn workspace_root(&self) -> &str;
}

/// Long output is truncated (keeping the tail) before it goes back to the model.
pub fn clamp_output(mut text: String, max_chars: usize) -> String {
    if text.len() > max_chars {
        let cut = text.len() - max_chars;
        // Re-slice at a char boundary.
        let boundary = (cut..text.len())
            .find(|&i| text.is_char_boundary(i))
            .unwrap_or(cut);
        text = format!(
            "--- truncated, showing last {max_chars} chars ---\n{}",
            &text[boundary..]
        );
    }
    text
}
