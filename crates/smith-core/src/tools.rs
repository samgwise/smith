//! Builtin tools the agent can call. Each tool is schema + behaviour; execution
//! always goes through the [`ToolExecutor`] seam.

use std::time::Duration;

use anyhow::bail;
use async_trait::async_trait;
use serde_json::json;

use crate::executor::{FileContents, ToolExecutor, clamp_output};
use crate::model::ToolDef;

/// Maximum characters of tool output fed back to the model.
const MAX_TOOL_OUTPUT: usize = 12_000;
/// Default shell timeout per command.
const SHELL_TIMEOUT: Duration = Duration::from_secs(120);

#[async_trait]
pub trait Tool: Send + Sync {
    fn def(&self) -> ToolDef;
    /// Whether the user should approve this call before it runs. File tools
    /// receive the call's workspace-relative path argument (raw; normalise
    /// it with [`normalise_workspace_path`] before consulting it); tools
    /// without a path argument (shell, MCP, orchestration) get `None`.
    fn needs_approval(&self, arguments: &str, path: Option<&str>) -> bool;
    async fn run(&self, executor: &dyn ToolExecutor, arguments: &str) -> anyhow::Result<String>;
}

/// Lexically normalise a workspace-relative path into its segments:
/// `/workspace`-rooted forms are accepted, `\` and `/` are both separators,
/// and `.`/`..` resolve away. Lexical only — the executor's `resolve` does
/// the real confinement at execution time.
pub fn normalise_workspace_path(raw: &str) -> Vec<String> {
    let unified = raw.trim().replace('\\', "/");
    let stripped = unified.strip_prefix("/workspace/").unwrap_or(&unified);
    let mut segments: Vec<String> = vec![];
    for segment in stripped.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            other => segments.push(other.to_string()),
        }
    }
    segments
}

/// Paths whose contents execute on the host (or configure tools that do):
/// git hooks, git config (`core.fsmonitor` lives there, as do credential
/// helpers), git submodule config, git attribute filters and direnv. Writes
/// here always demand approval, even though ordinary workspace writes do
/// not (S-06).
fn is_sensitive_write_path(raw: &str) -> bool {
    let segments = normalise_workspace_path(raw);
    for idx in 0..segments.len() {
        let rest = &segments[idx..];
        match rest.first().map(String::as_str) {
            Some(".git") => {
                if rest.len() >= 2 && rest[1] == "hooks" {
                    return true; // .git/hooks/<hook>
                }
                if rest.len() == 2 && rest[1] == "config" {
                    return true; // .git/config
                }
                if rest.len() >= 4 && rest[1] == "modules" && rest[rest.len() - 1] == "config" {
                    return true; // .git/modules/<name>/config
                }
            }
            Some(".gitattributes") | Some(".envrc") if rest.len() == 1 => return true,
            _ => {}
        }
    }
    false
}

/// Extract a string field from a JSON-arguments string.
fn arg_str(arguments: &str, field: &str) -> anyhow::Result<String> {
    let v: serde_json::Value = serde_json::from_str(arguments)
        .map_err(|e| anyhow::anyhow!("invalid tool arguments ({e}); expected JSON object"))?;
    match v.get(field).and_then(|s| s.as_str()) {
        Some(s) => Ok(s.to_string()),
        None => bail!("missing string argument `{field}`"),
    }
}

// ---------------------------------------------------------------- shell ----

pub struct ShellTool;

#[async_trait]
impl Tool for ShellTool {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "shell".into(),
            description: "Run a shell command inside the sandboxed workspace container and \
                return its output. Use for builds, tests, git, grep, and anything else. \
                Commands run as a non-root user with a timeout."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The shell command to run, e.g. `cargo test -q`"
                    }
                },
                "required": ["command"]
            }),
        }
    }

    fn needs_approval(&self, _arguments: &str, _path: Option<&str>) -> bool {
        true
    }

    async fn run(&self, executor: &dyn ToolExecutor, arguments: &str) -> anyhow::Result<String> {
        let command = arg_str(arguments, "command")?;
        let result = executor.exec_shell(&command, SHELL_TIMEOUT).await?;
        Ok(clamp_output(result.render(), MAX_TOOL_OUTPUT))
    }
}

// ------------------------------------------------------------ read_file ----

pub struct ReadFileTool;

#[async_trait]
impl Tool for ReadFileTool {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "read_file".into(),
            description: "Read a page of a text file from the workspace. Paths are relative \
                to the workspace root. Large files can be paged with `offset`/`limit`; the \
                result reports the file's total line count and whether more lines follow."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Relative file path" },
                    "offset": { "type": "integer", "description": "1-based line to start from (default 1)" },
                    "limit": { "type": "integer", "description": "Lines to return (default 2000)" }
                },
                "required": ["path"]
            }),
        }
    }

    fn needs_approval(&self, _arguments: &str, _path: Option<&str>) -> bool {
        false
    }

    async fn run(&self, executor: &dyn ToolExecutor, arguments: &str) -> anyhow::Result<String> {
        let parsed: serde_json::Value = serde_json::from_str(arguments)
            .map_err(|e| anyhow::anyhow!("invalid tool arguments ({e}); expected JSON object"))?;
        let path = arg_str(arguments, "path")?;
        let offset = parsed.get("offset").and_then(|v| v.as_u64());
        let limit = parsed.get("limit").and_then(|v| v.as_u64());

        let contents = executor.read_file(&path, offset, limit).await?;
        let FileContents {
            text,
            start_line,
            total_lines,
            truncated,
        } = contents;

        let mut out = String::new();
        if start_line > 1 || truncated {
            let end = start_line + text.lines().count().max(1) as u64 - 1;
            out.push_str(&format!("[lines {start_line}-{end} of {total_lines}]"));
            if truncated {
                out.push_str(" — more lines follow; continue with a larger `offset`");
            }
            out.push('\n');
        }
        out.push_str(&text);
        Ok(clamp_output(out, MAX_TOOL_OUTPUT))
    }
}

// ----------------------------------------------------------- write_file ----

pub struct WriteFileTool;

#[async_trait]
impl Tool for WriteFileTool {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "write_file".into(),
            description: "Create or overwrite a file in the workspace with the given full \
                content. Paths are relative to the workspace root."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Relative file path" },
                    "content": { "type": "string", "description": "Full file content" }
                },
                "required": ["path", "content"]
            }),
        }
    }

    /// Writes are approval-free except to paths whose contents execute on
    /// the host. An unparseable path fails closed (demands approval).
    fn needs_approval(&self, _arguments: &str, path: Option<&str>) -> bool {
        path.map(is_sensitive_write_path).unwrap_or(true)
    }

    async fn run(&self, executor: &dyn ToolExecutor, arguments: &str) -> anyhow::Result<String> {
        let path = arg_str(arguments, "path")?;
        let content = arg_str(arguments, "content")?;
        executor.write_file(&path, &content).await?;
        Ok(format!("wrote {} bytes to {path}", content.len()))
    }
}

// ------------------------------------------------------------ list_dir ----

pub struct ListDirTool;

#[async_trait]
impl Tool for ListDirTool {
    fn def(&self) -> ToolDef {
        ToolDef {
            name: "list_dir".into(),
            description: "List the entries of a directory in the workspace. Paths are \
                relative to the workspace root; use `.` for the root."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Relative directory path", "default": "." }
                },
                "required": []
            }),
        }
    }

    fn needs_approval(&self, _arguments: &str, _path: Option<&str>) -> bool {
        false
    }

    async fn run(&self, executor: &dyn ToolExecutor, arguments: &str) -> anyhow::Result<String> {
        let parsed: serde_json::Value = if arguments.trim().is_empty() {
            json!({})
        } else {
            serde_json::from_str(arguments)?
        };
        let path = parsed
            .get("path")
            .and_then(|p| p.as_str())
            .unwrap_or(".")
            .to_string();
        let entries = executor.list_dir(&path).await?;
        Ok(clamp_output(entries.join("\n"), MAX_TOOL_OUTPUT))
    }
}

/// The default v1 toolset.
pub fn builtin_tools() -> Vec<Box<dyn Tool>> {
    vec![
        Box::new(ShellTool),
        Box::new(ReadFileTool),
        Box::new(WriteFileTool),
        Box::new(ListDirTool),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// S-06: `.git/hooks/*` runs on the host, so a write there must demand
    /// approval even though ordinary workspace writes do not. The check
    /// consults the normalised path (separators, `/workspace` prefix and
    /// `..` segments resolved lexically).
    #[test]
    fn write_file_to_git_hooks_demands_approval() {
        let tool = WriteFileTool;
        assert!(
            tool.needs_approval(
                r#"{"path": ".git/hooks/post-commit", "content": "x"}"#,
                Some(".git/hooks/post-commit")
            ),
            "a git hook write must demand approval"
        );
        assert!(
            tool.needs_approval("{}", Some("/workspace/.git/config")),
            "/workspace-rooted form normalises to the same path"
        );
        assert!(
            tool.needs_approval("{}", Some("sub/../.git/config")),
            "`..` segments resolve away before the check"
        );
        assert!(
            !tool.needs_approval("{}", Some("src/lib.rs")),
            "ordinary writes stay approval-free"
        );
    }

    #[test]
    fn sensitive_write_paths() {
        assert!(is_sensitive_write_path(".git/hooks/pre-commit"));
        assert!(is_sensitive_write_path(".git/modules/sub/config"));
        assert!(is_sensitive_write_path(".gitattributes"));
        assert!(is_sensitive_write_path("nested/.envrc"));
        assert!(!is_sensitive_write_path("src/git/hooks/looking.txt"));
        assert!(!is_sensitive_write_path("README.md"));
    }
}
