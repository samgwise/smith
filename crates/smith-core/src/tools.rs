//! Builtin tools the agent can call. Each tool is schema + behaviour; execution
//! always goes through the [`ToolExecutor`] seam.

use std::time::Duration;

use anyhow::bail;
use async_trait::async_trait;
use serde_json::json;

use crate::executor::{ToolExecutor, clamp_output};
use crate::model::ToolDef;

/// Maximum characters of tool output fed back to the model.
const MAX_TOOL_OUTPUT: usize = 12_000;
/// Default shell timeout per command.
const SHELL_TIMEOUT: Duration = Duration::from_secs(120);

#[async_trait]
pub trait Tool: Send + Sync {
    fn def(&self) -> ToolDef;
    /// Whether the user should approve this call before it runs.
    fn needs_approval(&self, arguments: &str) -> bool;
    async fn run(&self, executor: &dyn ToolExecutor, arguments: &str) -> anyhow::Result<String>;
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

    fn needs_approval(&self, _arguments: &str) -> bool {
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
            description: "Read a text file from the workspace. Paths are relative to the \
                workspace root."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Relative file path" }
                },
                "required": ["path"]
            }),
        }
    }

    fn needs_approval(&self, _arguments: &str) -> bool {
        false
    }

    async fn run(&self, executor: &dyn ToolExecutor, arguments: &str) -> anyhow::Result<String> {
        let path = arg_str(arguments, "path")?;
        let content = executor.read_file(&path).await?;
        Ok(clamp_output(content, MAX_TOOL_OUTPUT))
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

    fn needs_approval(&self, _arguments: &str) -> bool {
        false
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

    fn needs_approval(&self, _arguments: &str) -> bool {
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
