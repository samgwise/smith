//! MCP server lifecycle: spawn, handshake, list tools once, expose as smith
//! tools, shut down at session end.

use std::time::Duration;

use anyhow::Context;
use rmcp::{
    RoleClient, ServiceExt,
    model::{CallToolResult, ContentBlock},
    service::RunningService,
    transport::TokioChildProcess,
};
use serde_json::Value;

use crate::{
    bridge::McpToolBridge,
    config::{ApprovalMode, McpServerConfig, expand_env, sanitise},
};

/// One configured server's connection state.
pub struct ServerSession {
    pub name: String,
    pub command: String,
    pub service: RunningService<RoleClient, ()>,
    pub tool_count: usize,
}

impl ServerSession {
    pub fn status(&self) -> String {
        format!(
            "{}  {}  connected, {} tool(s)",
            self.name, self.command, self.tool_count
        )
    }
}

/// A server that failed to start; kept for reporting, never fatal.
#[derive(Clone, Debug)]
pub struct FailedServer {
    pub name: String,
    pub command: String,
    pub error: String,
}

/// Live MCP connections for one smith session.
pub struct McpSession {
    servers: Vec<ServerSession>,
    failed: Vec<FailedServer>,
    tools: Vec<Box<dyn smith_core::Tool>>,
}

impl McpSession {
    /// Connect to every configured server, then list tools once (the tool
    /// array must stay stable for prompt caching). Server failures are
    /// recorded and skipped, never fatal.
    pub async fn connect(config: &crate::config::McpConfig) -> Self {
        let mut servers = vec![];
        let mut failed = vec![];
        let mut tools: Vec<Box<dyn smith_core::Tool>> = vec![];

        for (name, server_config) in &config.servers {
            let command_display = server_config.command.clone();
            match connect_server(name, server_config).await {
                Ok((session, server_tools)) => {
                    let tool_count = server_tools.len();
                    tools.extend(server_tools);
                    servers.push(ServerSession {
                        name: name.clone(),
                        command: command_display.clone(),
                        service: session,
                        tool_count,
                    });
                }
                Err(e) => {
                    tracing::warn!("MCP server `{name}` failed to start: {e:#}");
                    failed.push(FailedServer {
                        name: name.clone(),
                        command: command_display.clone(),
                        error: format!("{e:#}"),
                    });
                }
            }
        }

        Self {
            servers,
            failed,
            tools,
        }
    }

    /// Take all bridge tools from all connected servers.
    pub fn take_tools(&mut self) -> Vec<Box<dyn smith_core::Tool>> {
        std::mem::take(&mut self.tools)
    }

    /// Human-readable status lines for `/mcp`.
    pub fn status_lines(&self) -> Vec<String> {
        let mut lines = vec![];
        for s in &self.servers {
            lines.push(format!(
                "✓ {}  {}  {} tool(s)",
                s.name, s.command, s.tool_count
            ));
        }
        for f in &self.failed {
            lines.push(format!("✗ {}  {}  failed: {}", f.name, f.command, f.error));
        }
        lines
    }

    /// Shut every server down (consumes the session: `cancel` takes the
    /// service by value).
    pub async fn shutdown(self) {
        for session in self.servers {
            if let Err(e) = session.service.cancel().await {
                tracing::warn!("MCP server `{}` shutdown error: {e:#}", session.name);
            }
        }
    }
}

/// Resolve a configured program name to an executable path. The `which`
/// crate honours PATHEXT on Windows, where `npx` is really `npx.cmd` —
/// Rust's `Command` only appends `.exe` (S-26).
fn resolve_command(command: &str) -> anyhow::Result<String> {
    which::which(command)
        .map(|p| p.display().to_string())
        .with_context(|| format!("`{command}` not found on PATH"))
}

async fn connect_server(
    name: &str,
    server_config: &McpServerConfig,
) -> anyhow::Result<(
    RunningService<RoleClient, ()>,
    Vec<Box<dyn smith_core::Tool>>,
)> {
    // Secret hygiene: only explicitly configured env (with ${VAR} expansion)
    // plus PATH passthrough; never the full host environment.
    let expanded = expand_env(&server_config.env);
    let program = resolve_command(&server_config.command)?;
    let child = tokio::process::Command::new(&program);
    let mut command = child;
    command.args(&server_config.args);
    command.env_clear();
    for (key, value) in &expanded {
        command.env(key, value);
    }
    if let Some(path) = std::env::var_os("PATH") {
        command.env("PATH", path);
    }
    if cfg!(windows)
        && let Some(system_root) = std::env::var_os("SystemRoot")
    {
        command.env("SystemRoot", system_root);
    }

    // Every connect step is bounded: a hung server must fail session start
    // after the timeout instead of holding it forever (S-21). The spawn
    // itself is synchronous; the handshake and the tool listing can hang.
    let connect_timeout = Duration::from_secs(server_config.connect_timeout_secs.max(1));
    let transport = TokioChildProcess::new(command)
        .with_context(|| format!("spawning MCP server `{name}` ({})", server_config.command))?;
    let service = tokio::time::timeout(connect_timeout, ().serve(transport))
        .await
        .map_err(|_| anyhow::anyhow!("MCP handshake with server `{name}` timed out"))?
        .with_context(|| format!("MCP handshake with server `{name}` failed"))?;

    let peer = service.peer();
    let tools_on_server = tokio::time::timeout(connect_timeout, peer.list_all_tools())
        .await
        .map_err(|_| anyhow::anyhow!("listing tools of MCP server `{name}` timed out"))?
        .with_context(|| format!("listing tools of MCP server `{name}`"))?;

    let mut tools = vec![];
    for tool in tools_on_server {
        let smith_name = format!("mcp_{}_{}", sanitise(name), sanitise(&tool.name));
        tools.push(Box::new(McpToolBridge {
            smith_name,
            server: name.to_string(),
            tool_name: tool.name.to_string(),
            description: tool
                .description
                .as_ref()
                .map(|d| d.to_string())
                .unwrap_or_default(),
            input_schema: serde_json::to_value(tool.input_schema.as_ref())
                .unwrap_or_else(|_| Value::Object(Default::default())),
            peer: peer.clone(),
            approval: server_config.approval,
            timeout: Duration::from_secs(server_config.timeout_secs.max(1)),
        }) as Box<dyn smith_core::Tool>);
    }

    Ok((service, tools))
}

/// Render a tool result the way the agent loop expects: text blocks joined,
/// structured content appended, `is_error` mapped to an error.
pub(crate) fn render_result(result: CallToolResult) -> anyhow::Result<String> {
    let mut out = String::new();
    for block in &result.content {
        match block {
            ContentBlock::Text(text) => {
                out.push_str(text.text.trim_end());
                out.push('\n');
            }
            ContentBlock::Image(image) => {
                out.push_str(&format!("[image: {} bytes, not shown]\n", image.data.len()));
            }
            ContentBlock::Audio(audio) => {
                out.push_str(&format!("[audio: {} bytes, not shown]\n", audio.data.len()));
            }
            other => out.push_str(&format!("[{} content omitted]\n", other_type(other))),
        }
    }
    if let Some(structured) = &result.structured_content {
        out.push_str("structured: ");
        out.push_str(&structured.to_string());
        out.push('\n');
    }

    if result.is_error == Some(true) {
        anyhow::bail!("{}", out.trim_end());
    }
    Ok(out.trim_end().to_string())
}

fn other_type(block: &ContentBlock) -> &'static str {
    match block {
        ContentBlock::Text(_) | ContentBlock::Image(_) | ContentBlock::Audio(_) => "content",
        _ => "resource",
    }
}

/// Approval mode lookup used by the bridge.
pub(crate) fn approved(mode: ApprovalMode) -> bool {
    mode == ApprovalMode::Allow
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::McpConfig;
    use std::collections::BTreeMap;

    #[test]
    fn resolve_finds_programs_on_path() {
        // Everything on PATH resolves (PATHEXT-aware on Windows, where
        // `npx` is `npx.cmd`); a bogus program does not (S-26).
        assert!(resolve_command("cargo").is_ok());
        assert!(resolve_command("definitely-not-a-program-xyz").is_err());
    }

    /// S-21 red: a hung server must fail the connect within the connect
    /// timeout instead of holding session start forever.
    #[tokio::test]
    async fn hung_server_fails_within_the_connect_timeout() {
        let mut servers = BTreeMap::new();
        servers.insert(
            "hung".to_string(),
            McpServerConfig {
                // Starts and never speaks MCP. Bounded so a leaked process
                // (a connect failure cannot always reap the child) dies soon.
                command: if cfg!(windows) {
                    "ping".into()
                } else {
                    "sleep".into()
                },
                args: if cfg!(windows) {
                    vec!["-n".into(), "30".into(), "127.0.0.1".into()]
                } else {
                    vec!["30".into()]
                },
                env: BTreeMap::new(),
                approval: Default::default(),
                timeout_secs: 5,
                connect_timeout_secs: 2,
            },
        );
        let config = McpConfig { servers };

        let started = std::time::Instant::now();
        let session = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            McpSession::connect(&config),
        )
        .await
        .expect("MCP connect must be bounded (S-21)");
        assert!(
            session
                .status_lines()
                .iter()
                .any(|l| l.starts_with("✗ hung")),
            "the hung server is reported as failed: {:?}",
            session.status_lines()
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(12),
            "the connect timeout (2s) must fire long before the outer bound"
        );
        session.shutdown().await;
    }
}
