//! Bridge: one MCP tool exposed as a smith [`Tool`].

use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use rmcp::{RoleClient, model::CallToolRequestParams, service::Peer};
use serde_json::json;

use crate::{
    config::ApprovalMode,
    manager::{approved, render_result},
};

pub struct McpToolBridge {
    /// Smith-side name: `mcp_<server>_<tool>`.
    pub smith_name: String,
    pub server: String,
    /// Original tool name on the server.
    pub tool_name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
    pub peer: Peer<RoleClient>,
    pub approval: ApprovalMode,
    pub timeout: Duration,
}

#[async_trait]
impl smith_core::Tool for McpToolBridge {
    fn def(&self) -> smith_core::ToolDef {
        smith_core::ToolDef {
            name: self.smith_name.clone(),
            description: format!(
                "MCP tool from server `{}`: {}",
                self.server,
                if self.description.is_empty() {
                    "(no description provided)"
                } else {
                    &self.description
                }
            ),
            parameters: self.input_schema.clone(),
        }
    }

    fn needs_approval(&self, _arguments: &str) -> bool {
        // MCP servers run on the host with full user privileges — the sandbox
        // is not a boundary here — so ask-first is the safe default.
        !approved(self.approval)
    }

    async fn run(
        &self,
        _executor: &dyn smith_core::ToolExecutor,
        arguments: &str,
    ) -> anyhow::Result<String> {
        let args: serde_json::Map<String, serde_json::Value> = serde_json::from_str(arguments)
            .map_err(|e| {
                anyhow::anyhow!("invalid MCP tool arguments ({e}); expected JSON object")
            })?;

        let mut params = CallToolRequestParams::new(self.tool_name.clone());
        params.arguments = Some(args);
        let result = tokio::time::timeout(self.timeout, self.peer.call_tool(params))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "MCP tool `{}` timed out after {}s",
                    self.tool_name,
                    self.timeout.as_secs()
                )
            })?
            .context(format!("MCP server call for `{}` failed", self.tool_name))?;

        render_result(result)
    }
}

/// Shape reference for tests; not used at runtime.
#[allow(dead_code)]
pub(crate) fn schema_placeholder() -> serde_json::Value {
    json!({ "type": "object" })
}
