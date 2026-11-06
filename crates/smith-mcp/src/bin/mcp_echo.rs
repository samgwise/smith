//! Minimal MCP server used by smith's integration tests: one `echo` tool.
//! Speaks MCP over stdio via rmcp; no network, no external runtime.

use rmcp::{
    ServerHandler, ServiceExt, handler::server::wrapper::Parameters, schemars, tool, tool_handler,
    tool_router, transport::stdio,
};

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct EchoParams {
    /// Text to echo back.
    pub message: String,
}

#[derive(Clone)]
pub struct EchoServer;

#[tool_router]
impl EchoServer {
    #[tool(description = "Echo the given message back, prefixed with 'echo:'")]
    fn echo(&self, Parameters(params): Parameters<EchoParams>) -> String {
        format!("echo: {}", params.message)
    }
}

#[tool_handler]
impl ServerHandler for EchoServer {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let service = EchoServer.serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}
