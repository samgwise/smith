//! Smith MCP client: stdio MCP servers exposed as smith tools.
//!
//! Servers run host-side with only explicitly configured environment; every
//! tool call passes through the normal approval gate unless the server is
//! configured `allow`.

pub mod bridge;
pub mod config;
pub mod manager;

pub use bridge::McpToolBridge;
pub use config::{ApprovalMode, McpConfig, McpServerConfig, expand_env, sanitise};
pub use manager::{FailedServer, McpSession, ServerSession};
