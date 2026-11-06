//! Smith core: the agent loop and the traits everything hangs off.
//!
//! The core is headless and event-driven: it knows nothing about the terminal
//! or podman. Front-ends and back-ends plug in via [`agent::ApprovalGate`],
//! [`provider::LlmProvider`] and [`executor::ToolExecutor`].

pub mod agent;
pub mod executor;
pub mod model;
pub mod provider;
pub mod session;
pub mod tools;

pub use agent::{
    Agent, AlwaysAllowGate, ApprovalDecision, ApprovalGate, ApprovalRequest, CoreEvent, Limits,
    call_detail,
};
pub use executor::{ExecResult, ToolExecutor, clamp_output};
pub use model::{FinishReason, Message, ProviderEvent, Role, ToolCall, ToolDef, Usage};
pub use provider::{LlmProvider, ProviderStream};
pub use session::{SessionLogger, SessionMeta, SessionRecord, list_sessions, load_history};
pub use tools::{Tool, builtin_tools};
