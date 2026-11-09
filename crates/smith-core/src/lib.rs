//! Smith core: the agent loop and the traits everything hangs off.
//!
//! The core is headless and event-driven: it knows nothing about the terminal
//! or podman. Front-ends and back-ends plug in via [`agent::ApprovalGate`],
//! [`provider::LlmProvider`] and [`executor::ToolExecutor`].

pub mod agent;
pub mod compaction;
pub mod executor;
pub mod instructions;
pub mod model;
pub mod provider;
pub mod session;
pub mod testing;
pub mod tools;

pub use agent::{
    Agent, AlwaysAllowGate, ApprovalDecision, ApprovalGate, ApprovalRequest, CoreEvent, Limits,
    call_detail,
};
pub use compaction::{Compactor, ProviderCompactor, SharedProvider};
pub use executor::{ExecResult, FileContents, ToolExecutor, clamp_output};
pub use model::{FinishReason, Message, ProviderEvent, Role, ToolCall, ToolDef, Usage};
pub use provider::{LlmProvider, ProviderStream};
pub use session::{
    SessionLogger, SessionMeta, SessionRecord, list_sessions, load_history, workspace_identity,
};
pub use tools::{Tool, builtin_tools};
