//! The provider seam: one trait implemented by every LLM backend.

use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;

use crate::model::{Message, ProviderEvent, ToolDef};

/// Stream of provider events for a single chat request.
pub type ProviderStream = Pin<Box<dyn Stream<Item = anyhow::Result<ProviderEvent>> + Send>>;

/// A chat-completions backend. Implementations translate the normalised
/// message/tool types into their wire format and stream events back.
#[async_trait]
pub trait LlmProvider: Send + Sync {
    /// Model identifier, e.g. `qwen3:8b` or `gpt-4o`.
    fn model(&self) -> &str;

    /// Stream a chat completion. `history` must begin with the system message.
    async fn chat_stream(
        &self,
        history: &[Message],
        tools: &[ToolDef],
    ) -> anyhow::Result<ProviderStream>;
}
