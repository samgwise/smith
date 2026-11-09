//! Context compaction: replace blunt truncation with provider summarisation.
//!
//! [`SharedProvider`] is a cloneable handle to the current provider so a
//! mid-session model swap is seen by both the agent loop and the compactor.

use std::sync::{Arc, RwLock};

use anyhow::bail;
use async_trait::async_trait;
use futures::{StreamExt, pin_mut};

use crate::model::{Message, ProviderEvent};
use crate::provider::LlmProvider;

/// Cloneable, swappable handle to the active provider.
#[derive(Clone)]
pub struct SharedProvider(Arc<RwLock<Arc<dyn LlmProvider>>>);

impl SharedProvider {
    pub fn new(provider: Arc<dyn LlmProvider>) -> Self {
        Self(Arc::new(RwLock::new(provider)))
    }

    pub fn get(&self) -> Arc<dyn LlmProvider> {
        self.0.read().expect("provider lock poisoned").clone()
    }

    pub fn set(&self, provider: Arc<dyn LlmProvider>) {
        *self.0.write().expect("provider lock poisoned") = provider;
    }
}

/// Turns a block of older conversation into a dense summary.
#[async_trait]
pub trait Compactor: Send + Sync {
    async fn compact(&self, older: &[Message]) -> anyhow::Result<String>;
}

/// Summarises using the current provider (no tools; single shot).
pub struct ProviderCompactor {
    provider: SharedProvider,
}

impl ProviderCompactor {
    pub fn new(provider: SharedProvider) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl Compactor for ProviderCompactor {
    async fn compact(&self, older: &[Message]) -> anyhow::Result<String> {
        let mut history = vec![Message::system(
            "You compress coding-agent transcripts. Produce a dense summary of the \
             conversation: the task, decisions made, files changed (paths), tool results \
             that still matter, and the current state of the work. At most 300 words. \
             Output only the summary.",
        )];
        history.extend(older.iter().cloned());
        history.push(Message::user("Produce the summary now."));

        let stream = self.provider.get().chat_stream(&history, &[]).await?;
        pin_mut!(stream);
        let mut text = String::new();
        while let Some(ev) = stream.next().await {
            match ev? {
                ProviderEvent::TextDelta(delta) => text.push_str(&delta),
                ProviderEvent::Done { .. } => break,
                _ => {}
            }
        }
        let trimmed = text.trim();
        if trimmed.is_empty() {
            bail!("provider returned an empty summary");
        }
        Ok(trimmed.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{FinishReason, Role, ToolDef};
    use crate::provider::ProviderStream;

    struct StaticProvider;

    #[async_trait]
    impl LlmProvider for StaticProvider {
        fn model(&self) -> &str {
            "static"
        }

        async fn chat_stream(
            &self,
            _history: &[Message],
            _tools: &[ToolDef],
        ) -> anyhow::Result<ProviderStream> {
            let events: Vec<anyhow::Result<ProviderEvent>> = vec![
                Ok(ProviderEvent::TextDelta(
                    "task: refactor; files: a.rs, b.rs".into(),
                )),
                Ok(ProviderEvent::Done {
                    finish_reason: FinishReason::Stop,
                }),
            ];
            Ok(Box::pin(futures::stream::iter(events)))
        }
    }

    #[tokio::test]
    async fn provider_compactor_collects_summary() {
        let compactor = ProviderCompactor::new(SharedProvider::new(Arc::new(StaticProvider)));
        let older = vec![
            Message::user("do the refactor"),
            Message::assistant("done", vec![]),
        ];
        let summary = compactor.compact(&older).await.unwrap();
        assert!(summary.contains("refactor"));
    }

    struct EmptyProvider;

    #[async_trait]
    impl LlmProvider for EmptyProvider {
        fn model(&self) -> &str {
            "empty"
        }
        async fn chat_stream(
            &self,
            _history: &[Message],
            _tools: &[ToolDef],
        ) -> anyhow::Result<ProviderStream> {
            Ok(Box::pin(futures::stream::iter(vec![Ok(
                ProviderEvent::Done {
                    finish_reason: FinishReason::Stop,
                },
            )])))
        }
    }

    #[tokio::test]
    async fn empty_summary_is_an_error() {
        let compactor = ProviderCompactor::new(SharedProvider::new(Arc::new(EmptyProvider)));
        assert!(compactor.compact(&[Message::user("x")]).await.is_err());
    }

    #[test]
    fn shared_provider_swaps() {
        let shared = SharedProvider::new(Arc::new(StaticProvider));
        assert_eq!(shared.get().model(), "static");
        shared.set(Arc::new(EmptyProvider));
        assert_eq!(shared.get().model(), "empty");
        let _ = Role::User; // keep Role import used
    }
}
