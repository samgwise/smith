//! Smith LLM providers: Ollama (native `/api/chat`) and OpenAI (chat completions).
//!
//! Both implement [`smith_core::LlmProvider`]. Keys stay host-side; the
//! container never receives provider credentials.

pub mod ollama;
pub mod openai;
mod stream;

pub use ollama::OllamaProvider;
pub use openai::OpenAiProvider;
