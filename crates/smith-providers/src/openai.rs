//! OpenAI chat-completions client with SSE streaming and tool calls.
//!
//! The API key is held only in this struct and sent as a bearer header to the
//! configured base URL. It is never logged and never placed in a container.

use std::collections::BTreeMap;
use std::fmt;

use anyhow::{Context, bail};
use async_trait::async_trait;
use futures::{StreamExt, stream};
use serde_json::{Value, json};
use smith_core::{
    FinishReason, LlmProvider, Message, ProviderEvent, ProviderStream, Role, ToolCall, ToolDef,
    Usage,
};

use crate::stream::lines;

pub const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

#[derive(Clone)]
pub struct OpenAiProvider {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    model: String,
}

impl fmt::Debug for OpenAiProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiProvider")
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .field("api_key", &"<redacted>")
            .finish()
    }
}

impl OpenAiProvider {
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: api_key.into(),
            model: model.into(),
        }
    }
}

#[async_trait]
impl LlmProvider for OpenAiProvider {
    fn model(&self) -> &str {
        &self.model
    }

    async fn chat_stream(
        &self,
        history: &[Message],
        tools: &[ToolDef],
    ) -> anyhow::Result<ProviderStream> {
        let mut body = json!({
            "model": self.model,
            "messages": openai_messages(history),
            "stream": true,
            "stream_options": { "include_usage": true },
        });
        if !tools.is_empty() {
            body["tools"] = openai_tools(tools);
        }

        let resp = self
            .client
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .context("could not reach the OpenAI API")?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            bail!("OpenAI returned {status}: {}", text.trim());
        }

        let mut state = OpenAiState::default();
        let events = lines(resp)
            .map(|line| line.map(Some))
            .chain(stream::once(async {
                Ok::<Option<String>, anyhow::Error>(None)
            }))
            .map(move |line| match line {
                Ok(l) => parse_openai_line(l.as_deref(), &mut state),
                Err(e) => Err(e),
            })
            .flat_map(|res| {
                let items: Vec<anyhow::Result<ProviderEvent>> = match res {
                    Ok(evs) => evs.into_iter().map(Ok).collect(),
                    Err(e) => vec![Err(e)],
                };
                stream::iter(items)
            });
        Ok(Box::pin(events))
    }
}

#[derive(Default)]
struct PartialCall {
    id: String,
    name: String,
    arguments: String,
}

/// Parser state across the SSE stream. Tool-call fragments are aggregated by
/// their `index` until the stream finishes.
#[derive(Default)]
pub(crate) struct OpenAiState {
    pending: BTreeMap<u64, PartialCall>,
    finish_reason: Option<String>,
    finished: bool,
}

/// Parse one SSE line. `None` marks end-of-stream.
pub(crate) fn parse_openai_line(
    line: Option<&str>,
    state: &mut OpenAiState,
) -> anyhow::Result<Vec<ProviderEvent>> {
    if state.finished {
        return Ok(vec![]);
    }
    let Some(line) = line else {
        return Ok(finish(state));
    };
    let Some(payload) = line.trim().strip_prefix("data:") else {
        return Ok(vec![]); // comments, blank separators, event names
    };
    let payload = payload.trim();
    if payload.is_empty() {
        return Ok(vec![]);
    }
    if payload == "[DONE]" {
        return Ok(finish(state));
    }

    let v: Value = serde_json::from_str(payload).context("malformed OpenAI stream chunk")?;
    if let Some(err) = v.get("error") {
        bail!("OpenAI error: {err}");
    }

    let mut out = vec![];
    if let Some(usage) = v.get("usage").filter(|u| !u.is_null()) {
        out.push(ProviderEvent::Usage(Usage {
            input_tokens: usage
                .get("prompt_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            output_tokens: usage
                .get("completion_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0),
        }));
    }

    if let Some(choice) = v.get("choices").and_then(|c| c.get(0)) {
        let delta = choice.get("delta").unwrap_or(&Value::Null);
        if let Some(text) = delta.get("content").and_then(Value::as_str)
            && !text.is_empty()
        {
            out.push(ProviderEvent::TextDelta(text.to_string()));
        }
        if let Some(parts) = delta.get("tool_calls").and_then(Value::as_array) {
            for part in parts {
                let index = part.get("index").and_then(Value::as_u64).unwrap_or(0);
                let entry = state.pending.entry(index).or_default();
                if let Some(id) = part.get("id").and_then(Value::as_str) {
                    entry.id = id.to_string();
                }
                let function = part.get("function");
                if let Some(name) = function.and_then(|f| f.get("name")).and_then(Value::as_str)
                    && entry.name.is_empty()
                {
                    entry.name = name.to_string();
                }
                if let Some(args) = function
                    .and_then(|f| f.get("arguments"))
                    .and_then(Value::as_str)
                {
                    entry.arguments.push_str(args);
                }
            }
        }
        if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
            state.finish_reason = Some(reason.to_string());
        }
    }
    Ok(out)
}

fn finish(state: &mut OpenAiState) -> Vec<ProviderEvent> {
    state.finished = true;
    let mut out = vec![];
    let calls: Vec<ToolCall> = std::mem::take(&mut state.pending)
        .into_iter()
        .map(|(index, p)| ToolCall {
            id: if p.id.is_empty() {
                format!("call_{index}")
            } else {
                p.id
            },
            name: p.name,
            arguments: if p.arguments.is_empty() {
                "{}".into()
            } else {
                p.arguments
            },
        })
        .collect();

    let reason = if !calls.is_empty() {
        out.push(ProviderEvent::ToolCalls(calls));
        FinishReason::ToolCalls
    } else if state.finish_reason.as_deref() == Some("length") {
        FinishReason::Length
    } else {
        FinishReason::Stop
    };
    out.push(ProviderEvent::Done {
        finish_reason: reason,
    });
    out
}

/// Convert normalised history into OpenAI chat messages.
pub(crate) fn openai_messages(history: &[Message]) -> Value {
    Value::Array(
        history
            .iter()
            .map(|m| match m.role {
                Role::System => json!({ "role": "system", "content": m.content }),
                Role::User => json!({ "role": "user", "content": m.content }),
                Role::Assistant => {
                    let content = if m.content.is_empty() {
                        Value::Null
                    } else {
                        json!(m.content)
                    };
                    let mut v = json!({ "role": "assistant", "content": content });
                    if !m.tool_calls.is_empty() {
                        v["tool_calls"] = Value::Array(
                            m.tool_calls
                                .iter()
                                .map(|c| {
                                    json!({
                                        "id": c.id,
                                        "type": "function",
                                        "function": { "name": c.name, "arguments": c.arguments },
                                    })
                                })
                                .collect(),
                        );
                    }
                    v
                }
                Role::Tool => json!({
                    "role": "tool",
                    "tool_call_id": m.tool_call_id.clone().unwrap_or_default(),
                    "content": m.content,
                }),
            })
            .collect(),
    )
}

pub(crate) fn openai_tools(tools: &[ToolDef]) -> Value {
    Value::Array(
        tools
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.parameters,
                    }
                })
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(lines: &[&str]) -> Vec<ProviderEvent> {
        let mut state = OpenAiState::default();
        let mut out = vec![];
        for l in lines {
            out.extend(parse_openai_line(Some(l), &mut state).unwrap());
        }
        out.extend(parse_openai_line(None, &mut state).unwrap());
        out
    }

    #[test]
    fn aggregates_fragmented_tool_call() {
        let events = run(&[
            r#"data: {"choices":[{"delta":{"content":"Hi"}}]}"#,
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_a","function":{"name":"shell","arguments":"{\"com"}}]}}]}"#,
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"mand\":\"ls\"}"}}]}}]}"#,
            r#"data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}"#,
            r#"data: {"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":4}}"#,
            "data: [DONE]",
        ]);

        assert!(matches!(&events[0], ProviderEvent::TextDelta(t) if t == "Hi"));
        assert!(matches!(&events[1], ProviderEvent::Usage(u) if u.input_tokens == 3));
        match &events[2] {
            ProviderEvent::ToolCalls(calls) => {
                assert_eq!(calls[0].id, "call_a");
                assert_eq!(calls[0].name, "shell");
                assert_eq!(
                    serde_json::from_str::<Value>(&calls[0].arguments).unwrap(),
                    json!({"command": "ls"})
                );
            }
            other => panic!("expected ToolCalls, got {other:?}"),
        }
        assert!(matches!(
            events[3],
            ProviderEvent::Done {
                finish_reason: FinishReason::ToolCalls
            }
        ));
        assert_eq!(events.len(), 4);
    }

    #[test]
    fn finishes_on_eof_without_done_marker() {
        let events =
            run(&[r#"data: {"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}"#]);
        assert!(matches!(
            events.last(),
            Some(ProviderEvent::Done {
                finish_reason: FinishReason::Stop
            })
        ));
    }

    #[test]
    fn debug_redacts_api_key() {
        let p = OpenAiProvider::new(DEFAULT_BASE_URL, "sk-secret-value", "gpt-4o");
        let shown = format!("{p:?}");
        assert!(!shown.contains("sk-secret-value"));
        assert!(shown.contains("<redacted>"));
    }
}
