//! Ollama client using the native `/api/chat` endpoint (NDJSON streaming).
//!
//! Ollama does not assign tool-call ids, so smith generates them. Tool calls
//! arrive complete, typically on the final chunk, so they are emitted at
//! end-of-stream rather than incrementally.

use std::collections::HashMap;

use anyhow::{Context, bail};
use async_trait::async_trait;
use futures::{StreamExt, stream};
use serde_json::{Value, json};
use smith_core::{
    FinishReason, LlmProvider, Message, ProviderEvent, ProviderStream, Role, ToolCall, ToolDef,
    Usage,
};

use crate::stream::lines;

pub const DEFAULT_BASE_URL: &str = "http://localhost:11434";

#[derive(Clone, Debug)]
pub struct OllamaProvider {
    client: reqwest::Client,
    base_url: String,
    model: String,
}

impl OllamaProvider {
    pub fn new(base_url: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            model: model.into(),
        }
    }
}

#[async_trait]
impl LlmProvider for OllamaProvider {
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
            "messages": ollama_messages(history),
            "stream": true,
        });
        if !tools.is_empty() {
            body["tools"] = ollama_tools(tools);
        }

        let resp = self
            .client
            .post(format!("{}/api/chat", self.base_url))
            .json(&body)
            .send()
            .await
            .context("could not reach Ollama; is `ollama serve` running?")?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            bail!("Ollama returned {status}: {}", text.trim());
        }

        let mut state = OllamaState::default();
        let events = lines(resp)
            .map(|line| line.map(Some))
            .chain(stream::once(async {
                Ok::<Option<String>, anyhow::Error>(None)
            }))
            .map(move |line| match line {
                Ok(l) => parse_ollama_line(l.as_deref(), &mut state),
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

/// Parser state carried across lines of one response.
#[derive(Default)]
pub(crate) struct OllamaState {
    calls: Vec<ToolCall>,
    next_id: u64,
    finished: bool,
}

/// Parse one NDJSON line. `None` marks end-of-stream.
pub(crate) fn parse_ollama_line(
    line: Option<&str>,
    state: &mut OllamaState,
) -> anyhow::Result<Vec<ProviderEvent>> {
    if state.finished {
        return Ok(vec![]);
    }
    let Some(line) = line else {
        return Ok(finish(state, false));
    };
    let line = line.trim();
    if line.is_empty() {
        return Ok(vec![]);
    }

    let v: Value = serde_json::from_str(line).context("malformed Ollama stream line")?;
    if let Some(err) = v.get("error").and_then(Value::as_str) {
        bail!("Ollama error: {err}");
    }

    let mut out = vec![];
    if let Some(msg) = v.get("message") {
        if let Some(text) = msg.get("content").and_then(Value::as_str)
            && !text.is_empty()
        {
            out.push(ProviderEvent::TextDelta(text.to_string()));
        }
        if let Some(calls) = msg.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                let function = call.get("function").unwrap_or(&Value::Null);
                let name = function
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                // Ollama usually sends arguments as an object; normalise to a JSON string.
                let arguments = match function.get("arguments") {
                    Some(Value::String(s)) => s.clone(),
                    Some(other) => other.to_string(),
                    None => "{}".to_string(),
                };
                state.next_id += 1;
                state.calls.push(ToolCall {
                    id: format!("ollama_call_{}", state.next_id),
                    name,
                    arguments,
                });
            }
        }
    }

    if v.get("done").and_then(Value::as_bool) == Some(true) {
        let input = v
            .get("prompt_eval_count")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let output = v.get("eval_count").and_then(Value::as_u64).unwrap_or(0);
        if input + output > 0 {
            out.push(ProviderEvent::Usage(Usage {
                input_tokens: input,
                output_tokens: output,
            }));
        }
        let length = v.get("done_reason").and_then(Value::as_str) == Some("length");
        out.extend(finish(state, length));
    }
    Ok(out)
}

fn finish(state: &mut OllamaState, length: bool) -> Vec<ProviderEvent> {
    state.finished = true;
    let mut out = vec![];
    let calls = std::mem::take(&mut state.calls);
    let reason = if !calls.is_empty() {
        out.push(ProviderEvent::ToolCalls(calls));
        FinishReason::ToolCalls
    } else if length {
        FinishReason::Length
    } else {
        FinishReason::Stop
    };
    out.push(ProviderEvent::Done {
        finish_reason: reason,
    });
    out
}

/// Convert normalised history into Ollama's chat message shape.
pub(crate) fn ollama_messages(history: &[Message]) -> Value {
    let names: HashMap<&str, &str> = history
        .iter()
        .flat_map(|m| m.tool_calls.iter())
        .map(|c| (c.id.as_str(), c.name.as_str()))
        .collect();

    Value::Array(
        history
            .iter()
            .map(|m| match m.role {
                Role::System => json!({ "role": "system", "content": m.content }),
                Role::User => json!({ "role": "user", "content": m.content }),
                Role::Assistant => {
                    let mut v = json!({ "role": "assistant", "content": m.content });
                    if !m.tool_calls.is_empty() {
                        v["tool_calls"] = Value::Array(
                            m.tool_calls
                                .iter()
                                .map(|c| {
                                    json!({
                                        "function": {
                                            "name": c.name,
                                            "arguments": parse_args(&c.arguments),
                                        }
                                    })
                                })
                                .collect(),
                        );
                    }
                    v
                }
                Role::Tool => {
                    let mut v = json!({ "role": "tool", "content": m.content });
                    if let Some(name) = m.tool_call_id.as_deref().and_then(|id| names.get(id)) {
                        v["tool_name"] = json!(name);
                    }
                    v
                }
            })
            .collect(),
    )
}

fn parse_args(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| json!({}))
}

pub(crate) fn ollama_tools(tools: &[ToolDef]) -> Value {
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
        let mut state = OllamaState::default();
        let mut out = vec![];
        for l in lines {
            out.extend(parse_ollama_line(Some(l), &mut state).unwrap());
        }
        out.extend(parse_ollama_line(None, &mut state).unwrap());
        out
    }

    #[test]
    fn streams_text_and_emits_tool_calls_at_done() {
        let events = run(&[
            r#"{"model":"m","message":{"role":"assistant","content":"Hel"},"done":false}"#,
            r#"{"model":"m","message":{"role":"assistant","content":"lo"},"done":false}"#,
            r#"{"model":"m","message":{"role":"assistant","content":"","tool_calls":[{"function":{"name":"shell","arguments":{"command":"ls"}}}]},"done":false}"#,
            r#"{"model":"m","message":{"role":"assistant","content":""},"done":true,"done_reason":"stop","prompt_eval_count":10,"eval_count":5}"#,
        ]);

        assert!(matches!(&events[0], ProviderEvent::TextDelta(t) if t == "Hel"));
        assert!(matches!(&events[1], ProviderEvent::TextDelta(t) if t == "lo"));
        assert!(
            matches!(&events[2], ProviderEvent::Usage(u) if u.input_tokens == 10 && u.output_tokens == 5)
        );
        match &events[3] {
            ProviderEvent::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].name, "shell");
                assert_eq!(calls[0].id, "ollama_call_1");
                assert_eq!(
                    serde_json::from_str::<Value>(&calls[0].arguments).unwrap(),
                    json!({"command": "ls"})
                );
            }
            other => panic!("expected ToolCalls, got {other:?}"),
        }
        assert!(matches!(
            events[4],
            ProviderEvent::Done {
                finish_reason: FinishReason::ToolCalls
            }
        ));
        assert_eq!(events.len(), 5);
    }

    #[test]
    fn plain_text_finishes_with_stop() {
        let events = run(&[
            r#"{"message":{"role":"assistant","content":"Hi"},"done":false}"#,
            r#"{"message":{"role":"assistant","content":""},"done":true,"done_reason":"stop"}"#,
        ]);
        assert!(matches!(
            events.last(),
            Some(ProviderEvent::Done {
                finish_reason: FinishReason::Stop
            })
        ));
    }

    #[test]
    fn error_line_is_surfaced() {
        let mut state = OllamaState::default();
        let err =
            parse_ollama_line(Some(r#"{"error":"model not found"}"#), &mut state).unwrap_err();
        assert!(err.to_string().contains("model not found"));
    }
}
