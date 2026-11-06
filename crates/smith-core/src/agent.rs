//! The agent loop: stream a response, run approved tools, repeat.

use std::sync::{Arc, Mutex};

use anyhow::{Context, bail};
use async_trait::async_trait;
use futures::{StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::executor::ToolExecutor;
use crate::model::{FinishReason, Message, ProviderEvent, ToolCall, ToolDef, Usage};
use crate::provider::LlmProvider;
use crate::session::SessionLogger;
use crate::tools::Tool;

/// Loop guards for one session.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Maximum provider round-trips per user turn.
    pub max_steps: usize,
    /// Drop oldest messages (below the system prompt) beyond this budget.
    pub max_history_chars: usize,
    /// Maximum tool output characters fed back to the model.
    pub max_tool_output: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_steps: 24,
            max_history_chars: 200_000,
            max_tool_output: 12_000,
        }
    }
}

/// User-facing decision on a tool execution request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    AllowOnce,
    AllowAlways,
    Deny,
}

/// A request to execute something that needs user approval.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub tool: String,
    /// Human-readable description, e.g. the shell command.
    pub detail: String,
}

/// Where approval decisions come from: the TUI modal, or auto-allow in tests
/// and headless runs.
#[async_trait]
pub trait ApprovalGate: Send + Sync {
    async fn decide(&self, request: ApprovalRequest) -> ApprovalDecision;
}

pub struct AlwaysAllowGate;

#[async_trait]
impl ApprovalGate for AlwaysAllowGate {
    async fn decide(&self, _request: ApprovalRequest) -> ApprovalDecision {
        ApprovalDecision::AllowOnce
    }
}

/// Events streamed to a front-end (TUI or headless printer) during a turn.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CoreEvent {
    AssistantDelta {
        text: String,
    },
    AssistantMessage {
        text: String,
    },
    ToolStarted {
        call_id: String,
        name: String,
        detail: String,
    },
    ToolFinished {
        call_id: String,
        name: String,
        ok: bool,
        summary: String,
    },
    ApprovalRequested {
        call_id: String,
        detail: String,
    },
    Usage(Usage),
    TurnCompleted,
    TurnCancelled,
    Error {
        message: String,
    },
}

/// Aggregate agent: provider + executor + tools + approval gate.
pub struct Agent {
    pub provider: Arc<dyn LlmProvider>,
    pub executor: Arc<dyn ToolExecutor>,
    pub tools: Vec<Box<dyn Tool>>,
    pub gate: Arc<dyn ApprovalGate>,
    pub limits: Limits,
    pub logger: Option<Arc<SessionLogger>>,
    /// Command prefixes approved for the rest of the session.
    allowed_prefixes: Mutex<Vec<String>>,
}

impl Agent {
    pub fn new(
        provider: Arc<dyn LlmProvider>,
        executor: Arc<dyn ToolExecutor>,
        tools: Vec<Box<dyn Tool>>,
        gate: Arc<dyn ApprovalGate>,
        logger: Option<Arc<SessionLogger>>,
    ) -> Self {
        Self {
            provider,
            executor,
            tools,
            gate,
            limits: Limits::default(),
            logger,
            allowed_prefixes: Mutex::new(vec![]),
        }
    }

    fn tool_defs(&self) -> Vec<ToolDef> {
        self.tools.iter().map(|t| t.def()).collect()
    }

    /// Build the v1 system prompt from the executor's workspace view.
    pub fn system_prompt(&self) -> String {
        format!(
            "You are smith, a pragmatic coding agent working inside a sandboxed \
container with the user's project mounted at /workspace.\n\
Workspace root: {root}\n\n\
Rules:\n\
- All tool paths are relative to the workspace root.\n\
- You are in a Linux container; assume a POSIX environment.\n\
- Prefer `read_file`/`write_file` over shell redirection for file edits.\n\
- Keep going until the task is done, then summarise what changed.\n\
- If a command fails, read the error and adapt rather than repeating it.",
            root = self.executor.workspace_root()
        )
    }

    /// Run one user turn: append the input, stream, execute tools, repeat until
    /// the model answers without tool calls. Appends to and mutates `history`
    /// (which must already contain the system prompt).
    pub async fn run_turn(
        &self,
        history: &mut Vec<Message>,
        input: &str,
        events: mpsc::UnboundedSender<CoreEvent>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        history.push(Message::user(input));
        if let Some(log) = &self.logger {
            log.log_user(input)?;
        }

        for _step in 0..self.limits.max_steps {
            if cancel.is_cancelled() {
                let _ = events.send(CoreEvent::TurnCancelled);
                return Ok(());
            }
            self.trim_history(history);

            let stream = self
                .provider
                .chat_stream(history, &self.tool_defs())
                .await
                .context("provider chat request failed")?;
            pin_mut!(stream);

            let mut text = String::new();
            let mut calls: Vec<ToolCall> = vec![];
            let mut finish_reason = FinishReason::Stop;

            let stream_result: anyhow::Result<()> = async {
                loop {
                    let next = tokio::select! {
                        _ = cancel.cancelled() => {
                            let _ = events.send(CoreEvent::TurnCancelled);
                            return Ok(());
                        }
                        next = stream.next() => next,
                    };
                    match next {
                        None => return Ok(()),
                        Some(ev) => match ev? {
                            ProviderEvent::TextDelta(delta) => {
                                text.push_str(&delta);
                                let _ = events.send(CoreEvent::AssistantDelta { text: delta });
                            }
                            ProviderEvent::ToolCalls(batch) => {
                                calls.extend(batch);
                            }
                            ProviderEvent::Usage(usage) => {
                                let _ = events.send(CoreEvent::Usage(usage));
                            }
                            ProviderEvent::Done { finish_reason: fr } => {
                                finish_reason = fr;
                            }
                        },
                    }
                }
            }
            .await;

            // A cancellation mid-stream returns through the same path; honour it.
            stream_result?;
            if cancel.is_cancelled() {
                let _ = events.send(CoreEvent::TurnCancelled);
                return Ok(());
            }

            if !text.is_empty() {
                let _ = events.send(CoreEvent::AssistantMessage { text: text.clone() });
            }

            if calls.is_empty() && finish_reason != FinishReason::ToolCalls {
                history.push(Message::assistant(text.clone(), vec![]));
                if let Some(log) = &self.logger {
                    log.log_assistant(&text, &[])?;
                }
                let _ = events.send(CoreEvent::TurnCompleted);
                return Ok(());
            }

            let assistant_calls = calls.clone();
            if let Some(log) = &self.logger {
                log.log_assistant(&text, &assistant_calls)?;
            }
            history.push(Message::assistant(text, assistant_calls));

            for call in calls {
                let detail = call_detail(&call);
                let _ = events.send(CoreEvent::ToolStarted {
                    call_id: call.id.clone(),
                    name: call.name.clone(),
                    detail: detail.clone(),
                });

                let (ok, output) = self.execute_call(&call, &events, &cancel).await;
                if cancel.is_cancelled() {
                    let _ = events.send(CoreEvent::TurnCancelled);
                    return Ok(());
                }

                if let Some(log) = &self.logger {
                    log.log_tool(&call, ok, &output)?;
                }
                let summary: String = output.chars().take(200).collect();
                let _ = events.send(CoreEvent::ToolFinished {
                    call_id: call.id.clone(),
                    name: call.name.clone(),
                    ok,
                    summary,
                });
                history.push(Message::tool_result(&call.id, output));
            }
        }

        let _ = events.send(CoreEvent::Error {
            message: "stopped: exceeded the per-turn step limit".into(),
        });
        bail!("exceeded max steps ({})", self.limits.max_steps)
    }

    /// Execute one tool call, consulting the approval gate when required.
    async fn execute_call(
        &self,
        call: &ToolCall,
        events: &mpsc::UnboundedSender<CoreEvent>,
        cancel: &CancellationToken,
    ) -> (bool, String) {
        let Some(tool) = self.tools.iter().find(|t| t.def().name == call.name) else {
            return (
                false,
                format!(
                    "error: unknown tool `{}`; available: {}",
                    call.name,
                    self.tool_names()
                ),
            );
        };

        if tool.needs_approval(&call.arguments) {
            let detail = call_detail(call);
            // Already-approved prefixes run straight through.
            if !self.prefix_allowed(&detail) {
                let _ = events.send(CoreEvent::ApprovalRequested {
                    call_id: call.id.clone(),
                    detail: detail.clone(),
                });
                let decision = tokio::select! {
                    _ = cancel.cancelled() => return (false, "cancelled by user".into()),
                    decision = self.gate.decide(ApprovalRequest {
                        tool: call.name.clone(),
                        detail,
                    }) => decision,
                };
                match decision {
                    ApprovalDecision::Deny => {
                        return (
                            false,
                            "The user denied this command. Explain what you were trying to \
                             do or try a different approach."
                                .into(),
                        );
                    }
                    ApprovalDecision::AllowAlways => {
                        self.remember_prefix(call);
                    }
                    ApprovalDecision::AllowOnce => {}
                }
            }
        }

        match tool.run(&*self.executor, &call.arguments).await {
            Ok(output) => (true, output),
            Err(e) => (false, format!("error: {e:#}")),
        }
    }

    fn tool_names(&self) -> String {
        self.tools
            .iter()
            .map(|t| t.def().name)
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn prefix_allowed(&self, command: &str) -> bool {
        self.allowed_prefixes
            .lock()
            .expect("approval mutex poisoned")
            .iter()
            .any(|p| command.starts_with(p.as_str()))
    }

    /// Remember the first word of an always-approved command as its prefix.
    fn remember_prefix(&self, call: &ToolCall) {
        let detail = call_detail(call);
        if let Some(first) = detail.split_whitespace().next() {
            self.allowed_prefixes
                .lock()
                .expect("approval mutex poisoned")
                .push(first.to_string());
        }
    }

    /// Drop oldest non-system messages while over the character budget.
    fn trim_history(&self, history: &mut Vec<Message>) {
        let total = |h: &[Message]| h.iter().map(Message::size).sum::<usize>();
        if total(history) <= self.limits.max_history_chars {
            return;
        }
        // Keep the system prompt (index 0) and the most recent 8 messages.
        let keep_from_tail = 8;
        while history.len() > keep_from_tail + 1 && total(history) > self.limits.max_history_chars {
            history.remove(1);
        }
    }
}

/// Human-readable one-line description of a tool call (used for approvals and
/// the tool activity pane).
pub fn call_detail(call: &ToolCall) -> String {
    serde_json::from_str::<serde_json::Value>(&call.arguments)
        .ok()
        .and_then(|v| {
            v.get("command")
                .or_else(|| v.get("path"))
                .and_then(|s| s.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| call.arguments.chars().take(120).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{ExecResult, ToolExecutor};
    use crate::model::{Role, ToolCall};
    use std::time::Duration;

    // ---- mocks -----------------------------------------------------------

    #[derive(Clone)]
    struct MockResponse {
        text: String,
        calls: Vec<ToolCall>,
    }

    type ProviderStreamImpl =
        Pin<Box<dyn futures::Stream<Item = anyhow::Result<ProviderEvent>> + Send>>;

    struct RecordingExecutor {
        shell_commands: Mutex<Vec<String>>,
    }

    impl RecordingExecutor {
        fn new() -> Self {
            Self {
                shell_commands: Mutex::new(vec![]),
            }
        }
    }

    #[async_trait]
    impl ToolExecutor for RecordingExecutor {
        async fn exec_shell(
            &self,
            command: &str,
            _timeout: Duration,
        ) -> anyhow::Result<ExecResult> {
            self.shell_commands
                .lock()
                .unwrap()
                .push(command.to_string());
            Ok(ExecResult {
                exit_code: 0,
                stdout: "ok".into(),
                stderr: String::new(),
                timed_out: false,
                duration: Duration::from_millis(1),
            })
        }

        async fn read_file(&self, path: &str) -> anyhow::Result<String> {
            Ok(format!("contents of {path}"))
        }

        async fn write_file(&self, _path: &str, _contents: &str) -> anyhow::Result<()> {
            Ok(())
        }

        async fn list_dir(&self, _path: &str) -> anyhow::Result<Vec<String>> {
            Ok(vec!["Cargo.toml".into(), "src/".into()])
        }

        fn workspace_root(&self) -> &str {
            "/workspace"
        }
    }

    use std::pin::Pin;

    fn stream_from(events: Vec<ProviderEvent>) -> ProviderStreamImpl {
        Box::pin(futures::stream::iter(events.into_iter().map(Ok)))
    }

    /// Provider that plays scripted responses in sequence, one per request.
    struct ScriptedProvider {
        script: Vec<MockResponse>,
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl LlmProvider for ScriptedProvider {
        fn model(&self) -> &str {
            "scripted"
        }

        async fn chat_stream(
            &self,
            _history: &[Message],
            _tools: &[ToolDef],
        ) -> anyhow::Result<ProviderStreamImpl> {
            let idx = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let resp = self.script.get(idx).cloned().unwrap_or(MockResponse {
                text: "done".into(),
                calls: vec![],
            });
            let mut events = vec![];
            if !resp.text.is_empty() {
                events.push(ProviderEvent::TextDelta(resp.text.clone()));
            }
            if !resp.calls.is_empty() {
                events.push(ProviderEvent::ToolCalls(resp.calls));
            }
            events.push(ProviderEvent::Done {
                finish_reason: if events
                    .iter()
                    .any(|e| matches!(e, ProviderEvent::ToolCalls(_)))
                {
                    FinishReason::ToolCalls
                } else {
                    FinishReason::Stop
                },
            });
            Ok(stream_from(events))
        }
    }

    fn tool_call(id: &str, command: &str) -> ToolCall {
        ToolCall {
            id: id.into(),
            name: "shell".into(),
            arguments: serde_json::json!({ "command": command }).to_string(),
        }
    }

    async fn run(agent: &Agent, history: &mut Vec<Message>, input: &str) -> Vec<CoreEvent> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let events = std::sync::Arc::new(std::sync::Mutex::new(vec![]));
        let events2 = events.clone();
        let join = tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                events2.lock().unwrap().push(ev);
            }
        });
        agent.run_turn(history, input, tx, cancel).await.unwrap();
        join.await.unwrap();
        let out = events.lock().unwrap().clone();
        out
    }

    #[tokio::test]
    async fn loop_executes_tools_then_stops() {
        let provider: Arc<dyn LlmProvider> = Arc::new(ScriptedProvider {
            script: vec![
                MockResponse {
                    text: String::new(),
                    calls: vec![tool_call("c1", "cargo test"), tool_call("c2", "ls")],
                },
                MockResponse {
                    text: "All tests pass.".into(),
                    calls: vec![],
                },
            ],
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let executor = Arc::new(RecordingExecutor::new());
        let agent = Agent::new(
            provider,
            executor.clone(),
            crate::tools::builtin_tools(),
            Arc::new(AlwaysAllowGate),
            None,
        );
        let mut history = vec![Message::system(agent.system_prompt())];

        let events = run(&agent, &mut history, "run the tests").await;

        assert_eq!(
            executor.shell_commands.lock().unwrap().clone(),
            vec!["cargo test".to_string(), "ls".to_string()]
        );
        assert!(matches!(events.last(), Some(CoreEvent::TurnCompleted)));
        // History ends with the final assistant message, preceded by the tool result.
        assert_eq!(history.last().unwrap().role, Role::Assistant);
        assert_eq!(history.last().unwrap().content, "All tests pass.");
        assert_eq!(history[history.len() - 2].role, Role::Tool);
    }

    #[tokio::test]
    async fn denial_is_reported_to_the_model() {
        struct DenyGate;
        #[async_trait]
        impl ApprovalGate for DenyGate {
            async fn decide(&self, _r: ApprovalRequest) -> ApprovalDecision {
                ApprovalDecision::Deny
            }
        }

        let provider: Arc<dyn LlmProvider> = Arc::new(ScriptedProvider {
            script: vec![
                MockResponse {
                    text: String::new(),
                    calls: vec![tool_call("c1", "rm -rf /")],
                },
                MockResponse {
                    text: "Understood.".into(),
                    calls: vec![],
                },
            ],
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let executor = Arc::new(RecordingExecutor::new());
        let agent = Agent::new(
            provider,
            executor.clone(),
            crate::tools::builtin_tools(),
            Arc::new(DenyGate),
            None,
        );
        let mut history = vec![Message::system("s")];
        run(&agent, &mut history, "break things").await;

        // The denied command never ran.
        assert!(executor.shell_commands.lock().unwrap().is_empty());
        // And the model was told about the denial.
        let last_tool = history.iter().rev().find(|m| m.role == Role::Tool).unwrap();
        assert!(last_tool.content.contains("denied"));
    }
}
