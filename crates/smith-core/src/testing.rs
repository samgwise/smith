//! Shared test fakes: a scripted provider and a recording executor.
//!
//! Used by smith-core's own tests and by downstream crates (smith-server's
//! controller tests, for example) so the fakes have a single home and no
//! test needs podman or a real model service.

use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;

use crate::executor::{ExecResult, FileContents, ToolExecutor};
use crate::model::{FinishReason, Message, ProviderEvent, ToolCall, ToolDef};
use crate::provider::LlmProvider;

/// Stream of provider events for tests.
pub type ProviderStreamImpl =
    Pin<Box<dyn futures::Stream<Item = anyhow::Result<ProviderEvent>> + Send>>;

/// One scripted response: assistant text plus tool calls.
#[derive(Clone)]
pub struct MockResponse {
    pub text: String,
    pub calls: Vec<ToolCall>,
}

/// Provider that plays scripted responses in sequence, one per request.
/// When the script is exhausted it replies with a plain `done`.
pub struct ScriptedProvider {
    script: Vec<MockResponse>,
    calls: AtomicUsize,
}

impl ScriptedProvider {
    pub fn new(script: Vec<MockResponse>) -> Self {
        Self {
            script,
            calls: AtomicUsize::new(0),
        }
    }
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
        let idx = self.calls.fetch_add(1, Ordering::SeqCst);
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
        Ok(Box::pin(futures::stream::iter(events.into_iter().map(Ok))))
    }
}

/// Executor that records shell commands (for assertions) and returns canned
/// results; file ops return fixture-shaped data.
pub struct RecordingExecutor {
    shell_commands: Mutex<Vec<String>>,
}

impl Default for RecordingExecutor {
    fn default() -> Self {
        Self {
            shell_commands: Mutex::new(vec![]),
        }
    }
}

impl RecordingExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Commands passed to `exec_shell`, in order.
    pub fn shell_commands(&self) -> Vec<String> {
        self.shell_commands.lock().unwrap().clone()
    }
}

#[async_trait]
impl ToolExecutor for RecordingExecutor {
    async fn exec_shell(&self, command: &str, _timeout: Duration) -> anyhow::Result<ExecResult> {
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

    async fn read_file(
        &self,
        path: &str,
        _offset: Option<u64>,
        _limit: Option<u64>,
    ) -> anyhow::Result<FileContents> {
        Ok(FileContents {
            text: format!("contents of {path}"),
            start_line: 1,
            total_lines: 1,
            truncated: false,
        })
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

/// Shell tool call with the given id and command.
pub fn tool_call(id: &str, command: &str) -> ToolCall {
    tool_named(id, "shell", serde_json::json!({ "command": command }))
}

/// Tool call with an explicit name and arguments (approval-free tools such
/// as `list_dir` need this).
pub fn tool_named(id: &str, name: &str, arguments: serde_json::Value) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: name.into(),
        arguments: arguments.to_string(),
    }
}
