//! Append-only JSONL session log with resume support.
//!
//! Records use a logical workspace identity (a hash of the canonical path)
//! rather than absolute host paths, so session files stay portable.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};

use crate::model::{Message, ToolCall};

/// One JSONL line in a session file.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionRecord {
    /// First line of every session file.
    Meta {
        id: String,
        created_ms: u64,
        workspace_id: String,
        workspace_name: String,
        provider: String,
        model: String,
    },
    UserMessage {
        content: String,
    },
    AssistantMessage {
        content: String,
        tool_calls: Vec<ToolCall>,
    },
    ToolCall {
        id: String,
        name: String,
        arguments: String,
        ok: bool,
        output: String,
    },
}

/// Stable logical identity of a workspace directory (fnv-64 of the path).
pub fn workspace_identity(canon: &Path) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325; // fnv offset basis
    for byte in canon.display().to_string().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3); // fnv prime
    }
    format!("{hash:016x}")
}

/// Root state directory: `~/.smith`.
pub fn smith_dir() -> anyhow::Result<PathBuf> {
    let base = dirs::home_dir().context("cannot resolve home directory")?;
    Ok(base.join(".smith"))
}

pub fn sessions_dir() -> anyhow::Result<PathBuf> {
    let dir = smith_dir()?.join("sessions");
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    Ok(dir)
}

/// Writer for the active session's JSONL file.
pub struct SessionLogger {
    writer: Mutex<BufWriter<File>>,
    pub path: PathBuf,
    pub meta: SessionMeta,
}

/// Parsed meta record from a session file.
#[derive(Clone, Debug)]
pub struct SessionMeta {
    pub id: String,
    pub created_ms: u64,
    pub workspace_id: String,
    pub workspace_name: String,
    pub provider: String,
    pub model: String,
}

impl SessionLogger {
    /// Create a new session file in the default sessions directory.
    pub fn new(workspace_canon: &Path, provider: &str, model: &str) -> anyhow::Result<Self> {
        Self::new_in(&sessions_dir()?, workspace_canon, provider, model)
    }

    /// Create a new session file under an explicit directory (used by tests).
    pub fn new_in(
        dir: &Path,
        workspace_canon: &Path,
        provider: &str,
        model: &str,
    ) -> anyhow::Result<Self> {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        let id = uuid::Uuid::new_v4().simple().to_string();
        let path = dir.join(format!("{id}.jsonl"));
        let file = File::create(&path).with_context(|| format!("creating {}", path.display()))?;
        let logger = Self {
            writer: Mutex::new(BufWriter::new(file)),
            path: path.clone(),
            meta: SessionMeta {
                created_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0),
                id: id.clone(),
                workspace_id: workspace_identity(workspace_canon),
                workspace_name: workspace_canon
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "workspace".into()),
                provider: provider.to_string(),
                model: model.to_string(),
            },
        };
        logger.write_record(&SessionRecord::Meta {
            id,
            created_ms: logger.meta.created_ms,
            workspace_id: logger.meta.workspace_id.clone(),
            workspace_name: logger.meta.workspace_name.clone(),
            provider: provider.into(),
            model: model.into(),
        })?;
        Ok(logger)
    }

    pub fn write_record(&self, record: &SessionRecord) -> anyhow::Result<()> {
        let line = serde_json::to_string(record)?;
        let mut w = self.writer.lock().expect("session log mutex poisoned");
        writeln!(w, "{line}")?;
        Ok(w.flush()?)
    }

    pub fn log_user(&self, content: &str) -> anyhow::Result<()> {
        self.write_record(&SessionRecord::UserMessage {
            content: content.into(),
        })
    }

    pub fn log_assistant(&self, content: &str, tool_calls: &[ToolCall]) -> anyhow::Result<()> {
        self.write_record(&SessionRecord::AssistantMessage {
            content: content.into(),
            tool_calls: tool_calls.to_vec(),
        })
    }

    pub fn log_tool(&self, call: &ToolCall, ok: bool, output: &str) -> anyhow::Result<()> {
        self.write_record(&SessionRecord::ToolCall {
            id: call.id.clone(),
            name: call.name.clone(),
            arguments: call.arguments.clone(),
            ok,
            output: output.into(),
        })
    }
}

/// Load a session file and rebuild a conversation history from it. Tool call
/// results recorded after an assistant message become `role: tool` messages.
pub fn load_history(path: &Path) -> anyhow::Result<(SessionMeta, Vec<Message>)> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading session {}", path.display()))?;

    let mut meta: Option<SessionMeta> = None;
    let mut messages: Vec<Message> = vec![];

    for (idx, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let record: SessionRecord = serde_json::from_str(line)
            .with_context(|| format!("parsing {} line {}", path.display(), idx + 1))?;
        match record {
            SessionRecord::Meta {
                id,
                created_ms,
                workspace_id,
                workspace_name,
                provider,
                model,
            } => {
                meta = Some(SessionMeta {
                    id,
                    created_ms,
                    workspace_id,
                    workspace_name,
                    provider,
                    model,
                });
            }
            SessionRecord::UserMessage { content } => {
                messages.push(Message::user(content));
            }
            SessionRecord::AssistantMessage {
                content,
                tool_calls,
            } => {
                messages.push(Message::assistant(content, tool_calls));
            }
            SessionRecord::ToolCall { id, output, .. } => {
                // Pair with the most recent assistant tool_calls entry.
                match messages
                    .iter_mut()
                    .rev()
                    .find(|m| m.role == crate::model::Role::Assistant)
                {
                    Some(assistant) => {
                        if assistant.tool_calls.iter().any(|c| c.id == id) {
                            messages.push(Message::tool_result(id, output));
                        } else {
                            bail!("tool call {id} has no matching assistant message");
                        }
                    }
                    None => bail!("tool call {id} before any assistant message"),
                }
            }
        }
    }

    match meta {
        Some(meta) => Ok((meta, messages)),
        None => bail!("session file has no meta record: {}", path.display()),
    }
}

/// Newest-first list of session files (path + meta).
pub fn list_sessions() -> anyhow::Result<Vec<(PathBuf, SessionMeta)>> {
    let dir = sessions_dir()?;
    let mut out = vec![];
    for entry in std::fs::read_dir(&dir).with_context(|| format!("reading {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().is_none_or(|e| e != "jsonl") {
            continue;
        }
        // Read just the first line for the meta record.
        if let Ok(raw) = std::fs::read_to_string(&path)
            && let Some(first) = raw.lines().next()
            && let Ok(SessionRecord::Meta {
                id,
                created_ms,
                workspace_id,
                workspace_name,
                provider,
                model,
            }) = serde_json::from_str(first)
        {
            out.push((
                path.clone(),
                SessionMeta {
                    id,
                    created_ms,
                    workspace_id,
                    workspace_name,
                    provider,
                    model,
                },
            ));
        }
    }
    out.sort_by_key(|b| std::cmp::Reverse(b.1.created_ms));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Role;

    #[test]
    fn round_trip_and_resume() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().join("demo");
        std::fs::create_dir_all(&ws).unwrap();

        let logger =
            SessionLogger::new_in(&tmp.path().join("sessions"), &ws, "ollama", "test-model")
                .unwrap();
        logger.log_user("list the files").unwrap();
        let call = ToolCall {
            id: "call_1".into(),
            name: "list_dir".into(),
            arguments: r#"{"path":"."}"#.into(),
        };
        logger.log_assistant("", &[call.clone()]).unwrap();
        logger.log_tool(&call, true, "Cargo.toml\nsrc/").unwrap();
        logger
            .log_assistant("The workspace has Cargo.toml and src/.", &[])
            .unwrap();

        let (meta, history) = load_history(&logger.path).unwrap();
        assert_eq!(meta.provider, "ollama");
        assert_eq!(meta.workspace_name, "demo");
        assert_eq!(history.len(), 4);
        assert_eq!(history[0].role, Role::User);
        assert_eq!(history[1].role, Role::Assistant);
        assert_eq!(history[2].role, Role::Tool);
        assert_eq!(history[2].tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(history[3].role, Role::Assistant);
    }

    #[test]
    fn workspace_identity_is_stable() {
        let a = workspace_identity(Path::new("C:\\tmp\\x"));
        let b = workspace_identity(Path::new("C:\\tmp\\x"));
        let c = workspace_identity(Path::new("C:\\tmp\\y"));
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
