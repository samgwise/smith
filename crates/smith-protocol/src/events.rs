//! The wire contract between thin clients and the `smith serve` daemon.
//!
//! This is the M13 compatibility freeze point (see `docs/SERVE_PROTOCOL.md`):
//! message shapes are versioned via [`Envelope::v`] and pinned by round-trip
//! tests asserting on exact JSON. Transports may change (a WebSocket carrier
//! in the M14 mesh); these types are the contract.

use serde::{Deserialize, Serialize};

use smith_core::{ApprovalDecision, CoreEvent, ExecResult};

use crate::workers;

/// Protocol version carried in every envelope. Bumped on any breaking shape
/// change; clients and daemons must agree on the major.
pub const PROTOCOL_VERSION: u32 = 1;

/// Default TCP port for `smith serve` (the egress sidecar's control API is
/// 9239; the controller daemon sits next to it).
pub const DEFAULT_SERVE_PORT: u16 = 9240;

/// Status snapshot of one daemon-hosted session. Also the body of
/// `GET /v1/sessions/:id/status` and the payload of `WireEvent::Status`.
///
/// The M14 fields (executor/worker/repo/lease_state) are optional and
/// skipped when default, so M13 wire bytes are unchanged for local sessions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionStatus {
    pub id: String,
    /// Logical workspace identity (fnv-64 of the daemon-side canonical path).
    pub workspace_id: String,
    /// Workspace display name.
    pub workspace: String,
    pub provider: String,
    pub model: String,
    pub busy: bool,
    pub container: Option<String>,
    pub mcp_status: Vec<String>,
    pub created_ms: u64,
    /// Where the session executes (absent = local).
    #[serde(default, skip_serializing_if = "workers::ExecutorKind::is_default")]
    pub executor: workers::ExecutorKind,
    /// Worker holding the lease (worker-mode sessions).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
    /// Materialised repo URL (worker-mode sessions).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    /// Scheduler's lease view (absent = local session).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_state: Option<workers::LeaseState>,
    /// Orchestration children (parent sessions only; empty = omit, keeping
    /// the M14 bytes unchanged for every other session).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<ChildSummary>,
}

/// A child session in the parent status snapshot (M15).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildSummary {
    pub id: String,
    pub title: String,
    pub state: ChildState,
}

/// The orchestrator's view of a child session's lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildState {
    /// Created; waiting for a worker lease.
    Queued,
    /// Leased and executing (turns running or pending).
    Running,
    /// Findings collected (judge-gated); work merged or accepted.
    Completed,
    /// Spawn failed, judge rounds exhausted unproductively, or lease lost.
    Failed,
}

/// What happened in an orchestration (M15) — journalled on the parent so
/// replay tells the whole planner/judge/worker story.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrchestrationKind {
    Spawned,
    Started,
    Judged,
    Merged,
    Failed,
    Released,
}

/// One row of `GET /v1/sessions`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionSummary {
    pub id: String,
    pub workspace_id: String,
    pub workspace: String,
    pub provider: String,
    pub model: String,
    pub busy: bool,
    pub created_ms: u64,
    /// Session-file path when the summary describes an on-disk log
    /// (`LocalLink` sessions); daemon-hosted sessions omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// The event envelope: every stream payload is versioned and sequenced.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub v: u32,
    /// Monotonic per-session sequence, assigned by the daemon; replay is
    /// `?since=seq`.
    pub seq: u64,
    pub event: WireEvent,
}

impl Envelope {
    pub fn new(seq: u64, event: WireEvent) -> Self {
        Self {
            v: PROTOCOL_VERSION,
            seq,
            event,
        }
    }
}

/// One streamed event. `core` wraps a smith-core `CoreEvent` verbatim; the
/// other variants are protocol-level (multi-viewer status, clearance,
/// approval decisions).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WireEvent {
    /// An event from the agent loop.
    Core { core: CoreEvent },
    /// Session status snapshot (busy transitions, model/provider switches).
    Status(SessionStatus),
    /// Egress clearance request surfaced by the daemon's sidecar poller.
    ClearanceRequested { id: u64, host: String },
    /// An approval was decided (by this or another client); viewers close
    /// their modals. `call_id` echoes the client-supplied correlation id.
    ApprovalDecided {
        call_id: String,
        decision: ApprovalDecision,
    },
    /// Streamed worker exec output (M14 worker mesh) — journalled for
    /// audit/live tails; the tool result itself is the buffered op result.
    ExecOutput {
        exec_id: String,
        is_stderr: bool,
        text: String,
    },
    /// An orchestration lifecycle event (M15) — published on the PARENT's
    /// journal; viewers see the planner's fleet at work.
    Orchestration {
        parent: String,
        child: String,
        kind: OrchestrationKind,
        /// Human-readable detail (tool output shape; keep it short).
        detail: String,
    },
}

// ------------------------------------------------------- request bodies --

/// `POST /v1/sessions` body: create a daemon-hosted session for a workspace
/// (daemon-side path; sandbox cold start makes this a long call).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CreateSession {
    pub workspace: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    /// `auto` mirrors local headless runs (CI issuers); default asks.
    #[serde(default)]
    pub approval_mode: crate::workers::ApprovalMode,
    /// Worker-mode execution: pin a worker by name prefix, materialise the
    /// workspace from a git URL (executor defaults to config).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_ref: Option<String>,
    /// Multi-agent orchestration: the session becomes a planner with the
    /// five orchestration tools (M15; absent/false keeps the old bytes).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub orchestrate: bool,
}

/// `POST /v1/sessions/:id/turns` body.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StartTurn {
    pub input: String,
}

/// `POST /v1/sessions/:id/approvals` body. `call_id` is the client's
/// correlation id from `CoreEvent::ApprovalRequested`, echoed in
/// `WireEvent::ApprovalDecided`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApprovalPost {
    pub decision: ApprovalDecision,
    #[serde(default)]
    pub call_id: Option<String>,
}

/// `POST /v1/sessions/:id/provider` body (partial switch; daemon holds keys).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderPost {
    pub provider: Option<String>,
    pub model: Option<String>,
}

/// `POST /v1/sessions/:id/clearance` body: a decision on a pending egress
/// clearance, forwarded by the daemon to the sidecar.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClearancePost {
    pub id: u64,
    pub decision: ApprovalDecision,
}

/// `POST /v1/sessions/:id/resume` body: load a saved session log into the
/// hosted session (daemon-side history).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResumePost {
    pub prefix: String,
}

// ------------------------------------------------------ response bodies --

/// `POST /v1/sessions` response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateReply {
    pub session_id: String,
}

/// `GET /v1/sessions/:id/diff` response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiffReply {
    pub text: String,
}

/// `POST /v1/sessions/:id/resume` response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResumeReply {
    /// Full id of the resumed session log.
    pub resumed_id: String,
    /// Number of messages now in the hosted history.
    pub messages: usize,
}

/// Format `git --no-pager diff` output for the transcript (tail-capped).
/// Shared by the daemon (which runs the command) and the local link so both
/// render identically.
pub fn render_diff(res: &ExecResult) -> String {
    if res.stdout.trim().is_empty() && res.stderr.trim().is_empty() {
        return "git diff: (no changes)".to_string();
    }
    let out = format!("{}{}", res.stdout, res.stderr);
    let chars: Vec<char> = out.chars().collect();
    let start = chars.len().saturating_sub(4000);
    format!("git diff:\n{}", chars[start..].iter().collect::<String>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use smith_core::Usage;
    use std::time::Duration;

    #[test]
    fn envelope_json_is_frozen() {
        // The exact wire shape at v1 — change this test only with a version
        // bump and a SERVE_PROTOCOL.md update.
        let env = Envelope::new(
            42,
            WireEvent::Core {
                core: CoreEvent::AssistantDelta { text: "hi".into() },
            },
        );
        let json = serde_json::to_string(&env).unwrap();
        assert_eq!(
            json,
            r#"{"v":1,"seq":42,"event":{"type":"core","core":{"type":"assistant_delta","text":"hi"}}}"#
        );
        let back: Envelope = serde_json::from_str(&json).unwrap();
        assert_eq!(back, env);
    }

    #[test]
    fn core_event_variants_round_trip() {
        let samples: Vec<CoreEvent> = vec![
            CoreEvent::AssistantDelta { text: "a".into() },
            CoreEvent::AssistantMessage { text: "m".into() },
            CoreEvent::ToolStarted {
                call_id: "c1".into(),
                name: "shell".into(),
                detail: "ls".into(),
            },
            CoreEvent::ToolFinished {
                call_id: "c1".into(),
                name: "shell".into(),
                ok: true,
                summary: "ok".into(),
            },
            CoreEvent::ApprovalRequested {
                call_id: "c1".into(),
                detail: "ls".into(),
            },
            CoreEvent::Usage(Usage {
                input_tokens: 3,
                output_tokens: 4,
            }),
            CoreEvent::Compacted {
                messages: 7,
                summary: "s".into(),
            },
            CoreEvent::Notice { text: "n".into() },
            CoreEvent::UserMessage { text: "u".into() },
            CoreEvent::TurnCompleted,
            CoreEvent::TurnCancelled,
            CoreEvent::Error {
                message: "e".into(),
            },
        ];
        for ev in samples {
            let json = serde_json::to_string(&ev).unwrap();
            let back: CoreEvent = serde_json::from_str(&json).unwrap();
            assert_eq!(back, ev);
        }
        // The new variant keeps the house tag style.
        assert_eq!(
            serde_json::to_string(&CoreEvent::UserMessage { text: "u".into() }).unwrap(),
            r#"{"type":"user_message","text":"u"}"#
        );
    }

    #[test]
    fn wire_event_variants_round_trip() {
        let status = SessionStatus {
            id: "abc".into(),
            workspace_id: "0123".into(),
            workspace: "smith".into(),
            provider: "ollama".into(),
            model: "q".into(),
            busy: false,
            container: Some("smith-x".into()),
            mcp_status: vec![],
            created_ms: 1,
            executor: workers::ExecutorKind::Local,
            worker: None,
            repo: None,
            lease_state: None,
            children: vec![ChildSummary {
                id: "child".into(),
                title: "t".into(),
                state: ChildState::Running,
            }],
        };
        // The orchestration event keeps the house tag style.
        assert_eq!(
            serde_json::to_string(&WireEvent::Orchestration {
                parent: "p".into(),
                child: "c".into(),
                kind: OrchestrationKind::Judged,
                detail: "accept".into(),
            })
            .unwrap(),
            r#"{"type":"orchestration","parent":"p","child":"c","kind":"judged","detail":"accept"}"#
        );
        let events = vec![
            WireEvent::Status(status.clone()),
            WireEvent::ClearanceRequested {
                id: 9,
                host: "example.com".into(),
            },
            WireEvent::ApprovalDecided {
                call_id: "c1".into(),
                decision: ApprovalDecision::AllowAlways,
            },
        ];
        for ev in events {
            let json = serde_json::to_string(&ev).unwrap();
            let back: WireEvent = serde_json::from_str(&json).unwrap();
            assert_eq!(back, ev);
        }
        // Tag names are part of the freeze.
        assert_eq!(
            serde_json::to_string(&WireEvent::ClearanceRequested {
                id: 1,
                host: "h".into()
            })
            .unwrap(),
            r#"{"type":"clearance_requested","id":1,"host":"h"}"#
        );
        let summary = SessionSummary {
            id: "abc".into(),
            workspace_id: "0123".into(),
            workspace: "smith".into(),
            provider: "ollama".into(),
            model: "q".into(),
            busy: true,
            created_ms: 1,
            path: None,
        };
        let json = serde_json::to_string(&summary).unwrap();
        assert_eq!(
            json,
            r#"{"id":"abc","workspace_id":"0123","workspace":"smith","provider":"ollama","model":"q","busy":true,"created_ms":1}"#
        );
        let back: SessionSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back, summary);
    }

    #[test]
    fn request_bodies_round_trip() {
        let create: CreateSession =
            serde_json::from_str(r#"{"workspace":"C:\\tmp\\x","provider":null,"model":"m"}"#)
                .unwrap();
        assert_eq!(create.workspace, r"C:\tmp\x");
        assert_eq!(create.provider, None);
        assert_eq!(create.model, Some("m".into()));

        let approval: ApprovalPost =
            serde_json::from_str(r#"{"decision":"allow_always"}"#).unwrap();
        assert_eq!(approval.decision, ApprovalDecision::AllowAlways);
        assert_eq!(approval.call_id, None);

        let clearance: ClearancePost =
            serde_json::from_str(r#"{"id":5,"decision":"deny"}"#).unwrap();
        assert_eq!(clearance.id, 5);
        assert_eq!(clearance.decision, ApprovalDecision::Deny);
    }

    #[test]
    fn diff_rendering_matches_the_local_format() {
        let res = ExecResult {
            exit_code: 0,
            stdout: "diff --git a b\n".into(),
            stderr: String::new(),
            timed_out: false,
            duration: Duration::ZERO,
        };
        assert!(render_diff(&res).starts_with("git diff:\ndiff --git"));
        let empty = ExecResult {
            exit_code: 0,
            stdout: "  \n".into(),
            stderr: "".into(),
            timed_out: false,
            duration: Duration::ZERO,
        };
        assert_eq!(render_diff(&empty), "git diff: (no changes)");
    }
}
