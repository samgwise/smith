//! The worker-mesh wire formats (M14): executor ops, streamed exec events,
//! controller→worker commands and the enrolment/heartbeat bodies.
//!
//! `ExecEvent` is the milestone's freeze point (see `docs/SERVE_PROTOCOL.md`
//! and `docs/WORKER_MESH.md`): shapes are pinned by frozen-JSON tests.
//! The controller→worker command stream and worker→controller POSTs use the
//! same hand-rolled HTTP/SSE plumbing as the client API; both directions are
//! worker-initiated (outbound-only mesh).

use std::time::Duration;

use serde::{Deserialize, Serialize};

// ------------------------------------------------------------- session --

/// Where a session executes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExecutorKind {
    /// The controller host's own podman (M13 behaviour).
    #[default]
    Local,
    /// A leased worker from the fleet.
    Worker,
}

impl ExecutorKind {
    /// Skip-serialisation predicate: local sessions keep the M13 wire shape.
    pub fn is_default(value: &ExecutorKind) -> bool {
        *value == ExecutorKind::Local
    }
}

/// Scheduler's view of a session's lease. Absent field / `None` = local
/// session (no lease involved).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LeaseState {
    /// No worker available yet; waiting in the dispatch queue.
    Queued,
    /// Pinned to a worker; ops route through the lease.
    Granted,
    /// The worker went stale, was revoked or disconnected; the checkpoint
    /// branch holds the work.
    Lost,
}

// ----------------------------------------------------- executor shapes --

/// One executor op — mirrors `smith_core::ToolExecutor` exactly, so a worker
/// can implement the trait locally and the controller can implement it
/// remotely without the agent loop knowing anything changed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ExecOp {
    ExecShell {
        command: String,
        timeout_secs: u64,
    },
    ReadFile {
        path: String,
        offset: Option<u64>,
        limit: Option<u64>,
    },
    WriteFile {
        path: String,
        contents: String,
    },
    ListDir {
        path: String,
    },
}

/// Buffered exec result on the wire (`smith_core::ExecResult` shape).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecResultDto {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
    pub duration_ms: u64,
}

impl ExecResultDto {
    pub fn from_core(res: &smith_core::ExecResult) -> Self {
        Self {
            exit_code: res.exit_code,
            stdout: res.stdout.clone(),
            stderr: res.stderr.clone(),
            timed_out: res.timed_out,
            duration_ms: res.duration.as_millis() as u64,
        }
    }

    pub fn to_core(&self) -> smith_core::ExecResult {
        smith_core::ExecResult {
            exit_code: self.exit_code,
            stdout: self.stdout.clone(),
            stderr: self.stderr.clone(),
            timed_out: self.timed_out,
            duration: Duration::from_millis(self.duration_ms),
        }
    }
}

/// The result of one op. `Failed` carries the executor's error (the tool
/// layer turns it into the tool output the model sees). Struct variants —
/// internally-tagged newtype variants cannot serialise sequences.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum OpResult {
    ExecShell(ExecResultDto),
    ReadFile(smith_core::FileContents),
    WriteFile,
    ListDir { entries: Vec<String> },
    Failed { message: String },
}

/// One streamed exec telemetry event — the M14 freeze. Deltas are
/// observability only: the tool result travels in the correlated
/// `OpResult::ExecShell` with the proven buffered semantics.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecEvent {
    /// The session (== lease) the exec ran on.
    pub lease_id: String,
    /// Correlation id shared with the command and the op-result.
    pub exec_id: String,
    /// Monotonic per-exec sequence, assigned by the worker.
    pub seq: u64,
    #[serde(flatten)]
    pub kind: ExecEventKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExecEventKind {
    Started,
    StdoutDelta {
        text: String,
    },
    StderrDelta {
        text: String,
    },
    Finished {
        exit_code: i32,
        timed_out: bool,
        duration_ms: u64,
    },
}

// --------------------------------------------- controller → worker cmds --

/// Where the workspace comes from (worker-side materialisation).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSource {
    /// Plain git URL (HTTPS or ssh). Forge-agnostic.
    pub git_url: String,
    pub git_ref: String,
    /// Name of the env var on the WORKER holding the git token — never the
    /// token itself. Passed to git via env plumbing per invocation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_env: Option<String>,
}

/// Whether a hosted session asks its viewers for approvals or auto-allows
/// (headless/CI issuers; clearance still degrades to deny).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApprovalMode {
    #[default]
    Ask,
    Auto,
}

/// Commands the controller pushes over the worker's SSE stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum WorkerCommand {
    StartSession {
        session_id: String,
        /// Ack correlation: the worker reports the started sandbox via
        /// op-result with this id.
        exec_id: String,
        /// `None` = an empty workspace (the worker makes a scratch dir).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<WorkspaceSource>,
    },
    Exec {
        session_id: String,
        exec_id: String,
        #[serde(flatten)]
        op: ExecOp,
    },
    StopSession {
        session_id: String,
    },
    Checkpoint {
        session_id: String,
    },
}

// ------------------------------------------------ worker → controller  --

/// Registry status of a worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkerStatus {
    Active,
    Revoked,
}

/// One row of `GET /v1/workers`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerSummary {
    pub name: String,
    pub id: String,
    pub status: WorkerStatus,
    pub capability: Capability,
    pub load: Load,
    /// Milliseconds since the last heartbeat.
    pub last_seen_ms: u64,
}

/// What a worker can run (capability heartbeat).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capability {
    /// Sandbox image the worker is configured with.
    #[serde(default)]
    pub image: String,
    /// Detected toolchains, e.g. `["cargo 1.85", "node 22"]`.
    #[serde(default)]
    pub toolchains: Vec<String>,
    #[serde(default)]
    pub cpus: u32,
    #[serde(default)]
    pub mem_mb: u64,
    /// `"linux"` / `"windows"` — picks the enforcement transport.
    #[serde(default)]
    pub os: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Load {
    /// Active lease count (least-loaded selection).
    #[serde(default)]
    pub leases: u32,
    #[serde(default)]
    pub cpu_percent: u32,
}

/// `POST /v1/worker/enrol` — exchange a single-use enrolment token for a
/// long-lived (revocable) worker credential.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EnrolRequest {
    pub name: String,
    pub enrolment_token: String,
    pub capability: Capability,
}

/// Enrolment reply.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnrolReply {
    pub worker_id: String,
    pub worker_token: String,
}

/// `POST /v1/worker/heartbeat`. `live_leases` is the worker's actual set of
/// live session ids — on reconnect it reconciles the controller's board
/// (worker state wins).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub worker_id: String,
    pub live_leases: Vec<String>,
    pub capability: Capability,
    pub load: Load,
}

/// `POST /v1/worker/op-result` — the correlated result of one op.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpResultRequest {
    pub session_id: String,
    pub exec_id: String,
    pub result: OpResult,
}

/// `POST /v1/worker/exec-events` — a batch of streamed exec events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecEventsRequest {
    pub events: Vec<ExecEvent>,
}

/// `POST /v1/worker/checkpoint` — report a materialised checkpoint commit.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointRequest {
    pub session_id: String,
    /// Commit hash (local to the worker's worktree).
    pub commit: String,
    /// Whether the push to the remote succeeded.
    pub pushed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::SessionStatus;

    #[test]
    fn exec_event_json_is_frozen() {
        let ev = ExecEvent {
            lease_id: "s1".into(),
            exec_id: "e1".into(),
            seq: 3,
            kind: ExecEventKind::StdoutDelta {
                text: "compiling\n".into(),
            },
        };
        let json = serde_json::to_string(&ev).unwrap();
        assert_eq!(
            json,
            r#"{"lease_id":"s1","exec_id":"e1","seq":3,"kind":"stdout_delta","text":"compiling\n"}"#
        );
        let back: ExecEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ev);

        let finished = ExecEvent {
            lease_id: "s1".into(),
            exec_id: "e1".into(),
            seq: 4,
            kind: ExecEventKind::Finished {
                exit_code: 0,
                timed_out: false,
                duration_ms: 1234,
            },
        };
        assert_eq!(
            serde_json::to_string(&finished).unwrap(),
            r#"{"lease_id":"s1","exec_id":"e1","seq":4,"kind":"finished","exit_code":0,"timed_out":false,"duration_ms":1234}"#
        );
    }

    #[test]
    fn exec_ops_mirror_the_executor_shapes() {
        let shell = ExecOp::ExecShell {
            command: "cargo test -q".into(),
            timeout_secs: 120,
        };
        assert_eq!(
            serde_json::to_string(&shell).unwrap(),
            r#"{"op":"exec_shell","command":"cargo test -q","timeout_secs":120}"#
        );
        let round: ExecOp = serde_json::from_str(&serde_json::to_string(&shell).unwrap()).unwrap();
        assert_eq!(round, shell);

        let read = ExecOp::ReadFile {
            path: "src/main.rs".into(),
            offset: Some(10),
            limit: None,
        };
        let round: ExecOp = serde_json::from_str(&serde_json::to_string(&read).unwrap()).unwrap();
        assert_eq!(round, read);
    }

    #[test]
    fn op_results_round_trip() {
        let res = OpResult::ExecShell(ExecResultDto {
            exit_code: 0,
            stdout: "ok\n".into(),
            stderr: String::new(),
            timed_out: false,
            duration_ms: 42,
        });
        let json = serde_json::to_string(&res).unwrap();
        let back: OpResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back, res);

        // The DTO round-trips through smith-core's ExecResult.
        match &back {
            OpResult::ExecShell(dto) => {
                let core = dto.to_core();
                assert_eq!(ExecResultDto::from_core(&core), *dto);
            }
            other => panic!("unexpected variant: {other:?}"),
        }

        let failed: OpResult =
            serde_json::from_str(r#"{"result":"failed","message":"no such path"}"#).unwrap();
        assert_eq!(
            failed,
            OpResult::Failed {
                message: "no such path".into()
            }
        );

        // ListDir is a struct variant (sequences cannot ride internally-
        // tagged newtype variants).
        let list = OpResult::ListDir {
            entries: vec!["Cargo.toml".into()],
        };
        let back: OpResult = serde_json::from_str(&serde_json::to_string(&list).unwrap()).unwrap();
        assert_eq!(back, list);
    }

    #[test]
    fn worker_commands_round_trip() {
        let start = WorkerCommand::StartSession {
            session_id: "abc".into(),
            exec_id: "ack-1".into(),
            source: Some(WorkspaceSource {
                git_url: "https://git.example.com/x.git".into(),
                git_ref: "main".into(),
                token_env: Some("SMITH_GIT_TOKEN".into()),
            }),
        };
        let json = serde_json::to_string(&start).unwrap();
        let back: WorkerCommand = serde_json::from_str(&json).unwrap();
        assert_eq!(back, start);

        // A source-less start and a flattened exec op survive the wire.
        let plain: WorkerCommand =
            serde_json::from_str(r#"{"cmd":"start_session","session_id":"abc","exec_id":"ack"}"#)
                .unwrap();
        assert_eq!(
            plain,
            WorkerCommand::StartSession {
                session_id: "abc".into(),
                exec_id: "ack".into(),
                source: None
            }
        );

        let exec = WorkerCommand::Exec {
            session_id: "abc".into(),
            exec_id: "e9".into(),
            op: ExecOp::ListDir { path: ".".into() },
        };
        let json = serde_json::to_string(&exec).unwrap();
        assert!(json.contains(r#""cmd":"exec""#));
        assert!(json.contains(r#""op":"list_dir""#));
        let back: WorkerCommand = serde_json::from_str(&json).unwrap();
        assert_eq!(back, exec);
    }

    #[test]
    fn enrol_and_heartbeat_bodies_round_trip() {
        let enrol = EnrolRequest {
            name: "gpu-box".into(),
            enrolment_token: "tok".into(),
            capability: Capability {
                image: "docker.io/library/ubuntu:24.04".into(),
                toolchains: vec!["cargo 1.85".into()],
                cpus: 16,
                mem_mb: 32768,
                os: "linux".into(),
            },
        };
        let back: EnrolRequest =
            serde_json::from_str(&serde_json::to_string(&enrol).unwrap()).unwrap();
        assert_eq!(back, enrol);

        let hb = HeartbeatRequest {
            worker_id: "w1".into(),
            live_leases: vec!["abc".into()],
            capability: Capability::default(),
            load: Load {
                leases: 1,
                cpu_percent: 40,
            },
        };
        let back: HeartbeatRequest =
            serde_json::from_str(&serde_json::to_string(&hb).unwrap()).unwrap();
        assert_eq!(back, hb);
    }

    #[test]
    fn status_gains_backward_compatible_fields() {
        // Old M13 clients that never send the new fields still parse; the
        // fields are skipped when default so the M13 wire shapes are intact.
        let old = r#"{"id":"abc","workspace_id":"w","workspace":"smith","provider":"ollama","model":"q","busy":false,"container":null,"mcp_status":[],"created_ms":1}"#;
        let status: SessionStatus = serde_json::from_str(old).unwrap();
        assert_eq!(status.executor, ExecutorKind::Local);
        assert_eq!(status.lease_state, None);
        assert_eq!(status.worker, None);
        assert_eq!(status.repo, None);
        // And serialisation of a default-shaped status matches the old bytes.
        assert_eq!(serde_json::to_string(&status).unwrap(), old);
    }
}
