//! Integration tests for the `smith serve` daemon: the controller, HTTP
//! surface and protocol client end-to-end over loopback — no podman, no
//! model service.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use tempfile::TempDir;
use tokio::net::TcpListener;

use smith_core::{
    Agent, ApprovalDecision, ApprovalGate, LlmProvider, Message, ProviderEvent, ToolDef,
};
use smith_protocol::http::{read_request, write_json_response};
use smith_protocol::{
    Capability, EnrolRequest, Envelope, EventStream, OpResult, OpResultRequest, ServeClient,
    WireEvent, WorkerCommand,
};
use smith_server::Controller;
use smith_server::OrchestrateSection;
use smith_server::factory::{SessionFactory, SessionParts};

const TOKEN: &str = "test-token";

// ---- fakes -----------------------------------------------------------

/// Provider that hangs forever (tests cancel/busy paths).
struct HangingProvider;

#[async_trait]
impl LlmProvider for HangingProvider {
    fn model(&self) -> &str {
        "hanging"
    }

    async fn chat_stream(
        &self,
        _history: &[Message],
        _tools: &[ToolDef],
    ) -> anyhow::Result<smith_core::ProviderStream> {
        // Never yields: the turn only ends via cancellation.
        Ok(Box::pin(futures::stream::pending::<
            anyhow::Result<ProviderEvent>,
        >()))
    }
}

/// Provider that plays a script (from smith-core's shared test fakes).
use smith_core::testing::{
    MockResponse, RecordingExecutor, ScriptedProvider, tool_call, tool_named,
};

struct FakeFactory {
    script: Vec<MockResponse>,
    hang: bool,
    /// Script for the ORCHESTRATION judge (built when the orchestrator
    /// asks for its `judge_model` provider).
    judge_script: Vec<MockResponse>,
}

#[async_trait]
impl SessionFactory for FakeFactory {
    async fn create(
        &self,
        workspace: &std::path::Path,
        provider: Option<&str>,
        model: Option<&str>,
        gate: Arc<dyn ApprovalGate>,
    ) -> anyhow::Result<SessionParts> {
        let provider_impl: Arc<dyn LlmProvider> = if self.hang {
            Arc::new(HangingProvider)
        } else {
            Arc::new(ScriptedProvider::new(self.script.clone()))
        };
        let executor = Arc::new(RecordingExecutor::new());
        let agent = Agent::new(
            provider_impl,
            executor.clone(),
            smith_core::builtin_tools(),
            gate,
            None,
        );
        Ok(SessionParts {
            agent,
            executor,
            sandbox: None,
            container_label: Some("fake".into()),
            mcp_status: vec!["fake mcp: ok".into()],
            provider_name: provider.unwrap_or("scripted").into(),
            model_name: model.unwrap_or("scripted-m").into(),
            workspace_name: workspace
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "workspace".into()),
        })
    }

    fn build_provider(
        &self,
        provider: Option<&str>,
        model: Option<&str>,
    ) -> anyhow::Result<Arc<dyn LlmProvider>> {
        if model == Some("judge") {
            return Ok(Arc::new(ScriptedProvider::new(self.judge_script.clone())));
        }
        Ok(Arc::new(ScriptedProvider::new(vec![MockResponse {
            text: format!(
                "{}:{}",
                provider.unwrap_or("scripted"),
                model.unwrap_or("scripted-m")
            ),
            calls: vec![],
        }])))
    }

    fn mcp_config(&self) -> smith_mcp::McpConfig {
        smith_mcp::McpConfig::default()
    }
}

// ---- harness ---------------------------------------------------------

struct Harness {
    client: ServeClient,
    _ws: TempDir,
}

async fn harness(script: Vec<MockResponse>, approval_timeout: Duration) -> Harness {
    harness_custom(
        FakeFactory {
            script,
            hang: false,
            judge_script: vec![],
        },
        approval_timeout,
    )
    .await
}

async fn harness_custom(factory: FakeFactory, approval_timeout: Duration) -> Harness {
    let ws = tempfile::tempdir().unwrap();
    let controller = harness_controller(ws.path(), factory, approval_timeout);
    let listener = smith_server::http::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(smith_server::http::accept_loop(
        listener,
        controller.clone(),
        TOKEN.to_string(),
    ));
    Harness {
        client: ServeClient::new("127.0.0.1", port, TOKEN.to_string()),
        _ws: ws,
    }
}

/// Build a controller over a temp-dir fleet registry.
fn harness_controller(
    ws: &std::path::Path,
    factory: FakeFactory,
    approval_timeout: Duration,
) -> Arc<Controller> {
    let metrics = Arc::new(smith_protocol::metrics::Metrics::default());
    let fleet = Arc::new(smith_server::Fleet::new(
        ws.join("workers.json"),
        metrics.clone(),
    ));
    Controller::with_parts(
        Arc::new(factory),
        approval_timeout,
        fleet,
        metrics,
        smith_server::WorkerDefaults::default(),
        smith_server::OrchestrateSection::default(),
    )
}

impl Harness {
    async fn new_session(&self) -> String {
        self.client
            .create_session(self._ws.path().to_str().unwrap(), None, None)
            .await
            .unwrap()
    }
}

/// Collect the next `n` envelopes (with a generous timeout each).
async fn collect(stream: &mut EventStream, n: usize) -> Vec<Envelope> {
    let mut out = Vec::new();
    for _ in 0..n {
        let next = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("timed out waiting for an event")
            .expect("event stream error")
            .expect("stream ended early");
        out.push(next);
    }
    out
}

/// Collect envelopes until one matches, returning everything including the
/// match.
async fn collect_until(
    stream: &mut EventStream,
    mut pred: impl FnMut(&Envelope) -> bool,
) -> Vec<Envelope> {
    let mut out = Vec::new();
    loop {
        let next = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("timed out waiting for an event")
            .expect("event stream error")
            .expect("stream ended early");
        let done = pred(&next);
        out.push(next);
        if done {
            return out;
        }
    }
}

// ---- tests -----------------------------------------------------------

#[tokio::test]
async fn health_and_auth() {
    let h = harness(vec![], Duration::from_secs(60)).await;
    h.client.health().await.unwrap();

    let wrong = ServeClient::new("127.0.0.1", h.client.port(), "nope".into());
    let err = wrong.health().await.unwrap_err().to_string();
    assert!(err.contains("401"), "{err}");
}

#[tokio::test]
async fn turn_streams_in_order_and_closes_with_status() {
    // list_dir needs no approval: the flow runs to completion untouched.
    let h = harness(
        vec![
            MockResponse {
                text: "working".into(),
                calls: vec![tool_named(
                    "c1",
                    "list_dir",
                    serde_json::json!({ "path": "." }),
                )],
            },
            MockResponse {
                text: "done".into(),
                calls: vec![],
            },
        ],
        Duration::from_secs(60),
    )
    .await;
    let id = h.new_session().await;

    let mut events = h.client.events(&id, 0).await.unwrap();
    // Session announcement arrives first.
    let first = collect(&mut events, 1).await.remove(0);
    assert!(matches!(first.event, WireEvent::Status(_)));

    h.client.start_turn(&id, "do the thing").await.unwrap();
    let all = collect_until(&mut events, |e| {
        matches!(
            &e.event,
            WireEvent::Status(s) if !s.busy
        )
    })
    .await;

    // Sequence numbers are monotonic and the shape is the one asserted in
    // the protocol crate.
    let seqs: Vec<u64> = std::iter::once(first.seq)
        .chain(all.iter().map(|e| e.seq))
        .collect();
    let mut sorted = seqs.clone();
    sorted.sort();
    assert_eq!(seqs, sorted);

    let kinds: Vec<String> = std::iter::once(&first)
        .chain(all.iter())
        .map(|e: &Envelope| match &e.event {
            WireEvent::Core { core } => match core {
                smith_core::CoreEvent::UserMessage { .. } => "user".to_string(),
                smith_core::CoreEvent::AssistantDelta { .. } => "delta".to_string(),
                smith_core::CoreEvent::AssistantMessage { .. } => "assistant".to_string(),
                smith_core::CoreEvent::ToolStarted { .. } => "tool_start".to_string(),
                smith_core::CoreEvent::ToolFinished { .. } => "tool_end".to_string(),
                smith_core::CoreEvent::TurnCompleted => "done".to_string(),
                _ => "other".to_string(),
            },
            WireEvent::Status(_) => "status".to_string(),
            _ => "other".to_string(),
        })
        .collect();
    assert_eq!(kinds[0], "status");
    assert_eq!(kinds[1], "user");
    assert!(kinds.contains(&"tool_start".to_string()));
    assert!(kinds.contains(&"tool_end".to_string()));
    assert!(kinds.contains(&"done".to_string()));
    assert_eq!(kinds.last().unwrap(), "status"); // closing snapshot, busy = false
}

#[tokio::test]
async fn second_turn_while_busy_is_409_and_cancel_works() {
    let h = harness_custom(
        FakeFactory {
            script: vec![],
            hang: true,
            judge_script: vec![],
        },
        Duration::from_secs(60),
    )
    .await;
    let id = h.new_session().await;

    let mut events = h.client.events(&id, 0).await.unwrap();
    collect(&mut events, 1).await; // session announcement

    h.client.start_turn(&id, "long task").await.unwrap();
    // The busy flag is observable: the status inside the journal flips on
    // the closing event only; check via a fresh status read.
    let status = h.client.status(&id).await.unwrap();
    assert!(status.busy, "turn should be running");

    // A second prompt is rejected.
    let err = h
        .client
        .start_turn(&id, "me too")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("409"), "{err}");

    // Cancel ends the turn (the agent emits TurnCancelled on the select
    // arm and again on the post-stream guard — both fine).
    h.client.cancel(&id).await.unwrap();
    let all = collect_until(&mut events, |e| {
        matches!(
            &e.event,
            WireEvent::Core {
                core: smith_core::CoreEvent::TurnCancelled
            }
        )
    })
    .await;
    assert!(!all.is_empty());
    // The closing status reports idle again (the second TurnCancelled may
    // sit in front of it).
    let closing = collect_until(
        &mut events,
        |e| matches!(&e.event, WireEvent::Status(s) if !s.busy),
    )
    .await;
    assert!(matches!(
        &closing.last().unwrap().event,
        WireEvent::Status(s) if !s.busy
    ));
}

#[tokio::test]
async fn approval_round_trips_over_the_wire() {
    let h = harness(
        vec![
            MockResponse {
                text: String::new(),
                calls: vec![tool_call("c1", "cargo publish")],
            },
            MockResponse {
                text: "published".into(),
                calls: vec![],
            },
        ],
        Duration::from_secs(60),
    )
    .await;
    let id = h.new_session().await;

    // Client A watches the stream; client B (a separate connection) decides.
    let mut events = h.client.events(&id, 0).await.unwrap();
    collect(&mut events, 1).await; // announcement
    h.client.start_turn(&id, "ship it").await.unwrap();

    let requested = collect_until(&mut events, |e| {
        matches!(
            &e.event,
            WireEvent::Core {
                core: smith_core::CoreEvent::ApprovalRequested { .. }
            }
        )
    })
    .await;
    let last = requested.last().unwrap();
    let call_id = match &last.event {
        WireEvent::Core {
            core: smith_core::CoreEvent::ApprovalRequested { call_id, .. },
        } => call_id.clone(),
        other => panic!("expected approval request, got {other:?}"),
    };

    // B decides from its own connection.
    let client_b = ServeClient::new("127.0.0.1", h.client.port(), TOKEN.to_string());
    client_b
        .approval(&id, Some(&call_id), ApprovalDecision::AllowAlways)
        .await
        .unwrap();

    let all = collect_until(&mut events, |e| {
        matches!(
            &e.event,
            WireEvent::Core {
                core: smith_core::CoreEvent::TurnCompleted
            }
        )
    })
    .await;
    // The echo reaches A, and the command actually ran.
    assert!(all.iter().any(|e| matches!(
        &e.event,
        WireEvent::ApprovalDecided {
            decision: ApprovalDecision::AllowAlways,
            ..
        }
    )));
    assert!(all.iter().any(|e| matches!(
        &e.event,
        WireEvent::Core {
            core: smith_core::CoreEvent::ToolFinished { ok: true, .. }
        }
    )));
}

#[tokio::test]
async fn unanswered_approval_times_out_to_deny() {
    let h = harness(
        vec![
            MockResponse {
                text: String::new(),
                calls: vec![tool_call("c1", "rm -rf /")],
            },
            MockResponse {
                text: "understood".into(),
                calls: vec![],
            },
        ],
        Duration::from_millis(200),
    )
    .await;
    let id = h.new_session().await;

    let mut events = h.client.events(&id, 0).await.unwrap();
    collect(&mut events, 1).await; // announcement
    h.client.start_turn(&id, "break things").await.unwrap();

    let all = collect_until(&mut events, |e| {
        matches!(
            &e.event,
            WireEvent::Core {
                core: smith_core::CoreEvent::TurnCompleted
            }
        )
    })
    .await;

    // The timeout echo (empty call_id) reached viewers...
    assert!(all.iter().any(|e| matches!(
        &e.event,
        WireEvent::ApprovalDecided { call_id, decision: ApprovalDecision::Deny } if call_id.is_empty()
    )));
    // ...and the command never ran (the tool "finished" with the denial).
    assert!(all.iter().any(|e| matches!(
        &e.event,
        WireEvent::Core {
            core: smith_core::CoreEvent::ToolFinished { ok: false, summary, ..
            }
        } if summary.contains("denied")
    )));
}

#[tokio::test]
async fn late_viewer_replays_the_journal() {
    let h = harness(
        vec![MockResponse {
            text: "all done".into(),
            calls: vec![],
        }],
        Duration::from_secs(60),
    )
    .await;
    let id = h.new_session().await;

    // First client runs a full turn.
    let mut first = h.client.events(&id, 0).await.unwrap();
    h.client.start_turn(&id, "first task").await.unwrap();
    let upto = collect_until(&mut first, |e| {
        matches!(
            &e.event,
            WireEvent::Core {
                core: smith_core::CoreEvent::TurnCompleted
            }
        )
    })
    .await;
    let last_seq = upto.last().unwrap().seq;

    // A second client attaches from zero: sees the whole transcript.
    let mut second = h.client.events(&id, 0).await.unwrap();
    let replay = collect_until(&mut second, |e| {
        matches!(
            &e.event,
            WireEvent::Core {
                core: smith_core::CoreEvent::TurnCompleted
            }
        )
    })
    .await;
    assert!(replay.iter().any(|e| matches!(
        &e.event,
        WireEvent::Core {
            core: smith_core::CoreEvent::UserMessage { .. }
        }
    )));

    // Another turn happens; a third client subscribing from `last_seq`
    // receives only the new events.
    h.client.start_turn(&id, "again").await.unwrap();
    let mut second_of_second = collect_until(&mut second, |e| {
        matches!(
            &e.event,
            WireEvent::Core {
                core: smith_core::CoreEvent::TurnCompleted
            }
        )
    })
    .await;
    let new_last = second_of_second.pop().unwrap().seq;
    assert!(new_last > last_seq);

    let mut third = h.client.events(&id, last_seq).await.unwrap();
    let partial = collect_until(&mut third, |e| {
        matches!(
            &e.event,
            WireEvent::Core {
                core: smith_core::CoreEvent::TurnCompleted
            }
        )
    })
    .await;
    assert!(partial.iter().all(|e| e.seq > last_seq));
}

#[tokio::test]
async fn provider_switch_updates_status() {
    let h = harness(vec![], Duration::from_secs(60)).await;
    let id = h.new_session().await;

    let mut events = h.client.events(&id, 0).await.unwrap();
    collect(&mut events, 1).await; // announcement

    h.client
        .set_provider(&id, Some("ollama"), Some("q9"))
        .await
        .unwrap();

    let switched = collect_until(&mut events, |e| {
        matches!(
            &e.event,
            WireEvent::Status(s) if s.model == "q9"
        )
    })
    .await;
    assert!(!switched.is_empty());
    let status = h.client.status(&id).await.unwrap();
    assert_eq!(status.model, "q9");
    assert_eq!(status.provider, "ollama");
}

#[tokio::test]
async fn diff_returns_transcript_formatted_text() {
    let h = harness(vec![], Duration::from_secs(60)).await;
    let id = h.new_session().await;
    let text = h.client.diff(&id).await.unwrap();
    // The recording executor answers every shell command with "ok".
    assert_eq!(text, "git diff:\nok");
}

#[tokio::test]
async fn clearance_requests_fan_out_and_decisions_are_forwarded() {
    // Stub sidecar binding first so its port reaches the controller before
    // anything polls: /pending lists one request, /decide records the body.
    let stub_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stub_port = stub_listener.local_addr().unwrap().port();
    let decided: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));
    let stub_task = tokio::spawn({
        let decided = Arc::clone(&decided);
        async move {
            for _ in 0..2 {
                let (mut stream, _) = stub_listener.accept().await.unwrap();
                let req = read_request(&mut stream).await.unwrap();
                if req.path == "/pending" {
                    write_json_response(&mut stream, 200, r#"[{"id":7,"host":"example.com"}]"#)
                        .await
                        .unwrap();
                } else if req.path == "/decide" {
                    decided
                        .lock()
                        .unwrap()
                        .push(String::from_utf8_lossy(&req.body).into_owned());
                    write_json_response(&mut stream, 200, r#"{"applied":true}"#)
                        .await
                        .unwrap();
                    return;
                }
            }
            panic!("stub sidecar never received a decision");
        }
    });

    // Harness with the clearance target pointed at the stub (interior-mutable,
    // so it goes through the shared Arc) before the poller starts.
    let ws = tempfile::tempdir().unwrap();
    let controller = harness_controller(
        ws.path(),
        FakeFactory {
            script: vec![],
            hang: false,
            judge_script: vec![],
        },
        Duration::from_secs(60),
    );
    controller.set_clearance_target(stub_port, Some("stub-token".into()));
    let listener = smith_server::http::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(smith_server::http::accept_loop(
        listener,
        controller.clone(),
        TOKEN.to_string(),
    ));
    let client = ServeClient::new("127.0.0.1", port, TOKEN.to_string());

    // Subscribe BEFORE the poller runs: it publishes each request once.
    let id = client
        .create_session(ws.path().to_str().unwrap(), None, None)
        .await
        .unwrap();
    let mut events = client.events(&id, 0).await.unwrap();
    collect(&mut events, 1).await; // session announcement
    smith_server::controller::spawn_clearance_poller(controller.clone());

    // The poller surfaces the pending destination to the session.
    collect_until(&mut events, |e| {
        matches!(&e.event, WireEvent::ClearanceRequested { id: 7, .. })
    })
    .await;

    // The client answers; the daemon forwards the decision to the sidecar.
    client
        .clearance(&id, 7, ApprovalDecision::AllowOnce)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), stub_task)
        .await
        .expect("stub never received the decision")
        .unwrap();
    // Field order is serde's business; check the content.
    let recorded = decided.lock().unwrap().first().cloned().unwrap();
    let body: serde_json::Value = serde_json::from_str(&recorded).unwrap();
    assert_eq!(body["id"], 7);
    assert_eq!(body["decision"], "allow");
}

// ---- M15: orchestration over the mesh -------------------------------

/// A fake worker: enrols, dials the command stream and answers commands
/// with scripted/canned results (no podman, no real materialisation).
/// Records received MergeGate commands for assertions.
async fn spawn_fake_worker(
    client: &ServeClient,
    port: u16,
    name: &str,
    merge_results: Arc<Mutex<VecDeque<OpResult>>>,
    gate_commands: Arc<Mutex<Vec<String>>>,
) {
    let issue = client.worker_issue(name).await.unwrap();
    let enrol = ServeClient::new("127.0.0.1", port, issue.clone());
    let cred = enrol
        .worker_enrol(&EnrolRequest {
            name: name.into(),
            enrolment_token: issue,
            // Empty image matches every session's pick.
            capability: Capability {
                image: String::new(),
                toolchains: vec![],
                cpus: 1,
                mem_mb: 1,
                os: "linux".into(),
            },
        })
        .await
        .unwrap();
    let worker_client = ServeClient::new("127.0.0.1", port, cred.worker_token);
    tokio::spawn(async move {
        'reconnect: loop {
            let Ok(mut stream) = worker_client.stream("/v1/worker/events").await else {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue 'reconnect;
            };
            loop {
                match stream.next_raw().await {
                    Ok(Some(payload)) => {
                        let Ok(cmd) = serde_json::from_str::<WorkerCommand>(&payload) else {
                            continue;
                        };
                        let (session_id, exec_id, result) = match &cmd {
                            WorkerCommand::StartSession {
                                session_id,
                                exec_id,
                                ..
                            } => (session_id.clone(), exec_id.clone(), OpResult::WriteFile),
                            WorkerCommand::Exec {
                                session_id,
                                exec_id,
                                ..
                            } => (
                                session_id.clone(),
                                exec_id.clone(),
                                OpResult::Failed {
                                    message: "fake worker does not execute ops".into(),
                                },
                            ),
                            WorkerCommand::MergeGate {
                                session_id,
                                exec_id,
                                integration_ref,
                                ..
                            } => {
                                gate_commands.lock().unwrap().push(integration_ref.clone());
                                let result = merge_results.lock().unwrap().pop_front().unwrap_or(
                                    OpResult::Failed {
                                        message: "no scripted merge result".into(),
                                    },
                                );
                                (session_id.clone(), exec_id.clone(), result)
                            }
                            WorkerCommand::PushBranch {
                                session_id,
                                exec_id,
                                ..
                            } => (
                                session_id.clone(),
                                exec_id.clone(),
                                OpResult::Done {
                                    output: "pushed".into(),
                                },
                            ),
                            // Fire-and-forget commands: nothing to answer.
                            WorkerCommand::StopSession { .. }
                            | WorkerCommand::Checkpoint { .. } => continue,
                        };
                        let _ = worker_client
                            .post_body(
                                "/v1/worker/op-result",
                                serde_json::to_string(&OpResultRequest {
                                    session_id,
                                    exec_id,
                                    result,
                                })
                                .unwrap(),
                            )
                            .await;
                    }
                    _ => continue 'reconnect,
                }
            }
        }
    });
}

fn accept_verdict() -> MockResponse {
    MockResponse {
        text: r#"{"verdict":"accept","critique":"task complete, diff reviewed"}"#.into(),
        calls: vec![],
    }
}

fn rework_verdict() -> MockResponse {
    MockResponse {
        text: r#"{"verdict":"rework","critique":"the report lacks the test results"}"#.into(),
        calls: vec![],
    }
}

/// Full planner/judge/worker flow over loopback: a scripted planner spawns
/// two children, both execute (one needs a judge rework round), findings
/// are collected, merge gates run (one clean, one conflict reworked), and
/// finish releases the fleet. Journals, branch names, states and metrics
/// are asserted.
#[tokio::test]
async fn orchestration_end_to_end_over_the_mesh() {
    let ws = tempfile::tempdir().unwrap();
    let repo = "https://git.example.com/org/repo.git";
    let planner_script = vec![
        MockResponse {
            text: String::new(),
            calls: vec![tool_named(
                "c1",
                "spawn_subtasks",
                serde_json::json!({ "subtasks": [
                    { "title": "alpha", "instructions": "do alpha", "repo": repo },
                    { "title": "beta", "instructions": "do beta", "repo": repo },
                ]}),
            )],
        },
        MockResponse {
            text: "plan laid out".into(),
            calls: vec![],
        },
    ];
    let judge_script = vec![
        accept_verdict(), // child 1
        rework_verdict(), // child 2, first attempt
        accept_verdict(), // child 2 after its rework turn
    ];
    let merge_results = Arc::new(Mutex::new(VecDeque::from(vec![
        OpResult::MergeGate {
            ok: true,
            conflict: false,
            output: "merged clean".into(),
        },
        OpResult::MergeGate {
            ok: false,
            conflict: true,
            output: "CONFLICT (content): Merge conflict in lib.rs".into(),
        },
        OpResult::MergeGate {
            ok: true,
            conflict: false,
            output: "reworked and merged".into(),
        },
    ])));
    let gate_commands: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));

    // The test constructor takes the orchestration section directly: a
    // judge model override and short, test-friendly timeouts.
    let controller = {
        let metrics = Arc::new(smith_protocol::metrics::Metrics::default());
        let fleet = Arc::new(smith_server::Fleet::new(
            ws.path().join("workers2.json"),
            metrics.clone(),
        ));
        smith_server::Controller::with_parts(
            Arc::new(FakeFactory {
                script: planner_script,
                hang: false,
                judge_script,
            }),
            Duration::from_secs(60),
            fleet,
            metrics,
            smith_server::WorkerDefaults::default(),
            OrchestrateSection {
                max_children: 8,
                judge_rounds: 2,
                judge_model: "judge".into(),
                gate_command: String::new(),
                auto_push: false,
                collect_timeout_secs: 30,
            },
        )
    };
    let listener = smith_server::http::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(smith_server::http::accept_loop(
        listener,
        controller.clone(),
        TOKEN.to_string(),
    ));
    let client = ServeClient::new("127.0.0.1", port, TOKEN.to_string());
    spawn_fake_worker(
        &client,
        port,
        "fake-1",
        merge_results,
        gate_commands.clone(),
    )
    .await;

    // The planner: a local session with orchestration opt-in.
    let parent = client
        .create_session_req(&smith_protocol::CreateSession {
            workspace: ws.path().to_str().unwrap().to_string(),
            provider: None,
            model: None,
            approval_mode: smith_protocol::ApprovalMode::Auto,
            worker: None,
            repo: None,
            git_ref: None,
            orchestrate: true,
        })
        .await
        .unwrap();
    let mut events = client.events(&parent, 0).await.unwrap();
    client
        .start_turn(&parent, "orchestrate this")
        .await
        .unwrap();

    // The planner's spawn tool call lands: two children queued then granted.
    collect_until(&mut events, |e| {
        matches!(
            &e.event,
            WireEvent::Orchestration {
                kind: smith_protocol::OrchestrationKind::Spawned,
                ..
            }
        )
    })
    .await;
    // Wait for BOTH children to start (granted + first turns running).
    loop {
        let status = client.status(&parent).await.unwrap();
        if status.children.len() == 2
            && status
                .children
                .iter()
                .all(|c| c.state == smith_protocol::ChildState::Running)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let children: Vec<String> = client
        .status(&parent)
        .await
        .unwrap()
        .children
        .iter()
        .map(|c| c.id.clone())
        .collect();

    // One level deep: the children's agents have no orchestration tools.
    for child in &children {
        let agent = controller.get(child).unwrap().agent().unwrap();
        assert_eq!(
            agent.tools.len(),
            4,
            "builtin tools only, got {}",
            agent.tools.len()
        );
        assert!(!agent.tools.iter().any(|t| t.def().name == "spawn_subtasks"));
    }

    let orch = controller
        .get(&parent)
        .unwrap()
        .orchestration
        .clone()
        .expect("parent carries an orchestrator");

    // Collect child 1: the judge accepts.
    let f1 = orch.collect(&children[0]).await.unwrap();
    assert!(f1.contains("verdict: accept"), "{f1}");
    // Collect child 2: rework round, then accept.
    let f2 = orch.collect(&children[1]).await.unwrap();
    assert!(f2.contains("verdict: accept"), "{f2}");

    // Merge gates: clean, conflict, reworked.
    let m1 = orch.merge(&children[0]).await.unwrap();
    assert!(m1.contains("OK"), "{m1}");
    assert!(m1.contains("smith/"), "{m1}");
    let m2 = orch.merge(&children[1]).await.unwrap();
    assert!(m2.contains("CONFLICT"), "{m2}");
    let m3 = orch.merge(&children[1]).await.unwrap();
    assert!(m3.contains("OK"), "{m3}");

    // The gates merged against the parent's integration branch.
    let gates = gate_commands.lock().unwrap().clone();
    assert_eq!(gates, vec![format!("smith/integration/{parent}"); 3]);

    // Finish: releases leases, closes the orchestration.
    let fin = orch.finish("everything merged").await.unwrap();
    assert!(fin.contains("orchestration closed"), "{fin}");

    // Children's leases are Released; the parent status lists them done.
    for child in &children {
        let st = client.status(child).await.unwrap();
        assert_eq!(st.lease_state, Some(smith_protocol::LeaseState::Released));
    }
    let parent_status = client.status(&parent).await.unwrap();
    assert_eq!(parent_status.children.len(), 2);
    assert!(
        parent_status
            .children
            .iter()
            .all(|c| c.state == smith_protocol::ChildState::Completed)
    );

    // The parent's journal tells the whole story (fresh stream = replay).
    let mut journal = client.events(&parent, 0).await.unwrap();
    let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    loop {
        let next = tokio::time::timeout(Duration::from_secs(5), journal.next())
            .await
            .expect("timed out reading the journal")
            .expect("journal error")
            .expect("journal ended");
        let mut done = false;
        if let WireEvent::Orchestration { child, kind, .. } = &next.event {
            *counts.entry(format!("{:?}", kind)).or_default() += 1;
            done = *child == parent && *kind == smith_protocol::OrchestrationKind::Released;
        }
        if done {
            break;
        }
    }
    assert_eq!(counts["Spawned"], 2);
    assert_eq!(counts["Started"], 2);
    assert_eq!(counts["Judged"], 3);
    assert_eq!(counts["Merged"], 2);
    assert_eq!(counts["Failed"], 1);
    assert_eq!(counts["Released"], 3); // two children + the closing parent event

    // Judge verdict metrics.
    let metrics = client.metrics().await.unwrap();
    assert!(
        metrics.contains("smith_judge_total{verdict=\"accept\"} 2"),
        "{metrics}"
    );
    assert!(
        metrics.contains("smith_judge_total{verdict=\"rework\"} 1"),
        "{metrics}"
    );
}

/// Caps are config, not model judgement: spawn beyond max_children errors,
/// children stay queued without workers, and finish releases cleanly.
#[tokio::test]
async fn spawn_caps_are_enforced_and_finish_releases_queued_children() {
    let ws = tempfile::tempdir().unwrap();
    let metrics = Arc::new(smith_protocol::metrics::Metrics::default());
    let fleet = Arc::new(smith_server::Fleet::new(
        ws.path().join("workers3.json"),
        metrics.clone(),
    ));
    let controller = smith_server::Controller::with_parts(
        Arc::new(FakeFactory {
            script: vec![],
            hang: false,
            judge_script: vec![],
        }),
        Duration::from_secs(60),
        fleet,
        metrics,
        smith_server::WorkerDefaults::default(),
        OrchestrateSection {
            max_children: 1,
            ..Default::default()
        },
    );
    let listener = smith_server::http::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(smith_server::http::accept_loop(
        listener,
        controller.clone(),
        TOKEN.to_string(),
    ));
    let client = ServeClient::new("127.0.0.1", port, TOKEN.to_string());

    let parent = client
        .create_session_req(&smith_protocol::CreateSession {
            workspace: ws.path().to_str().unwrap().to_string(),
            provider: None,
            model: None,
            approval_mode: smith_protocol::ApprovalMode::Auto,
            worker: None,
            repo: None,
            git_ref: None,
            orchestrate: true,
        })
        .await
        .unwrap();
    let orch = controller
        .get(&parent)
        .unwrap()
        .orchestration
        .clone()
        .expect("parent carries an orchestrator");

    let spec = |title: &str| smith_server::orchestrator::SubtaskSpec {
        title: title.into(),
        instructions: format!("do {title}"),
        repo: Some("https://git.example.com/org/repo.git".into()),
        git_ref: None,
    };

    // Over the cap (1): refused.
    let err = orch.spawn(vec![spec("a"), spec("b")]).await.unwrap_err();
    assert!(err.message.contains("max_children"), "{err}");

    // Within the cap: queued immediately (no workers exist).
    orch.spawn(vec![spec("a")]).await.unwrap();
    let status = client.status(&parent).await.unwrap();
    assert_eq!(status.children.len(), 1);
    assert_eq!(status.children[0].state, smith_protocol::ChildState::Queued);

    // A closed orchestration refuses further spawns.
    orch.finish("abandoned").await.unwrap();
    let err = orch.spawn(vec![spec("z")]).await.unwrap_err();
    assert!(err.message.contains("closed"), "{err}");
    // And the queued child was released with no lease to free.
    let st = client.status(&status.children[0].id).await.unwrap();
    assert_eq!(st.lease_state, Some(smith_protocol::LeaseState::Released));
}
