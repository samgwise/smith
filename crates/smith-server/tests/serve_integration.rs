//! Integration tests for the `smith serve` daemon: the controller, HTTP
//! surface and protocol client end-to-end over loopback — no podman, no
//! model service.

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
use smith_protocol::{Envelope, EventStream, ServeClient, WireEvent};
use smith_server::Controller;
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
