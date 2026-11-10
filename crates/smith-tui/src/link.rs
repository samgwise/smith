//! The front-end seam: where the TUI's turns, events and approvals come
//! from. Two implementations, one UI:
//!
//! - [`LocalLink`] — the harness in-process (today's colocalised behaviour):
//!   owns the agent, history, turn task, TUI gate bridge and the local
//!   egress-clearance poller.
//! - [`RemoteLink`] — a thin client over the `smith serve` protocol: the
//!   daemon hosts the harness; this side streams envelopes and POSTs
//!   decisions.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;

use smith_core::{
    Agent, ApprovalDecision, ApprovalRequest, CoreEvent, LlmProvider, Message, list_sessions,
    load_history, workspace_identity,
};
use smith_protocol::{
    ServeClient, SessionStatus, SessionSummary, WireEvent, decision_word, render_diff,
};

/// Builds a provider for `/model` and `/provider` switches (local mode).
pub type ProviderFactory =
    Arc<dyn Fn(&str, &str) -> anyhow::Result<Arc<dyn LlmProvider>> + Send + Sync>;

/// One event from the session, view-agnostic.
#[derive(Clone, Debug)]
pub enum LinkEvent {
    /// An agent-loop event.
    Core(CoreEvent),
    /// Session status snapshot (remote; the local link never emits these).
    Status(SessionStatus),
    /// An egress clearance request needs a decision.
    Clearance { id: u64, host: String },
    /// An approval was decided (possibly by another viewer of the session).
    ApprovalDecided {
        call_id: String,
        decision: ApprovalDecision,
    },
}

/// The session a TUI talks to. The UI never touches the agent, history or
/// gates directly — everything crosses this seam.
#[async_trait]
pub trait SessionLink: Send + Sync {
    /// Whether prompts echo back as `CoreEvent::UserMessage` (the daemon
    /// emits them; the local link renders the input itself instead).
    fn echoes_user_input(&self) -> bool;

    async fn send_prompt(&self, input: String) -> anyhow::Result<()>;

    /// Next event; `None` when the link is closed. Reconnect handling is
    /// inside the implementations.
    async fn next_event(&mut self) -> Option<LinkEvent>;

    async fn respond_approval(&self, call_id: &str, decision: ApprovalDecision);

    async fn respond_clearance(&self, id: u64, decision: ApprovalDecision);

    async fn cancel(&self);

    /// Switch provider/model; returns a confirmation line for the transcript.
    async fn set_provider(&self, provider: &str, model: &str) -> anyhow::Result<String>;

    /// `git diff` of the session workspace, transcript-formatted.
    async fn diff(&self) -> anyhow::Result<String>;

    async fn sessions(&self) -> anyhow::Result<Vec<SessionSummary>>;

    /// Resume a saved session log; returns a confirmation line.
    async fn resume(&self, prefix: &str) -> anyhow::Result<String>;
}

// ----------------------------------------------------------- LocalLink --

type ReplyTx = mpsc::UnboundedSender<ApprovalDecision>;

pub struct LocalLink {
    agent: Arc<Agent>,
    history: Arc<tokio::sync::Mutex<Vec<Message>>>,
    build_provider: ProviderFactory,
    /// call_id → the gate's reply channel (the gate channel itself is
    /// consumed by the bridge task at construction).
    pending_replies: Arc<std::sync::Mutex<HashMap<String, ReplyTx>>>,
    event_tx: mpsc::UnboundedSender<LinkEvent>,
    event_rx: mpsc::UnboundedReceiver<LinkEvent>,
    busy: Arc<AtomicBool>,
    cancel: std::sync::Mutex<tokio_util::sync::CancellationToken>,
    workspace: PathBuf,
}

impl LocalLink {
    /// Build the link over an assembled agent + gate channel (what
    /// `smith`/main.rs wires today). Spawns the gate bridge and the local
    /// egress-clearance poller.
    pub fn new(
        agent: Arc<Agent>,
        history: Vec<Message>,
        gate_rx: mpsc::UnboundedReceiver<(ApprovalRequest, ReplyTx)>,
        build_provider: ProviderFactory,
        workspace: PathBuf,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        // Gate bridge: approval requests become events; decisions return
        // through the remembered reply channels (keyed by a synthesised
        // call_id — the modal keeps whatever it was shown).
        let pending_replies: Arc<std::sync::Mutex<HashMap<String, ReplyTx>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));
        {
            let pending = pending_replies.clone();
            let tx = event_tx.clone();
            let mut gate_rx = gate_rx;
            tokio::spawn(async move {
                while let Some((request, reply)) = gate_rx.recv().await {
                    let call_id = uuid::Uuid::new_v4().simple().to_string();
                    pending
                        .lock()
                        .expect("pending replies")
                        .insert(call_id.clone(), reply);
                    let _ = tx.send(LinkEvent::Core(CoreEvent::ApprovalRequested {
                        call_id,
                        detail: request.detail,
                    }));
                }
            });
        }

        // Local egress clearance poller (as the TUI has always done); quiet
        // when no sidecar token exists.
        if let Some(token) = smith_egress::client::read_token() {
            let tx = event_tx.clone();
            tokio::spawn(async move {
                let mut seen: Vec<u64> = vec![];
                loop {
                    let pending = tokio::task::spawn_blocking({
                        let token = token.clone();
                        move || {
                            smith_egress::client::get_pending(
                                smith_egress::client::CLEARANCE_CONTROL_PORT,
                                &token,
                            )
                        }
                    })
                    .await
                    .ok()
                    .and_then(|r| r.ok());
                    if let Some(pending) = pending {
                        for (id, host) in pending {
                            if !seen.contains(&id) {
                                seen.push(id);
                                if tx.send(LinkEvent::Clearance { id, host }).is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(700)).await;
                }
            });
        }

        Self {
            agent,
            history: Arc::new(tokio::sync::Mutex::new(history)),
            build_provider,
            pending_replies,
            event_tx,
            event_rx,
            busy: Arc::new(AtomicBool::new(false)),
            cancel: std::sync::Mutex::new(tokio_util::sync::CancellationToken::new()),
            workspace,
        }
    }
}

#[async_trait]
impl SessionLink for LocalLink {
    fn echoes_user_input(&self) -> bool {
        false
    }

    async fn send_prompt(&self, input: String) -> anyhow::Result<()> {
        if self.busy.swap(true, Ordering::SeqCst) {
            anyhow::bail!("a turn is already running");
        }

        let agent = Arc::clone(&self.agent);
        let history = Arc::clone(&self.history);
        let busy = Arc::clone(&self.busy);
        let out = self.event_tx.clone();
        let cancel = tokio_util::sync::CancellationToken::new();
        *self.cancel.lock().expect("cancel mutex") = cancel.clone();

        let (tx, mut rx) = mpsc::unbounded_channel::<CoreEvent>();
        let fwd_out = out.clone();
        let forwarder = tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                if fwd_out.send(LinkEvent::Core(ev)).is_err() {
                    return;
                }
            }
        });

        tokio::spawn(async move {
            let mut hist = history.lock().await;
            let result = agent.run_turn(&mut hist, &input, tx, cancel).await;
            drop(hist);
            // Drain the forwarder so any closing error follows the turn's
            // own events in order.
            let _ = forwarder.await;
            if let Err(e) = result {
                let _ = out.send(LinkEvent::Core(CoreEvent::Error {
                    message: format!("turn failed: {e:#}"),
                }));
            }
            busy.store(false, Ordering::SeqCst);
        });
        Ok(())
    }

    async fn next_event(&mut self) -> Option<LinkEvent> {
        self.event_rx.recv().await
    }

    async fn respond_approval(&self, call_id: &str, decision: ApprovalDecision) {
        let reply = self
            .pending_replies
            .lock()
            .expect("pending replies")
            .remove(call_id);
        if let Some(reply) = reply {
            let _ = reply.send(decision);
        }
    }

    async fn respond_clearance(&self, id: u64, decision: ApprovalDecision) {
        if let Some(token) = smith_egress::client::read_token() {
            let _ = tokio::task::spawn_blocking(move || {
                smith_egress::client::post_decide(
                    smith_egress::client::CLEARANCE_CONTROL_PORT,
                    &token,
                    id,
                    decision_word(decision),
                )
            })
            .await;
        }
    }

    async fn cancel(&self) {
        self.cancel.lock().expect("cancel mutex").cancel();
    }

    async fn set_provider(&self, provider: &str, model: &str) -> anyhow::Result<String> {
        let provider_impl = (self.build_provider)(provider, model)?;
        self.agent.set_provider(provider_impl);
        Ok(format!("[provider {provider}, model {model}]"))
    }

    async fn diff(&self) -> anyhow::Result<String> {
        match self
            .agent
            .executor()
            .exec_shell("git --no-pager diff", Duration::from_secs(30))
            .await
        {
            Ok(res) => Ok(render_diff(&res)),
            Err(e) => Ok(format!(
                "git diff failed: {e:#} (git may be missing in the base image)"
            )),
        }
    }

    async fn sessions(&self) -> anyhow::Result<Vec<SessionSummary>> {
        Ok(list_sessions()?
            .into_iter()
            .map(|(path, meta)| SessionSummary {
                id: meta.id.clone(),
                workspace_id: meta.workspace_id.clone(),
                workspace: meta.workspace_name.clone(),
                provider: meta.provider.clone(),
                model: meta.model.clone(),
                busy: false,
                created_ms: meta.created_ms,
                path: Some(path.display().to_string()),
            })
            .collect())
    }

    async fn resume(&self, prefix: &str) -> anyhow::Result<String> {
        let mut hist = self
            .history
            .try_lock()
            .map_err(|_| anyhow::anyhow!("cannot resume while a turn is running"))?;

        let sessions = list_sessions()?;
        let matches: Vec<_> = sessions
            .iter()
            .filter(|(_, meta)| meta.id.starts_with(prefix))
            .collect();
        let (path, meta) = match matches.len() {
            0 => anyhow::bail!("no session matches `{prefix}` (see /sessions)"),
            n if n > 1 => anyhow::bail!("{n} sessions match `{prefix}`; be more specific"),
            _ => matches[0].clone(),
        };
        let current = workspace_identity(&self.workspace);
        if meta.workspace_id != current {
            anyhow::bail!(
                "session {} belongs to workspace `{}`, not this one",
                &meta.id[..8.min(meta.id.len())],
                meta.workspace_name
            );
        }

        let (_, mut loaded) = load_history(&path)?;
        if !loaded
            .first()
            .is_some_and(|m| m.role == smith_core::Role::System)
        {
            let system = self.agent.system_prompt().await;
            loaded.insert(0, Message::system(system));
        }
        let count = loaded.len();
        *hist = loaded;
        Ok(format!(
            "[resumed session {}: {count} messages]",
            &meta.id[..8.min(meta.id.len())]
        ))
    }
}

/// One-word verb for an orchestration event kind.
fn kind_word(kind: smith_protocol::OrchestrationKind) -> &'static str {
    match kind {
        smith_protocol::OrchestrationKind::Spawned => "spawned",
        smith_protocol::OrchestrationKind::Started => "started",
        smith_protocol::OrchestrationKind::Judged => "judged",
        smith_protocol::OrchestrationKind::Merged => "merged",
        smith_protocol::OrchestrationKind::Failed => "failed",
        smith_protocol::OrchestrationKind::Released => "released",
    }
}

// ----------------------------------------------------------- RemoteLink --

pub struct RemoteLink {
    client: ServeClient,
    session_id: String,
    event_rx: mpsc::UnboundedReceiver<LinkEvent>,
}

impl RemoteLink {
    /// Attach to a daemon-hosted session and start the stream reader task
    /// (with journal replay and reconnect-on-drop).
    pub fn spawn(client: ServeClient, session_id: String) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let reader_client = client.clone();
        let stream_session_id = session_id.clone();
        tokio::spawn(async move {
            let mut since = 0u64;
            loop {
                match reader_client.events(&stream_session_id, since).await {
                    Ok(mut stream) => loop {
                        match stream.next().await {
                            Ok(Some(env)) => {
                                since = env.seq;
                                let event = match env.event {
                                    WireEvent::Core { core } => LinkEvent::Core(core),
                                    WireEvent::Status(status) => LinkEvent::Status(status),
                                    WireEvent::ClearanceRequested { id, host } => {
                                        LinkEvent::Clearance { id, host }
                                    }
                                    WireEvent::ApprovalDecided { call_id, decision } => {
                                        LinkEvent::ApprovalDecided { call_id, decision }
                                    }
                                    // Journalled worker exec output: shown as
                                    // a dim tail line (the tool pane already
                                    // shows start/finish).
                                    WireEvent::ExecOutput {
                                        is_stderr, text, ..
                                    } => LinkEvent::Core(CoreEvent::Notice {
                                        text: format!(
                                            "  {mark}{text}",
                                            mark = if is_stderr { "!" } else { "" }
                                        ),
                                    }),
                                    // Orchestration lifecycle (M15): a
                                    // Notice via replay today; the TUI can
                                    // style it as a tree later.
                                    WireEvent::Orchestration {
                                        child,
                                        kind,
                                        detail,
                                        ..
                                    } => LinkEvent::Core(CoreEvent::Notice {
                                        text: format!(
                                            "[fleet {} {}…] {detail}",
                                            kind_word(kind),
                                            &child[..8.min(child.len())]
                                        ),
                                    }),
                                };
                                if event_tx.send(event).is_err() {
                                    return; // the App is gone
                                }
                            }
                            Ok(None) => break, // daemon closed the stream
                            Err(e) => {
                                let _ = event_tx.send(LinkEvent::Core(CoreEvent::Notice {
                                    text: format!("[event stream error: {e:#} — reconnecting]"),
                                }));
                                break;
                            }
                        }
                    },
                    Err(e) => {
                        let _ = event_tx.send(LinkEvent::Core(CoreEvent::Notice {
                            text: format!("[daemon unreachable: {e:#} — retrying]"),
                        }));
                    }
                }
                if event_tx
                    .send(LinkEvent::Core(CoreEvent::Notice {
                        text: "[reconnecting...]".into(),
                    }))
                    .is_err()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        Self {
            client,
            session_id,
            event_rx,
        }
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}

#[async_trait]
impl SessionLink for RemoteLink {
    fn echoes_user_input(&self) -> bool {
        true
    }

    async fn send_prompt(&self, input: String) -> anyhow::Result<()> {
        self.client.start_turn(&self.session_id, &input).await
    }

    async fn next_event(&mut self) -> Option<LinkEvent> {
        self.event_rx.recv().await
    }

    async fn respond_approval(&self, call_id: &str, decision: ApprovalDecision) {
        let _ = self
            .client
            .approval(&self.session_id, Some(call_id), decision)
            .await;
    }

    async fn respond_clearance(&self, id: u64, decision: ApprovalDecision) {
        let _ = self.client.clearance(&self.session_id, id, decision).await;
    }

    async fn cancel(&self) {
        let _ = self.client.cancel(&self.session_id).await;
    }

    async fn set_provider(&self, provider: &str, model: &str) -> anyhow::Result<String> {
        self.client
            .set_provider(&self.session_id, Some(provider), Some(model))
            .await?;
        Ok(format!(
            "[switch requested: provider {provider}, model {model}]"
        ))
    }

    async fn diff(&self) -> anyhow::Result<String> {
        self.client.diff(&self.session_id).await
    }

    async fn sessions(&self) -> anyhow::Result<Vec<SessionSummary>> {
        self.client.sessions().await
    }

    async fn resume(&self, prefix: &str) -> anyhow::Result<String> {
        let reply = self.client.resume(&self.session_id, prefix).await?;
        Ok(format!(
            "[resumed session {}: {} messages]",
            &reply.resumed_id[..8.min(reply.resumed_id.len())],
            reply.messages
        ))
    }
}
