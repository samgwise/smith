//! The controller: the daemon's session table and the harness operations
//! clients invoke over the wire — starting turns, deciding approvals,
//! switching providers, answering clearance requests, resuming logs.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use smith_core::{Agent, ApprovalDecision, CoreEvent, Message, ToolExecutor};
use smith_protocol::{ResumeReply, SessionStatus, SessionSummary, WireEvent, render_diff};

use crate::factory::{ConfigFactory, SessionFactory};
use crate::gate::{GateSlot, WireGate};
use crate::journal::Journal;

/// An operation failed at the API level (status + message for the client).
#[derive(Clone, Debug)]
pub struct ApiError {
    pub status: u16,
    pub message: String,
}

impl ApiError {
    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: 404,
            message: message.into(),
        }
    }
    pub(crate) fn busy(message: impl Into<String>) -> Self {
        Self {
            status: 409,
            message: message.into(),
        }
    }
    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: 400,
            message: message.into(),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        Self {
            status: 500,
            message: format!("{e:#}"),
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.status, self.message)
    }
}

impl std::error::Error for ApiError {}

/// Display labels for one hosted session (mutated by provider switches).
struct Labels {
    provider: String,
    model: String,
    container: Option<String>,
    mcp_status: Vec<String>,
    workspace: String,
    workspace_id: String,
}

/// One daemon-hosted session: the harness pieces plus the event plumbing.
pub struct SessionHandle {
    pub id: String,
    pub agent: Arc<Agent>,
    pub journal: Arc<Journal>,
    pub gate_slot: Arc<GateSlot>,
    executor: Arc<dyn ToolExecutor>,
    history: tokio::sync::Mutex<Vec<Message>>,
    busy: AtomicBool,
    /// Cancels the in-flight turn: the session's parent token, cancelled by
    /// `cancel_turn`; each turn runs on a child of it.
    cancel: CancellationToken,
    /// Never held across an await (field reads/updates only).
    labels: std::sync::Mutex<Labels>,
    created_ms: u64,
    /// Keeps the sandbox handle alive (None in tests).
    _sandbox: Option<Arc<smith_sandbox::Sandbox>>,
}

impl SessionHandle {
    fn labels(&self) -> std::sync::MutexGuard<'_, Labels> {
        self.labels.lock().expect("labels mutex poisoned")
    }

    pub fn busy(&self) -> bool {
        self.busy.load(Ordering::SeqCst)
    }

    pub(crate) async fn status(&self) -> SessionStatus {
        let labels = self.labels();
        SessionStatus {
            id: self.id.clone(),
            workspace_id: labels.workspace_id.clone(),
            workspace: labels.workspace.clone(),
            provider: labels.provider.clone(),
            model: labels.model.clone(),
            busy: self.busy(),
            container: labels.container.clone(),
            mcp_status: labels.mcp_status.clone(),
            created_ms: self.created_ms,
        }
    }

    fn summary(&self) -> SessionSummary {
        let labels = self.labels();
        SessionSummary {
            id: self.id.clone(),
            workspace_id: labels.workspace_id.clone(),
            workspace: labels.workspace.clone(),
            provider: labels.provider.clone(),
            model: labels.model.clone(),
            busy: self.busy(),
            created_ms: self.created_ms,
            path: None,
        }
    }

    fn publish_status(self: &Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            let status = this.status().await;
            this.journal.publish(WireEvent::Status(status));
        });
    }
}

pub struct Controller {
    sessions: std::sync::Mutex<HashMap<String, Arc<SessionHandle>>>,
    factory: Arc<dyn SessionFactory>,
    approval_timeout: Duration,
    /// Egress sidecar control API (port + token) for clearance forwarding;
    /// interior-mutable so tests can retarget it through a shared Arc.
    clearance: std::sync::Mutex<(u16, Option<String>)>,
}

impl Controller {
    pub fn new(config: &crate::config::Config) -> Arc<Self> {
        Self::with_factory(
            Arc::new(ConfigFactory {
                config: config.clone(),
            }),
            Duration::from_secs(config.serve.approval_timeout_secs.max(1)),
        )
    }

    pub fn with_factory(factory: Arc<dyn SessionFactory>, approval_timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            sessions: std::sync::Mutex::new(HashMap::new()),
            factory,
            approval_timeout,
            clearance: std::sync::Mutex::new((
                smith_egress::client::CLEARANCE_CONTROL_PORT,
                smith_egress::client::read_token(),
            )),
        })
    }

    /// Override the sidecar control endpoint (tests point this at a stub).
    pub fn set_clearance_target(&self, port: u16, token: Option<String>) {
        *self.clearance.lock().expect("clearance mutex poisoned") = (port, token);
    }

    pub fn get(&self, id: &str) -> Result<Arc<SessionHandle>, ApiError> {
        self.sessions
            .lock()
            .expect("session table mutex poisoned")
            .get(id)
            .cloned()
            .ok_or_else(|| ApiError::not_found(format!("no session `{id}` (see GET /v1/sessions)")))
    }

    pub fn sessions(&self) -> Vec<SessionSummary> {
        let mut all: Vec<SessionSummary> = self
            .sessions
            .lock()
            .expect("session table mutex poisoned")
            .values()
            .map(|h| h.summary())
            .collect();
        all.sort_by_key(|s| std::cmp::Reverse(s.created_ms));
        all
    }

    /// Create a hosted session for a daemon-side workspace. Long call:
    /// sandbox cold start (image pull, sidecar) happens here.
    pub async fn create_session(
        &self,
        workspace: &str,
        provider: Option<&str>,
        model: Option<&str>,
    ) -> Result<String, ApiError> {
        let trimmed = workspace.trim();
        if trimmed.is_empty() {
            return Err(ApiError::bad_request("workspace path is required"));
        }
        let workspace_path = PathBuf::from(trimmed);
        if !workspace_path.is_dir() {
            return Err(ApiError::bad_request(format!(
                "workspace `{trimmed}` is not a directory on the daemon host"
            )));
        }

        let journal = Arc::new(Journal::new());
        let gate_slot = GateSlot::new();
        let gate: Arc<dyn smith_core::ApprovalGate> = Arc::new(WireGate::new(
            gate_slot.clone(),
            self.approval_timeout,
            journal.clone(),
        ));

        let parts = self
            .factory
            .create(&workspace_path, provider, model, gate)
            .await
            .map_err(ApiError::from)?;
        let canon = dunce::canonicalize(&workspace_path).unwrap_or(workspace_path);

        let id = uuid::Uuid::new_v4().simple().to_string();
        let handle = Arc::new(SessionHandle {
            id: id.clone(),
            agent: Arc::new(parts.agent),
            journal,
            gate_slot,
            executor: parts.executor,
            history: tokio::sync::Mutex::new(Vec::new()),
            busy: AtomicBool::new(false),
            cancel: CancellationToken::new(),
            labels: std::sync::Mutex::new(Labels {
                provider: parts.provider_name,
                model: parts.model_name,
                container: parts.container_label,
                mcp_status: parts.mcp_status,
                workspace: parts.workspace_name,
                workspace_id: smith_core::session::workspace_identity(&canon),
            }),
            created_ms: now_ms(),
            _sandbox: parts.sandbox,
        });

        // Seed the history with the system prompt (same as every front-end).
        let system = handle.agent.system_prompt().await;
        *handle.history.lock().await = vec![Message::system(system)];

        self.sessions
            .lock()
            .expect("session table mutex poisoned")
            .insert(id.clone(), handle.clone());

        // Announce the session so the first attacher sees it on replay.
        let status = handle.status().await;
        tracing::info!("session {id} created for workspace `{}`", status.workspace);
        handle.journal.publish(WireEvent::Status(status));
        Ok(id)
    }

    /// Start a turn (exclusive per session; a busy session answers 409).
    pub async fn start_turn(&self, id: &str, input: &str) -> Result<(), ApiError> {
        let handle = self.get(id)?;
        if input.trim().is_empty() {
            return Err(ApiError::bad_request("empty prompt"));
        }
        // Exclusive claim: exactly one of racing POSTs proceeds.
        if handle
            .busy
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(ApiError::busy("a turn is already running in this session"));
        }

        let turn_cancel = handle.cancel.child_token();
        let task_handle = handle.clone();
        let input = input.to_string();
        tokio::spawn(async move {
            turn_task(task_handle, input, turn_cancel).await;
        });
        Ok(())
    }

    pub fn cancel_turn(&self, id: &str) -> Result<(), ApiError> {
        let handle = self.get(id)?;
        handle.cancel.cancel();
        Ok(())
    }

    /// Deliver an approval decision to the waiting gate; echo it to viewers.
    pub async fn deliver_approval(
        &self,
        id: &str,
        call_id: Option<&str>,
        decision: ApprovalDecision,
    ) -> Result<(), ApiError> {
        let handle = self.get(id)?;
        let delivered = handle.gate_slot.deliver(decision).await;
        if delivered {
            handle.journal.publish(WireEvent::ApprovalDecided {
                call_id: call_id.unwrap_or_default().to_string(),
                decision,
            });
            Ok(())
        } else {
            Err(ApiError::busy("no approval is pending in this session"))
        }
    }

    /// Forward a clearance decision to the egress sidecar. The sidecar
    /// client is blocking std I/O (it is also built standalone inside the
    /// container), so it runs on the blocking pool.
    pub async fn forward_clearance(
        &self,
        id: &str,
        clearance_id: u64,
        decision: ApprovalDecision,
    ) -> Result<(), ApiError> {
        let _handle = self.get(id)?;
        let (port, token) = self.clearance_target();
        let Some(token) = token else {
            return Err(ApiError::bad_request(
                "no egress sidecar token on the daemon host",
            ));
        };
        let word = smith_protocol::decision_word(decision);
        tokio::task::spawn_blocking(move || {
            smith_egress::client::post_decide(port, &token, clearance_id, word)
        })
        .await
        .map_err(|e| ApiError::from(anyhow::anyhow!("clearance forward failed: {e}")))?
        .map_err(ApiError::from)
    }

    /// Switch provider/model for a hosted session (daemon holds the keys).
    pub async fn set_provider(
        &self,
        id: &str,
        provider: Option<&str>,
        model: Option<&str>,
    ) -> Result<(), ApiError> {
        let handle = self.get(id)?;
        if provider.is_none() && model.is_none() {
            return Err(ApiError::bad_request(
                "nothing to switch: give a provider, a model, or both",
            ));
        }
        let new_provider = self
            .factory
            .build_provider(provider, model)
            .map_err(ApiError::from)?;
        handle.agent.set_provider(new_provider);

        {
            let mut labels = handle.labels();
            if let Some(p) = provider {
                labels.provider = p.to_string();
            }
            if let Some(m) = model {
                labels.model = m.to_string();
            }
        }
        handle.publish_status();
        Ok(())
    }

    /// `git diff` of the session workspace, transcript-formatted.
    pub async fn diff(&self, id: &str) -> Result<String, ApiError> {
        let handle = self.get(id)?;
        match handle
            .executor
            .exec_shell("git --no-pager diff", Duration::from_secs(30))
            .await
        {
            Ok(res) => Ok(render_diff(&res)),
            Err(e) => Ok(format!(
                "git diff failed: {e:#} (git may be missing in the base image)"
            )),
        }
    }

    /// Load a saved session log into the hosted session (daemon-side
    /// history). Fails when busy, the prefix is ambiguous, or the log
    /// belongs to a different workspace.
    pub async fn resume(&self, id: &str, prefix: &str) -> Result<ResumeReply, ApiError> {
        let handle = self.get(id)?;
        if handle.busy() {
            return Err(ApiError::busy("cannot resume while a turn is running"));
        }
        if prefix.trim().is_empty() {
            return Err(ApiError::bad_request("resume requires a session id prefix"));
        }

        let saved = smith_core::list_sessions()
            .map_err(ApiError::from)?
            .into_iter()
            .filter(|(_, meta)| meta.id.starts_with(prefix.trim()))
            .collect::<Vec<_>>();
        let (path, meta) = match saved.len() {
            0 => {
                return Err(ApiError::not_found(format!(
                    "no saved session matches `{prefix}`"
                )));
            }
            1 => saved[0].clone(),
            n => {
                return Err(ApiError::bad_request(format!(
                    "{n} saved sessions match `{prefix}`; be more specific"
                )));
            }
        };

        let workspace_id = handle.labels().workspace_id.clone();
        if meta.workspace_id != workspace_id {
            return Err(ApiError::bad_request(format!(
                "saved session {} belongs to workspace `{}`, not this one",
                &meta.id[..8.min(meta.id.len())],
                meta.workspace_name
            )));
        }

        let (_, mut history) = smith_core::load_history(&path).map_err(ApiError::from)?;
        if !history
            .first()
            .is_some_and(|m| m.role == smith_core::Role::System)
        {
            let system = handle.agent.system_prompt().await;
            history.insert(0, Message::system(system));
        }
        let count = history.len();
        *handle.history.lock().await = history;

        handle.journal.publish_core(CoreEvent::Notice {
            text: format!("[resumed session {}: {count} messages]", meta.id),
        });
        handle.publish_status();
        Ok(ResumeReply {
            resumed_id: meta.id,
            messages: count,
        })
    }

    /// Fan an event out to every session's journal (daemon-global concerns,
    /// e.g. clearance requests from the shared sidecar).
    pub async fn broadcast_to_all(&self, event: WireEvent) {
        for handle in self
            .sessions
            .lock()
            .expect("session table mutex poisoned")
            .values()
        {
            handle.journal.publish(event.clone());
        }
    }

    pub fn clearance_target(&self) -> (u16, Option<String>) {
        self.clearance
            .lock()
            .expect("clearance mutex poisoned")
            .clone()
    }
}

/// One agent turn inside the daemon: user echo, event forwarding, busy
/// bookkeeping and a status snapshot when it ends.
async fn turn_task(handle: Arc<SessionHandle>, input: String, cancel: CancellationToken) {
    // The user's prompt is an event so late joiners see it.
    handle.journal.publish_core(CoreEvent::UserMessage {
        text: input.clone(),
    });

    let (tx, mut rx) = mpsc::unbounded_channel::<CoreEvent>();
    let journal = handle.journal.clone();
    let forwarder = tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            journal.publish_core(ev);
        }
    });

    let mut history = handle.history.lock().await;
    let result = handle
        .agent
        .run_turn(&mut history, &input, tx, cancel)
        .await;
    drop(history);

    // Drain the forwarder so the closing status follows every core event.
    let _ = forwarder.await;
    if let Err(e) = result {
        handle.journal.publish_core(CoreEvent::Error {
            message: format!("turn failed: {e:#}"),
        });
    }
    handle.busy.store(false, Ordering::SeqCst);

    let status = handle.status().await;
    handle.journal.publish(WireEvent::Status(status));
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Daemon-side poller for the egress sidecar's pending clearance requests.
/// Quietly exits when no sidecar token exists (nothing to clear).
pub fn spawn_clearance_poller(controller: Arc<Controller>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let (port, token) = controller.clearance_target();
        let Some(token) = token else {
            return;
        };
        let mut seen: Vec<u64> = vec![];
        loop {
            // Blocking std I/O on the blocking pool: the sidecar client is
            // sync by design (standalone container build).
            let pending = tokio::task::spawn_blocking({
                let token = token.clone();
                move || smith_egress::client::get_pending(port, &token)
            })
            .await
            .unwrap_or_else(|e| Err(anyhow::anyhow!("poll join failed: {e}")));
            if let Ok(pending) = pending {
                for (id, host) in pending {
                    if !seen.contains(&id) {
                        seen.push(id);
                        tracing::info!("clearance requested: {host} (request {id})");
                        controller
                            .broadcast_to_all(WireEvent::ClearanceRequested { id, host })
                            .await;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(700)).await;
        }
    })
}
