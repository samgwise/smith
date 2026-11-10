//! The controller: the daemon's session table and the harness operations
//! clients invoke over the wire — starting turns, deciding approvals,
//! switching providers, answering clearance requests, resuming logs, and
//! (M14) leasing workers from the fleet.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use smith_core::{Agent, ApprovalDecision, ApprovalGate, CoreEvent, Message, ToolExecutor};
use smith_protocol::{
    ApprovalMode, CreateSession, ExecutorKind, LeaseState, SessionStatus, SessionSummary,
    WireEvent, WorkerCommand, WorkspaceSource, render_diff,
};

use crate::factory::{ConfigFactory, SessionFactory};
use crate::gate::{GateSlot, WireGate};
use crate::journal::Journal;
use crate::workers::{Fleet, WorkerExecutor};

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
    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self {
            status: 503,
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

/// Worker-mode defaults, resolved from `[worker]` config at daemon start.
#[derive(Clone, Debug)]
pub struct WorkerDefaults {
    /// `local` | `worker`.
    pub executor: String,
    pub repo_url: String,
    pub git_ref: String,
    pub git_token_env: String,
    pub dispatch_timeout_secs: u64,
    /// Sandbox image capability-matched against workers.
    pub image: String,
    /// Display defaults for hosted-session labels.
    pub provider: String,
    pub model: String,
}

impl Default for WorkerDefaults {
    fn default() -> Self {
        Self {
            executor: "local".into(),
            repo_url: String::new(),
            git_ref: "main".into(),
            git_token_env: "SMITH_GIT_TOKEN".into(),
            dispatch_timeout_secs: 300,
            image: "docker.io/library/ubuntu:24.04".into(),
            provider: "ollama".into(),
            model: "unset".into(),
        }
    }
}

/// Display labels for one hosted session (mutated by provider switches
/// and the scheduler).
struct Labels {
    provider: String,
    model: String,
    container: Option<String>,
    mcp_status: Vec<String>,
    workspace: String,
    workspace_id: String,
    executor: ExecutorKind,
    worker: Option<String>,
    repo: Option<String>,
    lease_state: Option<LeaseState>,
}

/// One daemon-hosted session: the harness pieces plus the event plumbing.
pub struct SessionHandle {
    pub id: String,
    pub agent: Arc<Agent>,
    pub journal: Arc<Journal>,
    pub gate_slot: Arc<GateSlot>,
    pub metrics: Arc<smith_protocol::metrics::Metrics>,
    executor: Arc<dyn ToolExecutor>,
    history: tokio::sync::Mutex<Vec<Message>>,
    busy: AtomicBool,
    /// Cancels the in-flight turn: the session's parent token, cancelled by
    /// `cancel_turn`; each turn runs on a child of it.
    cancel: CancellationToken,
    /// Never held across an await (field reads/updates only).
    labels: std::sync::Mutex<Labels>,
    created_ms: u64,
    /// The fleet for worker-mode sessions (checkpointing, lease state).
    fleet: Option<Arc<Fleet>>,
    /// Keeps the sandbox handle alive (None in worker-mode and tests).
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
            executor: labels.executor,
            worker: labels.worker.clone(),
            repo: labels.repo.clone(),
            lease_state: labels.lease_state,
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
    /// The worker fleet (M14).
    pub fleet: Arc<Fleet>,
    /// `[worker]` defaults.
    worker_defaults: WorkerDefaults,
    pub(crate) metrics: Arc<smith_protocol::metrics::Metrics>,
}

impl Controller {
    pub fn new(config: &crate::config::Config) -> Arc<Self> {
        let metrics = Arc::new(smith_protocol::metrics::Metrics::default());
        let fleet = Arc::new(Fleet::new(
            crate::workers::workers_registry_path()
                .unwrap_or_else(|_| PathBuf::from("workers.json")),
            metrics.clone(),
        ));
        let defaults = WorkerDefaults {
            executor: config.worker.executor.clone(),
            repo_url: config.worker.repo_url.clone(),
            git_ref: config.worker.git_ref.clone(),
            git_token_env: config.worker.git_token_env.clone(),
            dispatch_timeout_secs: config.worker.dispatch_timeout_secs,
            image: config
                .sandbox_config()
                .map(|s| s.image)
                .unwrap_or_else(|_| WorkerDefaults::default().image),
            provider: config.provider.clone(),
            model: config.model.clone(),
        };
        Self::with_parts(
            Arc::new(ConfigFactory {
                config: config.clone(),
            }),
            Duration::from_secs(config.serve.approval_timeout_secs.max(1)),
            fleet,
            metrics,
            defaults,
        )
    }

    /// Test constructor: explicit factory, timeout, fleet and defaults.
    pub fn with_parts(
        factory: Arc<dyn SessionFactory>,
        approval_timeout: Duration,
        fleet: Arc<Fleet>,
        metrics: Arc<smith_protocol::metrics::Metrics>,
        worker_defaults: WorkerDefaults,
    ) -> Arc<Self> {
        Arc::new(Self {
            sessions: std::sync::Mutex::new(HashMap::new()),
            factory,
            approval_timeout,
            clearance: std::sync::Mutex::new((
                smith_egress::client::CLEARANCE_CONTROL_PORT,
                smith_egress::client::read_token(),
            )),
            fleet,
            worker_defaults,
            metrics,
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

    /// Create a hosted session (local or worker-mode per the request).
    pub async fn create_session(&self, req: CreateSession) -> Result<String, ApiError> {
        let worker_mode =
            req.worker.is_some() || req.repo.is_some() || self.worker_defaults.executor == "worker";
        if worker_mode {
            self.create_worker_session(req).await
        } else {
            self.create_local_session(req).await
        }
    }

    /// Local (M13) path: the daemon's own podman assembles the sandbox.
    async fn create_local_session(&self, req: CreateSession) -> Result<String, ApiError> {
        let trimmed = req.workspace.trim();
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
        let gate: Arc<dyn ApprovalGate> = self.build_gate(&req, gate_slot.clone(), &journal);

        let parts = self
            .factory
            .create(
                &workspace_path,
                req.provider.as_deref(),
                req.model.as_deref(),
                gate,
            )
            .await
            .map_err(ApiError::from)?;
        let canon = dunce::canonicalize(&workspace_path).unwrap_or(workspace_path);

        let id = uuid::Uuid::new_v4().simple().to_string();
        let handle = Arc::new(SessionHandle {
            id: id.clone(),
            agent: Arc::new(parts.agent),
            journal,
            gate_slot,
            metrics: self.metrics.clone(),
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
                executor: ExecutorKind::Local,
                worker: None,
                repo: None,
                lease_state: None,
            }),
            created_ms: now_ms(),
            fleet: None,
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
        self.metrics
            .sessions
            .set("executor=\"local\"", self.count_local());
        handle.journal.publish(WireEvent::Status(status));
        Ok(id)
    }

    /// Worker-mode path: the sandbox lives on a leased worker; the daemon
    /// holds only the harness (agent, history, approvals, keys).
    async fn create_worker_session(&self, req: CreateSession) -> Result<String, ApiError> {
        // Source: request > config default. No repo configured → a scratch
        // workspace on the worker (still disposable; nothing to checkpoint).
        let repo = req
            .repo
            .clone()
            .or_else(|| non_empty(self.worker_defaults.repo_url.clone()));
        let git_ref = req
            .git_ref
            .clone()
            .or_else(|| non_empty(self.worker_defaults.git_ref.clone()))
            .unwrap_or_else(|| "main".into());
        let source = repo.map(|git_url| WorkspaceSource {
            git_url,
            git_ref,
            token_env: Some(self.worker_defaults.git_token_env.clone()),
        });
        let repo_label = source
            .as_ref()
            .map(|s| {
                s.git_url
                    .rsplit('/')
                    .next()
                    .unwrap_or(&s.git_url)
                    .to_string()
            })
            .unwrap_or_else(|| "scratch".into());

        // Wait for a matching live worker (create is already a long call).
        let deadline = std::time::Instant::now()
            + Duration::from_secs(self.worker_defaults.dispatch_timeout_secs);
        let worker = loop {
            if let Some(name) = self
                .fleet
                .pick_worker(&self.worker_defaults.image, req.worker.as_deref())
            {
                break name;
            }
            if std::time::Instant::now() > deadline {
                return Err(ApiError::unavailable(
                    "no live worker matched this session (image capability or pinning)",
                ));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        };

        let journal = Arc::new(Journal::new());
        let gate_slot = GateSlot::new();
        let gate: Arc<dyn ApprovalGate> = self.build_gate(&req, gate_slot.clone(), &journal);

        let id = uuid::Uuid::new_v4().simple().to_string();

        // Dispatch the start and await the worker's ack (materialisation can
        // clone a large repo — generous budget).
        let ack_id = uuid::Uuid::new_v4().simple().to_string();
        let rx = self.fleet.streams.await_op_result(&ack_id);
        self.fleet
            .streams
            .send(
                &worker,
                WorkerCommand::StartSession {
                    session_id: id.clone(),
                    exec_id: ack_id,
                    source: source.clone(),
                },
            )
            .map_err(|e| {
                ApiError::unavailable(format!(
                    "worker `{worker}` took the lease but is unreachable: {e:#}"
                ))
            })?;
        let ack = tokio::time::timeout(Duration::from_secs(600), rx)
            .await
            .map_err(|_| {
                ApiError::unavailable(format!(
                    "worker `{worker}` did not acknowledge session start in time"
                ))
            })?
            .map_err(|_| {
                ApiError::unavailable(format!("worker `{worker}` dropped the session start"))
            })?;
        if let smith_protocol::OpResult::Failed { message } = ack {
            return Err(ApiError::bad_request(format!(
                "worker `{worker}` failed to start the session: {message}"
            )));
        }

        // Sticky lease granted.
        self.fleet.board.grant(&id, &worker);
        self.fleet
            .metrics
            .leases_active
            .fetch_add(1, Ordering::Relaxed);

        // Harness-side assembly: agent over a mesh executor, keys stay here.
        let executor: Arc<dyn ToolExecutor> =
            Arc::new(WorkerExecutor::new(self.fleet.clone(), id.clone()));

        let provider_impl = self
            .factory
            .build_provider(req.provider.as_deref(), req.model.as_deref())
            .map_err(ApiError::from)?;
        let model_label = provider_impl.model().to_string();

        let logger = smith_core::SessionLogger::new_labelled(
            &repo_label,
            smith_core::workspace_identity_of(source.as_ref().map_or("scratch", |s| &s.git_url)),
            req.provider
                .as_deref()
                .unwrap_or(&self.worker_defaults.provider),
            &model_label,
        )
        .map_err(ApiError::from)?;

        let mut mcp_session = smith_mcp::McpSession::connect(&self.factory.mcp_config()).await;
        let mcp_status = mcp_session.status_lines();
        let mut tools = smith_core::builtin_tools();
        tools.extend(mcp_session.take_tools());

        let mut agent = Agent::new(
            provider_impl,
            executor.clone(),
            tools,
            gate,
            Some(Arc::new(logger)),
        );
        agent.environment_notes = Some(format!(
            "executor: worker `{worker}` (egress enforced on the worker)"
        ));
        // Project instructions live in the materialised repo — fetch over
        // the mesh, best-effort.
        for candidate in ["AGENTS.md", "CLAUDE.md"] {
            if let Ok(contents) = executor.read_file(candidate, None, None).await {
                agent.project_instructions = Some(contents.text);
                break;
            }
        }
        let system = agent.system_prompt().await;

        let handle = Arc::new(SessionHandle {
            id: id.clone(),
            agent: Arc::new(agent),
            journal,
            gate_slot,
            metrics: self.metrics.clone(),
            executor,
            history: tokio::sync::Mutex::new(vec![Message::system(system)]),
            busy: AtomicBool::new(false),
            cancel: CancellationToken::new(),
            labels: std::sync::Mutex::new(Labels {
                provider: req
                    .provider
                    .clone()
                    .unwrap_or_else(|| self.worker_defaults.provider.clone()),
                model: model_label,
                container: None,
                mcp_status,
                workspace: repo_label.clone(),
                workspace_id: smith_core::workspace_identity_of(
                    source.as_ref().map_or("scratch", |s| &s.git_url),
                ),
                executor: ExecutorKind::Worker,
                worker: Some(worker.clone()),
                repo: source.map(|s| s.git_url),
                lease_state: Some(LeaseState::Granted),
            }),
            created_ms: now_ms(),
            fleet: Some(self.fleet.clone()),
            _sandbox: None,
        });

        self.sessions
            .lock()
            .expect("session table mutex poisoned")
            .insert(id.clone(), handle.clone());

        tracing::info!("session {id} leased to worker `{worker}` (repo {repo_label})");
        handle.journal.publish_core(CoreEvent::Notice {
            text: format!(
                "[executing on worker `{worker}`; checkpoint branch smith/{}]",
                &id[..8.min(id.len())]
            ),
        });
        self.metrics
            .sessions
            .set("executor=\"worker\"", self.count_worker());
        let status = handle.status().await;
        handle.journal.publish(WireEvent::Status(status));
        Ok(id)
    }

    fn build_gate(
        &self,
        req: &CreateSession,
        slot: Arc<GateSlot>,
        journal: &Arc<Journal>,
    ) -> Arc<dyn ApprovalGate> {
        match req.approval_mode {
            ApprovalMode::Auto => {
                // Headless/CI semantics: everything allowed, sandbox (or the
                // worker's own enforcement) is the boundary.
                Arc::new(smith_core::AlwaysAllowGate)
            }
            ApprovalMode::Ask => Arc::new(WireGate::new(
                slot,
                self.approval_timeout,
                journal.clone(),
                self.metrics.clone(),
            )),
        }
    }

    fn count_local(&self) -> u64 {
        self.count_by(|labels| labels.executor == ExecutorKind::Local)
    }

    fn count_worker(&self) -> u64 {
        self.count_by(|labels| labels.executor == ExecutorKind::Worker)
    }

    fn count_by(&self, pred: impl Fn(&Labels) -> bool) -> u64 {
        self.sessions
            .lock()
            .expect("session table mutex poisoned")
            .values()
            .filter(|h| pred(&h.labels()))
            .count() as u64
    }

    /// Refresh the fleet gauges (workers by state, sessions by executor).
    pub fn refresh_fleet_gauges(&self) {
        let mut live = 0u64;
        let mut stale = 0u64;
        let mut revoked = 0u64;
        for w in self.fleet.registry.list() {
            match w.status {
                smith_protocol::WorkerStatus::Revoked => revoked += 1,
                _ if self.fleet.streams.connected(&w.name)
                    && std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0)
                        .saturating_sub(w.last_seen_ms)
                        < crate::workers::HEARTBEAT_SECS * 3 * 1000 =>
                {
                    live += 1
                }
                _ => stale += 1,
            }
        }
        self.metrics.workers.set("state=\"live\"", live);
        self.metrics.workers.set("state=\"stale\"", stale);
        self.metrics.workers.set("state=\"revoked\"", revoked);
        self.metrics
            .sessions
            .set("executor=\"local\"", self.count_local());
        self.metrics
            .sessions
            .set("executor=\"worker\"", self.count_worker());
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
            handle.metrics.approvals_total.inc(&format!(
                "decision=\"{}\"",
                match decision {
                    ApprovalDecision::AllowOnce => "allow_once",
                    ApprovalDecision::AllowAlways => "allow_always",
                    ApprovalDecision::Deny => "deny",
                }
            ));
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
        self.metrics.clearance_total.inc(&format!(
            "decision=\"{}\"",
            smith_protocol::decision_word(decision)
        ));
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
    pub async fn resume(
        &self,
        id: &str,
        prefix: &str,
    ) -> Result<smith_protocol::ResumeReply, ApiError> {
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
        Ok(smith_protocol::ResumeReply {
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

    /// Mark sessions' leases lost (fleet sweeper / revocation).
    pub fn handle_lease_loss(&self, worker: &str, session_ids: &[String]) {
        for session_id in session_ids {
            let Ok(handle) = self.get(session_id) else {
                continue;
            };
            {
                let mut labels = handle.labels();
                labels.lease_state = Some(LeaseState::Lost);
                labels.worker = Some(worker.to_string());
            }
            self.fleet
                .metrics
                .leases_lost_total
                .fetch_add(1, Ordering::Relaxed);
            handle.journal.publish_core(CoreEvent::Notice {
                text: format!(
                    "[lease lost: worker `{worker}` went away — the checkpoint branch smith/{} holds the work]",
                    &session_id[..8.min(session_id.len())]
                ),
            });
            handle.publish_status();
        }
    }

    pub fn clearance_target(&self) -> (u16, Option<String>) {
        self.clearance
            .lock()
            .expect("clearance mutex poisoned")
            .clone()
    }
}

fn non_empty(s: String) -> Option<String> {
    (!s.trim().is_empty()).then_some(s.trim().to_string())
}

/// One agent turn inside the daemon: user echo, event forwarding, busy
/// bookkeeping, a closing status — and a git checkpoint on worker sessions.
async fn turn_task(handle: Arc<SessionHandle>, input: String, cancel: CancellationToken) {
    // The user's prompt is an event so late joiners see it.
    handle.journal.publish_core(CoreEvent::UserMessage {
        text: input.clone(),
    });

    let (tx, mut rx) = mpsc::unbounded_channel::<CoreEvent>();
    let journal = handle.journal.clone();
    let metrics = handle.metrics.clone();
    let forwarder = tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            if let CoreEvent::ToolFinished { name, ok, .. } = &ev {
                metrics.tool_calls_total.inc(&format!(
                    "tool=\"{name}\",ok=\"{}\"",
                    if *ok { "true" } else { "false" }
                ));
            }
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

    // Disposal safety: checkpoint the worker's worktree at the turn end.
    if let Some(fleet) = &handle.fleet
        && let Some((worker, LeaseState::Granted)) = fleet.board.lease_of(&handle.id)
    {
        let _ = fleet.streams.send(
            &worker,
            WorkerCommand::Checkpoint {
                session_id: handle.id.clone(),
            },
        );
    }

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

/// Background sweep: stale workers lose their leases (with notice), and the
/// fleet gauges stay fresh.
pub fn spawn_fleet_sweeper(controller: Arc<Controller>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            for (worker, sessions) in controller.fleet.sweep() {
                tracing::warn!(
                    "worker `{worker}` went stale; {} lease(s) lost",
                    sessions.len()
                );
                controller.handle_lease_loss(&worker, &sessions);
            }
            controller.refresh_fleet_gauges();
        }
    })
}
