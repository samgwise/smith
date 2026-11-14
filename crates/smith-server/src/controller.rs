//! The controller: the daemon's session table and the harness operations
//! clients invoke over the wire — starting turns, deciding approvals,
//! switching providers, answering clearance requests, resuming logs, and
//! (M14) leasing workers from the fleet.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Context as _;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use smith_core::{Agent, ApprovalDecision, ApprovalGate, CoreEvent, Message, Tool, ToolExecutor};
use smith_protocol::{
    ApprovalMode, CreateSession, ExecutorKind, LeaseState, SessionStatus, SessionSummary,
    WireEvent, WorkerCommand, WorkspaceSource, render_diff,
};

use crate::config::OrchestrateSection;
use crate::factory::{ConfigFactory, SessionFactory};
use crate::forge::Forge;
use crate::gate::{GateSlot, WireGate};
use crate::journal::Journal;
use crate::orchestrator::{Orchestrator, PLANNER_BRIEFING, orchestration_tools};
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

/// Resolved workspace source for a create (request > config default).
pub(crate) struct ResolvedSource {
    pub source: Option<WorkspaceSource>,
    pub repo_label: String,
}

/// The assembled harness pieces of a worker session (from
/// [`Controller::assemble_worker_harness`]).
pub(crate) struct AssembledWorker {
    pub agent: Agent,
    pub executor: Arc<dyn ToolExecutor>,
    pub mcp_status: Vec<String>,
    pub provider_label: String,
    pub model_label: String,
}

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
    /// The agent — `None` while a queued orchestrated child waits for its
    /// worker lease (assembled in place at grant time).
    agent: std::sync::Mutex<Option<Arc<Agent>>>,
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
    /// Orchestrator for planning sessions (None for everyone else).
    pub orchestration: Option<Arc<Orchestrator>>,
    /// Keeps the sandbox handle alive (None in worker-mode and tests).
    _sandbox: Option<Arc<smith_sandbox::Sandbox>>,
}

impl SessionHandle {
    fn labels(&self) -> std::sync::MutexGuard<'_, Labels> {
        self.labels.lock().expect("labels mutex poisoned")
    }

    /// The agent, once assembled. Queued orchestrated children have none.
    pub fn agent(&self) -> Result<Arc<Agent>, ApiError> {
        self.agent
            .lock()
            .expect("agent mutex poisoned")
            .clone()
            .ok_or_else(|| ApiError::busy("session is queued — no worker lease granted yet"))
    }

    pub fn install_agent(&self, agent: Agent) {
        *self.agent.lock().expect("agent mutex poisoned") = Some(Arc::new(agent));
    }

    /// The tool executor seam (diffs, orchestration finding gathering).
    pub fn executor(&self) -> Arc<dyn ToolExecutor> {
        self.executor.clone()
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
            children: self
                .orchestration
                .as_ref()
                .map(|o| o.child_summaries())
                .unwrap_or_default(),
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

    pub(crate) fn publish_status(self: &Arc<Self>) {
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
    /// `[orchestrate]` bounds and knobs (M15).
    pub(crate) orchestrate: OrchestrateSection,
    /// Daemon state dir (M16): durable journals, `.create` session records
    /// and orchestration sidecars. None = in-memory only (tests).
    pub(crate) state_dir: Option<PathBuf>,
    /// The controller's forge (M17) — hosted repos + session git tokens.
    pub forge: Option<Arc<Forge>>,
    /// Advertised base URL for `smith:` repo references.
    pub(crate) public_url: Option<String>,
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
        let state_dir = state_dir_for(config);
        let public_url = config
            .serve
            .public_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        Self::with_parts(
            Arc::new(ConfigFactory {
                config: config.clone(),
            }),
            Duration::from_secs(config.serve.approval_timeout_secs.max(1)),
            fleet,
            metrics,
            defaults,
            config.orchestrate.clone(),
            Some(state_dir),
            public_url,
        )
    }

    /// Test constructor: explicit factory, timeout, fleet and defaults.
    /// `state_dir = None` keeps every journal in-memory (tests that don't
    /// exercise recovery); `Some(dir)` persists journals + session records.
    #[allow(clippy::too_many_arguments)]
    pub fn with_parts(
        factory: Arc<dyn SessionFactory>,
        approval_timeout: Duration,
        fleet: Arc<Fleet>,
        metrics: Arc<smith_protocol::metrics::Metrics>,
        worker_defaults: WorkerDefaults,
        orchestrate: OrchestrateSection,
        state_dir: Option<PathBuf>,
        public_url: Option<String>,
    ) -> Arc<Self> {
        if let Some(dir) = &state_dir
            && let Err(e) = std::fs::create_dir_all(dir)
        {
            tracing::warn!("cannot create state dir {}: {e:#}", dir.display());
        }
        let forge = state_dir.as_ref().map(|dir| Arc::new(Forge::new(dir)));
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
            orchestrate,
            state_dir,
            forge,
            public_url,
            metrics,
        })
    }

    /// Resolve the workspace source for a session, minting a scoped forge
    /// token when the repo is controller-hosted (`smith:<name>`). `mint =
    /// false` (queued-child creation) resolves labels only — the token is
    /// minted once, at grant time.
    fn prepare_source(
        &self,
        req: &CreateSession,
        session_id: &str,
        mint: bool,
    ) -> Result<ResolvedSource, ApiError> {
        let mut resolved = self.resolve_source(req);
        if let Some(source) = &mut resolved.source
            && let Some(name) = source.git_url.strip_prefix("smith:").map(str::to_string)
        {
            let base = self
                .public_url
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| {
                    ApiError::bad_request(format!(
                        "repo `{name}` is controller-hosted but [serve].public_url is not configured"
                    ))
                })?;
            source.git_url = format!("{}/git/{name}.git", base.trim_end_matches('/'));
            if mint {
                let forge = self
                    .forge
                    .as_ref()
                    .ok_or_else(|| ApiError::unavailable("no forge (no state dir)"))?;
                source.token = Some(
                    forge
                        .mint(session_id, &name, true, true)
                        .map_err(ApiError::from)?,
                );
            }
            resolved.repo_label = name;
        }
        Ok(resolved)
    }

    /// The durable journal for a session id (loaded from the state dir when
    /// configured — a restart resumes the ring and the seq counter).
    fn journal_for(&self, id: &str) -> Arc<Journal> {
        if let Some(dir) = &self.state_dir {
            match Journal::load_persisted(
                &dir.join(format!("{id}.wire")),
                crate::journal::RING_CAPACITY,
            ) {
                Ok(j) => return Arc::new(j),
                Err(e) => tracing::warn!("journal for {id} not loadable: {e:#}"),
            }
        }
        Arc::new(Journal::new())
    }

    /// Record the create request for a session (`.create` sidecar) — the
    /// recovery path rebuilds the harness from it.
    fn record_create(&self, id: &str, req: &CreateSession) {
        if let Some(dir) = &self.state_dir {
            let path = dir.join(format!("{id}.create"));
            match serde_json::to_string_pretty(req) {
                Ok(body) => {
                    if let Err(e) = std::fs::write(&path, body) {
                        tracing::warn!("cannot write {}: {e}", path.display());
                    }
                }
                Err(e) => tracing::warn!("cannot serialise the create record: {e}"),
            }
        }
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
    /// Orchestration opt-ins get the five planner tools and a planner
    /// briefing; the `Arc<Self>` receiver lets the orchestrator spawn its
    /// dispatcher against the same controller (Weak, no cycle).
    pub async fn create_session(self: &Arc<Self>, req: CreateSession) -> Result<String, ApiError> {
        let worker_mode =
            req.worker.is_some() || req.repo.is_some() || self.worker_defaults.executor == "worker";
        // Common harness plumbing: identity, durable journal, approval
        // routing — and the `.create` record the recovery path rebuilds
        // from.
        let id = uuid::Uuid::new_v4().simple().to_string();
        let journal = self.journal_for(&id);
        self.record_create(&id, &req);
        let gate_slot = GateSlot::new();
        let gate: Arc<dyn ApprovalGate> = self.build_gate(&req, gate_slot.clone(), &journal);
        let orchestration = req.orchestrate.then(|| {
            Orchestrator::new(
                id.clone(),
                self.orchestrate.clone(),
                self.factory.clone(),
                journal.clone(),
                Arc::downgrade(self),
                self.state_dir
                    .as_ref()
                    .map(|d| d.join(format!("{id}.orch"))),
            )
        });
        if worker_mode {
            self.create_worker_session(req, id, journal, gate_slot, gate, orchestration)
                .await
        } else {
            self.create_local_session(req, id, journal, gate_slot, gate, orchestration)
                .await
        }
    }

    /// Local (M13) path: the daemon's own podman assembles the sandbox.
    #[allow(clippy::too_many_arguments)]
    async fn create_local_session(
        &self,
        req: CreateSession,
        id: String,
        journal: Arc<Journal>,
        gate_slot: Arc<GateSlot>,
        gate: Arc<dyn ApprovalGate>,
        orchestration: Option<Arc<Orchestrator>>,
    ) -> Result<String, ApiError> {
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

        let mut parts = self
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

        // Planner opt-in: the orchestration tools join the registry and the
        // briefing joins the system prompt (computed below, after mutation).
        if let Some(orch) = &orchestration {
            parts.agent.tools.extend(orchestration_tools(orch.clone()));
            parts.agent.environment_notes = Some(match parts.agent.environment_notes.take() {
                Some(notes) => format!("{notes}\n{PLANNER_BRIEFING}"),
                None => PLANNER_BRIEFING.to_string(),
            });
        }
        let system = parts.agent.system_prompt().await;

        let handle = Arc::new(SessionHandle {
            id: id.clone(),
            agent: std::sync::Mutex::new(Some(Arc::new(parts.agent))),
            journal: journal.clone(),
            gate_slot,
            metrics: self.metrics.clone(),
            executor: parts.executor,
            history: tokio::sync::Mutex::new(vec![Message::system(system)]),
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
            orchestration,
            _sandbox: parts.sandbox,
        });

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

    /// Resolve the workspace source for a create (request > config default).
    /// No repo configured → a scratch workspace on the worker (still
    /// disposable; nothing to checkpoint).
    pub(crate) fn resolve_source(&self, req: &CreateSession) -> ResolvedSource {
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
            token: None,
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
        ResolvedSource { source, repo_label }
    }

    /// Wait for a matching live worker (the blocking single-session create;
    /// the dispatcher polls `pick_worker` instead).
    pub(crate) async fn wait_for_worker(&self, req: &CreateSession) -> Result<String, ApiError> {
        let deadline = std::time::Instant::now()
            + Duration::from_secs(self.worker_defaults.dispatch_timeout_secs);
        loop {
            if let Some(name) = self
                .fleet
                .pick_worker(&self.worker_defaults.image, req.worker.as_deref())
            {
                return Ok(name);
            }
            if std::time::Instant::now() > deadline {
                return Err(ApiError::unavailable(
                    "no live worker matched this session (image capability or pinning)",
                ));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Dispatch the session start and await the worker's ack (materialising
    /// can clone a large repo — generous budget).
    pub(crate) async fn dispatch_start(
        &self,
        worker: &str,
        id: &str,
        source: Option<WorkspaceSource>,
    ) -> Result<(), ApiError> {
        let ack_id = uuid::Uuid::new_v4().simple().to_string();
        let rx = self.fleet.streams.await_op_result(&ack_id);
        self.fleet
            .streams
            .send(
                worker,
                WorkerCommand::StartSession {
                    session_id: id.to_string(),
                    exec_id: ack_id,
                    source,
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
        Ok(())
    }

    /// Harness-side assembly for a worker session: agent over the mesh
    /// executor, keys stay here. Extra tools join the registry (orchestration
    /// parents); a briefing suffix joins the system prompt. The logger is
    /// injected by recovery (`Some`) so the JSONL continues across a
    /// restart; fresh creates pass `None` (a new log keyed by session id).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn assemble_worker_harness(
        &self,
        req: &CreateSession,
        id: &str,
        gate: Arc<dyn ApprovalGate>,
        worker: &str,
        resolved: &ResolvedSource,
        extra_tools: Vec<Box<dyn Tool>>,
        briefing: Option<&str>,
        logger: Option<smith_core::SessionLogger>,
    ) -> Result<AssembledWorker, ApiError> {
        let executor: Arc<dyn ToolExecutor> =
            Arc::new(WorkerExecutor::new(self.fleet.clone(), id.to_string()));

        let provider_impl = self
            .factory
            .build_provider(req.provider.as_deref(), req.model.as_deref())
            .map_err(ApiError::from)?;
        let model_label = provider_impl.model().to_string();

        let logger = match logger {
            Some(l) => l,
            None => smith_core::SessionLogger::new_labelled_in_with_id(
                &smith_core::sessions_dir().map_err(ApiError::from)?,
                id,
                &resolved.repo_label,
                smith_core::workspace_identity_of(
                    resolved.source.as_ref().map_or("scratch", |s| &s.git_url),
                ),
                req.provider
                    .as_deref()
                    .unwrap_or(&self.worker_defaults.provider),
                &model_label,
            )
            .map_err(ApiError::from)?,
        };

        let mut mcp_session = smith_mcp::McpSession::connect(&self.factory.mcp_config()).await;
        let mcp_status = mcp_session.status_lines();
        let mut tools = smith_core::builtin_tools();
        tools.extend(mcp_session.take_tools());
        tools.extend(extra_tools);

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
        if let Some(briefing) = briefing {
            let notes = agent.environment_notes.take().unwrap_or_default();
            agent.environment_notes = Some(format!("{notes}\n{briefing}"));
        }
        // Project instructions live in the materialised repo — fetch over
        // the mesh, best-effort.
        for candidate in ["AGENTS.md", "CLAUDE.md"] {
            if let Ok(contents) = agent.executor.read_file(candidate, None, None).await {
                agent.project_instructions = Some(contents.text);
                break;
            }
        }
        Ok(AssembledWorker {
            agent,
            executor,
            mcp_status,
            provider_label: req
                .provider
                .clone()
                .unwrap_or_else(|| self.worker_defaults.provider.clone()),
            model_label,
        })
    }

    /// Worker-mode path: the sandbox lives on a leased worker; the daemon
    /// holds only the harness (agent, history, approvals, keys). Blocking
    /// create: waits for a matching worker (single-session creates keep
    /// today's behaviour; orchestrated children use the queued path).
    #[allow(clippy::too_many_arguments)]
    async fn create_worker_session(
        &self,
        req: CreateSession,
        id: String,
        journal: Arc<Journal>,
        gate_slot: Arc<GateSlot>,
        gate: Arc<dyn ApprovalGate>,
        orchestration: Option<Arc<Orchestrator>>,
    ) -> Result<String, ApiError> {
        let resolved = self.prepare_source(&req, &id, true)?;
        let worker = self.wait_for_worker(&req).await?;
        self.dispatch_start(&worker, &id, resolved.source.clone())
            .await?;

        // Sticky lease granted.
        self.fleet.board.grant(&id, &worker);
        self.fleet
            .metrics
            .leases_active
            .fetch_add(1, Ordering::Relaxed);

        let briefing = orchestration.as_ref().map(|_| PLANNER_BRIEFING);
        let extra_tools = orchestration
            .as_ref()
            .map(|orch| orchestration_tools(orch.clone()))
            .unwrap_or_default();
        let assembled = self
            .assemble_worker_harness(
                &req,
                &id,
                gate,
                &worker,
                &resolved,
                extra_tools,
                briefing,
                None,
            )
            .await?;
        let system = assembled.agent.system_prompt().await;
        let repo_label = resolved.repo_label.clone();
        let repo_url = resolved
            .source
            .as_ref()
            .map(|s| s.git_url.clone())
            .unwrap_or_else(|| "scratch".into());

        let handle = Arc::new(SessionHandle {
            id: id.clone(),
            agent: std::sync::Mutex::new(Some(Arc::new(assembled.agent))),
            journal,
            gate_slot,
            metrics: self.metrics.clone(),
            executor: assembled.executor,
            history: tokio::sync::Mutex::new(vec![Message::system(system)]),
            busy: AtomicBool::new(false),
            cancel: CancellationToken::new(),
            labels: std::sync::Mutex::new(Labels {
                provider: assembled.provider_label,
                model: assembled.model_label,
                container: None,
                mcp_status: assembled.mcp_status,
                workspace: repo_label.clone(),
                workspace_id: smith_core::workspace_identity_of(
                    resolved.source.as_ref().map_or("scratch", |s| &s.git_url),
                ),
                executor: ExecutorKind::Worker,
                worker: Some(worker.clone()),
                repo: Some(repo_url),
                lease_state: Some(LeaseState::Granted),
            }),
            created_ms: now_ms(),
            fleet: Some(self.fleet.clone()),
            orchestration,
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

    /// Create a queued orchestrated child: journal, gate and labels exist
    /// immediately; the agent/executor assembly happens when a lease is
    /// granted (lazy assembly — parallel spawns never serialise on worker
    /// availability).
    pub(crate) async fn create_queued_child(
        &self,
        req: &CreateSession,
        title: &str,
    ) -> Result<String, ApiError> {
        let resolved = self.prepare_source(req, "", false)?;
        let id = uuid::Uuid::new_v4().simple().to_string();
        let journal = self.journal_for(&id);
        self.record_create(&id, req);
        let gate_slot = GateSlot::new();
        // The approval gate itself is built at grant time (the dispatcher
        // rebuilds it from the same slot and journal — no turn can run
        // before assembly anyway).

        let handle = Arc::new(SessionHandle {
            id: id.clone(),
            agent: std::sync::Mutex::new(None),
            journal,
            gate_slot,
            metrics: self.metrics.clone(),
            executor: Arc::new(WorkerExecutor::new(self.fleet.clone(), id.clone())),
            history: tokio::sync::Mutex::new(Vec::new()),
            busy: AtomicBool::new(false),
            cancel: CancellationToken::new(),
            labels: std::sync::Mutex::new(Labels {
                provider: req
                    .provider
                    .clone()
                    .unwrap_or_else(|| self.worker_defaults.provider.clone()),
                model: req
                    .model
                    .clone()
                    .unwrap_or_else(|| self.worker_defaults.model.clone()),
                container: None,
                mcp_status: vec![],
                workspace: resolved.repo_label.clone(),
                workspace_id: smith_core::workspace_identity_of(
                    resolved.source.as_ref().map_or("scratch", |s| &s.git_url),
                ),
                executor: ExecutorKind::Worker,
                worker: None,
                repo: Some(
                    resolved
                        .source
                        .as_ref()
                        .map(|s| s.git_url.clone())
                        .unwrap_or_else(|| "scratch".into()),
                ),
                lease_state: Some(LeaseState::Queued),
            }),
            created_ms: now_ms(),
            fleet: Some(self.fleet.clone()),
            orchestration: None,
            _sandbox: None,
        });

        self.sessions
            .lock()
            .expect("session table mutex poisoned")
            .insert(id.clone(), handle.clone());
        handle.journal.publish_core(CoreEvent::Notice {
            text: format!("[queued subtask `{title}`: waiting for a worker lease]"),
        });
        self.metrics
            .sessions
            .set("executor=\"worker\"", self.count_worker());
        handle.publish_status();
        Ok(id)
    }

    /// Grant-time assembly for a queued orchestrated child: pick a worker,
    /// dispatch the start, grant the lease and assemble the harness in
    /// place. `Ok(false)` = no worker free yet (the dispatcher retries).
    pub(crate) async fn dispatch_queued_child(
        &self,
        req: &CreateSession,
        id: &str,
    ) -> Result<bool, ApiError> {
        let Some(worker) = self
            .fleet
            .pick_worker(&self.worker_defaults.image, req.worker.as_deref())
        else {
            return Ok(false);
        };
        let handle = self.get(id)?;
        let resolved = self.prepare_source(req, id, true)?;
        self.dispatch_start(&worker, id, resolved.source.clone())
            .await?;

        // Sticky lease granted.
        self.fleet.board.grant(id, &worker);
        self.fleet
            .metrics
            .leases_active
            .fetch_add(1, Ordering::Relaxed);

        let gate = self.build_gate(req, handle.gate_slot.clone(), &handle.journal);
        let assembled = self
            .assemble_worker_harness(req, id, gate, &worker, &resolved, vec![], None, None)
            .await?;
        let system = assembled.agent.system_prompt().await;
        handle.install_agent(assembled.agent);
        *handle.history.lock().await = vec![Message::system(system)];
        {
            let mut labels = handle.labels();
            labels.worker = Some(worker.clone());
            labels.lease_state = Some(LeaseState::Granted);
            labels.mcp_status = assembled.mcp_status;
            labels.provider = assembled.provider_label;
            labels.model = assembled.model_label;
        }
        tracing::info!("child session {id} leased to worker `{worker}`");
        handle.journal.publish_core(CoreEvent::Notice {
            text: format!(
                "[executing on worker `{worker}`; checkpoint branch smith/{}]",
                &id[..8.min(id.len())]
            ),
        });
        self.metrics
            .sessions
            .set("executor=\"worker\"", self.count_worker());
        handle.publish_status();
        Ok(true)
    }

    /// Release a session's lease explicitly (orchestration finish): stop
    /// the worker-side sandbox (best-effort), free the board slot, publish
    /// the final status. Returns the worker that held it, if any.
    pub(crate) fn release_session(&self, id: &str) -> Result<Option<String>, ApiError> {
        let handle = self.get(id)?;
        // A queued child has a label-lease but no board record yet; a
        // granted one has both. Release clears what exists.
        let worker = self.fleet.board.release(id).filter(|w| !w.is_empty());
        if worker.is_some() {
            self.fleet
                .metrics
                .leases_active
                .fetch_sub(1, Ordering::Relaxed);
        }
        {
            let mut labels = handle.labels();
            if labels.executor == ExecutorKind::Worker {
                labels.lease_state = Some(LeaseState::Released);
            }
        }
        if let Some(worker) = &worker {
            let _ = self.fleet.streams.send(
                worker,
                WorkerCommand::StopSession {
                    session_id: id.to_string(),
                },
            );
        }
        handle.publish_status();
        Ok(worker)
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
        // A queued orchestrated child has no agent to run a turn with yet.
        handle.agent()?;
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
        handle.agent()?.set_provider(new_provider);

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
            let system = handle.agent()?.system_prompt().await;
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
            // Orchestrators hear about their children's losses.
            for parent in self
                .sessions
                .lock()
                .expect("session table mutex poisoned")
                .values()
                .filter_map(|h| h.orchestration.clone())
            {
                parent.note_lease_loss(session_id);
            }
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

    /// Re-learn leases from a worker's heartbeat (the add direction of
    /// reconcile, M16): after a DAEMON restart the board is empty while the
    /// worker still holds its sandboxes; the first heartbeat re-grants.
    /// Never overrides a `Lost` — the sweeper's decision stands.
    pub fn relearn_leases(&self, worker: &str, live_leases: &[String]) {
        for session_id in live_leases {
            let Ok(handle) = self.get(session_id) else {
                continue; // unknown session: nothing to re-learn
            };
            match self.fleet.board.lease_of(session_id) {
                Some(_) => {} // already granted (or lost — sweeper wins)
                None => {
                    self.fleet.board.grant(session_id, worker);
                    self.fleet
                        .metrics
                        .leases_active
                        .fetch_add(1, Ordering::Relaxed);
                    {
                        let mut labels = handle.labels();
                        labels.worker = Some(worker.to_string());
                        labels.lease_state = Some(LeaseState::Granted);
                    }
                    handle.journal.publish_core(CoreEvent::Notice {
                        text: format!(
                            "[lease re-learned: worker `{worker}` still holds this session]"
                        ),
                    });
                    handle.publish_status();
                }
            }
        }
    }

    /// Recover hosted sessions from the state dir (M16): rebuild each
    /// worker-mode session's harness from its `.wire` journal (labels from
    /// the last status snapshot), its `.create` record and its JSONL
    /// transcript; re-queue undispached children; re-arm orchestration
    /// parents from their `.orch` sidecars. Local-mode sessions are skipped
    /// (dev-mode; their transcripts remain `smith resume`-able). Returns
    /// the number of sessions recovered.
    pub async fn recover(self: &Arc<Self>) -> usize {
        let Some(state_dir) = self.state_dir.clone() else {
            return 0;
        };
        // Forge tokens first: recovered sessions' delivered tokens stay
        // valid across the restart.
        if let Some(forge) = &self.forge {
            let n = forge.recover();
            if n > 0 {
                tracing::info!("forge: {n} session token grant(s) recovered");
            }
        }
        let mut orchestrators: Vec<Arc<Orchestrator>> = Vec::new();
        let mut recovered = 0usize;
        let Ok(entries) = std::fs::read_dir(&state_dir) else {
            return 0;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_none_or(|e| e != "wire") {
                continue;
            }
            let Some(id) = path.file_stem().map(|s| s.to_string_lossy().into_owned()) else {
                continue;
            };
            match self.recover_session(&id, &state_dir).await {
                Ok(Some(orch)) => {
                    orchestrators.push(orch);
                    recovered += 1;
                }
                Ok(None) => {
                    recovered += 1;
                }
                Err(e) => tracing::warn!("session {} not recovered: {e:#}", &id[..8.min(id.len())]),
            }
        }
        // Second pass: dispatchers and trackers — only safe once every
        // session (parents AND children) is back in the table.
        for orch in &orchestrators {
            orch.resume_after_daemon_restart(self);
        }
        if recovered > 0 {
            tracing::info!(
                "recovered {recovered} hosted session(s) from {}",
                state_dir.display()
            );
            self.refresh_fleet_gauges();
        }
        recovered
    }

    /// Rebuild one session from its state-dir records.
    /// `Ok(Some(orch))` = recovered with an orchestration sidecar (the
    /// caller re-arms it in the second pass); `Ok(None)` = recovered plain.
    async fn recover_session(
        self: &Arc<Self>,
        id: &str,
        state_dir: &Path,
    ) -> anyhow::Result<Option<Arc<Orchestrator>>> {
        let journal = self.journal_for(id);
        // Labels come from the last journalled status snapshot.
        let status = journal
            .replay(0)
            .into_iter()
            .filter_map(|env| match env.event {
                WireEvent::Status(s) => Some(s),
                _ => None,
            })
            .next_back()
            .context("no status snapshot in the journal")?;
        let raw = std::fs::read_to_string(state_dir.join(format!("{id}.create")))
            .context("no .create record")?;
        let req: CreateSession = serde_json::from_str(&raw).context("bad .create record")?;
        anyhow::ensure!(
            req.worker.is_some() || req.repo.is_some() || self.worker_defaults.executor == "worker",
            "local-mode session: not recovered (the transcript remains `smith resume`-able)"
        );

        // Orchestration parent? Rebuild from the sidecar first — the agent
        // registry needs the planner tools before the first turn.
        let sidecar = state_dir.join(format!("{id}.orch"));
        let orchestration = if sidecar.exists() {
            Orchestrator::recover(
                &sidecar,
                id.to_string(),
                self.orchestrate.clone(),
                self.factory.clone(),
                journal.clone(),
                Arc::downgrade(self),
            )
        } else {
            None
        };
        if sidecar.exists() && orchestration.is_none() {
            tracing::warn!("session {id}: .orch sidecar unreadable; recovering without it");
        }

        let gate_slot = GateSlot::new();
        let gate: Arc<dyn ApprovalGate> = self.build_gate(&req, gate_slot.clone(), &journal);
        let resolved = self.resolve_source(&req);
        let was_busy = status.busy;
        let lease = status.lease_state;
        let worker_label = status.worker.clone();

        let (agent, executor, mcp_status, provider_label, model_label, history) =
            match (lease, worker_label.as_deref()) {
                // Undispached child: no transcript, no agent — the
                // dispatcher re-assembles at grant time.
                (Some(LeaseState::Queued), _) => (
                    None,
                    None,
                    Vec::new(),
                    status.provider.clone(),
                    status.model.clone(),
                    Vec::new(),
                ),
                (Some(lease), Some(worker)) if lease != LeaseState::Queued => {
                    let jsonl = smith_core::sessions_dir()?.join(format!("{id}.jsonl"));
                    anyhow::ensure!(jsonl.exists(), "no transcript for this session");
                    let logger = smith_core::SessionLogger::open_for_append(&jsonl)?;
                    let briefing = orchestration.as_ref().map(|_| PLANNER_BRIEFING);
                    let extra_tools = orchestration
                        .as_ref()
                        .map(|o| orchestration_tools(o.clone()))
                        .unwrap_or_default();
                    // Trust-then-verify: re-grant on the recorded worker
                    // immediately; the next heartbeat or sweep verifies.
                    if lease == LeaseState::Granted {
                        self.fleet.board.grant(id, worker);
                        self.fleet
                            .metrics
                            .leases_active
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    let assembled = self
                        .assemble_worker_harness(
                            &req,
                            id,
                            gate,
                            worker,
                            &resolved,
                            extra_tools,
                            briefing,
                            Some(logger),
                        )
                        .await?;
                    let system = assembled.agent.system_prompt().await;
                    let (_, mut history) = smith_core::load_history(&jsonl)?;
                    history.insert(0, Message::system(system));
                    (
                        Some(assembled.agent),
                        Some(assembled.executor),
                        assembled.mcp_status,
                        assembled.provider_label,
                        assembled.model_label,
                        history,
                    )
                }
                _ => anyhow::bail!("unrecoverable lease state {lease:?}"),
            };

        let handle = Arc::new(SessionHandle {
            id: id.to_string(),
            agent: std::sync::Mutex::new(agent.map(Arc::new)),
            journal: journal.clone(),
            gate_slot,
            metrics: self.metrics.clone(),
            executor: executor.unwrap_or_else(|| {
                Arc::new(WorkerExecutor::new(self.fleet.clone(), id.to_string()))
            }),
            history: tokio::sync::Mutex::new(history),
            busy: AtomicBool::new(false),
            cancel: CancellationToken::new(),
            labels: std::sync::Mutex::new(Labels {
                provider: provider_label,
                model: model_label,
                container: None,
                mcp_status,
                workspace: status.workspace.clone(),
                workspace_id: status.workspace_id.clone(),
                executor: status.executor,
                worker: worker_label.clone(),
                repo: status.repo.clone(),
                lease_state: lease,
            }),
            created_ms: status.created_ms,
            fleet: Some(self.fleet.clone()),
            orchestration: orchestration.clone(),
            _sandbox: None,
        });

        self.sessions
            .lock()
            .expect("session table mutex poisoned")
            .insert(id.to_string(), handle.clone());
        handle.journal.publish_core(CoreEvent::Notice {
            text: format!(
                "[daemon restarted; session recovered — transcript continued{}]",
                if was_busy {
                    " (an in-flight turn was abandoned; work since the last checkpoint is on the branch)"
                } else {
 ""
                }
            ),
        });
        self.metrics
            .sessions
            .set("executor=\"worker\"", self.count_worker());
        handle.publish_status();
        Ok(orchestration)
    }
}

fn non_empty(s: String) -> Option<String> {
    (!s.trim().is_empty()).then_some(s.trim().to_string())
}

/// Resolve the daemon state dir (default `~/.smith/serve`); `~` expands.
fn state_dir_for(config: &crate::config::Config) -> PathBuf {
    match config.serve.state_dir.as_deref() {
        Some(dir) if !dir.trim().is_empty() => {
            let trimmed = dir.trim();
            match trimmed.strip_prefix("~/") {
                Some(rest) => match std::env::var("USERPROFILE").or_else(|_| std::env::var("HOME"))
                {
                    Ok(home) => PathBuf::from(home).join(rest),
                    Err(_) => PathBuf::from(trimmed),
                },
                None => PathBuf::from(trimmed),
            }
        }
        _ => smith_core::session::smith_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("serve"),
    }
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

    let Ok(agent) = handle.agent() else {
        // Queued orchestrated child: no worker yet; surface and stop.
        handle.journal.publish_core(CoreEvent::Error {
            message: "turn rejected: session is queued (no worker lease yet)".into(),
        });
        handle.busy.store(false, Ordering::SeqCst);
        let status = handle.status().await;
        handle.journal.publish(WireEvent::Status(status));
        return;
    };
    let mut history = handle.history.lock().await;
    let result = agent.run_turn(&mut history, &input, tx, cancel).await;
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
