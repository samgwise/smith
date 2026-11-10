//! The fleet: worker registry (enrolment + revocable credentials), the
//! live command streams, and the lease board.
//!
//! Trust model (the linguine split): the controller **mints access keys**
//! (opaque random tokens, stored hashed, revocable); workers verify the
//! controller by its TLS certificate (the reverse proxy in front of
//! `smith serve`), which also encrypts the traffic. No signing.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use sha2::Sha256;
use tokio::sync::mpsc;

use smith_protocol::{
    Capability, ExecOp, LeaseState, Load, OpResult, WorkerCommand, WorkerStatus, metrics::Metrics,
};

/// Heartbeats per worker before it is marked stale.
const STALE_MISSES: u32 = 3;
/// Default heartbeat cadence (worker-driven; the sweep uses the timestamp).
pub const HEARTBEAT_SECS: u64 = 10;
/// Margin of the lease/queue wait on op round trips.
const OP_TIMEOUT: Duration = Duration::from_secs(300);

fn token_hash(token: &str) -> String {
    use sha2::Digest as _;
    let digest = Sha256::digest(token.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ------------------------------------------------------------ registry --

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct WorkerRecord {
    id: String,
    token_hash: String,
    status: WorkerStatus,
    created_ms: u64,
    #[serde(default)]
    capability: Capability,
    #[serde(default)]
    last_heartbeat_ms: u64,
    #[serde(default)]
    load: Load,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct EnrolmentRecord {
    /// Hash of the single-use enrolment token.
    token_hash: String,
    name: String,
    used: bool,
    created_ms: u64,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct RegistryFile {
    #[serde(default)]
    enrolments: Vec<EnrolmentRecord>,
    #[serde(default)]
    workers: HashMap<String, WorkerRecord>, // keyed by name
}

/// Readable worker info for `/v1/workers` and the scheduler.
#[derive(Clone, Debug)]
pub struct WorkerInfo {
    pub name: String,
    pub id: String,
    pub status: WorkerStatus,
    pub capability: Capability,
    pub load: Load,
    /// Milliseconds since the last heartbeat.
    pub last_seen_ms: u64,
}

/// Persistent registry: enrolment tokens and worker credentials, hashed.
pub struct WorkerRegistry {
    path: PathBuf,
    state: std::sync::Mutex<RegistryFile>,
}

impl WorkerRegistry {
    pub fn load(path: PathBuf) -> Self {
        let state = std::fs::read_to_string(&path)
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default();
        Self {
            path,
            state: std::sync::Mutex::new(state),
        }
    }

    fn persist(&self, state: &RegistryFile) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(&self.path, serde_json::to_string_pretty(state)?)
            .with_context(|| format!("writing {}", self.path.display()))
    }

    /// Mint a single-use enrolment token for `name`.
    pub fn issue_enrolment(&self, name: &str) -> anyhow::Result<String> {
        let token = uuid::Uuid::new_v4().simple().to_string();
        let mut state = self.state.lock().expect("registry poisoned");
        state.enrolments.push(EnrolmentRecord {
            token_hash: token_hash(&token),
            name: name.to_string(),
            used: false,
            created_ms: now_ms(),
        });
        self.persist(&state)?;
        Ok(token)
    }

    /// Exchange a single-use enrolment token for a worker credential.
    pub fn enrol(
        &self,
        name: &str,
        enrolment_token: &str,
        capability: &Capability,
    ) -> anyhow::Result<(String, String)> {
        let mut state = self.state.lock().expect("registry poisoned");
        let hash = token_hash(enrolment_token);
        let enrolment = state
            .enrolments
            .iter_mut()
            .find(|e| e.token_hash == hash && !e.used)
            .context("unknown or already-used enrolment token")?;
        anyhow::ensure!(
            enrolment.name == name,
            "enrolment token was issued for `{}`, not `{name}`",
            enrolment.name
        );
        enrolment.used = true;

        let worker_token = uuid::Uuid::new_v4().simple().to_string();
        let id = uuid::Uuid::new_v4().simple().to_string();
        let record = WorkerRecord {
            id: id.clone(),
            token_hash: token_hash(&worker_token),
            status: WorkerStatus::Active,
            created_ms: now_ms(),
            capability: capability.clone(),
            last_heartbeat_ms: now_ms(),
            load: Load::default(),
        };
        state.workers.insert(name.to_string(), record);
        self.persist(&state)?;
        tracing::info!("worker `{name}` enrolled (id {id})");
        Ok((id, worker_token))
    }

    /// Authenticate a worker token → worker name. Revoked workers fail.
    pub fn authenticate(&self, worker_token: &str) -> Option<String> {
        let hash = token_hash(worker_token);
        let state = self.state.lock().expect("registry poisoned");
        state
            .workers
            .iter()
            .find(|(_, w)| w.token_hash == hash && w.status == WorkerStatus::Active)
            .map(|(name, _)| name.clone())
    }

    pub fn revoke(&self, name: &str) -> anyhow::Result<bool> {
        let mut state = self.state.lock().expect("registry poisoned");
        let Some(worker) = state.workers.get_mut(name) else {
            return Ok(false);
        };
        worker.status = WorkerStatus::Revoked;
        self.persist(&state)?;
        tracing::info!("worker `{name}` revoked");
        Ok(true)
    }

    pub fn get(&self, name: &str) -> Option<WorkerInfo> {
        let state = self.state.lock().expect("registry poisoned");
        state.workers.get(name).map(|w| WorkerInfo {
            name: name.to_string(),
            id: w.id.clone(),
            status: w.status,
            capability: w.capability.clone(),
            load: w.load,
            last_seen_ms: w.last_heartbeat_ms,
        })
    }

    pub fn list(&self) -> Vec<WorkerInfo> {
        let state = self.state.lock().expect("registry poisoned");
        let mut all: Vec<WorkerInfo> = state
            .workers
            .iter()
            .map(|(name, w)| WorkerInfo {
                name: name.clone(),
                id: w.id.clone(),
                status: w.status,
                capability: w.capability.clone(),
                load: w.load,
                last_seen_ms: w.last_heartbeat_ms,
            })
            .collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        all
    }

    /// Record a heartbeat; updates capability/load/liveness.
    pub fn record_heartbeat(&self, name: &str, capability: &Capability, load: &Load) -> bool {
        let mut state = self.state.lock().expect("registry poisoned");
        let Some(worker) = state.workers.get_mut(name) else {
            return false;
        };
        worker.capability = capability.clone();
        worker.load = *load;
        worker.last_heartbeat_ms = now_ms();
        let _ = self.persist(&state);
        true
    }
}

// ------------------------------------------------------- live streams --

/// Live command streams + pending op results for connected workers.
#[derive(Default)]
pub struct WorkerStreams {
    /// Worker name → its command queue (drained by its SSE handler).
    streams: std::sync::Mutex<HashMap<String, mpsc::UnboundedSender<WorkerCommand>>>,
    /// exec_id → the waiting requester's oneshot.
    pending: std::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<OpResult>>>,
}

impl WorkerStreams {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a connected worker's command queue (replaces any stale one).
    pub fn connect(&self, name: &str) -> mpsc::UnboundedReceiver<WorkerCommand> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.streams
            .lock()
            .expect("streams poisoned")
            .insert(name.to_string(), tx);
        rx
    }

    pub fn disconnect(&self, name: &str) {
        self.streams.lock().expect("streams poisoned").remove(name);
    }

    pub fn connected(&self, name: &str) -> bool {
        self.streams
            .lock()
            .expect("streams poisoned")
            .contains_key(name)
    }

    /// Push a command to a connected worker. Errors when the worker is not
    /// connected (callers decide between queueing and failing).
    pub fn send(&self, name: &str, command: WorkerCommand) -> anyhow::Result<()> {
        let tx = self
            .streams
            .lock()
            .expect("streams poisoned")
            .get(name)
            .cloned()
            .context(format!("worker `{name}` is not connected"))?;
        tx.send(command)
            .map_err(|_| anyhow::anyhow!("worker `{name}` stream closed"))
    }

    /// Register a waiter for an op result before sending the command.
    pub fn await_op_result(&self, exec_id: &str) -> tokio::sync::oneshot::Receiver<OpResult> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending
            .lock()
            .expect("pending poisoned")
            .insert(exec_id.to_string(), tx);
        rx
    }

    /// Deliver an op result from the worker (route handler).
    pub fn deliver_op_result(&self, exec_id: &str, result: OpResult) -> bool {
        if let Some(waiter) = self
            .pending
            .lock()
            .expect("pending poisoned")
            .remove(exec_id)
        {
            waiter.send(result).is_ok()
        } else {
            false
        }
    }
}

// --------------------------------------------------------- lease board --

#[derive(Clone, Debug)]
struct LeaseRecord {
    worker: String,
    state: LeaseState,
    queued_since_ms: u64,
}

/// The lease board: session → (worker, state). Sticky: a granted lease
/// survives until release or loss.
#[derive(Default)]
pub struct LeaseBoard {
    leases: std::sync::Mutex<HashMap<String, LeaseRecord>>,
}

impl LeaseBoard {
    pub fn lease_of(&self, session_id: &str) -> Option<(String, LeaseState)> {
        self.leases
            .lock()
            .expect("board poisoned")
            .get(session_id)
            .map(|r| (r.worker.clone(), r.state))
    }

    pub fn grant(&self, session_id: &str, worker: &str) {
        self.leases.lock().expect("board poisoned").insert(
            session_id.to_string(),
            LeaseRecord {
                worker: worker.to_string(),
                state: LeaseState::Granted,
                queued_since_ms: now_ms(),
            },
        );
    }

    pub fn queue(&self, session_id: &str) {
        self.leases.lock().expect("board poisoned").insert(
            session_id.to_string(),
            LeaseRecord {
                worker: String::new(),
                state: LeaseState::Queued,
                queued_since_ms: now_ms(),
            },
        );
    }

    /// Mark a lease lost; returns the worker that held it.
    pub fn lose(&self, session_id: &str) -> Option<String> {
        let mut leases = self.leases.lock().expect("board poisoned");
        let record = leases.get_mut(session_id)?;
        if record.state == LeaseState::Lost {
            return None;
        }
        record.state = LeaseState::Lost;
        Some(record.worker.clone())
    }

    /// Sessions waiting for a worker.
    pub fn queued(&self) -> Vec<String> {
        self.leases
            .lock()
            .expect("board poisoned")
            .iter()
            .filter(|(_, r)| r.state == LeaseState::Queued)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Sessions leased to a specific worker.
    pub fn sessions_of(&self, worker: &str) -> Vec<String> {
        self.leases
            .lock()
            .expect("board poisoned")
            .iter()
            .filter(|(_, r)| r.worker == worker && r.state == LeaseState::Granted)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Wait time of a queued session (for the queue-wait metric).
    pub fn queued_since(&self, session_id: &str) -> Option<u64> {
        self.leases
            .lock()
            .expect("board poisoned")
            .get(session_id)
            .map(|r| r.queued_since_ms)
    }
}

// --------------------------------------------------------------- fleet --

/// Registry + streams + board, with the scheduling policy.
pub struct Fleet {
    pub registry: WorkerRegistry,
    pub streams: WorkerStreams,
    pub board: LeaseBoard,
    pub metrics: Arc<Metrics>,
    connected: AtomicBool,
}

impl Fleet {
    pub fn new(registry_path: PathBuf, metrics: Arc<Metrics>) -> Self {
        Self {
            registry: WorkerRegistry::load(registry_path),
            streams: WorkerStreams::new(),
            board: LeaseBoard::default(),
            metrics,
            connected: AtomicBool::new(false),
        }
    }

    /// Pick a live worker: honour an explicit name prefix, otherwise
    /// least-loaded with a matching sandbox image.
    pub fn pick_worker(&self, image: &str, pinned: Option<&str>) -> Option<String> {
        let workers = self.registry.list();
        let mut candidates: Vec<&WorkerInfo> = workers
            .iter()
            .filter(|w| {
                w.status == WorkerStatus::Active
                    && self.streams.connected(&w.name)
                    && (pinned.is_none_or(|p| w.name.starts_with(p)))
                    && (w.capability.image.is_empty() || w.capability.image == image)
            })
            .collect();
        candidates.sort_by_key(|w| (w.load.leases, w.load.cpu_percent));
        candidates.first().map(|w| w.name.clone())
    }

    /// Send one op to the worker holding the lease and await its result.
    pub async fn exec_op(&self, session_id: &str, op: ExecOp) -> anyhow::Result<OpResult> {
        let (worker, state) = self
            .board
            .lease_of(session_id)
            .context("session has no lease (not a worker-mode session)")?;
        anyhow::ensure!(
            state == LeaseState::Granted,
            "session lease is {state:?}; cannot execute ops"
        );
        let exec_id = uuid::Uuid::new_v4().simple().to_string();
        let rx = self.streams.await_op_result(&exec_id);
        self.streams.send(
            &worker,
            WorkerCommand::Exec {
                session_id: session_id.to_string(),
                exec_id: exec_id.clone(),
                op,
            },
        )?;
        let result = tokio::time::timeout(OP_TIMEOUT, rx)
            .await
            .map_err(|_| anyhow::anyhow!("worker `{worker}` did not answer the op in time"))?
            .map_err(|_| anyhow::anyhow!("worker `{worker}` dropped the op"))?;
        Ok(result)
    }

    /// Reconcile the board to a worker's reported live leases; returns the
    /// session ids marked lost (the worker no longer holds them).
    pub fn reconcile(&self, worker: &str, live_leases: &[String]) -> Vec<String> {
        let mine = self.board.sessions_of(worker);
        let mut lost = vec![];
        for session_id in mine {
            if !live_leases.contains(&session_id) && self.board.lose(&session_id).is_some() {
                lost.push(session_id);
            }
        }
        lost
    }

    /// Sweep for stale workers; returns (worker, lost sessions) pairs.
    pub fn sweep(&self) -> Vec<(String, Vec<String>)> {
        let ttl_ms = (HEARTBEAT_SECS * STALE_MISSES as u64) * 1000;
        let now = now_ms();
        let mut out = vec![];
        for worker in self.registry.list() {
            let alive = worker.status == WorkerStatus::Active
                && now.saturating_sub(worker.last_seen_ms) < ttl_ms;
            if !alive {
                for session_id in self.board.sessions_of(&worker.name) {
                    if self.board.lose(&session_id).is_some() {
                        out.push((worker.name.clone(), vec![session_id]));
                    }
                }
            }
        }
        out
    }

    /// Whether the fleet has any live connected worker (used by metrics).
    pub fn has_live_workers(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    pub fn set_connected(&self, any: bool) {
        self.connected.store(any, Ordering::Relaxed);
    }
}

/// The path of the worker registry: `~/.smith/workers.json`.
pub fn workers_registry_path() -> anyhow::Result<PathBuf> {
    Ok(smith_core::session::smith_dir()?.join("workers.json"))
}

// ----------------------------------------------------- mesh executor --

/// A `ToolExecutor` whose ops cross the mesh to the worker holding the
/// session's lease. The agent loop cannot tell this apart from a local
/// executor — the seam doing its job.
pub struct WorkerExecutor {
    fleet: Arc<Fleet>,
    session_id: String,
}

impl WorkerExecutor {
    pub fn new(fleet: Arc<Fleet>, session_id: String) -> Self {
        Self { fleet, session_id }
    }

    async fn exec(&self, op: ExecOp) -> anyhow::Result<OpResult> {
        self.fleet.exec_op(&self.session_id, op).await
    }
}

#[async_trait]
impl smith_core::ToolExecutor for WorkerExecutor {
    async fn exec_shell(
        &self,
        command: &str,
        timeout: Duration,
    ) -> anyhow::Result<smith_core::ExecResult> {
        match self
            .exec(ExecOp::ExecShell {
                command: command.to_string(),
                timeout_secs: timeout.as_secs(),
            })
            .await?
        {
            OpResult::ExecShell(dto) => Ok(dto.to_core()),
            OpResult::Failed { message } => anyhow::bail!("{message}"),
            _ => anyhow::bail!("unexpected op result for exec_shell"),
        }
    }

    async fn read_file(
        &self,
        path: &str,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> anyhow::Result<smith_core::FileContents> {
        match self
            .exec(ExecOp::ReadFile {
                path: path.to_string(),
                offset,
                limit,
            })
            .await?
        {
            OpResult::ReadFile(contents) => Ok(contents),
            OpResult::Failed { message } => anyhow::bail!("{message}"),
            _ => anyhow::bail!("unexpected op result for read_file"),
        }
    }

    async fn write_file(&self, path: &str, contents: &str) -> anyhow::Result<()> {
        match self
            .exec(ExecOp::WriteFile {
                path: path.to_string(),
                contents: contents.to_string(),
            })
            .await?
        {
            OpResult::WriteFile => Ok(()),
            OpResult::Failed { message } => anyhow::bail!("{message}"),
            _ => anyhow::bail!("unexpected op result for write_file"),
        }
    }

    async fn list_dir(&self, path: &str) -> anyhow::Result<Vec<String>> {
        match self
            .exec(ExecOp::ListDir {
                path: path.to_string(),
            })
            .await?
        {
            OpResult::ListDir { entries } => Ok(entries),
            OpResult::Failed { message } => anyhow::bail!("{message}"),
            _ => anyhow::bail!("unexpected op result for list_dir"),
        }
    }

    fn workspace_root(&self) -> &str {
        "/workspace"
    }
}

/// Helper used by tests: a registry in a temp dir.
#[allow(dead_code)]
pub(crate) fn temp_registry(dir: &Path) -> WorkerRegistry {
    WorkerRegistry::load(dir.join("workers.json"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capability(image: &str) -> Capability {
        Capability {
            image: image.into(),
            toolchains: vec![],
            cpus: 4,
            mem_mb: 8192,
            os: "linux".into(),
        }
    }

    #[test]
    fn enrolment_is_single_use_and_revocable() {
        let dir = tempfile::tempdir().unwrap();
        let registry = WorkerRegistry::load(dir.path().join("workers.json"));
        let token = registry.issue_enrolment("gpu-box").unwrap();

        let (id, worker_token) = registry
            .enrol("gpu-box", &token, &capability("img"))
            .unwrap();
        assert!(!id.is_empty());

        // Second use of the same enrolment token fails.
        assert!(
            registry
                .enrol("gpu-box", &token, &capability("img"))
                .is_err()
        );

        // The credential authenticates until revoked.
        assert_eq!(
            registry.authenticate(&worker_token).as_deref(),
            Some("gpu-box")
        );
        assert!(registry.revoke("gpu-box").unwrap());
        assert_eq!(registry.authenticate(&worker_token), None);
    }

    #[test]
    fn registry_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("workers.json");
        {
            let registry = WorkerRegistry::load(path.clone());
            let token = registry.issue_enrolment("box").unwrap();
            let (_id, worker_token) = registry.enrol("box", &token, &capability("i")).unwrap();
            // The credential must outlive the daemon process.
            std::fs::write(dir.path().join("cred"), worker_token).unwrap();
        }
        let registry = WorkerRegistry::load(path);
        let cred = std::fs::read_to_string(dir.path().join("cred")).unwrap();
        assert_eq!(registry.authenticate(cred.trim()).as_deref(), Some("box"));
    }

    #[test]
    fn least_loaded_worker_wins_and_pinning_filters() {
        let dir = tempfile::tempdir().unwrap();
        let registry = WorkerRegistry::load(dir.path().join("workers.json"));
        for name in ["a", "b"] {
            let token = registry.issue_enrolment(name).unwrap();
            registry.enrol(name, &token, &capability("img")).unwrap();
        }
        // Simulate load.
        registry.record_heartbeat(
            "a",
            &capability("img"),
            &Load {
                leases: 2,
                cpu_percent: 10,
            },
        );
        registry.record_heartbeat(
            "b",
            &capability("img"),
            &Load {
                leases: 0,
                cpu_percent: 90,
            },
        );

        let fleet = Fleet::new(
            dir.path().join("workers.json"),
            Arc::new(Metrics::default()),
        );
        // Not connected → nothing picks.
        assert!(fleet.pick_worker("img", None).is_none());

        fleet.streams.connect("a");
        fleet.streams.connect("b");
        assert_eq!(fleet.pick_worker("img", None).as_deref(), Some("b"));
        assert_eq!(fleet.pick_worker("img", Some("a")).as_deref(), Some("a"));
        assert_eq!(fleet.pick_worker("other-image", None), None);
    }

    #[tokio::test]
    async fn exec_op_correlates_results_and_times_out() {
        let dir = tempfile::tempdir().unwrap();
        let fleet = Arc::new(Fleet::new(
            dir.path().join("workers.json"),
            Arc::new(Metrics::default()),
        ));
        let token = fleet.registry.issue_enrolment("w").unwrap();
        fleet
            .registry
            .enrol("w", &token, &capability("img"))
            .unwrap();
        let mut rx = fleet.streams.connect("w");
        fleet.board.grant("sess", "w");

        // The "worker" side: take the command, deliver the op result back
        // through the streams map (exactly what the HTTP route does).
        let responder_fleet = Arc::clone(&fleet);
        let responder = tokio::spawn(async move {
            let cmd = rx.recv().await.expect("command");
            match cmd {
                WorkerCommand::Exec {
                    session_id,
                    exec_id,
                    ..
                } => {
                    assert_eq!(session_id, "sess");
                    responder_fleet.streams.deliver_op_result(
                        &exec_id,
                        OpResult::ListDir {
                            entries: vec!["Cargo.toml".into()],
                        },
                    );
                }
                other => panic!("unexpected command: {other:?}"),
            }
        });

        // Delivered result → the op completes with it.
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            fleet.exec_op("sess", ExecOp::ListDir { path: ".".into() }),
        )
        .await
        .expect("op must not time out when the worker answers")
        .unwrap();
        assert!(
            matches!(result, OpResult::ListDir { ref entries } if entries == &["Cargo.toml".to_string()])
        );
        responder.await.unwrap();

        // A lease whose worker's stream is dead → the op fails fast and
        // bounded (no hang), with a clear error for the tool layer.
        fleet.board.grant("sess2", "w");
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            fleet.exec_op("sess2", ExecOp::ListDir { path: ".".into() }),
        )
        .await
        .expect("op must stay bounded")
        .unwrap_err();
        assert!(result.to_string().contains("stream closed"), "{result}");
    }
}
