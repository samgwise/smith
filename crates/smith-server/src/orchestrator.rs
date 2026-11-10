//! The orchestrator: the planner side of multi-agent sessions (M15).
//!
//! One prompt in, many agents out: a planning session decomposes work into
//! subtasks (`spawn_subtasks`), each executed by its own agent on its own
//! worker lease and worktree; a tool-less judge reviews every sub-agent's
//! findings before the planner sees them (`collect_findings`); merge gates
//! converge the branches reviewably (`merge_subtask`); and
//! `finish_orchestration` releases the fleet and reports the integration
//! branch.
//!
//! Everything here is controller-level: the tools close over an
//! [`Arc<Orchestrator>`] and ignore the executor, so no smith-core change
//! is needed — the planner is just an agent with more tools. Children are
//! one level deep (they never receive orchestration tools).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;

use smith_core::{LlmProvider, Message, ProviderEvent, Tool, ToolExecutor};
use smith_protocol::{
    ChildState, ChildSummary, CreateSession, LeaseState, OpResult, OrchestrationKind, WireEvent,
    WorkerCommand,
};

use crate::config::OrchestrateSection;
use crate::controller::{ApiError, Controller};
use crate::factory::SessionFactory;
use crate::journal::Journal;

/// The planner's briefing — injected into the parent's system prompt.
pub const PLANNER_BRIEFING: &str = "Orchestration mode: you are the PLANNER of a multi-agent session. \
Delegate concrete work with `spawn_subtasks` — each subtask runs as its own agent on its own worker \
and git worktree. Track progress with `subtask_status`, then gather each child's judge-gated report \
with `collect_findings`. Converge the work with `merge_subtask` (runs the repo's merge gate; \
conflicts and gate failures come back to you for rework), and call `finish_orchestration` when done. \
Children cannot spawn further agents. You keep your own tools for reading files and running commands \
in your own workspace.";

/// The sub-agent's preamble — prepended to each child's first turn.
const CHILD_PREAMBLE: &str = "You are a sub-agent spawned by a planning session. Complete the task \
below with your tools, then finish with a clear summary report of what you did and found — your \
report is reviewed by a judge before the planner sees it.\n\nTask:\n";

/// How the judge may answer (verdict + critique, JSON).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Accept,
    Rework(String),
    /// Unparseable or unavailable judge output — passed through flagged.
    Unjudged,
}

/// One orchestrated child's bookkeeping.
#[derive(Clone)]
struct ChildRecord {
    id: String,
    title: String,
    instructions: String,
    /// The child's create request (replayed by the dispatcher at grant
    /// time: worker pin, repo, provider overrides).
    create_req: CreateSession,
    /// Resolved repo url (gate sequencing keys; None = scratch).
    repo: Option<String>,
    state: ChildState,
    judge_rounds_used: usize,
    /// Turns the ORCHESTRATOR requested (incremented after a successful
    /// start — the first turn by the dispatcher, rework turns by collect).
    turns_requested: usize,
    /// Settled turns (the tracker counts TurnCompleted/Cancelled/Error).
    turns_done: usize,
    /// Last journal event summary (tracker task).
    last_event: String,
}

/// The planner-side orchestrator: children, judge, gates, visibility.
pub struct Orchestrator {
    parent: String,
    config: OrchestrateSection,
    /// The controller, weakly held (the parent's SessionHandle owns this
    /// orchestrator, which lives in the controller's table — no cycle).
    controller: std::sync::Weak<Controller>,
    factory: Arc<dyn SessionFactory>,
    /// The PARENT's journal: orchestration events are journalled there.
    journal: Arc<Journal>,
    children: std::sync::Mutex<Vec<ChildRecord>>,
    /// One in-flight gate per (parent, repo) — repo url → serialising lock.
    gate_locks: std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Judge provider (built on first use; None = inherit session config).
    judge_provider: Mutex<Option<Arc<dyn LlmProvider>>>,
    /// Repos with an integration branch on the origin (finish reports /
    /// auto-pushes these).
    integrated: std::sync::Mutex<HashMap<String, String>>, // repo → base ref
    closed: AtomicBool,
    /// Self-reference for starting the dispatcher lazily (first spawn).
    me: std::sync::OnceLock<std::sync::Weak<Orchestrator>>,
    dispatcher_started: AtomicBool,
}

impl Orchestrator {
    pub fn new(
        parent: String,
        config: OrchestrateSection,
        factory: Arc<dyn SessionFactory>,
        journal: Arc<Journal>,
        controller: std::sync::Weak<Controller>,
    ) -> Arc<Self> {
        let orch = Arc::new(Self {
            parent,
            config,
            controller,
            factory,
            journal,
            children: std::sync::Mutex::new(Vec::new()),
            gate_locks: std::sync::Mutex::new(HashMap::new()),
            judge_provider: Mutex::new(None),
            integrated: std::sync::Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
            me: std::sync::OnceLock::new(),
            dispatcher_started: AtomicBool::new(false),
        });
        orch.me
            .set(Arc::downgrade(&orch))
            .expect("self-reference set once");
        orch
    }

    // ---------------------------------------------------- visibility ---

    /// The parent status snapshot's `children` rows.
    pub fn child_summaries(&self) -> Vec<ChildSummary> {
        self.children
            .lock()
            .expect("children mutex poisoned")
            .iter()
            .map(|c| ChildSummary {
                id: c.id.clone(),
                title: c.title.clone(),
                state: c.state,
            })
            .collect()
    }

    /// Publish an orchestration event on the parent's journal.
    fn publish(&self, child: &str, kind: OrchestrationKind, detail: &str) {
        self.journal.publish(WireEvent::Orchestration {
            parent: self.parent.clone(),
            child: child.to_string(),
            kind,
            detail: detail.chars().take(300).collect(),
        });
    }

    /// The sweeper marks a child's lease lost → the child has failed.
    pub fn note_lease_loss(&self, child_id: &str) {
        let mut children = self.children.lock().expect("children mutex poisoned");
        if let Some(c) = children
            .iter_mut()
            .find(|c| c.id == child_id && c.state != ChildState::Completed)
        {
            c.state = ChildState::Failed;
            drop(children);
            self.publish(child_id, OrchestrationKind::Failed, "lease lost");
        }
    }

    /// Bookkeeping: the orchestrator requested a turn for this child.
    fn note_turn_requested(&self, child_id: &str) {
        if let Some(c) = self
            .children
            .lock()
            .expect("children mutex poisoned")
            .iter_mut()
            .find(|c| c.id == child_id)
        {
            c.turns_requested += 1;
        }
    }

    /// Tracker task bookkeeping: count a completed turn.
    fn note_turn_done(&self, child_id: &str) {
        if let Some(c) = self
            .children
            .lock()
            .expect("children mutex poisoned")
            .iter_mut()
            .find(|c| c.id == child_id)
        {
            c.turns_done += 1;
            c.last_event = "turn completed".into();
        }
    }

    /// Tracker task bookkeeping: remember a child's last event summary.
    fn note_event(&self, child_id: &str, summary: &str) {
        if let Some(c) = self
            .children
            .lock()
            .expect("children mutex poisoned")
            .iter_mut()
            .find(|c| c.id == child_id)
        {
            c.last_event = summary.chars().take(160).collect();
        }
    }

    // ------------------------------------------------------- spawning ---

    /// `spawn_subtasks`: create queued child sessions. Children never get
    /// orchestration tools (one level deep) and run approval-free (Auto) —
    /// nobody is attached to a child to answer modalities; the judge and
    /// the merge gate are the quality control.
    pub async fn spawn(&self, specs: Vec<SubtaskSpec>) -> Result<String, ApiError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(ApiError::bad_request(
                "the orchestration is closed (finish_orchestration already ran)",
            ));
        }
        // The dispatcher assembles children as workers free up; started on
        // the first spawn and it exits when the orchestration closes.
        if !self.dispatcher_started.swap(true, Ordering::SeqCst)
            && let Some(me) = self.me.get().and_then(std::sync::Weak::upgrade)
        {
            tokio::spawn(me.dispatch_loop());
        }
        let controller = self
            .controller
            .upgrade()
            .ok_or_else(|| ApiError::unavailable("the controller is gone"))?;
        {
            let children = self.children.lock().expect("children mutex poisoned");
            if children.len() + specs.len() > self.config.max_children {
                return Err(ApiError::bad_request(format!(
                    "spawn would exceed max_children ({} live + {} requested > {})",
                    children.len(),
                    specs.len(),
                    self.config.max_children
                )));
            }
        }

        let mut out = String::from("spawned subtasks:");
        for spec in specs {
            // Children always run on workers; scratch only when the planner
            // gave no repo and none is configured. One level deep: children
            // never get orchestration tools.
            let create_req = CreateSession {
                workspace: String::new(),
                provider: None,
                model: None,
                approval_mode: smith_protocol::ApprovalMode::Auto,
                worker: None,
                repo: spec.repo.clone(),
                git_ref: spec.git_ref.clone(),
                orchestrate: false,
            };
            let id = controller
                .create_queued_child(&create_req, &spec.title)
                .await?;
            self.children
                .lock()
                .expect("children mutex poisoned")
                .push(ChildRecord {
                    id: id.clone(),
                    title: spec.title.clone(),
                    instructions: spec.instructions.clone(),
                    create_req,
                    repo: spec.repo.clone(),
                    state: ChildState::Queued,
                    judge_rounds_used: 0,
                    turns_requested: 0,
                    turns_done: 0,
                    last_event: "queued".into(),
                });
            self.publish(
                &id,
                OrchestrationKind::Spawned,
                &format!("subtask `{}`", spec.title),
            );
            out.push_str(&format!("\n- {} `{}`", &id[..8.min(id.len())], spec.title));
        }
        Ok(out)
    }

    /// Grant leases as workers free up: pick → dispatch → grant → assemble
    /// in place → start the subtask's first turn.
    async fn dispatch_loop(self: Arc<Self>) {
        loop {
            if self.closed.load(Ordering::SeqCst) {
                return;
            }
            if let Some(controller) = self.controller.upgrade() {
                // Snapshot the queued children (id, instructions, create req).
                let queued: Vec<(String, String, CreateSession)> = self
                    .children
                    .lock()
                    .expect("children mutex poisoned")
                    .iter()
                    .filter(|c| c.state == ChildState::Queued)
                    .map(|c| (c.id.clone(), c.instructions.clone(), c.create_req.clone()))
                    .collect();
                for (id, instructions, create_req) in queued {
                    if self.closed.load(Ordering::SeqCst) {
                        return;
                    }
                    match controller.dispatch_queued_child(&create_req, &id).await {
                        Ok(true) => {
                            self.mark_state(&id, ChildState::Running);
                            self.publish(&id, OrchestrationKind::Started, "");
                            // Track the child's events BEFORE its first turn
                            // runs: a scripted turn can complete in
                            // milliseconds, and the completion must be
                            // counted for collect_findings.
                            self.spawn_tracker(&controller, &id);
                            // The subtask's first turn, started by the
                            // controller (not the model) — the child's job
                            // is the subtask, not chatting.
                            match controller
                                .start_turn(&id, &format!("{CHILD_PREAMBLE}{instructions}"))
                                .await
                            {
                                Ok(()) => {
                                    self.note_turn_requested(&id);
                                }
                                Err(e) => {
                                    self.publish(
                                        &id,
                                        OrchestrationKind::Failed,
                                        &format!("first turn failed to start: {}", e.message),
                                    );
                                    self.mark_failed(&id);
                                }
                            }
                        }
                        Ok(false) => {} // no worker free yet; retry next tick
                        Err(e) => {
                            self.publish(
                                &id,
                                OrchestrationKind::Failed,
                                &format!("assembly failed: {}", e.message),
                            );
                            self.mark_failed(&id);
                        }
                    }
                }
            } else {
                return; // the controller is gone (daemon shutdown)
            }
            tokio::time::sleep(Duration::from_millis(700)).await;
        }
    }

    /// One consumer per child: keep `last_event` fresh for subtask_status.
    fn spawn_tracker(&self, controller: &Arc<Controller>, child_id: &str) {
        let Ok(handle) = controller.get(child_id) else {
            return;
        };
        let mut rx = handle.journal.subscribe();
        let Some(orch) = self.me.get().and_then(std::sync::Weak::upgrade) else {
            return;
        };
        let id = child_id.to_string();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(env) => {
                        if orch.closed.load(Ordering::SeqCst) {
                            return;
                        }
                        if let WireEvent::Core { core } = &env.event {
                            match core {
                                smith_core::CoreEvent::ToolFinished { summary, .. } => {
                                    orch.note_event(&id, summary);
                                }
                                smith_core::CoreEvent::Error { message } => {
                                    // An errored turn is also over: record the
                                    // message and settle it (the judge sees
                                    // whatever report exists).
                                    orch.note_event(&id, message);
                                    orch.note_turn_done(&id);
                                }
                                // Either ending means the turn is over.
                                smith_core::CoreEvent::TurnCompleted
                                | smith_core::CoreEvent::TurnCancelled => {
                                    orch.note_turn_done(&id);
                                }
                                _ => {}
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });
    }

    /// Update a child's lifecycle state (events are published separately
    /// by the callers that own the semantics).
    fn mark_state(&self, child_id: &str, state: ChildState) {
        {
            let mut children = self.children.lock().expect("children mutex poisoned");
            if let Some(c) = children.iter_mut().find(|c| c.id == child_id) {
                c.state = state;
            }
        }
        self.push_parent_status();
    }

    fn mark_failed(&self, child_id: &str) {
        {
            let mut children = self.children.lock().expect("children mutex poisoned");
            if let Some(c) = children.iter_mut().find(|c| c.id == child_id) {
                c.state = ChildState::Failed;
            }
        }
        self.push_parent_status();
    }

    /// Refresh the parent's published status (children rows changed).
    fn push_parent_status(&self) {
        let Some(controller) = self.controller.upgrade() else {
            return;
        };
        if let Ok(handle) = controller.get(&self.parent) {
            handle.publish_status();
        }
    }

    // ------------------------------------------------------- lookup ---

    /// Resolve a child by id prefix (exact or unique) — a cloned record.
    fn resolve_child(&self, prefix: &str) -> Result<ChildRecord, ApiError> {
        let children = self.children.lock().expect("children mutex poisoned");
        let matches: Vec<&ChildRecord> = children
            .iter()
            .filter(|c| c.id.starts_with(prefix.trim()))
            .collect();
        match matches.len() {
            0 => Err(ApiError::not_found(format!(
                "no child matches `{prefix}` (see subtask_status)"
            ))),
            1 => Ok(matches[0].clone()),
            n => Err(ApiError::bad_request(format!(
                "{n} children match `{prefix}`; be more specific"
            ))),
        }
    }

    // ------------------------------------------------------ judging ---

    /// The judge provider: built on first use (`judge_model` override, or
    /// the config default — inherit).
    async fn judge(&self, task: &str, report: &str, diffstat: &str, diff: &str) -> Verdict {
        let mut guard = self.judge_provider.lock().await;
        if guard.is_none() {
            let model = Some(self.config.judge_model.trim())
                .filter(|m| !m.is_empty())
                .map(str::to_string);
            match self.factory.build_provider(None, model.as_deref()) {
                Ok(p) => *guard = Some(p),
                Err(e) => {
                    tracing::warn!("judge provider unavailable: {e:#}");
                    return Verdict::Unjudged;
                }
            }
        }
        let provider = guard.as_ref().expect("judge provider built").clone();
        drop(guard);

        let system = "You are the judge in a multi-agent coding workflow. You review a \
sub-agent's report on its subtask against the actual changes. Be pragmatic: accept work that \
genuinely completes the task; send it back for rework when it is incomplete, wrong, or contradicts \
the task. Respond with ONLY a JSON object, no prose: \
{\"verdict\": \"accept\" | \"rework\", \"critique\": \"...\"} \
— `critique` states what is wrong (for rework) or what was verified (for accept).";
        let user = format!(
            "Subtask:\n{task}\n\nFinal report:\n{report}\n\nDiff summary:\n{diffstat}\n\nDiff (truncated):\n{diff}"
        );
        let history = [Message::system(system), Message::user(user)];

        let stream = match provider.chat_stream(&history, &[]).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("judge turn failed: {e:#}");
                return Verdict::Unjudged;
            }
        };
        let mut text = String::new();
        {
            use futures::StreamExt;
            let mut stream = stream;
            while let Some(ev) = stream.next().await {
                match ev {
                    Ok(ProviderEvent::TextDelta(delta)) => text.push_str(&delta),
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!("judge stream failed: {e:#}");
                        return Verdict::Unjudged;
                    }
                }
            }
        }
        parse_verdict(&text)
    }

    /// The requested/settled turn counts (0 for an unknown child).
    fn turn_counts(&self, child_id: &str) -> (usize, usize) {
        self.children
            .lock()
            .expect("children mutex poisoned")
            .iter()
            .find(|c| c.id == child_id)
            .map_or((0, 0), |c| (c.turns_requested, c.turns_done))
    }

    /// Bounded wait until every requested turn has settled (and at least
    /// one ran): the requested/done accounting cannot race an unstarted or
    /// already-finished turn.
    async fn await_settled(
        &self,
        controller: &Arc<Controller>,
        child_id: &str,
        timeout: Duration,
    ) -> Result<(), ApiError> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            // Lease loss surfaces as a failure state, not a hang.
            if let Some((_, LeaseState::Lost)) = controller.fleet.board.lease_of(child_id) {
                return Err(ApiError::unavailable("the child's lease was lost"));
            }
            let (requested, done) = self.turn_counts(child_id);
            if done >= requested && done >= 1 {
                // The turn's closing status may still be publishing.
                if !controller.get(child_id)?.busy() {
                    return Ok(());
                }
            }
            if std::time::Instant::now() > deadline {
                return Err(ApiError::unavailable(
                    "the child did not finish in time (collect_findings again later)",
                ));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Gather a child's finding: report + diffstat + truncated diff (over
    /// the child's own executor — the planner needs no tools of its own).
    async fn gather_finding(
        controller: &Arc<Controller>,
        child_id: &str,
    ) -> (String, String, String) {
        let handle = match controller.get(child_id) {
            Ok(h) => h,
            Err(e) => {
                return (
                    format!("(child unavailable: {e})"),
                    String::new(),
                    String::new(),
                );
            }
        };
        let report = handle
            .journal
            .replay(0)
            .into_iter()
            .filter_map(|env| match env.event {
                WireEvent::Core {
                    core: smith_core::CoreEvent::AssistantMessage { text },
                } => Some(text),
                _ => None,
            })
            .next_back()
            .unwrap_or_else(|| "(no report message)".into());
        let executor = handle.executor();
        let diffstat = executor
            .exec_shell("git --no-pager diff --stat", Duration::from_secs(30))
            .await
            .map(|r| {
                if r.stdout.trim().is_empty() {
                    "(no changes)".into()
                } else {
                    r.stdout
                }
            })
            .unwrap_or_else(|e| format!("(diffstat failed: {e:#})"));
        let diff = executor
            .exec_shell("git --no-pager diff", Duration::from_secs(30))
            .await
            .map(|r| truncate_chars(&r.stdout, 4000))
            .unwrap_or_else(|e| format!("(diff failed: {e:#})"));
        (report, diffstat, diff)
    }

    // ------------------------------------------------------ tools ---

    /// `subtask_status`.
    pub fn status(&self, child_prefix: &str) -> Result<String, ApiError> {
        let child = self.resolve_child(child_prefix)?;
        let (state, last) = {
            let children = self.children.lock().expect("children mutex poisoned");
            match children.iter().find(|c| c.id == child.id) {
                Some(c) => (c.state, c.last_event.clone()),
                None => return Err(ApiError::not_found("child record vanished")),
            }
        };
        Ok(json!({
            "child": child.id,
            "title": child.title,
            "state": serde_json::to_value(state).unwrap_or(json!("unknown")),
            "last_event": last,
        })
        .to_string())
    }

    /// `collect_findings`: bounded wait for the child's completion, then
    /// the judge loop — rework critiques become the child's next turn, up
    /// to `judge_rounds`; past that the finding passes through flagged.
    pub async fn collect(&self, child_prefix: &str) -> Result<String, ApiError> {
        let child = self.resolve_child(child_prefix)?;
        let controller = self
            .controller
            .upgrade()
            .ok_or_else(|| ApiError::unavailable("the controller is gone"))?;
        let timeout = Duration::from_secs(self.config.collect_timeout_secs.max(1));

        for _round in 0..=self.config.judge_rounds {
            self.await_settled(&controller, &child.id, timeout).await?;
            if self.config.judge_rounds == 0 {
                // Judging disabled: plain parallel spawning — the report
                // passes through unjudged.
                let (report, diffstat, _diff) = Self::gather_finding(&controller, &child.id).await;
                return Ok(format!(
                    "finding from `{}` (verdict: unjudged, judging disabled):\n\n{report}\n\n{diffstat}",
                    child.title
                ));
            }
            let (report, diffstat, diff) = Self::gather_finding(&controller, &child.id).await;
            let verdict = self
                .judge(&child.instructions, &report, &diffstat, &diff)
                .await;
            let (verdict_word, _critique) = match &verdict {
                Verdict::Accept => ("accept", String::new()),
                Verdict::Rework(c) => ("rework", c.clone()),
                Verdict::Unjudged => ("unjudged", String::new()),
            };
            self.metrics_verdict(verdict_word);
            self.publish(
                &child.id,
                OrchestrationKind::Judged,
                &format!("verdict: {verdict_word}"),
            );

            match verdict {
                Verdict::Accept => {
                    self.mark_state(&child.id, ChildState::Running);
                    return Ok(format!(
                        "finding from `{}` (verdict: accept):\n\n{report}\n\n{diffstat}",
                        child.title
                    ));
                }
                Verdict::Unjudged => {
                    return Ok(format!(
                        "finding from `{}` (verdict: unjudged — the judge could not produce a \
verdict; no quality claim is made):\n\n{report}\n\n{diffstat}",
                        child.title
                    ));
                }
                Verdict::Rework(critique) => {
                    let used = {
                        let mut children = self.children.lock().expect("children mutex poisoned");
                        let Some(c) = children.iter_mut().find(|c| c.id == child.id) else {
                            return Err(ApiError::not_found("child record vanished"));
                        };
                        c.judge_rounds_used += 1;
                        c.last_event =
                            format!("judge rework: {}", &critique[..60.min(critique.len())]);
                        c.judge_rounds_used
                    };
                    if used >= self.config.judge_rounds {
                        // Bounded cost: pass through, flagged.
                        return Ok(format!(
                            "finding from `{}` (verdict: unjudged — rework rounds exhausted \
without an accept; the last critique: {critique}):\n\n{report}\n\n{diffstat}",
                            child.title
                        ));
                    }
                    // The verify-gate pattern: the critique becomes the
                    // child's next turn.
                    controller
                        .start_turn(
                            &child.id,
                            &format!(
                                "The judge sent your work back for rework. Critique:\n{critique}\n\n\
Address it, then summarise what changed in response."
                            ),
                        )
                        .await?;
                    self.note_turn_requested(&child.id);
                }
            }
        }
        unreachable!("the rework loop returns in every branch")
    }

    fn metrics_verdict(&self, verdict: &str) {
        let Some(controller) = self.controller.upgrade() else {
            return;
        };
        controller
            .metrics
            .judge_total
            .inc(&format!("verdict=\"{verdict}\""));
    }

    /// `merge_subtask`: run the worker-side merge gate against the parent's
    /// integration branch. Sequencing: one in-flight gate per (parent,
    /// repo); each gate merges against the latest integration state.
    pub async fn merge(&self, child_prefix: &str) -> Result<String, ApiError> {
        let child = self.resolve_child(child_prefix)?;
        let controller = self
            .controller
            .upgrade()
            .ok_or_else(|| ApiError::unavailable("the controller is gone"))?;
        let repo = child
            .repo
            .clone()
            .ok_or_else(|| ApiError::bad_request("scratch children cannot be merged"))?;
        let worker = match controller.fleet.board.lease_of(&child.id) {
            Some((w, LeaseState::Granted)) => w,
            Some((_, state)) => {
                return Err(ApiError::busy(format!(
                    "child lease is {state:?}; cannot run the merge gate"
                )));
            }
            None => {
                return Err(ApiError::bad_request(
                    "child has no lease (not a worker session)",
                ));
            }
        };

        let gate_lock = self
            .gate_locks
            .lock()
            .expect("gate locks poisoned")
            .entry(repo.clone())
            .or_default()
            .clone();
        let _guard = gate_lock.lock().await;

        let integration_ref = format!("smith/integration/{}", self.parent);
        let exec_id = uuid::Uuid::new_v4().simple().to_string();
        let rx = controller.fleet.streams.await_op_result(&exec_id);
        controller
            .fleet
            .streams
            .send(
                &worker,
                WorkerCommand::MergeGate {
                    session_id: child.id.clone(),
                    exec_id,
                    integration_ref: integration_ref.clone(),
                    gate_command: self.config.gate_command.clone(),
                },
            )
            .map_err(|e| ApiError::unavailable(format!("worker unreachable: {e:#}")))?;
        let result = tokio::time::timeout(
            Duration::from_secs(self.config.collect_timeout_secs.max(1)),
            rx,
        )
        .await
        .map_err(|_| ApiError::unavailable("the merge gate did not answer in time (retry)"))?
        .map_err(|_| ApiError::unavailable("the worker dropped the merge gate"))?;

        let outcome = match result {
            OpResult::MergeGate {
                ok,
                conflict,
                output,
            } => {
                let verdict = if ok {
                    "OK"
                } else if conflict {
                    "CONFLICT"
                } else {
                    "GATE FAILED"
                };
                if ok {
                    self.integrated
                        .lock()
                        .expect("integrated mutex poisoned")
                        .insert(repo, child.create_req.git_ref.clone().unwrap_or_default());
                    self.publish(&child.id, OrchestrationKind::Merged, "gate passed");
                    self.mark_state(&child.id, ChildState::Completed);
                } else {
                    self.publish(
                        &child.id,
                        OrchestrationKind::Failed,
                        if conflict {
                            "merge conflict"
                        } else {
                            "gate failed"
                        },
                    );
                    self.mark_failed(&child.id);
                }
                format!(
                    "merge gate for `{}` (branch smith/{}): {}\n\n{}",
                    child.title,
                    &child.id[..8.min(child.id.len())],
                    verdict,
                    output
                )
            }
            OpResult::Failed { message } => {
                format!("merge gate for `{}` could not run: {message}", child.title)
            }
            other => format!("unexpected merge gate result: {other:?}"),
        };
        Ok(outcome)
    }

    /// `finish_orchestration`: release every remaining lease, report the
    /// integration branch(es), close the orchestration.
    pub async fn finish(&self, summary: &str) -> Result<String, ApiError> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Err(ApiError::bad_request("the orchestration is already closed"));
        }
        let controller = self
            .controller
            .upgrade()
            .ok_or_else(|| ApiError::unavailable("the controller is gone"))?;

        // Opt-in convergence: push the integration branch onto the base
        // ref. Off by default — the fleet produces reviewable branches.
        let mut pushed: Vec<String> = vec![];
        if self.config.auto_push {
            let integrated = self
                .integrated
                .lock()
                .expect("integrated mutex poisoned")
                .clone();
            for (repo, base_ref) in integrated {
                if base_ref.is_empty() {
                    continue; // unknown base ref: nothing to push onto
                }
                // Any granted child of the same repo can run the push (the
                // integration branch lives in that worker's bare cache).
                let pusher = {
                    let children = self.children.lock().expect("children mutex poisoned");
                    children.iter().find_map(|c| {
                        (c.repo.as_deref() == Some(repo.as_str())).then(|| c.id.clone())
                    })
                };
                let Some(pusher) = pusher else {
                    continue;
                };
                if let Ok(()) = self
                    .push_integration(&controller, &pusher, &repo, &base_ref)
                    .await
                {
                    pushed.push(format!("{repo}: {base_ref}"));
                }
            }
        }

        // Release every child's lease.
        let children: Vec<String> = self
            .children
            .lock()
            .expect("children mutex poisoned")
            .iter()
            .map(|c| c.id.clone())
            .collect();
        let len = children.len();
        let mut released = 0usize;
        for child in &children {
            if controller.release_session(child)?.is_some() {
                released += 1;
            }
            self.publish(child, OrchestrationKind::Released, "released");
        }

        let integrated = self
            .integrated
            .lock()
            .expect("integrated mutex poisoned")
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        let branches = if integrated.is_empty() {
            "none (no successful merge gate)".to_string()
        } else {
            integrated
        };
        self.publish(
            &self.parent,
            OrchestrationKind::Released,
            &format!("orchestration closed; integration: {branches}"),
        );
        self.push_parent_status();
        Ok(format!(
            "orchestration closed. Integration branch(es): {branches}. Children released: \
{released}/{len}. Auto-pushed: {}{}",
            if pushed.is_empty() { "no" } else { "yes" },
            if pushed.is_empty() {
                String::new()
            } else {
                format!(" ({})", pushed.join(", "))
            }
        ) + &format!("\n\nPlanner summary: {summary}"))
    }

    async fn push_integration(
        &self,
        controller: &Arc<Controller>,
        pusher: &str,
        _repo: &str,
        base_ref: &str,
    ) -> Result<(), ApiError> {
        let worker = match controller.fleet.board.lease_of(pusher) {
            Some((w, LeaseState::Granted)) => w,
            _ => return Err(ApiError::bad_request("no granted lease for the pusher")),
        };
        let exec_id = uuid::Uuid::new_v4().simple().to_string();
        let rx = controller.fleet.streams.await_op_result(&exec_id);
        controller
            .fleet
            .streams
            .send(
                &worker,
                WorkerCommand::PushBranch {
                    session_id: pusher.to_string(),
                    exec_id,
                    from_ref: format!("smith/integration/{}", self.parent),
                    to_ref: base_ref.to_string(),
                },
            )
            .map_err(|e| ApiError::unavailable(format!("worker unreachable: {e:#}")))?;
        let result = tokio::time::timeout(Duration::from_secs(300), rx)
            .await
            .map_err(|_| ApiError::unavailable("the push did not answer in time"))?
            .map_err(|_| ApiError::unavailable("the worker dropped the push"))?;
        match result {
            OpResult::Done { .. } => Ok(()),
            OpResult::Failed { message } => {
                tracing::warn!("auto-push failed: {message}");
                Err(ApiError::bad_request(message))
            }
            other => Err(ApiError::bad_request(format!(
                "unexpected push result: {other:?}"
            ))),
        }
    }
}

/// Parse the judge's answer: the first JSON object in the text wins;
/// anything else is Unjudged (no silent quality claims).
fn parse_verdict(text: &str) -> Verdict {
    #[derive(Deserialize)]
    struct JudgeReply {
        verdict: String,
        #[serde(default)]
        critique: String,
    }
    let Some(start) = text.find('{') else {
        return Verdict::Unjudged;
    };
    let Some(end) = text.rfind('}') else {
        return Verdict::Unjudged;
    };
    if end < start {
        return Verdict::Unjudged;
    }
    match serde_json::from_str::<JudgeReply>(&text[start..=end]) {
        Ok(reply) if reply.verdict == "accept" => Verdict::Accept,
        Ok(reply) if reply.verdict == "rework" => Verdict::Rework(reply.critique),
        _ => Verdict::Unjudged,
    }
}

fn truncate_chars(text: &str, cap: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= cap {
        text.to_string()
    } else {
        chars[chars.len() - cap..].iter().collect()
    }
}

// -------------------------------------------------------- the tools ---

/// Tool-argument shapes.
#[derive(Deserialize)]
pub struct SubtaskSpec {
    pub title: String,
    pub instructions: String,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default, rename = "ref")]
    pub git_ref: Option<String>,
}

#[derive(Deserialize)]
struct SpawnArgs {
    subtasks: Vec<SubtaskSpec>,
}

#[derive(Deserialize)]
struct ChildArgs {
    child: String,
}

#[derive(Deserialize)]
struct FinishArgs {
    summary: String,
}

/// Extract a JSON object from tool arguments.
fn parse_args<T: serde::de::DeserializeOwned>(arguments: &str) -> anyhow::Result<T> {
    serde_json::from_str(arguments)
        .map_err(|e| anyhow::anyhow!("invalid tool arguments ({e}); expected JSON object"))
}

async fn run_spawn(orch: &Orchestrator, arguments: &str) -> anyhow::Result<String> {
    let args: SpawnArgs = parse_args(arguments)?;
    if args.subtasks.is_empty() {
        anyhow::bail!("at least one subtask is required");
    }
    orch.spawn(args.subtasks)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e.message))
}

async fn run_status(orch: &Orchestrator, arguments: &str) -> anyhow::Result<String> {
    let args: ChildArgs = parse_args(arguments)?;
    orch.status(&args.child)
        .map_err(|e| anyhow::anyhow!("{}", e.message))
}

async fn run_collect(orch: &Orchestrator, arguments: &str) -> anyhow::Result<String> {
    let args: ChildArgs = parse_args(arguments)?;
    orch.collect(&args.child)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e.message))
}

async fn run_merge(orch: &Orchestrator, arguments: &str) -> anyhow::Result<String> {
    let args: ChildArgs = parse_args(arguments)?;
    orch.merge(&args.child)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e.message))
}

async fn run_finish(orch: &Orchestrator, arguments: &str) -> anyhow::Result<String> {
    let args: FinishArgs = parse_args(arguments)?;
    orch.finish(&args.summary)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e.message))
}

/// Boilerplate shared by the five orchestration tools: they close over the
/// orchestrator handle and ignore the executor (controller-level tools).
macro_rules! orchestrator_tool {
    ($name:ident, $tool_name:literal, $description:literal, $schema:expr, $approval:expr, $run:ident) => {
        struct $name {
            orch: Arc<Orchestrator>,
        }

        #[async_trait]
        impl Tool for $name {
            fn def(&self) -> smith_core::ToolDef {
                smith_core::ToolDef {
                    name: $tool_name.into(),
                    description: $description.into(),
                    parameters: $schema,
                }
            }

            fn needs_approval(&self, _arguments: &str) -> bool {
                $approval
            }

            async fn run(
                &self,
                _executor: &dyn ToolExecutor,
                arguments: &str,
            ) -> anyhow::Result<String> {
                $run(&self.orch, arguments).await
            }
        }
    };
}

orchestrator_tool!(
    SpawnSubtasks,
    "spawn_subtasks",
    "Spawn one sub-agent per subtask. Each runs as its own agent on its own worker lease and \
git worktree, with the given instructions as its task. Children cannot spawn further agents. \
Returns the child ids to use with subtask_status / collect_findings / merge_subtask.",
    json!({
        "type": "object",
        "properties": {
            "subtasks": {
                "type": "array",
                "description": "The subtasks to spawn",
                "items": {
                    "type": "object",
                    "properties": {
                        "title": { "type": "string", "description": "Short subtask title" },
                        "instructions": { "type": "string", "description": "Full instructions for the sub-agent" },
                        "repo": { "type": "string", "description": "Optional git URL to materialise (defaults to the configured repo)" },
                        "ref": { "type": "string", "description": "Optional git ref to base the work on (defaults to the configured ref)" }
                    },
                    "required": ["title", "instructions"]
                }
            }
        },
        "required": ["subtasks"]
    }),
    true,
    run_spawn
);

orchestrator_tool!(
    SubtaskStatus,
    "subtask_status",
    "Report one child session's state and last event summary. `child` is the id (or id prefix) \
returned by spawn_subtasks.",
    json!({
        "type": "object",
        "properties": {
            "child": { "type": "string", "description": "Child session id (or unique prefix)" }
        },
        "required": ["child"]
    }),
    false,
    run_status
);

orchestrator_tool!(
    CollectFindings,
    "collect_findings",
    "Wait (bounded) for a child to finish, then return its judge-gated finding: the verdict, \
the final report and the diff summary. A rework verdict sends the judge's critique back to the \
child automatically (bounded rounds); a failure state means spawn a replacement.",
    json!({
        "type": "object",
        "properties": {
            "child": { "type": "string", "description": "Child session id (or unique prefix)" }
        },
        "required": ["child"]
    }),
    false,
    run_collect
);

orchestrator_tool!(
    MergeSubtask,
    "merge_subtask",
    "Run the merge gate for a finished child: merges its checkpoint branch into the \
orchestration's integration branch and runs the configured gate command. Conflicts and gate \
failures come back to you for rework; a passing gate is on the integration branch.",
    json!({
        "type": "object",
        "properties": {
            "child": { "type": "string", "description": "Child session id (or unique prefix)" }
        },
        "required": ["child"]
    }),
    true,
    run_merge
);

orchestrator_tool!(
    FinishOrchestration,
    "finish_orchestration",
    "Close the orchestration: releases every remaining child lease and reports the integration \
branch(es). Call this when all subtasks are collected and merged (or abandoned).",
    json!({
        "type": "object",
        "properties": {
            "summary": { "type": "string", "description": "Short summary of what the orchestration achieved" }
        },
        "required": ["summary"]
    }),
    true,
    run_finish
);

/// The five controller-level tools for a planning session's registry.
pub fn orchestration_tools(orch: Arc<Orchestrator>) -> Vec<Box<dyn Tool>> {
    vec![
        Box::new(SpawnSubtasks { orch: orch.clone() }),
        Box::new(SubtaskStatus { orch: orch.clone() }),
        Box::new(CollectFindings { orch: orch.clone() }),
        Box::new(MergeSubtask { orch: orch.clone() }),
        Box::new(FinishOrchestration { orch }),
    ]
}

// ---------------------------------------------------------- tests ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verdict_parsing() {
        assert_eq!(
            parse_verdict("{\"verdict\":\"accept\",\"critique\":\"fine\"}"),
            Verdict::Accept
        );
        assert_eq!(
            parse_verdict("prefix {\"verdict\":\"rework\",\"critique\":\"fix the tests\"} suffix"),
            Verdict::Rework("fix the tests".into())
        );
        // Unparseable → unjudged, never a silent accept.
        assert_eq!(parse_verdict("looks good to me"), Verdict::Unjudged);
        assert_eq!(parse_verdict("{nope"), Verdict::Unjudged);
        assert_eq!(parse_verdict("{\"verdict\":\"maybe\"}"), Verdict::Unjudged);
    }

    #[test]
    fn truncation_keeps_the_tail() {
        assert_eq!(truncate_chars("abcdef", 10), "abcdef");
        assert_eq!(truncate_chars("abcdef", 3), "def");
    }
}
