//! Server-side approval routing.
//!
//! The agent's `ApprovalRequested` event already reaches every attached
//! client through the journal; the gate waits on a shared per-session slot.
//! The first decision delivered wins; an unanswered request auto-denies
//! after the timeout (matching the headless ask→deny philosophy).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;

use smith_core::{ApprovalDecision, ApprovalGate, ApprovalRequest};
use smith_protocol::WireEvent;

use crate::journal::Journal;

/// Shared slot for a session's single in-flight approval.
pub struct GateSlot {
    pending: tokio::sync::Mutex<Option<mpsc::UnboundedSender<ApprovalDecision>>>,
}

impl GateSlot {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            pending: tokio::sync::Mutex::new(None),
        })
    }

    /// Deliver a decision to the waiting gate. Returns false when nothing
    /// was pending (e.g. the approval already timed out).
    pub async fn deliver(&self, decision: ApprovalDecision) -> bool {
        match self.pending.lock().await.take() {
            Some(tx) => tx.send(decision).is_ok(),
            None => false,
        }
    }
}

/// `ApprovalGate` hosted in the daemon.
pub struct WireGate {
    slot: Arc<GateSlot>,
    timeout: Duration,
    /// Echoes timeout-denies so viewers close their modals (delivered
    /// decisions are echoed by the route handler instead, which knows the
    /// client's call_id).
    journal: Arc<Journal>,
}

impl WireGate {
    pub fn new(slot: Arc<GateSlot>, timeout: Duration, journal: Arc<Journal>) -> Self {
        Self {
            slot,
            timeout,
            journal,
        }
    }
}

#[async_trait]
impl ApprovalGate for WireGate {
    async fn decide(&self, _request: ApprovalRequest) -> ApprovalDecision {
        let (tx, mut rx) = mpsc::unbounded_channel();
        *self.slot.pending.lock().await = Some(tx);
        match tokio::time::timeout(self.timeout, rx.recv()).await {
            Ok(Some(decision)) => decision,
            // Timeout (or a closed channel): deny. A decision delivered in
            // the same instant beats the timer; anything else was a timeout,
            // and the echo tells viewers why their modal closed.
            _ => {
                let late = rx.try_recv().unwrap_or(ApprovalDecision::Deny);
                if matches!(late, ApprovalDecision::Deny) {
                    self.journal.publish(WireEvent::ApprovalDecided {
                        call_id: String::new(),
                        decision: ApprovalDecision::Deny,
                    });
                }
                late
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn first_delivery_wins_and_second_is_rejected() {
        let slot = GateSlot::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        *slot.pending.lock().await = Some(tx);

        assert!(slot.deliver(ApprovalDecision::AllowAlways).await);
        // The slot is now empty: a second delivery finds nothing.
        assert!(!slot.deliver(ApprovalDecision::Deny).await);
        assert_eq!(rx.recv().await, Some(ApprovalDecision::AllowAlways));
    }

    #[tokio::test]
    async fn timeout_denies_and_echoes() {
        let journal = Arc::new(Journal::new());
        let gate = WireGate::new(GateSlot::new(), Duration::from_millis(20), journal.clone());
        let decision = gate
            .decide(ApprovalRequest {
                tool: "shell".into(),
                detail: "sleep".into(),
            })
            .await;
        assert_eq!(decision, ApprovalDecision::Deny);
        // A timeout echo was published so viewers close their modals.
        let replay = journal.replay(0);
        assert!(matches!(
            replay.last().map(|e| &e.event),
            Some(WireEvent::ApprovalDecided {
                decision: ApprovalDecision::Deny,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn a_just_in_time_delivery_beats_the_timeout() {
        let slot = GateSlot::new();
        let journal = Arc::new(Journal::new());
        let gate = WireGate::new(slot.clone(), Duration::from_millis(40), journal.clone());
        let deliverer = tokio::spawn({
            let slot = slot.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                slot.deliver(ApprovalDecision::AllowOnce).await;
            }
        });
        let decision = gate
            .decide(ApprovalRequest {
                tool: "shell".into(),
                detail: "x".into(),
            })
            .await;
        deliverer.await.unwrap();
        assert_eq!(decision, ApprovalDecision::AllowOnce);
        // No timeout echo: the route handler echoes delivered decisions.
        assert!(journal.replay(0).is_empty());
    }
}
