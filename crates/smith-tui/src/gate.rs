//! Approval gate that surfaces requests in the TUI as a modal.

use async_trait::async_trait;
use tokio::sync::mpsc;

use smith_core::{ApprovalDecision, ApprovalGate, ApprovalRequest};

/// Sent to the UI loop: show the modal, then reply through the oneshot-style
/// sender with the user's decision.
pub type ApprovalTx =
    mpsc::UnboundedSender<(ApprovalRequest, mpsc::UnboundedSender<ApprovalDecision>)>;
pub type ApprovalRx =
    mpsc::UnboundedReceiver<(ApprovalRequest, mpsc::UnboundedSender<ApprovalDecision>)>;

pub fn approval_channel() -> (ApprovalTx, ApprovalRx) {
    mpsc::unbounded_channel()
}

/// Gate implementation handed to the agent; forwards to the UI loop.
pub struct TuiGate {
    tx: ApprovalTx,
}

impl TuiGate {
    pub fn new(tx: ApprovalTx) -> Self {
        Self { tx }
    }
}

#[async_trait]
impl ApprovalGate for TuiGate {
    async fn decide(&self, request: ApprovalRequest) -> ApprovalDecision {
        let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();
        if self.tx.send((request, reply_tx)).is_err() {
            return ApprovalDecision::Deny;
        }
        reply_rx.recv().await.unwrap_or(ApprovalDecision::Deny)
    }
}
