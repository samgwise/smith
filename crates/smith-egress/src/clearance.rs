//! Per-destination clearance: when a destination is not on the static
//! allowlist, hold the connection and let the controller (TUI or headless
//! run) decide. The control API (see the proxy binary) exposes pending
//! requests and answers over a localhost-published port.

use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::oneshot;

/// What the user (or policy) decided for a pending destination.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    AllowOnce,
    AllowAlways,
    Deny,
}

/// Clearance policy for non-allowlisted destinations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ClearanceMode {
    /// Hold the connection and ask the controller (TUI modal).
    #[default]
    Ask,
    /// Auto-deny non-allowlisted destinations.
    Deny,
    /// Auto-allow non-allowlisted destinations (explicit trust).
    Allow,
}

impl ClearanceMode {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "ask" => Some(Self::Ask),
            "deny" => Some(Self::Deny),
            "allow" => Some(Self::Allow),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ask => "ask",
            Self::Deny => "deny",
            Self::Allow => "allow",
        }
    }
}

struct PendingEntry {
    host: String,
    created_ms: u64,
    reply: oneshot::Sender<Decision>,
}

/// Shared clearance state: pending requests, session-scoped runtime
/// allowlist, and the active mode.
#[derive(Default)]
pub struct Clearance {
    next_id: AtomicU64,
    pending: Mutex<BTreeMap<u64, PendingEntry>>,
    /// Hosts approved with "always" this session (lowercase).
    runtime_allow: Mutex<HashSet<String>>,
    mode: Mutex<ClearanceMode>,
}

impl Clearance {
    pub fn new(mode: ClearanceMode) -> Self {
        Self {
            mode: Mutex::new(mode),
            ..Self::default()
        }
    }

    pub fn mode(&self) -> ClearanceMode {
        *self.mode.lock().expect("clearance mode mutex poisoned")
    }

    pub fn set_mode(&self, mode: ClearanceMode) {
        *self.mode.lock().expect("clearance mode mutex poisoned") = mode;
    }

    /// Whether `host` is covered by the static allowlist or a session
    /// "always" approval.
    pub fn is_allowed(&self, static_allowlist: &crate::Allowlist, host: &str) -> bool {
        let host = host.to_lowercase();
        if static_allowlist.allows(&host) {
            return true;
        }
        self.runtime_allow
            .lock()
            .expect("runtime allowlist mutex poisoned")
            .contains(&host)
    }

    /// Record an "always" approval for `host` (session-scoped).
    pub fn allow_always(&self, host: &str) {
        self.runtime_allow
            .lock()
            .expect("runtime allowlist mutex poisoned")
            .insert(host.to_lowercase());
    }

    /// Register a pending decision for `host`; returns its id and the
    /// receiver for the answer.
    pub fn create_pending(&self, host: &str) -> (u64, oneshot::Receiver<Decision>) {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().expect("pending mutex poisoned").insert(
            id,
            PendingEntry {
                host: host.to_string(),
                created_ms: now_ms(),
                reply: tx,
            },
        );
        (id, rx)
    }

    /// Answer a pending request; `false` when the id is gone (timed out or
    /// already answered).
    pub fn decide(&self, id: u64, decision: Decision) -> bool {
        match self
            .pending
            .lock()
            .expect("pending mutex poisoned")
            .remove(&id)
        {
            Some(entry) => {
                if decision == Decision::AllowAlways {
                    self.allow_always(&entry.host);
                }
                entry.reply.send(decision).is_ok()
            }
            None => false,
        }
    }

    /// Snapshot of pending requests (id, host), oldest first.
    pub fn pending_list(&self) -> Vec<(u64, String)> {
        self.pending
            .lock()
            .expect("pending mutex poisoned")
            .iter()
            .map(|(id, entry)| (*id, entry.host.clone()))
            .collect()
    }

    /// Drop stale pending entries older than `max_age`; returns how many
    /// were removed (their waiters see the channel close = deny).
    pub fn reap_expired(&self, max_age: Duration) -> usize {
        let cutoff = now_ms().saturating_sub(max_age.as_millis() as u64);
        let mut pending = self.pending.lock().expect("pending mutex poisoned");
        let expired: Vec<u64> = pending
            .iter()
            .filter(|(_, e)| e.created_ms <= cutoff)
            .map(|(id, _)| *id)
            .collect();
        for id in &expired {
            if let Some(entry) = pending.remove(id) {
                // Dropping the sender closes the channel; the waiter denies.
                let _ = entry.reply;
            }
        }
        expired.len()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pending_create_decide_and_always() {
        let clearance = Clearance::new(ClearanceMode::Ask);
        let (id, rx) = clearance.create_pending("api.example.com");
        assert_eq!(
            clearance.pending_list(),
            vec![(id, "api.example.com".into())]
        );

        assert!(clearance.decide(id, Decision::AllowAlways));
        assert_eq!(rx.await.unwrap(), Decision::AllowAlways);
        // "always" covers the host for the rest of the session.
        assert!(clearance.is_allowed(&crate::Allowlist::default(), "api.example.com"));
        // Deciding again fails: the request is gone.
        assert!(!clearance.decide(id, Decision::Deny));
        assert!(clearance.pending_list().is_empty());
    }

    #[tokio::test]
    async fn reaped_requests_close_their_channel() {
        let clearance = Clearance::new(ClearanceMode::Ask);
        let (_id, rx) = clearance.create_pending("slow.example.com");
        // Nothing waits yet; reap with a large budget after the entry is
        // definitely older than it (created_ms is stamped at creation).
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(clearance.reap_expired(Duration::from_millis(1)), 1);
        // The waiter sees the channel close: deny.
        assert!(rx.await.is_err());
        assert!(clearance.pending_list().is_empty());
    }

    #[test]
    fn mode_round_trip() {
        let clearance = Clearance::new(ClearanceMode::Ask);
        assert_eq!(clearance.mode(), ClearanceMode::Ask);
        clearance.set_mode(ClearanceMode::Deny);
        assert_eq!(clearance.mode(), ClearanceMode::Deny);
    }
}
