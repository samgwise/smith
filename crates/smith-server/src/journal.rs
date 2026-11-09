//! Per-session event journal: assigns monotonic sequence numbers, keeps a
//! bounded replay buffer, and fans events out live to every attached client.
//!
//! Replay semantics: `replay(since)` returns every journalled envelope with
//! `seq > since`. Subscribers deduplicate by sequence (an event published
//! between subscribing and snapshotting the replay appears in both).

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use smith_core::CoreEvent;
use smith_protocol::{Envelope, WireEvent};

/// Journal capacity: how far back a new/reconnecting viewer can replay.
const RING_CAPACITY: usize = 10_000;
/// Broadcast queue per subscriber; overflow is recovered from the ring.
const BROADCAST_QUEUE: usize = 4096;

pub struct Journal {
    seq: AtomicU64,
    cap: usize,
    buffer: Mutex<Vec<Envelope>>,
    tx: tokio::sync::broadcast::Sender<Envelope>,
}

impl Journal {
    pub fn new() -> Self {
        Self::with_capacity(RING_CAPACITY)
    }

    pub fn with_capacity(cap: usize) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(BROADCAST_QUEUE);
        Self {
            seq: AtomicU64::new(0),
            cap: cap.max(1),
            buffer: Mutex::new(Vec::new()),
            tx,
        }
    }

    /// Publish an event; returns its assigned sequence number.
    pub fn publish(&self, event: WireEvent) -> u64 {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let env = Envelope::new(seq, event);
        {
            let mut buf = self.buffer.lock().expect("journal mutex poisoned");
            buf.push(env.clone());
            // Trim the head to keep the ring bounded.
            if buf.len() > self.cap {
                let cut = buf.len() - self.cap;
                buf.drain(..cut);
            }
        }
        // No subscribers is fine (a session nobody is watching).
        let _ = self.tx.send(env);
        seq
    }

    pub fn publish_core(&self, event: CoreEvent) -> u64 {
        self.publish(WireEvent::Core { core: event })
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<Envelope> {
        self.tx.subscribe()
    }

    /// Every journalled envelope after `since`, in order.
    pub fn replay(&self, since: u64) -> Vec<Envelope> {
        self.buffer
            .lock()
            .expect("journal mutex poisoned")
            .iter()
            .filter(|e| e.seq > since)
            .cloned()
            .collect()
    }

    /// Highest sequence assigned so far.
    pub fn last_seq(&self) -> u64 {
        self.seq.load(Ordering::Relaxed)
    }
}

impl Default for Journal {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notice(text: &str) -> WireEvent {
        WireEvent::Core {
            core: CoreEvent::Notice { text: text.into() },
        }
    }

    #[test]
    fn sequences_are_monotonic_and_replay_is_filtered() {
        let j = Journal::new();
        assert_eq!(j.publish(notice("a")), 1);
        assert_eq!(j.publish(notice("b")), 2);
        assert_eq!(j.publish(notice("c")), 3);

        let all = j.replay(0);
        assert_eq!(all.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 2, 3]);
        let tail = j.replay(2);
        assert_eq!(tail.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![3]);
        assert!(j.replay(99).is_empty());
        assert_eq!(j.last_seq(), 3);
    }

    #[test]
    fn ring_is_bounded() {
        let j = Journal::with_capacity(10);
        for i in 0..100 {
            j.publish(notice(&format!("n{i}")));
        }
        let replay = j.replay(0);
        assert_eq!(replay.len(), 10);
        assert_eq!(replay[0].seq, 91);
        assert_eq!(replay[9].seq, 100);
    }

    #[tokio::test]
    async fn subscribers_receive_live_events() {
        let j = Journal::new();
        let mut rx = j.subscribe();
        j.publish(notice("live"));
        let env = rx.recv().await.unwrap();
        assert_eq!(env.seq, 1);
    }
}
