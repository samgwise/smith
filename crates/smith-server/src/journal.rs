//! Per-session event journal: assigns monotonic sequence numbers, keeps a
//! bounded replay buffer, and fans events out live to every attached client.
//!
//! Replay semantics: `replay(since)` returns every journalled envelope with
//! `seq > since`. Subscribers deduplicate by sequence (an event published
//! between subscribing and snapshotting the replay appears in both).
//!
//! Durability (M16): a journal opened over a file (one JSONL envelope per
//! line, the `.wire` file of a hosted session) survives the daemon — on
//! restart the ring, the seq counter and the last status snapshot rebuild
//! from it. Sink writes are best-effort: a failed write warns and degrades
//! to the in-memory ring (replay after that point starts from restart).

use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use smith_core::CoreEvent;
use smith_protocol::{Envelope, WireEvent};

/// Journal capacity: how far back a new/reconnecting viewer can replay.
pub const RING_CAPACITY: usize = 10_000;
/// Broadcast queue per subscriber; overflow is recovered from the ring.
const BROADCAST_QUEUE: usize = 4096;

pub struct Journal {
    seq: AtomicU64,
    cap: usize,
    buffer: Mutex<Vec<Envelope>>,
    tx: tokio::sync::broadcast::Sender<Envelope>,
    /// The append-only `.wire` sink (None for ephemeral journals).
    sink: Mutex<Option<BufWriter<File>>>,
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
            sink: Mutex::new(None),
        }
    }

    /// Open (or resume) a durable journal over `path`: rebuild the ring and
    /// the seq counter from whatever is on disk (a torn trailing line from a
    /// crash is skipped), then append. Ring cap trims the loaded window.
    pub fn load_persisted(path: &Path, cap: usize) -> anyhow::Result<Self> {
        let cap = cap.max(1);
        let mut buffer: Vec<Envelope> = Vec::new();
        let mut seq = 0u64;
        if let Ok(raw) = std::fs::read_to_string(path) {
            for line in raw.lines() {
                let Ok(env) = serde_json::from_str::<Envelope>(line) else {
                    continue; // torn line (or hand-edit): skip
                };
                seq = env.seq;
                buffer.push(env);
            }
        }
        if buffer.len() > cap {
            buffer.drain(..buffer.len() - cap);
        }
        let (tx, _) = tokio::sync::broadcast::channel(BROADCAST_QUEUE);
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            seq: AtomicU64::new(seq),
            cap,
            buffer: Mutex::new(buffer),
            tx,
            sink: Mutex::new(Some(BufWriter::new(file))),
        })
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
        self.persist(&env);
        // No subscribers is fine (a session nobody is watching).
        let _ = self.tx.send(env);
        seq
    }

    /// Append one envelope to the `.wire` sink. Best-effort: a failed write
    /// warns once and degrades the journal to in-memory (restart replay
    /// then starts from the daemon's next start).
    fn persist(&self, env: &Envelope) {
        let mut sink = self.sink.lock().expect("journal sink mutex poisoned");
        if let Some(w) = sink.as_mut() {
            let line = match serde_json::to_string(env) {
                Ok(l) => l,
                Err(e) => {
                    tracing::warn!("journal envelope serialisation failed: {e}");
                    return;
                }
            };
            use std::io::Write as _;
            if let Err(e) = writeln!(w, "{line}").and_then(|_| w.flush()) {
                tracing::warn!("journal sink write failed ({e:#}); degrading to in-memory replay");
                *sink = None;
            }
        }
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

    #[test]
    fn persisted_journal_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("abc.wire");
        {
            let j = Journal::load_persisted(&path, 100).unwrap();
            j.publish(notice("a"));
            j.publish(notice("b"));
            j.publish(notice("c"));
        }
        // A "restart": reload from the same file.
        let j2 = Journal::load_persisted(&path, 100).unwrap();
        let replay = j2.replay(0);
        assert_eq!(replay.len(), 3);
        assert_eq!(replay[2].seq, 3);
        // The seq counter continues (no reuse across the restart).
        assert_eq!(j2.publish(notice("d")), 4);
        let replay = j2.replay(0);
        assert_eq!(replay.len(), 4);
        assert!(
            matches!(&replay[3].event, WireEvent::Core { core } if matches!(core, CoreEvent::Notice { text } if text == "d"))
        );
    }

    #[test]
    fn torn_trailing_line_is_tolerated() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("abc.wire");
        let j = Journal::load_persisted(&path, 100).unwrap();
        j.publish(notice("a"));
        j.publish(notice("b"));
        drop(j);
        // Simulate a crash mid-write: garbage appended.
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        // A torn envelope: `{"v": 1,` with no close.
        write!(f, "{}", r#"{"v": 1,"#).unwrap();
        drop(f);
        let j2 = Journal::load_persisted(&path, 100).unwrap();
        assert_eq!(j2.replay(0).len(), 2);
        // And the next publish continues the sequence.
        assert_eq!(j2.publish(notice("c")), 3);
    }

    #[test]
    fn loading_trims_to_the_ring_cap() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("abc.wire");
        let j = Journal::load_persisted(&path, 10).unwrap();
        for i in 0..50 {
            j.publish(notice(&format!("n{i}")));
        }
        drop(j);
        let j2 = Journal::load_persisted(&path, 10).unwrap();
        let replay = j2.replay(0);
        assert_eq!(replay.len(), 10);
        assert_eq!(replay[0].seq, 41);
        assert_eq!(replay[9].seq, 50);
    }
}
