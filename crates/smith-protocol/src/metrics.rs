//! Minimal metrics: atomic counters, label counters, gauges and a
//! fixed-bucket histogram rendered in Prometheus text exposition format.
//!
//! Hand-rolled on purpose (the linguine-review advice, taken from the first
//! worker): the format is just text, so scraping needs zero dependencies —
//! the daemon exposes `/v1/metrics`, the worker its own loopback port.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Counter with one label value per series (e.g. `{tool="shell",ok="true"}`).
#[derive(Default)]
pub struct LabelCounter {
    series: Mutex<HashMap<String, AtomicU64>>,
}

impl LabelCounter {
    pub fn inc(&self, label: &str) {
        self.add(label, 1);
    }

    pub fn add(&self, label: &str, n: u64) {
        self.series
            .lock()
            .expect("label counter poisoned")
            .entry(label.to_string())
            .or_default()
            .fetch_add(n, Ordering::Relaxed);
    }

    fn render(&self, name: &str, out: &mut String) {
        let series = self.series.lock().expect("label counter poisoned");
        let mut keys: Vec<&String> = series.keys().collect();
        keys.sort();
        for key in keys {
            let value = series.get(key).expect("key from its own map");
            out.push_str(&format!(
                "{name}{{{key}}} {}\n",
                value.load(Ordering::Relaxed)
            ));
        }
    }
}

/// Gauge per label value — set (not incremented) from current state.
#[derive(Default)]
pub struct GaugeMap {
    series: Mutex<HashMap<String, u64>>,
}

impl GaugeMap {
    pub fn set(&self, label: &str, value: u64) {
        self.series
            .lock()
            .expect("gauge map poisoned")
            .insert(label.to_string(), value);
    }

    pub fn get(&self, label: &str) -> u64 {
        self.series
            .lock()
            .expect("gauge map poisoned")
            .get(label)
            .copied()
            .unwrap_or(0)
    }

    fn render(&self, name: &str, out: &mut String) {
        let series = self.series.lock().expect("gauge map poisoned");
        let mut keys: Vec<&String> = series.keys().collect();
        keys.sort();
        for key in keys {
            out.push_str(&format!(
                "{name}{{{key}}} {}\n",
                series.get(key).expect("key from its own map")
            ));
        }
    }
}

/// Fixed-bucket histogram (seconds), Prometheus-style cumulative counts.
pub struct Histogram {
    buckets_ms: &'static [u64],
    buckets: Mutex<HashMap<u64, AtomicU64>>, // bucket -> cumulative count
    count: AtomicU64,
    sum_ms: AtomicU64,
}

/// Default bucket edges in milliseconds.
pub const DEFAULT_BUCKETS_MS: &[u64] = &[50, 100, 250, 500, 1_000, 5_000, 15_000, 60_000, 300_000];

impl Default for Histogram {
    fn default() -> Self {
        Self::new(DEFAULT_BUCKETS_MS)
    }
}

impl Histogram {
    /// Bucket edges in milliseconds.
    pub fn new(buckets_ms: &'static [u64]) -> Self {
        Self {
            buckets_ms,
            buckets: Mutex::new(HashMap::new()),
            count: AtomicU64::new(0),
            sum_ms: AtomicU64::new(0),
        }
    }

    pub fn observe_ms(&self, ms: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ms.fetch_add(ms, Ordering::Relaxed);
        let mut buckets = self.buckets.lock().expect("histogram poisoned");
        for edge in self.buckets_ms {
            if ms <= *edge {
                buckets
                    .entry(*edge)
                    .or_default()
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn render(&self, name: &str, out: &mut String) {
        let buckets = self.buckets.lock().expect("histogram poisoned");
        for edge in self.buckets_ms {
            let count = buckets
                .get(edge)
                .map(|c| c.load(Ordering::Relaxed))
                .unwrap_or(0);
            out.push_str(&format!(
                "{name}_bucket{{le=\"{:.3}\"}} {count}\n",
                *edge as f64 / 1000.0
            ));
        }
        out.push_str(&format!(
            "{name}_bucket{{le=\"+Inf\"}} {}\n",
            self.count.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "{name}_sum {}\n",
            self.sum_ms.load(Ordering::Relaxed) as f64 / 1000.0
        ));
        out.push_str(&format!(
            "{name}_count {}\n",
            self.count.load(Ordering::Relaxed)
        ));
    }
}

/// The daemon's metric set. Everything is `Arc`-shareable and lock-free on
/// the hot path except the (rarely contended) label maps.
#[derive(Default)]
pub struct Metrics {
    /// Tool calls by `tool="…",ok="true|false"`.
    pub tool_calls_total: LabelCounter,
    /// Exec duration in seconds (worker exec path).
    pub exec_duration_seconds: Histogram,
    /// Session queue wait in seconds (worker-mode creation → dispatch).
    pub queue_wait_seconds: Histogram,
    /// Live leases on the board.
    pub leases_active: AtomicU64,
    /// Lease losses (stale/revoked/disconnect).
    pub leases_lost_total: AtomicU64,
    /// Clearance decisions by `decision="allow|always|deny"`.
    pub clearance_total: LabelCounter,
    /// Approvals by `decision="allow_once|allow_always|deny"` (and `timeout`).
    pub approvals_total: LabelCounter,
    /// Workers by `state="live|stale|revoked"` (gauge).
    pub workers: GaugeMap,
    /// Worker heartbeats received.
    pub worker_heartbeats_total: AtomicU64,
    /// Sessions by `executor="local|worker"`.
    pub sessions: GaugeMap,
}

impl Metrics {
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("# smith build {}\n", env!("CARGO_PKG_VERSION")));
        self.tool_calls_total
            .render("smith_tool_calls_total", &mut out);
        self.clearance_total
            .render("smith_clearance_total", &mut out);
        self.approvals_total
            .render("smith_approvals_total", &mut out);
        self.workers.render("smith_workers", &mut out);
        self.sessions.render("smith_sessions", &mut out);
        out.push_str(&format!(
            "smith_leases_active {}\n",
            self.leases_active.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "smith_leases_lost_total {}\n",
            self.leases_lost_total.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "smith_worker_heartbeats_total {}\n",
            self.worker_heartbeats_total.load(Ordering::Relaxed)
        ));
        self.exec_duration_seconds
            .render("smith_exec_duration_seconds", &mut out);
        self.queue_wait_seconds
            .render("smith_queue_wait_seconds", &mut out);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_format_is_well_formed() {
        let m = Metrics::default();
        m.tool_calls_total.inc("tool=\"shell\",ok=\"true\"");
        m.tool_calls_total.inc("tool=\"shell\",ok=\"true\"");
        m.tool_calls_total.inc("tool=\"list_dir\",ok=\"true\"");
        m.clearance_total.inc("decision=\"allow\"");
        m.workers.set("state=\"live\"", 2);
        m.leases_active.store(3, Ordering::Relaxed);
        m.exec_duration_seconds.observe_ms(250);

        let text = m.render();
        assert!(text.contains("smith_tool_calls_total{tool=\"shell\",ok=\"true\"} 2"));
        assert!(text.contains("smith_workers{state=\"live\"} 2"));
        assert!(text.contains("smith_leases_active 3"));
        assert!(text.contains("smith_exec_duration_seconds_bucket{le=\"0.250\"} 1"));
        assert!(text.contains("smith_exec_duration_seconds_count 1"));
        // Every non-comment line is `name{labels} value`.
        for line in text.lines().filter(|l| !l.starts_with('#')) {
            assert!(
                line.contains('}') || !line.contains('{'),
                "malformed metric line: {line}"
            );
        }
    }
}
