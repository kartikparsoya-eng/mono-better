//! Lightweight process-wide metrics — a Rust analog of the OTel counters the TS
//! view-syncer maintains (`#hydrations`, `#pipelineResets`,
//! `#queryTransformations`, pokes, …). The CG threads (which own the `!Send`
//! `SyncEngine`) increment these atomically; the HTTP `/statz` handler reads a
//! snapshot on the tokio thread.
//!
//! This is intentionally a counter layer, not a full OpenTelemetry pipeline:
//! there is no exporter, no histograms, and no attribute dimensions yet (those
//! are the remaining part of the metrics/OTel task). It gives real, queryable
//! observability of the hot path without pulling in the OTel SDK.

use std::sync::atomic::{AtomicU64, Ordering};

/// Shared counters. Cheap to clone the `Arc`; every field is an atomic.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Query hydrations (a `config_and_hydrate` that added ≥1 query).
    pub hydrations: AtomicU64,
    /// Advances applied from a change-streamer notification.
    pub advances: AtomicU64,
    /// Pipeline resets (advance reported a reset → re-init + rehydrate).
    pub resets: AtomicU64,
    /// Queries evicted by the TTL scheduler.
    pub expired_queries: AtomicU64,
    /// `updateAuth` messages that changed the resolved auth (re-transform).
    pub auth_changes: AtomicU64,
    /// deleteClients operations processed.
    pub client_deletions: AtomicU64,
}

impl Metrics {
    pub fn inc(field: &AtomicU64) {
        field.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(field: &AtomicU64, n: u64) {
        field.fetch_add(n, Ordering::Relaxed);
    }

    /// A JSON snapshot for the `/statz` endpoint.
    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "hydrations": self.hydrations.load(Ordering::Relaxed),
            "advances": self.advances.load(Ordering::Relaxed),
            "resets": self.resets.load(Ordering::Relaxed),
            "expiredQueries": self.expired_queries.load(Ordering::Relaxed),
            "authChanges": self.auth_changes.load(Ordering::Relaxed),
            "clientDeletions": self.client_deletions.load(Ordering::Relaxed),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_increment_and_snapshot() {
        let m = Metrics::default();
        Metrics::inc(&m.hydrations);
        Metrics::inc(&m.hydrations);
        Metrics::add(&m.expired_queries, 3);
        let s = m.snapshot();
        assert_eq!(s["hydrations"], 2);
        assert_eq!(s["expiredQueries"], 3);
        assert_eq!(s["advances"], 0);
    }
}
