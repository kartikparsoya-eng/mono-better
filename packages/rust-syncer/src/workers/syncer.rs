//! Cross-CG serving-lag statistics — faithful port of the serving-lag helpers in
//! `zero-cache/src/workers/syncer.ts` (lines 52-253).
//!
//! The TS `Syncer` maintains a process-wide, watermark-ordered log of
//! replica-ready states (`{watermark, replicaReadyTimeMs}`) and, on a 60s timer +
//! observable-gauge callbacks, computes how far behind the replica each active
//! view-syncer is (the "serving lag") — emitting the `serving_lag`,
//! `serving_lag_stats` (min/p50/p75/p99/max) and `serving_lagging_client_groups`
//! gauges plus the `view_syncer_lag` native histogram.
//!
//! These functions are pure: they take the state log + a snapshot of each active
//! view-syncer's serving-lag fields and compute the distribution. The stateful
//! wiring (the shared `ServingLagRegistry` CGs publish into + the replica-ready
//! feed) lives below in this module; the router feeds it and the 60s sampler +
//! OTel gauges live in [`crate::metrics`]. Names are the snake_case of the exact
//! TS identifiers.

/// TS `MAX_REPLICA_READY_STATES`.
pub const MAX_REPLICA_READY_STATES: usize = 10_000;
/// TS `VIEW_SYNCER_LAG_SAMPLE_INTERVAL_MS`.
pub const VIEW_SYNCER_LAG_SAMPLE_INTERVAL_MS: u64 = 60_000;

/// TS `ReplicaReadyState`. `watermark` is a lexicographically-ordered version
/// string; `replica_ready_time_ms` is a wall-clock epoch millisecond.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaReadyState {
    pub watermark: String,
    pub replica_ready_time_ms: i64,
}

/// TS `ServingLagViewSyncer` — the `Pick<ViewSyncer, 'createdAtMs' |
/// 'servedVersion' | 'servingLagEligible'>` projection used by the lag math. A
/// snapshot (not a live handle) so the sampler can read it off the CG threads.
#[derive(Clone, Debug)]
pub struct ServingLagViewSyncer {
    pub created_at_ms: i64,
    pub served_version: Option<String>,
    pub serving_lag_eligible: bool,
}

/// TS `ServingLagStats`.
#[derive(Clone, Debug, PartialEq)]
pub struct ServingLagStats {
    pub active_client_groups: usize,
    pub lagging_client_groups: usize,
    pub min_ms: i64,
    pub p50_ms: i64,
    pub p75_ms: i64,
    pub p99_ms: i64,
    pub max_ms: i64,
}

/// TS `ServingLagDistribution` = `ServingLagStats & {lagsMs}`.
#[derive(Clone, Debug, PartialEq)]
pub struct ServingLagDistribution {
    pub active_client_groups: usize,
    pub lagging_client_groups: usize,
    pub min_ms: i64,
    pub p50_ms: i64,
    pub p75_ms: i64,
    pub p99_ms: i64,
    pub max_ms: i64,
    pub lags_ms: Vec<i64>,
}

impl ServingLagDistribution {
    /// The `ServingLagStats` subset (TS `computeServingLagStatsMs` projection).
    pub fn stats(&self) -> ServingLagStats {
        ServingLagStats {
            active_client_groups: self.active_client_groups,
            lagging_client_groups: self.lagging_client_groups,
            min_ms: self.min_ms,
            p50_ms: self.p50_ms,
            p75_ms: self.p75_ms,
            p99_ms: self.p99_ms,
            max_ms: self.max_ms,
        }
    }
}

/// TS `boundReplicaReadyStates`: cap the log at `MAX_REPLICA_READY_STATES`,
/// dropping the oldest entries.
pub fn bound_replica_ready_states(replica_ready_states: &mut Vec<ReplicaReadyState>) {
    if replica_ready_states.len() > MAX_REPLICA_READY_STATES {
        let drop = replica_ready_states.len() - MAX_REPLICA_READY_STATES;
        replica_ready_states.drain(0..drop);
    }
}

/// TS `pruneReplicaReadyStates`: drop everything before `first_needed_index`,
/// then re-bound.
pub fn prune_replica_ready_states(
    replica_ready_states: &mut Vec<ReplicaReadyState>,
    first_needed_index: usize,
) {
    if first_needed_index > 0 {
        let drop = first_needed_index.min(replica_ready_states.len());
        replica_ready_states.drain(0..drop);
    }
    bound_replica_ready_states(replica_ready_states);
}

/// TS `lowerBoundReplicaReadyTimeMs`: first index whose `replicaReadyTimeMs` is
/// `>= replica_ready_time_ms` (binary search over the time-ordered log).
pub fn lower_bound_replica_ready_time_ms(
    replica_ready_states: &[ReplicaReadyState],
    replica_ready_time_ms: i64,
) -> usize {
    let mut low = 0;
    let mut high = replica_ready_states.len();
    while low < high {
        let mid = low + (high - low) / 2;
        if replica_ready_states[mid].replica_ready_time_ms < replica_ready_time_ms {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}

/// TS `upperBoundWatermark`: first index whose `watermark` is strictly `>`
/// `watermark` (binary search over the watermark-ordered log).
pub fn upper_bound_watermark(replica_ready_states: &[ReplicaReadyState], watermark: &str) -> usize {
    let mut low = 0;
    let mut high = replica_ready_states.len();
    while low < high {
        let mid = low + (high - low) / 2;
        if replica_ready_states[mid].watermark.as_str() <= watermark {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}

/// TS `findFirstUnservedIndex`: the earliest replica-ready state this view-syncer
/// has NOT yet served (later than both its creation and its served version), or
/// `-1` if fully served. Returned as `Option<usize>` (`None` == TS `-1`).
pub fn find_first_unserved_index(
    replica_ready_states: &[ReplicaReadyState],
    view_syncer: &ServingLagViewSyncer,
) -> Option<usize> {
    let first_ready_after_creation =
        lower_bound_replica_ready_time_ms(replica_ready_states, view_syncer.created_at_ms);
    let first_after_served_version = match &view_syncer.served_version {
        None => 0,
        Some(v) => upper_bound_watermark(replica_ready_states, v),
    };
    let first_unserved_index = first_ready_after_creation.max(first_after_served_version);
    if first_unserved_index < replica_ready_states.len() {
        Some(first_unserved_index)
    } else {
        None
    }
}

/// TS `percentileNearestRank`: the nearest-rank percentile of an ascending slice.
pub fn percentile_nearest_rank(sorted_values: &[i64], percentile: f64) -> i64 {
    if sorted_values.is_empty() {
        return 0;
    }
    let len = sorted_values.len();
    // Math.min(len-1, Math.max(0, Math.ceil((pct/100)*len) - 1))
    let raw = ((percentile / 100.0) * len as f64).ceil() - 1.0;
    let index = raw.max(0.0).min((len - 1) as f64) as usize;
    sorted_values[index]
}

/// TS `computeServingLagDistributionMs`: for each serving-lag-eligible
/// view-syncer, measure how long its earliest-unserved replica change has been
/// waiting, prune the now-unneeded prefix of the log, and summarize the
/// distribution. Mutates `replica_ready_states` (the prune), matching TS.
pub fn compute_serving_lag_distribution_ms<'a>(
    now: i64,
    replica_ready_states: &mut Vec<ReplicaReadyState>,
    view_syncers: impl IntoIterator<Item = &'a ServingLagViewSyncer>,
) -> ServingLagDistribution {
    let mut lags: Vec<i64> = Vec::new();
    let mut lagging_client_groups = 0usize;
    let mut first_needed_index = replica_ready_states.len();

    for view_syncer in view_syncers {
        if !view_syncer.serving_lag_eligible {
            continue;
        }
        match find_first_unserved_index(replica_ready_states, view_syncer) {
            None => {
                lags.push(0);
            }
            Some(first_unserved_index) => {
                first_needed_index = first_needed_index.min(first_unserved_index);
                let lag_ms =
                    (now - replica_ready_states[first_unserved_index].replica_ready_time_ms).max(0);
                lags.push(lag_ms);
                if lag_ms > 0 {
                    lagging_client_groups += 1;
                }
            }
        }
    }

    prune_replica_ready_states(replica_ready_states, first_needed_index);

    lags.sort_unstable();
    ServingLagDistribution {
        active_client_groups: lags.len(),
        lagging_client_groups,
        // The percentile fields are for the legacy serving_lag_stats gauge. The
        // native view_syncer_lag histogram records lags_ms and computes
        // percentiles in Prometheus/AMP.
        min_ms: lags.first().copied().unwrap_or(0),
        p50_ms: percentile_nearest_rank(&lags, 50.0),
        p75_ms: percentile_nearest_rank(&lags, 75.0),
        p99_ms: percentile_nearest_rank(&lags, 99.0),
        max_ms: lags.last().copied().unwrap_or(0),
        lags_ms: lags,
    }
}

/// TS `computeServingLagStatsMs`.
pub fn compute_serving_lag_stats_ms<'a>(
    now: i64,
    replica_ready_states: &mut Vec<ReplicaReadyState>,
    view_syncers: impl IntoIterator<Item = &'a ServingLagViewSyncer>,
) -> ServingLagStats {
    compute_serving_lag_distribution_ms(now, replica_ready_states, view_syncers).stats()
}

/// TS `computeMaxServingLagMs`.
pub fn compute_max_serving_lag_ms<'a>(
    now: i64,
    replica_ready_states: &mut Vec<ReplicaReadyState>,
    view_syncers: impl IntoIterator<Item = &'a ServingLagViewSyncer>,
) -> i64 {
    compute_serving_lag_stats_ms(now, replica_ready_states, view_syncers).max_ms
}

// ---------------------------------------------------------------------------
// Stateful wiring: the process-wide registry the cross-CG sampler reads.
//
// Port of the `Syncer` class's serving-lag state (`#replicaReadyStates`,
// `#recordReplicaReadyState`, `#computeServingLagDistribution` with its
// microtask cache, `#recordViewSyncerLagSamples`). Because rust-syncer runs each
// CG as a `!Send` `spawn_local` task on a sharded executor thread, the sampler
// (on the main runtime) cannot read the CG states directly — so each CG
// publishes a small `Send` snapshot into this shared registry, and the `/notify`
// broadcast (the one process-wide replica-ready feed) records the ready states.
// ---------------------------------------------------------------------------

use dashmap::DashMap;
use std::sync::Mutex;
use std::time::Instant;

/// A CG's contribution to the serving-lag computation: its lag-relevant fields
/// (TS `ServingLagViewSyncer`) plus its query/row counts (TS `vs.queryCount` /
/// `vs.rowCount`, summed for the `queries` / `rows` gauges).
#[derive(Clone, Debug)]
pub struct CgServingSnapshot {
    pub lag: ServingLagViewSyncer,
    pub num_queries: usize,
    pub num_rows: usize,
}

/// How long a computed distribution stays valid — the Rust analog of TS's
/// per-microtask `#servingLagDistributionCache` (so the 5 gauge callbacks in one
/// scrape share a single compute + prune rather than pruning five times).
const DISTRIBUTION_CACHE_TTL_MS: u128 = 200;

/// Process-wide serving-lag state. Held as `Arc<ServingLagRegistry>` by the
/// `Syncer` and cloned into every CG. `Send + Sync`.
pub struct ServingLagRegistry {
    replica_ready_states: Mutex<Vec<ReplicaReadyState>>,
    view_syncers: DashMap<String, CgServingSnapshot>,
    cache: Mutex<Option<(Instant, ServingLagDistribution)>>,
}

impl std::fmt::Debug for ServingLagRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServingLagRegistry")
            .field("view_syncers", &self.view_syncers.len())
            .finish()
    }
}

impl Default for ServingLagRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ServingLagRegistry {
    pub fn new() -> Self {
        Self {
            replica_ready_states: Mutex::new(Vec::new()),
            view_syncers: DashMap::new(),
            cache: Mutex::new(None),
        }
    }

    /// TS `#recordReplicaReadyState`: append a replica-ready state (monotonic by
    /// watermark), clearing the log when no CG is active. Fed once per commit
    /// from the `/notify` broadcast.
    pub fn record_replica_ready_state(&self, watermark: &str, replica_ready_time_ms: i64) {
        let mut states = self.replica_ready_states.lock().unwrap();
        if let Some(last) = states.last()
            && last.watermark.as_str() >= watermark
        {
            return;
        }
        states.push(ReplicaReadyState {
            watermark: watermark.to_string(),
            replica_ready_time_ms,
        });
        if self.view_syncers.is_empty() {
            states.clear();
            return;
        }
        bound_replica_ready_states(&mut states);
    }

    /// A CG publishes (or refreshes) its serving-lag snapshot.
    pub fn upsert_view_syncer(&self, cg_id: &str, snapshot: CgServingSnapshot) {
        self.view_syncers.insert(cg_id.to_string(), snapshot);
    }

    /// A CG is torn down: drop its snapshot.
    pub fn remove_view_syncer(&self, cg_id: &str) {
        self.view_syncers.remove(cg_id);
    }

    /// TS `#computeServingLagDistribution`: compute (and cache for one scrape) the
    /// current lag distribution across all published CGs, pruning the now-unneeded
    /// prefix of the replica-ready log.
    pub fn compute_serving_lag_distribution(&self, now: i64) -> ServingLagDistribution {
        {
            let cache = self.cache.lock().unwrap();
            if let Some((at, dist)) = cache.as_ref()
                && at.elapsed().as_millis() < DISTRIBUTION_CACHE_TTL_MS
            {
                return dist.clone();
            }
        }
        let snapshots: Vec<ServingLagViewSyncer> = self
            .view_syncers
            .iter()
            .map(|e| e.value().lag.clone())
            .collect();
        let mut states = self.replica_ready_states.lock().unwrap();
        let dist = compute_serving_lag_distribution_ms(now, &mut states, snapshots.iter());
        drop(states);
        *self.cache.lock().unwrap() = Some((Instant::now(), dist.clone()));
        dist
    }

    /// Sum of active queries across all CGs (TS `queries` gauge).
    pub fn total_queries(&self) -> u64 {
        self.view_syncers
            .iter()
            .map(|e| e.value().num_queries as u64)
            .sum()
    }

    /// Sum of tracked rows across all CGs (TS `rows` gauge).
    pub fn total_rows(&self) -> u64 {
        self.view_syncers
            .iter()
            .map(|e| e.value().num_rows as u64)
            .sum()
    }

    /// Number of active CGs (TS `active-client-groups` gauge companion).
    pub fn active_client_groups(&self) -> usize {
        self.view_syncers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(w: &str, t: i64) -> ReplicaReadyState {
        ReplicaReadyState {
            watermark: w.to_string(),
            replica_ready_time_ms: t,
        }
    }
    fn vs(created: i64, served: Option<&str>, eligible: bool) -> ServingLagViewSyncer {
        ServingLagViewSyncer {
            created_at_ms: created,
            served_version: served.map(|s| s.to_string()),
            serving_lag_eligible: eligible,
        }
    }

    #[test]
    fn percentile_nearest_rank_matches_ts() {
        assert_eq!(percentile_nearest_rank(&[], 50.0), 0);
        assert_eq!(percentile_nearest_rank(&[5], 50.0), 5);
        // 1..=10, p50 -> ceil(0.5*10)-1 = 4 -> value 5
        let v: Vec<i64> = (1..=10).collect();
        assert_eq!(percentile_nearest_rank(&v, 50.0), 5);
        assert_eq!(percentile_nearest_rank(&v, 75.0), 8);
        assert_eq!(percentile_nearest_rank(&v, 99.0), 10);
        assert_eq!(percentile_nearest_rank(&v, 0.0), 1);
        assert_eq!(percentile_nearest_rank(&v, 100.0), 10);
    }

    #[test]
    fn lower_bound_and_upper_bound() {
        let s = vec![state("02", 100), state("04", 200), state("06", 300)];
        assert_eq!(lower_bound_replica_ready_time_ms(&s, 100), 0);
        assert_eq!(lower_bound_replica_ready_time_ms(&s, 150), 1);
        assert_eq!(lower_bound_replica_ready_time_ms(&s, 400), 3);
        assert_eq!(upper_bound_watermark(&s, "01"), 0);
        assert_eq!(upper_bound_watermark(&s, "04"), 2);
        assert_eq!(upper_bound_watermark(&s, "09"), 3);
    }

    #[test]
    fn find_first_unserved_index_none_when_all_served() {
        let s = vec![state("02", 100), state("04", 200)];
        // served up to 04 -> fully served -> None
        assert_eq!(
            find_first_unserved_index(&s, &vs(0, Some("04"), true)),
            None
        );
        // never served, created before all -> index 0
        assert_eq!(find_first_unserved_index(&s, &vs(0, None, true)), Some(0));
        // created after everything -> None
        assert_eq!(find_first_unserved_index(&s, &vs(999, None, true)), None);
    }

    /// Layer-2 body-differential: the real TS `computeServingLagStatsMs` /
    /// `computeMaxServingLagMs` outputs (captured in `serving-lag-fixture.json`
    /// by `generate-serving-lag-fixture.mjs`) must be reproduced byte-for-byte.
    /// Exercises the whole chain (percentile / findFirstUnserved / bounds /
    /// distribution + prune) against TS, not the porter's reading.
    #[test]
    fn serving_lag_parity_against_ts() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/agentic/parity/serving-lag-fixture.json"
        );
        let bytes = std::fs::read(path)
            .unwrap_or_else(|e| panic!("failed to read serving-lag fixture {path}: {e}"));
        let fixture: serde_json::Value =
            serde_json::from_slice(&bytes).expect("serving-lag fixture is not valid JSON");
        let cases = fixture
            .get("cases")
            .and_then(serde_json::Value::as_array)
            .expect("fixture.cases missing");
        assert!(!cases.is_empty(), "fixture has no cases");

        let i = |v: &serde_json::Value, k: &str| v.get(k).and_then(|x| x.as_i64()).unwrap();
        for case in cases {
            let desc = case.get("desc").and_then(|v| v.as_str()).unwrap_or("");
            let now = i(case, "now");
            let states0: Vec<ReplicaReadyState> = case["replicaReadyStates"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| ReplicaReadyState {
                    watermark: s["watermark"].as_str().unwrap().to_string(),
                    replica_ready_time_ms: s["replicaReadyTimeMs"].as_i64().unwrap(),
                })
                .collect();
            let syncers: Vec<ServingLagViewSyncer> = case["viewSyncers"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| ServingLagViewSyncer {
                    created_at_ms: v["createdAtMs"].as_i64().unwrap(),
                    served_version: v["servedVersion"].as_str().map(str::to_string),
                    serving_lag_eligible: v["servingLagEligible"].as_bool().unwrap(),
                })
                .collect();

            let ts = &case["stats"];
            let expected = ServingLagStats {
                active_client_groups: i(ts, "activeClientGroups") as usize,
                lagging_client_groups: i(ts, "laggingClientGroups") as usize,
                min_ms: i(ts, "minMs"),
                p50_ms: i(ts, "p50Ms"),
                p75_ms: i(ts, "p75Ms"),
                p99_ms: i(ts, "p99Ms"),
                max_ms: i(ts, "maxMs"),
            };
            // Each fn mutates its states arg (prune) — feed fresh copies.
            let mut s_stats = states0.clone();
            let got = compute_serving_lag_stats_ms(now, &mut s_stats, syncers.iter());
            assert_eq!(got, expected, "stats mismatch for case: {desc}");

            let mut s_max = states0.clone();
            let got_max = compute_max_serving_lag_ms(now, &mut s_max, syncers.iter());
            assert_eq!(got_max, i(case, "maxMs"), "maxMs mismatch for case: {desc}");
        }
    }

    #[test]
    fn distribution_prunes_and_summarizes() {
        let mut s = vec![state("02", 100), state("04", 200), state("06", 300)];
        let syncers = [
            vs(0, Some("02"), true), // unserved from idx 1 (t=200) -> lag 800
            vs(0, Some("04"), true), // unserved from idx 2 (t=300) -> lag 700
            vs(0, None, false),      // ineligible -> skipped
        ];
        let d = compute_serving_lag_distribution_ms(1000, &mut s, syncers.iter());
        assert_eq!(d.active_client_groups, 2);
        assert_eq!(d.lagging_client_groups, 2);
        assert_eq!(d.min_ms, 700);
        assert_eq!(d.max_ms, 800);
        // first_needed_index was min(1,2)=1 -> pruned first entry
        assert_eq!(s.len(), 2);
    }
}

// ─── Syncer connection management (L9 Stage 2a) ──────────────────────────────
// Port of the `Syncer` class's connection-management half
// (workers/syncer.ts:288+): accept-path connection creation, the live
// connection map, group user pinning, drain. Moved verbatim from router.rs;
// the CG executor substrate (CGMessage/CGHandle/executors) remains in
// router.rs until the Stage-3 quarantine into workers/cg_executor.rs.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::mpsc;

use std::thread::JoinHandle;

use crate::router::{
    AuthValidator, CGHandle, CGMessage, CGServicesFactory, Executor, ExecutorCommand,
    decrement_nonzero, default_num_shards, forward_inbound, lock_unpoisoned, now_ms, run_executor,
    shard_for,
};
use crate::ws_server::ConnectionContext;
use crate::ws_sink::DirectWebSocketSink;
use rust_cvr::shards::ShardID;

/// Group auth state — tracks the pinned user for a client group.
///
/// Port of `GroupAuthState` in `connection-context-manager.ts`.
#[derive(Debug, Clone, Default)]
pub struct GroupAuthState {
    /// The user ID that this client group is pinned to.
    /// `None` = no user has been validated yet.
    pub pinned_user_id: Option<String>,
}

/// Check the incoming userID against the group's pin and, on the first
/// connection, BIND it. `Ok` = allowed (and now pinned); `Err` = the group is
/// already pinned to a different userID and the connection must be rejected.
/// Port of the pin logic in TS `ConnectionContextManager.validateConnection`.
pub(crate) fn check_and_pin_user(group: &mut GroupAuthState, incoming: &str) -> Result<(), ()> {
    match group.pinned_user_id.clone() {
        Some(pinned) if pinned != incoming => Err(()),
        Some(_) => Ok(()),
        None => {
            group.pinned_user_id = Some(incoming.to_string());
            Ok(())
        }
    }
}

/// The connection router — hosts client groups on a bounded pool of `K` executor
/// threads and routes connections to them (doc 91, sharded async executors).
///
/// Port of the `Syncer` class's connection management.
pub struct Syncer {
    /// Map of client_group_id → CG handle.
    pub(crate) cg_handles: Arc<DashMap<String, CGHandle>>,
    /// Serializes lookup/create/evict so two first connections cannot register
    /// two tasks for the same client group.
    cg_creation_lock: Arc<Mutex<()>>,
    max_client_groups: usize,
    /// The `K` executor threads. A new client group is placed on the least-loaded
    /// executor (see [`place_cg`](Self::place_cg)) and hosted there for its
    /// lifetime, pinning its `!Send` `SyncEngine` to one thread by construction.
    /// Each executor holds its own clone of the services factory, so the router
    /// does not retain one.
    executors: Vec<Executor>,
    /// Auth validator (used to validate a connection's JWT before admission).
    auth_validator: Arc<dyn AuthValidator>,
    /// Shared process metrics (read by `/statz`, written by CG threads).
    metrics: Arc<crate::metrics::Metrics>,
    /// Active connections: client_id → connection info.
    connections: Arc<Mutex<HashMap<String, ConnectionInfo>>>,
    /// Group auth states: client_group_id → GroupAuthState.
    group_auth_states: Arc<Mutex<HashMap<String, GroupAuthState>>>,
    /// The most recent broadcast notification. A client group created AFTER the
    /// last commit would otherwise never learn that commit's watermark/commit
    /// time until the NEXT commit — TS's in-process notifier replays the latest
    /// `ReplicaState` to every new subscriber (notifier.ts). Handed to each
    /// newly spawned CG to arm its serving-lag tracker.
    last_notification: Arc<Mutex<Option<serde_json::Value>>>,
    /// Process-wide serving-lag state (replica-ready log + per-CG snapshots),
    /// read by the 60s sampler + the `serving_lag*`/`queries`/`rows` gauges. Port
    /// of the `Syncer` class's `#replicaReadyStates` + view-syncer iteration.
    serving_lag_registry: Arc<crate::workers::syncer::ServingLagRegistry>,
    /// Whether the router is shutting down.
    shutting_down: Arc<AtomicBool>,
    /// Server shard identity ({appID, shardNum}). Read on the accept task to
    /// build the `connected` message body (`handle_connection`). TS reads the
    /// same from the shard config in `syncer.ts#handleConnection` when
    /// constructing `['connected', {wsid, timestamp, appID, shardNum}]`.
    shard: ShardID,
}

/// Info about an active connection.
#[derive(Clone)]
pub(crate) struct ConnectionInfo {
    pub(crate) client_group_id: String,
    pub(crate) ws_id: String,
    /// The connection's downstream sink. Cloneable + `Send + Sync` (an
    /// `UnboundedSender` + `Arc<SinkLimits>`), so services running on the tokio
    /// runtime — e.g. the push relay drainer — can deliver a frame to this
    /// client's socket without reaching into the CG executor threads.
    pub(crate) sink: DirectWebSocketSink,
}

/// Sink registry handed to services that must deliver frames to a specific
/// client's socket from the tokio runtime (the push-relay drainer, which learns
/// of a POST failure long after the message-handling path has returned). Wraps
/// the router's live connection map, so delivery follows the exact
/// insert/remove lifecycle already maintained for `ConnectionInfo` — no second
/// structure to leak. Delivery is `ws_id`-guarded (see `send_error_if_current`).
#[derive(Clone)]
pub struct ConnectionSinks(Arc<Mutex<HashMap<String, ConnectionInfo>>>);

impl ConnectionSinks {
    /// A fresh, empty registry. Prod shares one instance between the router
    /// (which populates it) and the services factory (which hands it to the
    /// pusher); tests/other constructors get their own.
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    /// Send a non-fatal error frame to `client_id`'s CURRENT socket iff it is
    /// still `ws_id`. Never closes the connection. Returns whether delivered.
    ///
    /// The `ws_id` guard matters: by the time a relay POST fails the client may
    /// have reconnected (new socket, same `client_id`). The replacement
    /// connection re-pushes anything above the server lmid on reconnect, so
    /// failing the *new* socket for the *old* socket's push would be a spurious
    /// disconnect. Rust is deliberately stricter here than TS (which routes by
    /// `clientID` only) — a documented, strictly-safer divergence.
    pub fn send_error_if_current(
        &self,
        client_id: &str,
        ws_id: &str,
        error: &crate::protocol::ErrorBody,
    ) -> bool {
        let sink = {
            let conns = lock_unpoisoned(&self.0);
            match conns.get(client_id) {
                Some(info) if info.ws_id == ws_id => info.sink.clone(),
                _ => {
                    tracing::debug!(
                        client_id,
                        ws_id,
                        "push-failure target is no longer the current socket; dropping frame"
                    );
                    return false;
                }
            }
            // guard dropped here — never hold the lock across the push
        };
        sink.push(crate::protocol::error_message(error));
        true
    }
}

impl Default for ConnectionSinks {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl ConnectionSinks {
    /// Register a sink under `client_id`/`ws_id` for tests that exercise
    /// delivery without going through the full connection-admission path.
    pub(crate) fn insert_for_test(&self, client_id: &str, ws_id: &str, sink: DirectWebSocketSink) {
        lock_unpoisoned(&self.0).insert(
            client_id.to_string(),
            ConnectionInfo {
                client_group_id: "cg-test".to_string(),
                ws_id: ws_id.to_string(),
                sink,
            },
        );
    }
}

impl Syncer {
    pub fn new(
        services_factory: Arc<dyn CGServicesFactory>,
        auth_validator: Arc<dyn AuthValidator>,
        metrics: Arc<crate::metrics::Metrics>,
    ) -> Self {
        Self::new_with_limit(services_factory, auth_validator, metrics, 100)
    }

    /// Construct with a client-group cap but no CVR pool and the default executor
    /// count. Used by tests and in-memory dev (storeless CGs).
    pub fn new_with_limit(
        services_factory: Arc<dyn CGServicesFactory>,
        auth_validator: Arc<dyn AuthValidator>,
        metrics: Arc<crate::metrics::Metrics>,
        max_client_groups: usize,
    ) -> Self {
        Self::new_sharded(
            services_factory,
            auth_validator,
            metrics,
            max_client_groups,
            default_num_shards(),
            None,
            ConnectionSinks::new(),
            // Storeless/test default; the real shard is threaded in from `main`.
            ShardID {
                app_id: "zero".to_string(),
                shard_num: 0,
            },
        )
    }

    /// Full constructor: spawn `num_shards` executor threads, each running a
    /// `current_thread` runtime + `LocalSet` hosting a hash-shard of client
    /// groups (doc 91). `cvr_pool` is the ONE shared CVR `PgPool` (built on the
    /// process's main runtime); a clone is handed to every executor so groups
    /// draw from a single bounded connection budget, and CVR I/O is offloaded
    /// back onto that pool's runtime (`SyncEngine::offload`). `None` selects
    /// storeless CGs (tests / no-PG dev).
    #[allow(clippy::too_many_arguments)]
    pub fn new_sharded(
        services_factory: Arc<dyn CGServicesFactory>,
        auth_validator: Arc<dyn AuthValidator>,
        metrics: Arc<crate::metrics::Metrics>,
        max_client_groups: usize,
        num_shards: usize,
        cvr_pool: Option<sqlx::PgPool>,
        connection_sinks: ConnectionSinks,
        shard: ShardID,
    ) -> Self {
        let num_shards = num_shards.max(1);
        let cg_handles: Arc<DashMap<String, CGHandle>> = Arc::new(DashMap::new());
        // Share the registry's map so the pusher (given a clone of the same
        // `ConnectionSinks`) sees the connections this router admits.
        let connections = connection_sinks.0.clone();

        let shutting_down = Arc::new(AtomicBool::new(false));
        let mut executors = Vec::with_capacity(num_shards);
        for idx in 0..num_shards {
            let (ctrl_tx, ctrl_rx) = mpsc::unbounded_channel::<ExecutorCommand>();
            let factory = services_factory.clone();
            let validator = auth_validator.clone();
            let conns = connections.clone();
            let handles = cg_handles.clone();
            let pool = cvr_pool.clone();
            let shutdown_flag = shutting_down.clone();
            let join = std::thread::Builder::new()
                .name(format!("cg-exec-{idx}"))
                .spawn(move || {
                    run_executor(idx, ctrl_rx, factory, validator, conns, handles, pool);
                    // An executor thread must outlive the process outside of
                    // shutdown. Before this line, a dead shard was discovered
                    // only when a later CG *placement* happened to target it —
                    // operators learned about it from tail latency. Make the
                    // death loud and countable the moment it happens.
                    if !shutdown_flag.load(Ordering::SeqCst) {
                        tracing::error!(
                            "CG executor {idx} exited outside shutdown — its client groups are                              orphaned until their clients reconnect and re-place"
                        );
                        crate::metrics::record_fail_group("executor_exit");
                    }
                })
                .expect("failed to spawn CG executor thread");
            executors.push(Executor {
                ctrl_tx,
                join: Mutex::new(Some(join)),
                dead: AtomicBool::new(false),
            });
        }

        Self {
            cg_handles,
            cg_creation_lock: Arc::new(Mutex::new(())),
            max_client_groups: max_client_groups.max(1),
            executors,
            auth_validator,
            metrics,
            connections,
            group_auth_states: Arc::new(Mutex::new(HashMap::new())),
            last_notification: Arc::new(Mutex::new(None)),
            serving_lag_registry: Arc::new(crate::workers::syncer::ServingLagRegistry::new()),
            shutting_down,
            shard,
        }
    }

    /// The process-wide serving-lag registry (for `main` to register its gauges
    /// + spawn the 60s sampler).
    pub fn serving_lag_registry(&self) -> Arc<crate::workers::syncer::ServingLagRegistry> {
        self.serving_lag_registry.clone()
    }

    /// A JSON snapshot of the process metrics (for `/statz`).
    pub fn metrics_snapshot(&self) -> serde_json::Value {
        self.metrics.snapshot()
    }

    /// Prometheus text-format metrics (for `/metrics`), including the live
    /// active-client-groups gauge.
    pub fn metrics_prometheus(&self) -> String {
        self.metrics.render_prometheus(self.cg_count() as u64)
    }

    /// Handle a new WebSocket connection.
    ///
    /// Port of `Syncer.#createConnection()` (1:1 name since L9 Stage 2b).
    /// This runs on the tokio runtime (async) because auth validation
    /// may require HTTP fetches (JWKS).
    pub async fn create_connection(&self, ctx: ConnectionContext) {
        if self.shutting_down.load(Ordering::SeqCst) {
            ctx.sink
                .fail(crate::protocol::ErrorBody::rehome("Server is draining"));
            return;
        }
        let client_id = ctx.params.client_id.clone();
        let client_group_id = ctx.params.client_group_id.clone();
        let ws_id = ctx.params.ws_id.clone();
        let user_id = ctx.params.user_id.clone();
        let auth = ctx.params.auth.clone();
        let pv = ctx.params.protocol_version;

        tracing::debug!(
            "creating connection: cg={client_group_id}, client={client_id}, ws={ws_id}"
        );

        // 1. Validate auth BEFORE touching existing connections.
        // This prevents unauthenticated attackers from force-disconnecting
        // legitimate users via DoS.
        if let Some(auth_str) = &auth
            && !auth_str.is_empty()
        {
            match self
                .auth_validator
                .validate_auth(
                    &client_group_id,
                    &client_id,
                    user_id.as_deref(),
                    Some(auth_str),
                )
                .await
            {
                Ok(()) => {}
                Err(error_body) => {
                    tracing::warn!(
                        "Rejecting sync connection during initial auth resolution: \
                             cg={client_group_id}, client={client_id}, user={user_id:?}"
                    );
                    crate::metrics::record_ws_connection_failure(pv, "auth");
                    // Send error and close.
                    ctx.sink.fail(error_body);
                    return;
                }
            }
        }

        // 2. Reserve/create the bounded CG worker before retaining any
        //    per-group auth or connection state. Rejected group IDs therefore
        //    cannot grow either map without bound.
        let cg_handle = match self.get_or_create_cg(&client_group_id) {
            Ok(handle) => handle,
            Err(message) => {
                // Shed load gracefully: REHOME the client (reconnect — a load
                // balancer can place it on another instance) rather than a hard
                // `ServerOverloaded` reject. This mirrors TS, which never rejects
                // for capacity; it drains/rehomes via DrainCoordinator. A hard
                // reject at the (formerly too-low) cap turned a reconnect blip
                // near saturation into a retry storm; Rehome is the retryable,
                // spread-the-load signal. Covers both cap-overflow and
                // executor-shutdown Errs from get_or_create_cg.
                tracing::warn!("rehoming connection for {client_group_id}: {message}");
                crate::metrics::record_ws_connection_failure(pv, "rehome");
                ctx.sink.fail(crate::protocol::ErrorBody::rehome(message));
                return;
            }
        };

        // 3. Check (and, on the first connection, BIND) the group's userID.
        //    Port of TS `ConnectionContextManager.validateConnection`: the first
        //    successful connection pins the client group to its userID; every
        //    later connection must match it. Without the bind step the check
        //    below is inert — the group is never pinned, so two different users
        //    could share one client group.
        {
            let mut states = lock_unpoisoned(&self.group_auth_states);
            // CG workers are the lifetime boundary for auth pins. Failed or
            // terminated workers may leave a pin until the next admission;
            // prune those stale entries before inserting the current group.
            states.retain(|group_id, _| self.cg_handles.contains_key(group_id));
            let group = states.entry(client_group_id.clone()).or_default();
            let incoming = user_id.as_deref().unwrap_or("");
            if check_and_pin_user(group, incoming).is_err() {
                let error = crate::protocol::ErrorBody::unauthorized(
                    "Client groups are pinned to a single userID. \
                     Connection userID does not match existing client group userID.",
                );
                tracing::warn!(
                    "User ID mismatch: pinned={:?}, incoming={incoming}",
                    group.pinned_user_id
                );
                decrement_nonzero(&cg_handle.connection_count);
                crate::metrics::record_ws_connection_failure(pv, "user_mismatch");
                ctx.sink.fail(error);
                return;
            }
        }

        // 4. Close existing connection for same clientID (replacement).
        let superseded = {
            let mut conns = lock_unpoisoned(&self.connections);
            let existing = conns.get(&client_id).cloned();
            if existing.is_some() {
                tracing::debug!(
                    "client {client_id} already connected, closing existing connection"
                );
                conns.remove(&client_id);
            }
            conns.insert(
                client_id.clone(),
                ConnectionInfo {
                    client_group_id: client_group_id.clone(),
                    ws_id: ws_id.clone(),
                    sink: ctx.sink.clone(),
                },
            );
            existing
        };
        if let Some(existing) = superseded
            && let Some(handle) = self.cg_handles.get(&existing.client_group_id)
        {
            let _ = handle.send(CGMessage::CloseConnection {
                client_id: Arc::from(client_id.as_str()),
                ws_id: Arc::from(existing.ws_id.as_str()),
            });
        }

        // 5. Emit `connected` HERE, on the per-connection accept task, BEFORE the
        //    connection is handed to the serial CG thread. TS parity:
        //    `syncer.ts#handleConnection` sends `connection.init()`'s `connected`
        //    before `await connection.handleInitConnection` (which drives
        //    hydration). Emitting it on this task decouples the connect-ack from
        //    `config_and_hydrate`: a client whose CG thread is mid-hydrate is
        //    still acknowledged immediately and never trips its 10s connect
        //    timeout. Previously `connected` was sent by `Connection::init()`
        //    inside `on_new_connection` on the CG thread, so a reconnect arriving
        //    during an in-flight hydrate was queued behind it → connect-timeout →
        //    idle reap → cold re-hydrate thrash. The protocol version — TS
        //    `init()`'s other effect — is validated in `accept_connection` with
        //    the byte-identical `VersionNotSupported` message, so `on_new_connection`
        //    never version-checks.
        ctx.sink.push(crate::protocol::connected_message(
            &ws_id,
            &self.shard.app_id,
            self.shard.shard_num,
        ));

        // 6. Split the context: the CG thread owns connection setup + the sink,
        //    while a lightweight forwarder task funnels inbound WS frames into
        //    the CG's unified channel (so the CG loop never blocks on one conn).
        let ConnectionContext {
            params,
            sink,
            upstream_rx,
        } = ctx;
        match cg_handle.send(CGMessage::NewConnection {
            params: Box::new(params),
            sink,
        }) {
            Ok(()) => {
                tokio::spawn(forward_inbound(
                    upstream_rx,
                    cg_handle.tx.clone(),
                    Arc::from(client_id.as_str()),
                    Arc::from(ws_id.as_str()),
                ));
            }
            Err(err) => {
                tracing::error!("Failed to send connection to CG thread for {client_group_id}");
                decrement_nonzero(&cg_handle.connection_count);
                let mut conns = lock_unpoisoned(&self.connections);
                if conns
                    .get(&client_id)
                    .is_some_and(|info| info.ws_id == ws_id)
                {
                    conns.remove(&client_id);
                }
                drop(conns);
                if !self.cg_handles.contains_key(&client_group_id) {
                    lock_unpoisoned(&self.group_auth_states).remove(&client_group_id);
                }
                if let CGMessage::NewConnection { sink, .. } = err.0 {
                    sink.fail(crate::protocol::ErrorBody::rehome(
                        "Client-group worker restarted; reconnect required",
                    ));
                }
            }
        }
    }

    /// Get or create the hosting task for a client group ID. On the create path
    /// the group is placed by [`place_cg`](Self::place_cg) (least-loaded) and a
    /// `SpawnCg` is dispatched to that executor, which builds the `!Send`
    /// `SyncEngine` (bound to its own pool) and `spawn_local`s the event loop.
    pub(crate) fn get_or_create_cg(&self, client_group_id: &str) -> Result<Arc<CGHandle>, String> {
        let _creation = lock_unpoisoned(&self.cg_creation_lock);
        // Fast path: CG already exists.
        if let Some(handle) = self.cg_handles.get(client_group_id) {
            if !handle.accepting.load(Ordering::SeqCst) {
                drop(handle);
                if let Some((_, mut stale)) = self.cg_handles.remove(client_group_id) {
                    stale.shutdown();
                }
            } else {
                // We can't just return a reference to the DashMap entry because
                // we need to potentially create a new CG if it doesn't exist.
                // Instead, we clone the necessary parts.
                handle.connection_count.fetch_add(1, Ordering::Relaxed);
                return Ok(Arc::new(CGHandle {
                    tx: handle.tx.clone(),
                    connection_count: handle.connection_count.clone(),
                    accepting: handle.accepting.clone(),
                    executor_idx: handle.executor_idx,
                }));
            }
        }

        // Keep the process bounded. Idle groups remain warm, but are evicted on
        // demand once the configured capacity is reached.
        if self.cg_handles.len() >= self.max_client_groups {
            let idle = self
                .cg_handles
                .iter()
                .find(|entry| entry.connection_count() == 0)
                .map(|entry| entry.key().clone());
            if let Some(idle_id) = idle {
                if let Some((_, mut handle)) = self.cg_handles.remove(&idle_id) {
                    handle.shutdown();
                    lock_unpoisoned(&self.group_auth_states).remove(&idle_id);
                }
            } else {
                return Err(format!(
                    "maximum active client groups ({}) reached",
                    self.max_client_groups
                ));
            }
        }

        // Create path: allocate the group's channel + shared counters, register
        // the handle, and hand ownership of the receiver to the placed executor.
        let (tx, rx) = mpsc::unbounded_channel::<CGMessage>();
        let connection_count = Arc::new(AtomicU64::new(1));
        let accepting = Arc::new(AtomicBool::new(true));

        let mut spawn = ExecutorCommand::SpawnCg {
            cg_id: client_group_id.to_string(),
            rx,
            self_tx: tx.clone(),
            connection_count: connection_count.clone(),
            accepting: accepting.clone(),
            last_notification: lock_unpoisoned(&self.last_notification).clone(),
            serving_lag_registry: self.serving_lag_registry.clone(),
        };
        // A closed control channel means the executor THREAD died. Mark it dead
        // (so `place_cg` stops ranking its empty slot least-loaded) and retry on
        // the remaining executors instead of failing every new group forever.
        let mut placed: Option<usize> = None;
        for _ in 0..self.executors.len() {
            let shard = self.place_cg(client_group_id);
            match self.executors[shard].ctrl_tx.send(spawn) {
                Ok(()) => {
                    placed = Some(shard);
                    break;
                }
                Err(mpsc::error::SendError(returned)) => {
                    if !self.executors[shard].dead.swap(true, Ordering::SeqCst) {
                        tracing::error!(
                            "executor {shard} is dead (control channel closed); \
                             excluding it from client-group placement"
                        );
                    }
                    spawn = returned;
                }
            }
        }
        let Some(shard) = placed else {
            return Err("no executor is accepting new client groups".to_string());
        };

        self.cg_handles.insert(
            client_group_id.to_string(),
            CGHandle {
                tx: tx.clone(),
                connection_count: connection_count.clone(),
                accepting: accepting.clone(),
                executor_idx: shard,
            },
        );

        Ok(Arc::new(CGHandle {
            tx,
            connection_count,
            accepting,
            executor_idx: shard,
        }))
    }

    /// Choose the executor to host a NEW client group: the one currently hosting
    /// the fewest groups (least-loaded placement, doc 91). Replaces blind
    /// `shard_for` hashing, which is load-oblivious and leaves executors lumpy
    /// when the hash happens to cluster. A group's `!Send` `SyncEngine` pins it to
    /// its executor for life, so we balance by *placement*, never by migration
    /// (migration would force a full IVM rehydrate — rejected by design).
    ///
    /// V1 metric is **group count per executor**. Because placement is serialized
    /// under `cg_creation_lock` and the just-placed group is inserted into
    /// `cg_handles` before the lock is released, consecutive placements observe
    /// each other, so this degenerates to round-robin and keeps per-executor group
    /// counts within 1 of each other (max−min ≤ 1) absent churn. When a group is
    /// evicted (idle) or exits, it simply drops out of `cg_handles` and its slot is
    /// refilled by the next placement — no decrement bookkeeping to keep in sync.
    ///
    /// Known caveat: group count is a coarse proxy — a single hot group still pins
    /// one core and this can't correct it post-placement. A connection-weighted or
    /// advance-cost metric is a deliberate follow-up (V2/V3), not part of V1.
    ///
    /// Cost is O(N) over live groups per placement; placement is rare relative to
    /// message routing and runs under the creation lock, so this is not on any hot
    /// path. If N grows large this can move to an incremental per-executor counter.
    pub(crate) fn place_cg(&self, cg_id: &str) -> usize {
        let k = self.executors.len();
        let mut load = vec![0u64; k];
        for entry in self.cg_handles.iter() {
            // Defensive: an entry's executor_idx is always a valid index (set at
            // placement), but guard against an out-of-range value rather than
            // panic on the placement path.
            if let Some(slot) = load.get_mut(entry.executor_idx) {
                *slot += 1;
            }
        }
        // Only live executors are candidates — a dead one hosts 0 groups and
        // would otherwise be ranked least-loaded forever (see `Executor::dead`).
        // If EVERY executor is marked dead, fall back to all of them; the
        // subsequent send fails and the caller surfaces the error.
        let live: Vec<usize> = (0..k)
            .filter(|&i| !self.executors[i].dead.load(Ordering::SeqCst))
            .collect();
        let pool = if live.is_empty() {
            (0..k).collect::<Vec<usize>>()
        } else {
            live
        };
        let min = pool.iter().map(|&i| load[i]).min().unwrap_or(0);
        // Deterministically break ties AMONG the least-loaded executors by hashing
        // the cg_id, so a cold/uniform system still spreads groups (rather than
        // always piling the first ones onto executor 0).
        let candidates: Vec<usize> = pool.into_iter().filter(|&i| load[i] == min).collect();
        candidates[shard_for(cg_id, candidates.len())]
    }

    /// Drain and stop: fail every connection with a Rehome error (so clients
    /// reconnect elsewhere), then shut the executor threads down and join them so
    /// their CVR pools close before the process exits.
    pub async fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);

        // Ask each hosted CG to drain. The task fails its sockets with a Rehome
        // error and terminates on its executor.
        let ids: Vec<String> = self.cg_handles.iter().map(|e| e.key().clone()).collect();
        for id in ids {
            if let Some((_, mut handle)) = self.cg_handles.remove(&id) {
                handle.shutdown();
                lock_unpoisoned(&self.group_auth_states).remove(&id);
            }
        }
        lock_unpoisoned(&self.group_auth_states).clear();

        // Tell each executor to stop accepting and drain its remaining tasks,
        // then join the thread. Joining is a blocking op, so run it off the async
        // runtime via `spawn_blocking` to avoid stalling the caller's reactor.
        for exec in &self.executors {
            let _ = exec.ctrl_tx.send(ExecutorCommand::Shutdown);
        }
        let joins: Vec<JoinHandle<()>> = self
            .executors
            .iter()
            .filter_map(|exec| lock_unpoisoned(&exec.join).take())
            .collect();
        let _ = tokio::task::spawn_blocking(move || {
            for join in joins {
                let _ = join.join();
            }
        })
        .await;
    }

    /// Staggered graceful drain on SIGTERM — port of TS `Syncer.drain()`
    /// (workers/syncer.ts:732) paced by the `DrainCoordinator`. Rehomes ONE
    /// client group per drain interval instead of failing every socket at once
    /// (`shutdown`), so a deploy does not stampede the receiving servers with
    /// simultaneous reconnect+rehydrate storms.
    ///
    /// Pacing: TS re-arms each interval with the drained view-syncer's
    /// hydration time; the router does not track per-CG hydration time, so the
    /// drain budget is spread evenly across the live groups instead. The whole
    /// drain is bounded by `MAX_DRAIN_MS`: the parent ProcessManager
    /// (life-cycle.ts) waits indefinitely for the child after SIGTERM, but
    /// orchestrators SIGKILL after their stop-grace period (commonly 30s), so
    /// staying inside it keeps the final `shutdown()` sweep + executor join
    /// (and e.g. a dhat profile dump) graceful.
    pub async fn drain(&self) {
        /// Upper bound on the elective/staggered phase; the final sweep runs after.
        const MAX_DRAIN_MS: u64 = 25_000;

        // Refuse new connections for the whole drain, not just the final
        // sweep — a socket accepted mid-drain would only be rehomed moments
        // later anyway.
        self.shutting_down.store(true, Ordering::SeqCst);

        let start = std::time::Instant::now();
        let deadline = start + std::time::Duration::from_millis(MAX_DRAIN_MS);
        let total = self.cg_handles.len() as u64;
        tracing::info!("draining {total} client groups");

        if total > 0 {
            let coordinator =
                crate::services::view_syncer::drain_coordinator::DrainCoordinator::new();
            // Kick off with `drainNextIn(0)` (TS Syncer.drain): the first
            // force-drain timeout fires ~immediately, then each drained CG
            // re-arms it for the next interval.
            coordinator.drain_next_in(0);
            // Spacing such that the full sweep fits inside the budget.
            // `drain_next_in` divides by TARGET_UTILIZATION (0.6) internally,
            // so pre-scale to make the EFFECTIVE spacing budget/total.
            let interval_ms = MAX_DRAIN_MS.saturating_mul(6) / 10 / total.max(1);
            while !self.cg_handles.is_empty() {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    tracing::warn!("drain budget exhausted; rehoming remaining groups at once");
                    break;
                }
                tokio::select! {
                    () = coordinator.force_drain_timeout() => {}
                    () = tokio::time::sleep(remaining) => break,
                }
                // Pick an arbitrary live CG and rehome it (TS picks the first
                // view-syncer in its service map).
                let Some(id) = self.cg_handles.iter().next().map(|e| e.key().clone()) else {
                    break;
                };
                if let Some((_, mut handle)) = self.cg_handles.remove(&id) {
                    tracing::debug!("draining client group {id}");
                    handle.shutdown();
                    lock_unpoisoned(&self.group_auth_states).remove(&id);
                }
                coordinator.drain_next_in(interval_ms);
            }
        }

        // Final sweep: rehome anything left and join the executor threads.
        self.shutdown().await;
        tracing::info!("finished draining ({} ms)", start.elapsed().as_millis());
    }

    /// Number of active CG threads.
    pub fn cg_count(&self) -> usize {
        self.cg_handles.len()
    }

    /// Send a change-streamer notification to the CG thread for the given client group.
    /// Returns false if no CG thread exists for the given ID.
    pub fn send_notification(&self, cg_id: &str, notification: serde_json::Value) -> bool {
        if let Some(handle) = self.cg_handles.get(cg_id) {
            handle.send(CGMessage::Notification(notification)).is_ok()
        } else {
            false
        }
    }

    /// Broadcast a change-streamer notification to EVERY CG thread. A replica
    /// commit advances the whole replica to a new head, so all client groups
    /// hosted by this syncer must advance + poke — mirroring TS, where a single
    /// `version-ready` from the replicator's `Subscription<ReplicaState>` drives
    /// every pipeline. Returns the number of CG threads notified.
    pub fn broadcast_notification(&self, notification: serde_json::Value) -> usize {
        // Remember the newest state so a CG created between commits can arm its
        // serving-lag tracker at spawn (TS notifier latest-state replay).
        *lock_unpoisoned(&self.last_notification) = Some(notification.clone());
        // Feed the process-wide replica-ready log (TS `#recordReplicaReadyState`):
        // once per commit, watermark + upstream commit time. This is the single
        // process-wide replica-ready feed in the per-CG Rust arch.
        if let Some(watermark) = notification.get("watermark").and_then(|v| v.as_str()) {
            let ready_ms = notification
                .get("upstreamCommitTimeMs")
                .and_then(|v| v.as_f64())
                .map(|f| f as i64)
                .unwrap_or_else(now_ms);
            self.serving_lag_registry
                .record_replica_ready_state(watermark, ready_ms);
        }
        let mut sent = 0;
        for entry in self.cg_handles.iter() {
            if entry
                .value()
                .send(CGMessage::Notification(notification.clone()))
                .is_ok()
            {
                sent += 1;
            }
        }
        sent
    }
}
