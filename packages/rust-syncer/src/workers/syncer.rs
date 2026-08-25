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
/// `ConnectionRouter` and cloned into every CG. `Send + Sync`.
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
