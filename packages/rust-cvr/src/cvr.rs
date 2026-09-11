//! Port of `cvr.ts` — the CVR pure helpers plus the three updater classes.
//!
//! Pure helpers (from the top of `cvr.ts`):
//! - `mergeRefCounts`, `getInactiveQueries`, `nextEvictionTime`,
//!   `newQueryRecord`, `getMutationResultsQuery`, `assertNotInternal`.
//!
//! Updaters (`CVRUpdater`, `CVRConfigDrivenUpdater`, `CVRQueryDrivenUpdater`):
//! manage a mutable working copy of the CVR and collect `StoreOp`s in a buffer.
//! After each public method the caller drains the buffer via `drain_store_ops()`
//! and replays the ops against the real CVRStore. This mirrors the TS pattern
//! where the updater calls store methods inline as side effects. For `received()`
//! and `deleteUnreferencedRows()`, the caller passes in the current row records.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::client_handler::{Patch, PatchToVersion, RowPatch, RowPatchInfo};
use crate::cvr_store::CVRFlushStats;
use crate::shards::ShardID;
use crate::shards::upstream_schema;
use crate::shared::string_compare::string_compare;

use crate::schema::types::*;
use crate::ttl::{DEFAULT_TTL_MS, TTL, clamp_ttl, compare_ttl};
use crate::ttl_clock::TTLClock;

/// Merge existing refCounts with received refCounts, optionally removing
/// hashes in `remove_hashes` from the existing set.
///
/// Returns `None` if no positive refs remain (i.e. the row should be deleted).
///
/// This is a pure function — byte-identical behavior to the TS implementation.
/// Key properties:
/// - `merge_ref_counts(None, None, _) == None`
/// - `merge_ref_counts(x, None, None) == normalize(x)` (drops zeros)
/// - Hashes in `remove_hashes` are skipped from `existing` (index 0) only.
/// - Zero entries are dropped inline.
pub fn merge_ref_counts(
    existing: Option<&RefCounts>,
    received: Option<&RefCounts>,
    remove_hashes: Option<&std::collections::HashSet<String>>,
) -> Option<RefCounts> {
    let mut merged: RefCounts = BTreeMap::new();

    match existing {
        None => {
            // TS: `merged = received ?? {}` — a raw copy that RETAINS zero
            // entries (the final positive-count check only decides null-vs-map,
            // it does not strip zeros). Dropping zeros here diverges from TS for
            // any `received` carrying a literal 0 alongside a positive count.
            if let Some(recv) = received {
                merged = recv.clone();
            }
        }
        Some(existing) => {
            // Index 0: existing (with remove_hashes filter)
            for (hash, count) in existing {
                if let Some(rh) = remove_hashes
                    && rh.contains(hash)
                {
                    continue;
                }
                let val = merged.get(hash).copied().unwrap_or(0) + count;
                if val == 0 {
                    merged.remove(hash);
                } else {
                    merged.insert(hash.clone(), val);
                }
            }

            // Index 1: received (no filter)
            if let Some(recv) = received {
                for (hash, count) in recv {
                    let val = merged.get(hash).copied().unwrap_or(0) + count;
                    if val == 0 {
                        merged.remove(hash);
                    } else {
                        merged.insert(hash.clone(), val);
                    }
                }
            }
        }
    }

    // Return None if no positive refs remain.
    if merged.values().any(|&v| v > 0) {
        Some(merged)
    } else {
        None
    }
}

/// Create a new query record from a desired query spec.
/// Returns a Client or Custom query record (never Internal).
pub fn new_query_record(
    id: &str,
    ast: Option<&Value>,
    name: Option<&str>,
    args: Option<&[Value]>,
) -> QueryRecord {
    if let Some(ast) = ast {
        assert!(
            name.is_none() && args.is_none(),
            "Cannot provide name or args with ast"
        );
        QueryRecord::Client(ClientQueryRecord {
            base: BaseQueryRecord {
                id: id.to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
            },
            ast: ast.clone(),
            client_state: BTreeMap::new(),
            patch_version: None,
        })
    } else {
        let name = name.expect("Must provide name and args");
        let args = args.expect("Must provide name and args");
        QueryRecord::Custom(CustomQueryRecord {
            base: BaseQueryRecord {
                id: id.to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
            },
            name: name.to_string(),
            args: args.to_vec(),
            client_state: BTreeMap::new(),
            patch_version: None,
        })
    }
}

/// Create the internal mutation-results query for a client group.
pub fn get_mutation_results_query(
    upstream_schema: &str,
    client_group_id: &str,
) -> InternalQueryRecord {
    InternalQueryRecord {
        base: BaseQueryRecord {
            id: CLIENT_MUTATION_RESULTS_QUERY_ID.to_string(),
            transformation_hash: None,
            transformation_version: None,
            row_set_signature: None,
        },
        ast: serde_json::json!({
            "schema": "",
            "table": format!("{}.mutations", upstream_schema),
            "where": {
                "type": "and",
                "conditions": [{
                    "type": "simple",
                    "left": {"type": "column", "name": "clientGroupID"},
                    "op": "=",
                    "right": {"type": "literal", "value": client_group_id}
                }]
            },
            "orderBy": [
                ["clientGroupID", "asc"],
                ["clientID", "asc"],
                ["mutationID", "asc"]
            ]
        }),
    }
}

/// Assert that a query is not internal. Panics with the same message as TS.
pub fn assert_not_internal(query: &QueryRecord) {
    if let QueryRecord::Internal(r) = query {
        panic!("Query ID {} is reserved for internal use", r.base.id);
    }
}

/// A query that is inactive for all clients, with its inactivation time and TTL.
#[derive(Debug, Clone, PartialEq)]
pub struct InactiveQuery {
    pub hash: String,
    pub inactivated_at: TTLClock,
    pub ttl: i64,
}

/// Find queries that are inactive for ALL clients in the CVR.
/// A query is inactive if every client's `inactivated_at` is set.
/// Returns the one with the furthest-future expiration per query.
///
/// Port of `getInactiveQueries` from cvr.ts.
pub fn get_inactive_queries(cvr: &CVR) -> Vec<InactiveQuery> {
    let mut inactive: BTreeMap<String, InactiveQuery> = BTreeMap::new();

    for (query_id, query) in &cvr.queries {
        if query.is_internal() {
            continue;
        }
        let client_state = match query.client_state() {
            Some(cs) => cs,
            None => continue,
        };

        for state in client_state.values() {
            let inactivated_at = match state.inactivated_at {
                Some(t) => t,
                None => {
                    // Query is still active for this client — not inactive.
                    inactive.remove(query_id);
                    break;
                }
            };

            let clamped_ttl = clamp_ttl(TTL::Ms(state.ttl));
            let existing = inactive.get(query_id);

            match existing {
                Some(existing) => {
                    let existing_ttl = clamp_ttl(TTL::Ms(existing.ttl));
                    // Use the last eviction time (furthest in the future).
                    if existing_ttl + existing.inactivated_at < inactivated_at + clamped_ttl {
                        inactive.insert(
                            query_id.clone(),
                            InactiveQuery {
                                hash: query_id.clone(),
                                inactivated_at,
                                ttl: clamped_ttl,
                            },
                        );
                    }
                }
                None => {
                    inactive.insert(
                        query_id.clone(),
                        InactiveQuery {
                            hash: query_id.clone(),
                            inactivated_at,
                            ttl: clamped_ttl,
                        },
                    );
                }
            }
        }
    }

    // Sort by eviction time (inactivated_at + ttl), oldest first. TS breaks
    // ties by `cvr.queries` INSERTION order, but the TS CVR load issues no
    // ORDER BY (cvr-store.ts:361 — `SELECT … FROM queries WHERE …`), so that
    // insertion order is arbitrary PG heap order, NOT a stable contract. We
    // therefore pick a deterministic TOTAL order — expire, then query hash —
    // which is stable run-to-run and cannot diverge observably (both consumers,
    // `next_eviction_time` (min) and the sync-engine expiry filter (whole-set),
    // are order-independent). The explicit `.then_with` tie-break keeps this
    // total even if `inactive` ever stops being a key-sorted BTreeMap.
    let mut result: Vec<InactiveQuery> = inactive.into_values().collect();
    result.sort_by(|a, b| {
        let a_expire = a.inactivated_at + a.ttl;
        let b_expire = b.inactivated_at + b.ttl;
        a_expire.cmp(&b_expire).then_with(|| a.hash.cmp(&b.hash))
    });
    result
}

/// Compute the next eviction time for the CVR.
/// Returns the earliest (inactivated_at + ttl) across all inactive queries.
pub fn next_eviction_time(cvr: &CVR) -> Option<TTLClock> {
    let mut next: Option<i64> = None;
    for q in get_inactive_queries(cvr) {
        let expire = q.inactivated_at + q.ttl;
        if next.is_none_or(|n| expire < n) {
            next = Some(expire);
        }
    }
    next
}

/// Row records keyed by rowIDString for O(1) lookup.
pub type RowRecordMap = HashMap<String, RowRecord>;
type RowSetSignatureProvider = dyn Fn(&str) -> Option<u64> + Send + Sync;

// ─── Base Updater ──────────────────────────────────────────────────────────

/// Base CVR updater — common logic for both config-driven and query-driven updaters.
/// Mirrors the TS `CVRUpdater` class.
pub struct CVRUpdater {
    pub orig: CVR,
    pub cvr: CVR,
    pub store_ops: Vec<StoreOp>,
}

impl CVRUpdater {
    pub fn new(cvr: CVR, replica_version: Option<String>) -> Self {
        let orig = cvr.clone();
        let mut working = cvr;
        working.replica_version = replica_version;
        Self {
            orig,
            cvr: working,
            store_ops: Vec::new(),
        }
    }

    pub fn set_version(&mut self, version: CVRVersion) -> CVRVersion {
        assert!(
            cmp_cvr(&self.cvr.version, &version) == Ordering::Less,
            "Expected new version to be greater than current version"
        );
        self.cvr.version = version.clone();
        version
    }

    /// Ensures that the working CVR has a higher version than the original.
    /// Idempotent — always returns the same (possibly bumped) version.
    pub fn ensure_new_version(&mut self) -> CVRVersion {
        if cmp_versions(
            &Some(self.orig.version.clone()),
            &Some(self.cvr.version.clone()),
        ) == Ordering::Equal
        {
            let new = one_after(&Some(self.cvr.version.clone()));
            self.set_version(new);
        }
        self.cvr.version.clone()
    }

    /// Drain collected store operations. TS replays these against the real CVRStore.
    pub fn drain_store_ops(&mut self) -> Vec<StoreOp> {
        std::mem::take(&mut self.store_ops)
    }

    /// The flush method. In TS this calls `cvrStore.flush(...)`.
    /// Here we just collect the flush op and return the CVR snapshot.
    /// The caller (TS) is responsible for calling the real store flush.
    pub fn flush(
        &mut self,
        _last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
    ) -> (CVR, Option<CVRFlushStats>) {
        self.cvr.ttl_clock = ttl_clock;
        self.cvr.last_active = last_active;
        // The flush op is collected for TS to replay.
        // TS will call cvrStore.flush(lc, origVersion, cvr, lastConnectTime).
        // The return value (flushed stats or false) determines if the CVR changed.
        // For the Rust port, we return the working CVR and let TS handle the actual flush.
        (self.cvr.clone(), Some(CVRFlushStats::default()))
    }
}

// ─── Config-Driven Updater ─────────────────────────────────────────────────

/// Updater for config-driven changes: client connect/disconnect, desired query changes.
/// Mirrors the TS `CVRConfigDrivenUpdater` class.
pub struct CVRConfigDrivenUpdater {
    pub base: CVRUpdater,
    shard: ShardID,
    /// Live-instance census guard (leak hunting). Transient per-advance; the
    /// census should return to 0 at rest. Not `Clone`, so a field guard is fine.
    _census: crate::live_count::Guard,
}

impl CVRConfigDrivenUpdater {
    pub fn new(cvr: CVR, shard: ShardID) -> Self {
        let replica_version = cvr.replica_version.clone();
        Self {
            base: CVRUpdater::new(cvr, replica_version),
            shard,
            _census: crate::live_count::Guard::new(&crate::live_count::CONFIG_DRIVEN_UPDATER),
        }
    }

    /// Ensure a client record exists. Creates internal queries on first client.
    pub fn ensure_client(&mut self, id: &str) -> &mut ClientRecord {
        if self.base.cvr.clients.contains_key(id) {
            return self
                .base
                .cvr
                .clients
                .get_mut(id)
                .expect("checked by contains_key above");
        }

        // Add the ClientRecord
        let client = ClientRecord {
            id: id.to_string(),
            desired_query_ids: Vec::new(),
        };
        self.base.cvr.clients.insert(id.to_string(), client.clone());
        self.base.store_ops.push(StoreOp::InsertClient(client));

        self.base.ensure_new_version();

        // Ensure internal queries exist
        if !self.base.cvr.queries.contains_key(CLIENT_LMID_QUERY_ID) {
            let lmids_query = QueryRecord::Internal(InternalQueryRecord {
                base: BaseQueryRecord {
                    id: CLIENT_LMID_QUERY_ID.to_string(),
                    transformation_hash: None,
                    transformation_version: None,
                    row_set_signature: None,
                },
                // NB: TS builds the `lmids` query's `where` as a BARE `simple`
                // condition (cvr.ts ensureClient), unlike `getMutationResultsQuery`
                // which wraps its single condition in an `and`. This asymmetry is
                // load-bearing: the AST is persisted verbatim into `queries.clientAST`,
                // so an `and`-wrapper here writes structurally different CVR state than
                // TS (caught by the sequence differential). Keep it bare to match.
                ast: serde_json::json!({
                    "schema": "",
                    "table": format!("{}.clients", upstream_schema(&self.shard)),
                    "where": {
                        "type": "simple",
                        "left": {"type": "column", "name": "clientGroupID"},
                        "op": "=",
                        "right": {"type": "literal", "value": self.base.cvr.id}
                    },
                    "orderBy": [
                        ["clientGroupID", "asc"],
                        ["clientID", "asc"]
                    ]
                }),
            });
            self.base
                .cvr
                .queries
                .insert(CLIENT_LMID_QUERY_ID.to_string(), lmids_query.clone());
            self.base.store_ops.push(StoreOp::PutQuery(lmids_query));
        }

        if !self
            .base
            .cvr
            .queries
            .contains_key(CLIENT_MUTATION_RESULTS_QUERY_ID)
        {
            let mr_query = QueryRecord::Internal(get_mutation_results_query(
                &upstream_schema(&self.shard),
                &self.base.cvr.id,
            ));
            self.base.cvr.queries.insert(
                CLIENT_MUTATION_RESULTS_QUERY_ID.to_string(),
                mr_query.clone(),
            );
            self.base.store_ops.push(StoreOp::PutQuery(mr_query));
        }

        self.base.cvr.clients.get_mut(id).expect("inserted above")
    }

    /// Set the client schema. Must match existing schema if already set.
    pub fn set_client_schema(&mut self, client_schema: ClientSchema) -> Result<(), String> {
        match &self.base.cvr.client_schema {
            None => {
                self.base.cvr.client_schema = Some(client_schema);
                self.base
                    .store_ops
                    .push(StoreOp::PutInstance(self.base.cvr.clone()));
                Ok(())
            }
            Some(existing) => {
                if existing != &client_schema {
                    // TS cvr.ts:283-289 warns with both schemas before throwing.
                    tracing::warn!(
                        "New schema {} does not match existing schema {}",
                        serde_json::to_string(&client_schema).unwrap_or_default(),
                        serde_json::to_string(existing).unwrap_or_default()
                    );
                    Err("Provided schema does not match previous schema".to_string())
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Set the profile ID.
    pub fn set_profile_id(&mut self, profile_id: &str) {
        if self.base.cvr.profile_id.as_deref() != Some(profile_id) {
            if let Some(ref existing) = self.base.cvr.profile_id
                && !existing.starts_with("cg")
            {
                // TS cvr.ts:308: profile IDs are expected to change only from
                // null / the back-filled "cg…" value; anything else is surfaced.
                tracing::warn!("changing profile ID from {existing} to {profile_id}");
            }
            self.base.cvr.profile_id = Some(profile_id.to_string());
            self.base
                .store_ops
                .push(StoreOp::PutInstance(self.base.cvr.clone()));
        }
    }

    /// Add or update desired queries for a client.
    /// Returns patches to send to the client.
    pub fn put_desired_queries(
        &mut self,
        client_id: &str,
        queries: &[DesiredQuerySpec],
    ) -> Vec<PatchToVersion> {
        let mut patches = Vec::new();
        self.ensure_client(client_id);

        let current: HashSet<String> = self
            .base
            .cvr
            .clients
            .get(client_id)
            .expect("client record exists: ensure_client registered this clientID")
            .desired_query_ids
            .iter()
            .cloned()
            .collect();

        // Find new/changed desired queries.
        let mut needed: HashSet<String> = HashSet::new();

        // TS `recordQueryForTelemetry` (cvr.ts:335-342) feeds ONLY the
        // anonymous-telemetry meter (anonymous-otel-start.ts:181-187, a separate
        // MeterProvider with its own exporter) — nothing on the operator's OTLP
        // pipeline. Rust has no anonymous-telemetry subsystem, so there is
        // nothing to record here.

        for q in queries {
            let ttl = q.ttl.unwrap_or(DEFAULT_TTL_MS);
            let query = self.base.cvr.queries.get(&q.hash);
            match query {
                None => {
                    // New query - record for telemetry
                    needed.insert(q.hash.clone());
                    continue;
                }
                Some(query) if query.is_internal() => {
                    continue;
                }
                Some(query) => {
                    let old_client_state = query.client_state().and_then(|cs| cs.get(client_id));
                    match old_client_state {
                        None => {
                            // Reactivated query - record for telemetry
                            needed.insert(q.hash.clone());
                            continue;
                        }
                        Some(state) if state.inactivated_at.is_some() => {
                            // Reactivated query - record for telemetry
                            needed.insert(q.hash.clone());
                            continue;
                        }
                        Some(state) => {
                            if compare_ttl(TTL::Ms(ttl), TTL::Ms(state.ttl)) > 0 {
                                // TTL update only
                                needed.insert(q.hash.clone());
                            }
                        }
                    }
                }
            }
        }

        if needed.is_empty() {
            return patches;
        }

        let new_version = self.base.ensure_new_version();

        // Update desiredQueryIDs: sorted union of current and needed. Both are
        // HashSets, so the union is already duplicate-free — just sort it
        // (TS cvr.ts:376 `toSorted(union(current, needed), stringCompare)`).
        let mut combined: Vec<String> = current.union(&needed).cloned().collect();
        combined.sort_by(|a, b| string_compare(a, b));
        self.base
            .cvr
            .clients
            .get_mut(client_id)
            .expect("client record exists: ensure_client registered this clientID")
            .desired_query_ids = combined;

        // Emit in input order (TS iterates an insertion-ordered Set), deduping
        // repeated hashes. Iterating `needed` (a HashSet) directly would give
        // nondeterministic patch/StoreOp order and diverge from TS.
        let mut emitted: HashSet<&str> = HashSet::new();
        for q in queries {
            let id = &q.hash;
            if !needed.contains(id) || !emitted.insert(id.as_str()) {
                continue;
            }
            let ttl = clamp_ttl(TTL::Ms(q.ttl.unwrap_or(DEFAULT_TTL_MS)));

            // Get or create the query record.
            let query = match self.base.cvr.queries.get(id) {
                Some(existing) => existing.clone(),
                None => new_query_record(id, q.ast.as_ref(), q.name.as_deref(), q.args.as_deref()),
            };
            assert_not_internal(&query);

            // Update client state.
            let mut query = query;
            if let Some(cs) = query.client_state_mut() {
                cs.insert(
                    client_id.to_string(),
                    ClientState {
                        inactivated_at: None,
                        ttl,
                        version: new_version.clone(),
                    },
                );
            }

            self.base.cvr.queries.insert(id.clone(), query.clone());
            self.base.store_ops.push(StoreOp::PutQuery(query.clone()));

            self.base.store_ops.push(StoreOp::PutDesiredQuery {
                version: new_version.clone(),
                query_id: id.clone(),
                client_id: client_id.to_string(),
                deleted: false,
                inactivated_at: None,
                ttl,
            });

            patches.push(PatchToVersion {
                patch: Patch::Query(QueryPatch::Put {
                    id: id.clone(),
                    client_id: Some(client_id.to_string()),
                }),
                to_version: new_version.clone(),
            });
        }

        patches
    }

    /// Mark desired queries as inactive (with a TTL clock for expiration).
    pub fn mark_desired_queries_as_inactive(
        &mut self,
        client_id: &str,
        query_hashes: &[String],
        ttl_clock: TTLClock,
    ) -> Vec<PatchToVersion> {
        self.delete_queries(client_id, query_hashes, Some(ttl_clock))
    }

    /// Delete desired queries (immediate, no TTL).
    pub fn delete_desired_queries(
        &mut self,
        client_id: &str,
        query_hashes: &[String],
    ) -> Vec<PatchToVersion> {
        self.delete_queries(client_id, query_hashes, None)
    }

    fn delete_queries(
        &mut self,
        client_id: &str,
        query_hashes: &[String],
        inactivated_at: Option<TTLClock>,
    ) -> Vec<PatchToVersion> {
        let mut patches = Vec::new();
        self.ensure_client(client_id);

        let current: HashSet<String> = self
            .base
            .cvr
            .clients
            .get(client_id)
            .expect("client record exists: ensure_client registered this clientID")
            .desired_query_ids
            .iter()
            .cloned()
            .collect();

        let unwanted: HashSet<String> = query_hashes.iter().cloned().collect();
        let remove: HashSet<String> = current.intersection(&unwanted).cloned().collect();

        if remove.is_empty() {
            return patches;
        }

        let new_version = self.base.ensure_new_version();

        // Update desiredQueryIDs: sorted difference (TS cvr.ts:445-448
        // `toSorted(difference(current, remove), stringCompare)`).
        let mut remaining: Vec<String> = current.difference(&remove).cloned().collect();
        remaining.sort_by(|a, b| string_compare(a, b));
        self.base
            .cvr
            .clients
            .get_mut(client_id)
            .expect("client record exists: ensure_client registered this clientID")
            .desired_query_ids = remaining;

        // Iterate `remove` in a STABLE (sorted) order. TS iterates the smaller of
        // {unwanted, current} (a size-based optimization in `intersection`), so its
        // emitted-patch / store-op order is not a stable contract; a raw `HashSet`
        // iteration here would additionally be nondeterministic run-to-run (unstable
        // poke ordering). Sorting makes the Rust output deterministic. The sequence
        // differential compares the returned patches order-independently for this
        // reason.
        let mut remove: Vec<String> = remove.into_iter().collect();
        remove.sort();

        for id in &remove {
            let query = match self.base.cvr.queries.get(id) {
                Some(q) => q.clone(),
                None => continue,
            };
            assert_not_internal(&query);

            let mut query = query;
            let mut ttl = DEFAULT_TTL_MS;

            match inactivated_at {
                None => {
                    // Delete: remove client state entirely.
                    if let Some(cs) = query.client_state_mut() {
                        cs.remove(client_id);
                    }
                }
                Some(inactivated_at) => {
                    // Inactivate: set inactivatedAt — but ONLY if the client
                    // already has a clientState entry. TS (cvr.ts:463-476) guards
                    // the whole assignment with `if (clientState !== undefined)`;
                    // a query the client DESIRES but never transformed has no
                    // clientState, and TS leaves it absent (the desires row is
                    // still written below with ttl=DEFAULT). Unconditionally
                    // inserting here fabricated an in-memory clientState entry TS
                    // never creates, skewing an intra-pass getInactiveQueries /
                    // nextEvictionTime read. See parity/BEHAVIORAL-SWEEP-FINDINGS.md.
                    if let Some(cs) = query.client_state_mut() {
                        let existing_ttl = cs.get(client_id).map(|state| {
                            assert!(
                                state.inactivated_at.is_none(),
                                "Query {} is already inactivated",
                                id
                            );
                            clamp_ttl(TTL::Ms(state.ttl))
                        });
                        if let Some(t) = existing_ttl {
                            ttl = t;
                            cs.insert(
                                client_id.to_string(),
                                ClientState {
                                    inactivated_at: Some(inactivated_at),
                                    ttl,
                                    version: new_version.clone(),
                                },
                            );
                        }
                    }
                }
            }

            self.base.cvr.queries.insert(id.clone(), query.clone());
            self.base.store_ops.push(StoreOp::PutQuery(query.clone()));
            self.base.store_ops.push(StoreOp::PutDesiredQuery {
                version: new_version.clone(),
                query_id: id.clone(),
                client_id: client_id.to_string(),
                // TS `#deleteQueries` writes `putDesiredQuery(..., /*deleted=*/true, ...)`
                // for BOTH hard-delete and inactivation — the desires `deleted`
                // column means "no longer actively desired"; `inactivatedAtMs`
                // (null vs set) is what distinguishes an inactive desire from a
                // hard delete. Keying `deleted` off inactivation (the old
                // `inactivated_at.is_none()`) wrote `deleted=false` for inactive
                // rows, diverging from the persisted TS CVR state (caught by the
                // sequence differential).
                deleted: true,
                inactivated_at,
                ttl,
            });

            patches.push(PatchToVersion {
                patch: Patch::Query(QueryPatch::Del {
                    id: id.clone(),
                    client_id: Some(client_id.to_string()),
                }),
                to_version: new_version.clone(),
            });
        }

        patches
    }

    /// Clear all desired queries for a client.
    pub fn clear_desired_queries(&mut self, client_id: &str) -> Vec<PatchToVersion> {
        self.ensure_client(client_id);
        let desired = self
            .base
            .cvr
            .clients
            .get(client_id)
            .expect("client record exists: ensure_client registered this clientID")
            .desired_query_ids
            .clone();
        self.delete_queries(client_id, &desired, None)
    }

    /// Delete a client and mark all its queries as inactive.
    pub fn delete_client(&mut self, client_id: &str, ttl_clock: TTLClock) -> Vec<PatchToVersion> {
        let client = match self.base.cvr.clients.get(client_id) {
            Some(c) => c.clone(),
            None => return Vec::new(),
        };

        let patches =
            self.mark_desired_queries_as_inactive(client_id, &client.desired_query_ids, ttl_clock);

        self.base.cvr.clients.remove(client_id);
        self.base
            .store_ops
            .push(StoreOp::DeleteClient(client_id.to_string()));

        patches
    }

    /// Flush — delegates to base flush.
    pub fn flush(
        &mut self,
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
    ) -> (CVR, Option<CVRFlushStats>) {
        self.base.flush(last_connect_time, last_active, ttl_clock)
    }
}

// ─── Query-Driven Updater ──────────────────────────────────────────────────

/// Updater for query execution: trackQueries, received, deleteUnreferencedRows, flush.
/// Mirrors the TS `CVRQueryDrivenUpdater` class.
pub struct CVRQueryDrivenUpdater {
    pub base: CVRUpdater,
    removed_or_executed_query_ids: HashSet<String>,
    pub received_rows: HashMap<String, Option<RefCounts>>, // keyed by rowIDString
    last_patches: HashMap<String, RowPatchInfo>,           // keyed by rowIDString
    row_set_signature_provider: Option<Box<RowSetSignatureProvider>>,

    // Whether trackQueries has been called.
    tracked: bool,
    /// Live-instance census guard (leak hunting). Transient per-advance; the
    /// census should return to 0 at rest. Not `Clone`, so a field guard is fine.
    _census: crate::live_count::Guard,
}

impl CVRQueryDrivenUpdater {
    pub fn new(
        cvr: CVR,
        state_version: String,
        replica_version: String,
        row_set_signature_provider: Option<Box<RowSetSignatureProvider>>,
    ) -> Self {
        let cvr_replica = cvr.replica_version.clone();
        let mut base = CVRUpdater::new(cvr, Some(replica_version.clone()));

        // Assert: replica version must be >= cvr.replicaVersion
        assert!(
            cvr_replica.as_deref() <= Some(replica_version.as_str()),
            "Cannot sync from an older replicaVersion"
        );

        // Assert: stateVersion >= cvr.version.stateVersion
        assert!(
            state_version >= base.orig.version.state_version,
            "stateVersion must be >= cvr.version.stateVersion"
        );

        // Port of TS cvr.ts:599-601: bump ONLY when stateVersion advanced. At
        // equality the constructor must NOT bump — the version bump comes lazily
        // from trackQueries (_ensureNewVersion) and received() hard-asserts it
        // happened when a row actually needs a new patchVersion
        // (#assertNewVersion). An eager bump here produced a configVersion
        // advance + extra poke/cookie on same-stateVersion passes that TS
        // leaves version-unchanged (F-CVR-STORE-12).
        if state_version > base.orig.version.state_version {
            base.set_version(CVRVersion {
                state_version: state_version.clone(),
                config_version: None,
            });
        }

        Self {
            base,
            removed_or_executed_query_ids: HashSet::new(),
            received_rows: HashMap::new(),
            last_patches: HashMap::new(),
            row_set_signature_provider,
            tracked: false,
            _census: crate::live_count::Guard::new(&crate::live_count::QUERY_DRIVEN_UPDATER),
        }
    }

    /// The updated CVR version.
    pub fn updated_version(&self) -> CVRVersion {
        self.base.cvr.version.clone()
    }

    /// Force a config version bump (public alias for ensure_new_version).
    pub fn ensure_new_version(&mut self) -> CVRVersion {
        self.base.ensure_new_version()
    }

    /// Initiate tracking of executed and removed queries.
    /// Returns the new CVR version and query patches.
    pub fn track_queries(
        &mut self,
        executed: &[(&str, &str)], // (queryID, transformationHash)
        removed: &[&str],          // queryID
    ) -> (CVRVersion, Vec<PatchToVersion>) {
        assert!(!self.tracked, "trackQueries already called");
        self.tracked = true;

        let mut query_patches: Vec<Patch> = Vec::new();

        for (id, transformation_hash) in executed {
            let patches = self.track_executed(id, transformation_hash);
            query_patches.extend(patches);
        }

        for id in removed {
            let patches = self.track_removed(id);
            query_patches.extend(patches);
        }

        let patches: Vec<PatchToVersion> = query_patches
            .into_iter()
            .map(|patch| PatchToVersion {
                patch,
                to_version: self.base.cvr.version.clone(),
            })
            .collect();

        // TS `lc.info?.(`trackQueries: ${executed.length} executed,
        // ${removed.length} removed, version ${versionBumped ? 'bumped' :
        // 'unchanged'}`)` (cvr.ts:637-640).
        let version_bumped =
            crate::schema::types::cmp_cvr(&self.base.orig.version, &self.base.cvr.version)
                == std::cmp::Ordering::Less;
        tracing::info!(
            "trackQueries: {} executed, {} removed, version {}",
            executed.len(),
            removed.len(),
            if version_bumped {
                "bumped"
            } else {
                "unchanged"
            }
        );

        (self.base.cvr.version.clone(), patches)
    }

    fn track_executed(&mut self, query_id: &str, transformation_hash: &str) -> Vec<Patch> {
        assert!(
            !self.removed_or_executed_query_ids.contains(query_id),
            "Query {} already tracked as executed or removed",
            query_id
        );
        self.removed_or_executed_query_ids
            .insert(query_id.to_string());

        let mut got_query_patch: Option<Patch> = None;

        // Check if transformation hash changed.
        let current_hash = self
            .base
            .cvr
            .queries
            .get(query_id)
            .and_then(|q| q.base().transformation_hash.clone());

        if current_hash.as_deref() != Some(transformation_hash) {
            let transformation_version = self.base.ensure_new_version();

            let query = self
                .base
                .cvr
                .queries
                .get_mut(query_id)
                .expect("queryID is present in cvr.queries (TS indexes it non-null)");

            if !query.is_internal() && query.patch_version().is_none() {
                // Client query: desired -> gotten
                *query.patch_version_mut() = Some(transformation_version.clone());
                got_query_patch = Some(Patch::Query(QueryPatch::Put {
                    id: query_id.to_string(),
                    client_id: None,
                }));
            }

            query.base_mut().transformation_hash = Some(transformation_hash.to_string());
            query.base_mut().transformation_version = Some(transformation_version);
            self.base
                .store_ops
                .push(StoreOp::UpdateQuery(query.clone()));
        }

        match got_query_patch {
            Some(p) => vec![p],
            None => vec![],
        }
    }

    fn track_removed(&mut self, query_id: &str) -> Vec<Patch> {
        let query = self
            .base
            .cvr
            .queries
            .get(query_id)
            .cloned()
            .unwrap_or_else(|| panic!("Query {} not found", query_id));
        assert_not_internal(&query);

        assert!(
            !self.removed_or_executed_query_ids.contains(query_id),
            "Query {} already tracked as executed or removed",
            query_id
        );
        self.removed_or_executed_query_ids
            .insert(query_id.to_string());

        self.base.cvr.queries.remove(query_id);

        let new_version = self.base.ensure_new_version();
        let query_patch = Patch::Query(QueryPatch::Del {
            id: query_id.to_string(),
            client_id: None,
        });
        self.base.store_ops.push(StoreOp::MarkQueryAsDeleted {
            version: new_version,
            patch: QueryPatch::Del {
                id: query_id.to_string(),
                client_id: None,
            },
        });

        vec![query_patch]
    }

    /// Assert that a new version has been set (trackQueries or ensureNewVersion was called).
    ///
    /// Port of TS `#assertNewVersion` (cvr.ts:769). TS uses `assert()`, which
    /// THROWS a catchable Error; the CVR flush is transactional (store_ops are
    /// buffered and only drained/committed on success), so the throw aborts the
    /// update with nothing persisted — the client reconnects and re-hydrates
    /// from the last consistent CVR. Rust MUST mirror that recoverable throw:
    /// return `Err`, never `assert!`/panic. A panic here unwinds mid-mutation,
    /// poisons the CG's locks, and takes down every client on the group; the
    /// client then re-hydrates the same state and re-panics, wedging until a
    /// cache clear (a production incident). TS-prod logs ZERO of these
    /// over 7 days precisely because a thrown-and-caught assert is a benign
    /// retry, not a crash — matching that error semantics is AGENTS.md rule 1.
    fn assert_new_version(&self) -> Result<CVRVersion, String> {
        // Compare in place; this runs once per received row.
        if cmp_cvr(&self.base.orig.version, &self.base.cvr.version) != Ordering::Less {
            return Err("Expected CVR version to have been bumped above original".to_string());
        }
        Ok(self.base.cvr.version.clone())
    }

    /// Track rows received from executing queries.
    /// `existing_rows` is the current row records from the RowRecordCache.
    /// Returns patches to send to clients.
    pub fn received(
        &mut self,
        // Taken by value (Rust-only, AGENTS.md rule 5): TS iterates the Map it
        // is handed and stores the same `id`/`refCounts` objects by reference;
        // owning the batch lets each row's id, key string and merged
        // ref-counts MOVE into the record, the patch and the bookkeeping maps
        // instead of being cloned once per destination on every row.
        rows: HashMap<String, (RowID, RowUpdate)>, // keyed by rowIDString
        existing_rows: &RowRecordMap,
    ) -> Result<Vec<PatchToVersion>, String> {
        if crate::tracer::enabled() {
            crate::tracer::recv(
                "QueryUpdater",
                &format!(
                    "received batch={} existing={}",
                    rows.len(),
                    existing_rows.len()
                ),
            );
        }
        let mut patches: Vec<PatchToVersion> = Vec::new();

        for (id_str, (id, update)) in rows {
            let RowUpdate {
                contents,
                version,
                ref_counts,
            } = update;

            let existing = existing_rows.get(&id_str);
            let previously_received = self.received_rows.get(&id_str);

            // Merge refCounts. Branch on ENTRY PRESENCE, not the flattened
            // value: TS keys on `previouslyReceived !== undefined`, so a
            // present-but-null entry (a row merged to null in an EARLIER batch
            // of this pass) re-merges as `mergeRefCounts(null, refCounts)` — raw
            // received counts, with NO existing row and NO removed/executed
            // filter. The old `match &previously_received` flattened null→None
            // and wrongly took the existing+filter path, diverging the persisted
            // refCounts and the client patch (put vs del) on cross-batch
            // re-receipt of a shared row. See parity/BEHAVIORAL-SWEEP-FINDINGS.md.
            let merged = match previously_received {
                Some(prev_opt) => merge_ref_counts(prev_opt.as_ref(), Some(&ref_counts), None),
                None => merge_ref_counts(
                    existing.and_then(|e| e.ref_counts.as_ref()),
                    Some(&ref_counts),
                    Some(&self.removed_or_executed_query_ids),
                ),
            };
            // `existing || previouslyReceived` below only needs presence.
            let was_previously_received = previously_received.is_some();

            self.received_rows.insert(id_str.clone(), merged.clone());

            // TS (cvr.ts:865): `newRowVersion = merged === null ? undefined : version`,
            // then `existing && existing.rowVersion === newRowVersion`. `new_row_version`
            // is None exactly when `merged` is None (or `version` is None), so compare the
            // Options directly — `Some(rv) == None` is false, matching TS's
            // `rowVersion === undefined`. (The old `.unwrap_or("")` sentinel would have
            // spuriously kept the existing patch_version if a row_version were ever "".)
            let new_row_version: Option<&str> = merged.as_ref().and(version.as_deref());
            let patch_version = match existing {
                Some(e) if new_row_version == Some(e.row_version.as_str()) => {
                    e.patch_version.clone()
                }
                _ => self.assert_new_version()?,
            };

            // Determine the rowVersion to use for the put.
            let row_version = version.or_else(|| existing.map(|e| e.row_version.clone()));

            // Dedupe against lastPatch and ensure toVersion never backtracks.
            let last_patch = self.last_patches.get(&id_str);
            let to_version = match last_patch {
                Some(lp) if cmp_cvr(&lp.to_version, &patch_version) == Ordering::Greater => {
                    lp.to_version.clone()
                }
                _ => patch_version.clone(),
            };

            // The store op takes its own copy of `id`; the patch (if any) takes
            // `id` itself below. The record owns `merged` outright — the match
            // below only needs to know whether it was null.
            let merged_is_null = merged.is_none();
            match &row_version {
                Some(rv) => {
                    self.base.store_ops.push(StoreOp::PutRowRecord(RowRecord {
                        id: id.clone(),
                        row_version: rv.clone(),
                        patch_version,
                        ref_counts: merged,
                    }));
                }
                None => {
                    self.base.store_ops.push(StoreOp::DelRowRecord(id.clone()));
                }
            }

            match merged_is_null {
                true => {
                    // All refCounts gone to zero — delete if previously existed.
                    if existing.is_some() || was_previously_received {
                        let should_send = match last_patch {
                            Some(lp) => lp.row_version.is_some(),
                            None => true,
                        };
                        if should_send {
                            patches.push(PatchToVersion {
                                patch: Patch::Row(RowPatch::Del { id }),
                                to_version: to_version.clone(),
                            });
                            self.last_patches.insert(
                                id_str,
                                RowPatchInfo {
                                    row_version: None,
                                    to_version,
                                },
                            );
                        }
                    }
                }
                false => {
                    if let Some(contents) = contents {
                        let rv =
                            row_version.expect("a merged (non-deleted) row carries its rowVersion");
                        let should_send = match last_patch {
                            Some(lp) => lp
                                .row_version
                                .as_deref()
                                .is_none_or(|lrv| lrv < rv.as_str()),
                            None => true,
                        };
                        if should_send {
                            patches.push(PatchToVersion {
                                patch: Patch::Row(RowPatch::Put { id, contents }),
                                to_version: to_version.clone(),
                            });
                            self.last_patches.insert(
                                id_str,
                                RowPatchInfo {
                                    row_version: Some(rv),
                                    to_version,
                                },
                            );
                        }
                    }
                }
            }
        }

        Ok(patches)
    }

    /// Delete rows that are no longer referenced by any query.
    /// `existing_rows` is the set of rows associated with executed/removed queries.
    pub fn delete_unreferenced_rows<'a>(
        &mut self,
        existing_rows: impl IntoIterator<Item = &'a RowRecord>,
    ) -> Result<Vec<PatchToVersion>, String> {
        let mut patches: Vec<PatchToVersion> = Vec::new();

        if self.removed_or_executed_query_ids.is_empty() {
            assert!(
                self.received_rows.is_empty(),
                "Expected no received rows for query-less update, got {}",
                self.received_rows.len()
            );
            return Ok(patches);
        }

        for existing in existing_rows {
            let id_str = crate::row_key::row_id_string(&existing.id);

            // TS `#receivedRows.get(id)` is a TRUTHY check: an entry whose
            // merged refCounts collapsed to null (received then fully
            // retracted within this pass) is falsy, and TS REPROCESSES the
            // row below — persisting `existing.rowVersion` rather than the
            // retracted update's. `contains_key` skipped those.
            if self
                .received_rows
                .get(&id_str)
                .is_some_and(|rc| rc.is_some())
            {
                continue;
            }

            // TS only looks up rows that reference an executed or removed
            // query (`#lookupRowsForExecutedAndRemovedQueries`); rows
            // referencing neither are untouched. For those, the merge below
            // is an identity (nothing to subtract), so skipping is
            // behavior-identical and turns the per-pass cost from O(all CVR
            // rows) into O(rows of the executed/removed queries).
            let references_relevant = existing.ref_counts.as_ref().is_some_and(|rc| {
                rc.keys()
                    .any(|q| self.removed_or_executed_query_ids.contains(q))
            });
            if !references_relevant {
                continue;
            }

            let new_ref_counts = merge_ref_counts(
                existing.ref_counts.as_ref(),
                None,
                Some(&self.removed_or_executed_query_ids),
            );

            let patch_version = match &new_ref_counts {
                Some(_) => existing.patch_version.clone(),
                None => self.assert_new_version()?,
            };

            let row_record = RowRecord {
                id: existing.id.clone(),
                row_version: existing.row_version.clone(),
                patch_version: patch_version.clone(),
                ref_counts: new_ref_counts.clone(),
            };
            self.base.store_ops.push(StoreOp::PutRowRecord(row_record));

            if new_ref_counts.is_none() {
                // Dedupe against lastPatch: skip if we already emitted a delete
                // for this row (rowVersion == None), and never let toVersion
                // backtrack. Mirrors TS deleteUnreferencedRows (zero/v1.9.0).
                let (already_deleted, to_version) = match self.last_patches.get(&id_str) {
                    Some(lp) => (
                        lp.row_version.is_none(),
                        max_version(self.base.cvr.version.clone(), Some(lp.to_version.clone())),
                    ),
                    None => (false, self.base.cvr.version.clone()),
                };
                if !already_deleted {
                    patches.push(PatchToVersion {
                        patch: Patch::Row(RowPatch::Del {
                            id: existing.id.clone(),
                        }),
                        to_version: to_version.clone(),
                    });
                    self.last_patches.insert(
                        id_str.clone(),
                        RowPatchInfo {
                            row_version: None,
                            to_version,
                        },
                    );
                }
            }
        }

        Ok(patches)
    }

    /// Flush — persists row-set signatures before base flush.
    pub fn flush(
        &mut self,
        last_connect_time: i64,
        last_active: i64,
        ttl_clock: TTLClock,
    ) -> (CVR, Option<CVRFlushStats>) {
        // Persist per-query row-set signatures if the provider reports a drift.
        if let Some(ref provider) = self.row_set_signature_provider {
            let query_ids: Vec<String> = self.base.cvr.queries.keys().cloned().collect();
            for query_id in query_ids {
                let sig = provider(&query_id);
                let sig = match sig {
                    Some(s) => s,
                    None => continue,
                };
                let stored = self
                    .base
                    .cvr
                    .queries
                    .get(&query_id)
                    .and_then(|q| q.base().row_set_signature.as_deref())
                    .and_then(|s| crate::row_set_signature::parse_signature(Some(s)).ok());

                if stored == Some(sig) {
                    continue;
                }

                // Persist the changed signature — TS `cvr.ts` flush
                // (view-syncer/cvr.ts:808-825) PERSISTS only; it does NOT count
                // drift. The `row-set-signature-drifts` counter lives in
                // `hydrate_unchanged_queries` (TS `#hydrateUnchangedQueries`),
                // which checks drift ONLY at the same db state — counting here
                // would also fire when the db legitimately advanced (a new row set
                // for a new db state, not non-determinism), over-counting.
                let hex = crate::row_set_signature::format_signature(sig);
                if let Some(query) = self.base.cvr.queries.get_mut(&query_id) {
                    query.base_mut().row_set_signature = Some(hex.clone());
                }
                self.base
                    .store_ops
                    .push(StoreOp::UpdateRowSetSignature { query_id, hex });
            }
        }

        self.base.flush(last_connect_time, last_active, ttl_clock)
    }
}

// ─── CVR data types (cvr.ts) + StoreOp bridge ───

/// RefCounts: query hash → count. Using BTreeMap for deterministic ordering.
pub type RefCounts = BTreeMap<String, i64>;
/// RowUpdate — what the replicator sends for a row.
///
/// `contents` is `Arc`-shared: a hydrated row's contents flow unchanged from
/// the engine callback through `RowPatch::Put` into each client's poke body,
/// so sharing one allocation avoids a deep `Value` clone per stage (the
/// per-row deliver path was the dominant hydration cost for large queries).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contents: Option<std::sync::Arc<Value>>,
    pub ref_counts: RefCounts,
}
/// The mutable CVR type (matches TS `CVR`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CVR {
    pub id: String,
    pub version: CVRVersion,
    pub last_active: i64,
    pub ttl_clock: TTLClock,
    pub replica_version: Option<String>,
    pub clients: BTreeMap<String, ClientRecord>,
    pub queries: BTreeMap<String, QueryRecord>,
    pub client_schema: Option<ClientSchema>,
    pub profile_id: Option<String>,
}
/// Desired query spec — what the client wants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DesiredQuerySpec {
    pub hash: String,
    pub ast: Option<AST>,
    pub name: Option<String>,
    pub args: Option<Vec<Value>>,
    pub ttl: Option<i64>, // milliseconds; None = DEFAULT_TTL_MS
}
/// Store operations collected by the updater for TS to replay.
/// Mirrors the CVRStore method calls that the TS updaters make inline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StoreOp {
    InsertClient(ClientRecord),
    PutQuery(QueryRecord),
    PutDesiredQuery {
        version: CVRVersion,
        query_id: String,
        client_id: String,
        deleted: bool,
        inactivated_at: Option<TTLClock>,
        ttl: i64,
    },
    PutInstance(CVR),
    DeleteClient(String),
    UpdateQuery(QueryRecord),
    MarkQueryAsDeleted {
        version: CVRVersion,
        patch: QueryPatch,
    },
    PutRowRecord(RowRecord),
    DelRowRecord(RowID),
    UpdateRowSetSignature {
        query_id: String,
        hex: String,
    },
}
pub const CLIENT_LMID_QUERY_ID: &str = "lmids";
pub const CLIENT_MUTATION_RESULTS_QUERY_ID: &str = "mutationResults";

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "tests/cvr_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/cvr_updater_tests.rs"]
mod updater_tests;
