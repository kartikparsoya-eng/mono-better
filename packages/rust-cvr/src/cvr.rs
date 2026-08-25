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
        if next.is_none() || expire < next.unwrap() {
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
            return self.base.cvr.clients.get_mut(id).unwrap();
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

        self.base.cvr.clients.get_mut(id).unwrap()
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
                // Warning in TS — here we just proceed.
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
            .unwrap()
            .desired_query_ids
            .iter()
            .cloned()
            .collect();

        // Find new/changed desired queries.
        let mut needed: HashSet<String> = HashSet::new();

        for q in queries {
            let ttl = q.ttl.unwrap_or(DEFAULT_TTL_MS);
            let query = self.base.cvr.queries.get(&q.hash);
            match query {
                None => {
                    // New query
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
                            // Reactivated query
                            needed.insert(q.hash.clone());
                            continue;
                        }
                        Some(state) if state.inactivated_at.is_some() => {
                            // Reactivated query
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
        // HashSets, so the union is already duplicate-free — just sort it.
        let mut combined: Vec<String> = current.union(&needed).cloned().collect();
        combined.sort();
        self.base
            .cvr
            .clients
            .get_mut(client_id)
            .unwrap()
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
            .unwrap()
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

        // Update desiredQueryIDs: sorted difference.
        let mut remaining: Vec<String> = current.difference(&remove).cloned().collect();
        remaining.sort();
        self.base
            .cvr
            .clients
            .get_mut(client_id)
            .unwrap()
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
            .unwrap()
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

        if state_version > base.orig.version.state_version {
            base.set_version(CVRVersion {
                state_version: state_version.clone(),
                config_version: None,
            });
        } else if state_version == base.orig.version.state_version {
            // Bump config version for row changes.
            base.ensure_new_version();
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

            let query = self.base.cvr.queries.get_mut(query_id).unwrap();

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
    fn assert_new_version(&self) -> CVRVersion {
        assert!(
            cmp_versions(
                &Some(self.base.orig.version.clone()),
                &Some(self.base.cvr.version.clone())
            ) == Ordering::Less,
            "Expected CVR version to have been bumped above original"
        );
        self.base.cvr.version.clone()
    }

    /// Track rows received from executing queries.
    /// `existing_rows` is the current row records from the RowRecordCache.
    /// Returns patches to send to clients.
    pub fn received(
        &mut self,
        rows: &HashMap<String, (RowID, RowUpdate)>, // keyed by rowIDString
        existing_rows: &RowRecordMap,
    ) -> Vec<PatchToVersion> {
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
            let contents = &update.contents;
            let version = &update.version;
            let ref_counts = &update.ref_counts;

            let existing = existing_rows.get(id_str);
            let previously_received = self.received_rows.get(id_str).and_then(|o| o.clone());

            // Merge refCounts. Branch on ENTRY PRESENCE, not the flattened
            // value: TS keys on `previouslyReceived !== undefined`, so a
            // present-but-null entry (a row merged to null in an EARLIER batch
            // of this pass) re-merges as `mergeRefCounts(null, refCounts)` — raw
            // received counts, with NO existing row and NO removed/executed
            // filter. The old `match &previously_received` flattened null→None
            // and wrongly took the existing+filter path, diverging the persisted
            // refCounts and the client patch (put vs del) on cross-batch
            // re-receipt of a shared row. See parity/BEHAVIORAL-SWEEP-FINDINGS.md.
            let merged = match self.received_rows.get(id_str) {
                Some(prev_opt) => merge_ref_counts(prev_opt.as_ref(), Some(ref_counts), None),
                None => merge_ref_counts(
                    existing.and_then(|e| e.ref_counts.as_ref()),
                    Some(ref_counts),
                    Some(&self.removed_or_executed_query_ids),
                ),
            };

            self.received_rows.insert(id_str.clone(), merged.clone());

            // TS (cvr.ts:865): `newRowVersion = merged === null ? undefined : version`,
            // then `existing && existing.rowVersion === newRowVersion`. `new_row_version`
            // is None exactly when `merged` is None (or `version` is None), so compare the
            // Options directly — `Some(rv) == None` is false, matching TS's
            // `rowVersion === undefined`. (The old `.unwrap_or("")` sentinel would have
            // spuriously kept the existing patch_version if a row_version were ever "".)
            let new_row_version: Option<String> = merged.as_ref().and_then(|_| version.clone());
            let patch_version = match existing {
                Some(e) if new_row_version.as_deref() == Some(e.row_version.as_str()) => {
                    e.patch_version.clone()
                }
                _ => self.assert_new_version(),
            };

            // Determine the rowVersion to use for the put.
            let row_version = version
                .clone()
                .or_else(|| existing.map(|e| e.row_version.clone()));

            match &row_version {
                Some(rv) => {
                    let record = RowRecord {
                        id: id.clone(),
                        row_version: rv.clone(),
                        patch_version: patch_version.clone(),
                        ref_counts: merged.clone(),
                    };
                    self.base.store_ops.push(StoreOp::PutRowRecord(record));
                }
                None => {
                    self.base.store_ops.push(StoreOp::DelRowRecord(id.clone()));
                }
            }

            // Dedupe against lastPatch and ensure toVersion never backtracks.
            let last_patch = self.last_patches.get(id_str);
            let to_version = match last_patch {
                Some(lp) => max_version(patch_version.clone(), Some(lp.to_version.clone())),
                None => patch_version.clone(),
            };

            match &merged {
                None => {
                    // All refCounts gone to zero — delete if previously existed.
                    if existing.is_some() || previously_received.is_some() {
                        let should_send = match last_patch {
                            Some(lp) => lp.row_version.is_some(),
                            None => true,
                        };
                        if should_send {
                            patches.push(PatchToVersion {
                                patch: Patch::Row(RowPatch::Del { id: id.clone() }),
                                to_version: to_version.clone(),
                            });
                            self.last_patches.insert(
                                id_str.clone(),
                                RowPatchInfo {
                                    row_version: None,
                                    to_version: to_version.clone(),
                                },
                            );
                        }
                    }
                }
                Some(_) => {
                    if let Some(contents) = contents {
                        let rv = row_version.as_ref().unwrap();
                        let should_send = match last_patch {
                            Some(lp) => lp
                                .row_version
                                .as_deref()
                                .is_none_or(|lrv| lrv < rv.as_str()),
                            None => true,
                        };
                        if should_send {
                            patches.push(PatchToVersion {
                                patch: Patch::Row(RowPatch::Put {
                                    id: id.clone(),
                                    contents: contents.clone(),
                                }),
                                to_version: to_version.clone(),
                            });
                            self.last_patches.insert(
                                id_str.clone(),
                                RowPatchInfo {
                                    row_version: Some(rv.clone()),
                                    to_version: to_version.clone(),
                                },
                            );
                        }
                    }
                }
            }
        }

        patches
    }

    /// Delete rows that are no longer referenced by any query.
    /// `existing_rows` is the set of rows associated with executed/removed queries.
    pub fn delete_unreferenced_rows<'a>(
        &mut self,
        existing_rows: impl IntoIterator<Item = &'a RowRecord>,
    ) -> Vec<PatchToVersion> {
        let mut patches: Vec<PatchToVersion> = Vec::new();

        if self.removed_or_executed_query_ids.is_empty() {
            assert!(
                self.received_rows.is_empty(),
                "Expected no received rows for query-less update, got {}",
                self.received_rows.len()
            );
            return patches;
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
                None => self.assert_new_version(),
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

        patches
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

                // A stored signature that CHANGED (vs first-time None) is a real
                // drift: the same transformation hash produced a different row
                // set → non-deterministic execution. Count it (TS
                // query.row-set-signature-drifts canary); a first-time signature
                // is not a drift.
                if stored.is_some() {
                    crate::otel_metrics::record_row_set_signature_drift();
                }

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn rc(pairs: &[(&str, i64)]) -> RefCounts {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    #[test]
    fn test_merge_both_none() {
        assert_eq!(merge_ref_counts(None, None, None), None);
    }

    #[test]
    fn test_merge_existing_only() {
        let existing = rc(&[("a", 1), ("b", 2)]);
        let result = merge_ref_counts(Some(&existing), None, None);
        assert_eq!(result, Some(rc(&[("a", 1), ("b", 2)])));
    }

    #[test]
    fn test_merge_received_only() {
        let received = rc(&[("a", 1), ("b", 2)]);
        let result = merge_ref_counts(None, Some(&received), None);
        assert_eq!(result, Some(rc(&[("a", 1), ("b", 2)])));
    }

    #[test]
    fn test_merge_adds_counts() {
        let existing = rc(&[("a", 1), ("b", 2)]);
        let received = rc(&[("a", 1), ("c", 3)]);
        let result = merge_ref_counts(Some(&existing), Some(&received), None);
        assert_eq!(result, Some(rc(&[("a", 2), ("b", 2), ("c", 3)])));
    }

    #[test]
    fn test_merge_drops_zeros() {
        let existing = rc(&[("a", 1), ("b", 2)]);
        let received = rc(&[("a", -1)]);
        let result = merge_ref_counts(Some(&existing), Some(&received), None);
        assert_eq!(result, Some(rc(&[("b", 2)])));
    }

    #[test]
    fn test_merge_all_zero_returns_none() {
        let existing = rc(&[("a", 1)]);
        let received = rc(&[("a", -1)]);
        let result = merge_ref_counts(Some(&existing), Some(&received), None);
        assert_eq!(result, None);
    }

    #[test]
    fn test_merge_remove_hashes() {
        let existing = rc(&[("a", 1), ("b", 2), ("c", 3)]);
        let received = rc(&[("d", 4)]);
        let mut remove = HashSet::new();
        remove.insert("a".to_string());
        remove.insert("c".to_string());
        let result = merge_ref_counts(Some(&existing), Some(&received), Some(&remove));
        // a and c removed from existing, b kept, d added
        assert_eq!(result, Some(rc(&[("b", 2), ("d", 4)])));
    }

    #[test]
    fn test_merge_negative_in_received() {
        let existing = rc(&[("a", 2), ("b", 1)]);
        let received = rc(&[("a", -1), ("b", -1)]);
        let result = merge_ref_counts(Some(&existing), Some(&received), None);
        assert_eq!(result, Some(rc(&[("a", 1)])));
    }

    #[test]
    fn test_merge_all_negative_returns_none() {
        let existing = rc(&[("a", 1)]);
        let received = rc(&[("a", -2)]);
        let result = merge_ref_counts(Some(&existing), Some(&received), None);
        // a = 1 + (-2) = -1, which is not > 0, so None
        assert_eq!(result, None);
    }

    #[test]
    fn test_merge_received_only_with_negative() {
        let received = rc(&[("a", -1)]);
        let result = merge_ref_counts(None, Some(&received), None);
        // -1 is not > 0, so None
        assert_eq!(result, None);
    }

    #[test]
    fn test_merge_received_only_retains_zero() {
        // TS `mergeRefCounts(null, received)` does `merged = received ?? {}` —
        // a raw copy that RETAINS zero entries (verified by the TS golden
        // fixture in parity_check). The result is non-null because at least one
        // count is positive.
        let received = rc(&[("a", 0), ("b", 1)]);
        let result = merge_ref_counts(None, Some(&received), None);
        assert_eq!(result, Some(rc(&[("a", 0), ("b", 1)])));
    }

    #[test]
    fn test_merge_remove_hashes_from_existing_only() {
        let existing = rc(&[("a", 1)]);
        let received = rc(&[("a", 1)]);
        let mut remove = HashSet::new();
        remove.insert("a".to_string());
        // remove_hashes skips "a" from existing, but received "a" is still added
        let result = merge_ref_counts(Some(&existing), Some(&received), Some(&remove));
        assert_eq!(result, Some(rc(&[("a", 1)])));
    }

    #[test]
    fn test_merge_empty_existing() {
        let existing = rc(&[]);
        let received = rc(&[("a", 1)]);
        let result = merge_ref_counts(Some(&existing), Some(&received), None);
        assert_eq!(result, Some(rc(&[("a", 1)])));
    }

    #[test]
    fn test_merge_empty_received() {
        let existing = rc(&[("a", 1)]);
        let received = rc(&[]);
        let result = merge_ref_counts(Some(&existing), Some(&received), None);
        assert_eq!(result, Some(rc(&[("a", 1)])));
    }

    // Property-style tests for merge_ref_counts
    #[test]
    fn test_prop_merge_idempotent_received_none() {
        // merge(x, None, None) == normalize(x)
        let x = rc(&[("a", 1), ("b", 0), ("c", 3), ("d", -1)]);
        let result = merge_ref_counts(Some(&x), None, None);
        // "b" (0) dropped, "d" (-1) not > 0 so dropped
        // But wait: in the existing path, -1 is added to merged.
        // merged["d"] = 0 + (-1) = -1, which != 0 so not deleted.
        // Then at the end, values().any(|v| v > 0) is true (a=1, c=3).
        // So result includes d=-1.
        // Actually TS: "merged[hash] = (merged[hash] ?? 0) + count; if (merged[hash] === 0) delete merged[hash];"
        // So -1 stays in merged. Then the final check is ".some(v => v > 0)".
        // So d=-1 is in the result but doesn't cause None.
        assert_eq!(result, Some(rc(&[("a", 1), ("c", 3), ("d", -1)])));
    }

    #[test]
    fn test_prop_merge_no_positive_returns_none() {
        let x = rc(&[("a", -1), ("b", -2)]);
        let result = merge_ref_counts(Some(&x), None, None);
        assert_eq!(result, None);
    }

    #[test]
    fn test_new_query_record_client() {
        let ast = serde_json::json!({"schema": "s", "table": "t"});
        let q = new_query_record("hash1", Some(&ast), None, None);
        match q {
            QueryRecord::Client(r) => {
                assert_eq!(r.base.id, "hash1");
                assert_eq!(r.ast, ast);
                assert!(r.client_state.is_empty());
                assert!(r.patch_version.is_none());
            }
            _ => panic!("expected Client query"),
        }
    }

    #[test]
    fn test_new_query_record_custom() {
        let args = vec![serde_json::json!(1), serde_json::json!("x")];
        let q = new_query_record("hash1", None, Some("myQuery"), Some(&args));
        match q {
            QueryRecord::Custom(r) => {
                assert_eq!(r.base.id, "hash1");
                assert_eq!(r.name, "myQuery");
                assert_eq!(r.args, args);
            }
            _ => panic!("expected Custom query"),
        }
    }

    #[test]
    #[should_panic(expected = "Cannot provide name or args with ast")]
    fn test_new_query_record_ast_and_name_panics() {
        let ast = serde_json::json!({});
        new_query_record("h", Some(&ast), Some("n"), None);
    }

    #[test]
    fn test_assert_not_internal_client() {
        let q = new_query_record("h", Some(&serde_json::json!({})), None, None);
        assert_not_internal(&q); // should not panic
    }

    #[test]
    #[should_panic(expected = "reserved for internal use")]
    fn test_assert_not_internal_panics() {
        let q = QueryRecord::Internal(InternalQueryRecord {
            base: BaseQueryRecord {
                id: "lmids".to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
            },
            ast: serde_json::json!({}),
        });
        assert_not_internal(&q);
    }

    // ④ Property invariant for merge_ref_counts.
    use proptest::prelude::*;

    proptest! {
        // The null rule (TS: `... .some(v => v > 0) ? merged : null`): a non-null
        // merge result always contains at least one positive count. Regression
        // guard for the no-existing zero-retention fix.
        #[test]
        fn prop_merge_some_has_positive(
            ex in proptest::option::of(proptest::collection::btree_map("[a-c]", -3i64..3, 0..4)),
            rv in proptest::option::of(proptest::collection::btree_map("[a-c]", -3i64..3, 0..4)),
        ) {
            if let Some(m) = merge_ref_counts(ex.as_ref(), rv.as_ref(), None) {
                prop_assert!(m.values().any(|&v| v > 0));
            }
        }

        // mergeRefCounts algebra. TS (cvr.ts `mergeRefCounts`) has TWO branches
        // with DIFFERENT semantics, so the invariants are branch-specific:
        //
        //  • existing = Some: per-hash integer ADDITION with the remove-filter on
        //    the existing side only, zeros stripped from the map. So for every
        //    hash, merged[h] (absent = 0) == filtered_existing[h] + received[h],
        //    and every retained value is nonzero.
        //  • existing = None: `merged = received ?? {}` — a raw copy that RETAINS
        //    zero entries and ignores remove_hashes. So a Some result equals
        //    `received` verbatim (this pins the documented zero-retention
        //    asymmetry — a prior real bug dropped those zeros).
        //
        // Both branches share the null rule: Some iff some count is > 0.
        #[test]
        fn prop_merge_ref_counts_algebra(
            ex in proptest::option::of(proptest::collection::btree_map("[a-e]", -3i64..4, 0..5)),
            rv in proptest::option::of(proptest::collection::btree_map("[a-e]", -3i64..4, 0..5)),
            rh in proptest::collection::hash_set("[a-e]", 0..3),
        ) {
            let rh = if rh.is_empty() { None } else { Some(rh) };
            let out = merge_ref_counts(ex.as_ref(), rv.as_ref(), rh.as_ref());

            match ex.as_ref() {
                Some(_) => {
                    if let Some(m) = out.as_ref() {
                        // zeros are stripped in the existing=Some branch
                        for &v in m.values() {
                            prop_assert_ne!(v, 0);
                        }
                        // additive law over the union of hashes (remove-filter on existing)
                        let mut hashes: std::collections::BTreeSet<String> = Default::default();
                        if let Some(e) = ex.as_ref() { hashes.extend(e.keys().cloned()); }
                        if let Some(r) = rv.as_ref() { hashes.extend(r.keys().cloned()); }
                        for h in hashes {
                            let removed = rh.as_ref().is_some_and(|s| s.contains(&h));
                            let e = if removed {
                                0
                            } else {
                                ex.as_ref().and_then(|m| m.get(&h)).copied().unwrap_or(0)
                            };
                            let r = rv.as_ref().and_then(|m| m.get(&h)).copied().unwrap_or(0);
                            prop_assert_eq!(m.get(&h).copied().unwrap_or(0), e + r);
                        }
                    }
                }
                None => {
                    // existing=None: Some(received-verbatim, zeros kept) iff any positive.
                    match rv.as_ref() {
                        Some(r) if r.values().any(|&v| v > 0) => {
                            prop_assert_eq!(out.as_ref(), Some(r));
                        }
                        _ => prop_assert!(out.is_none()),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod updater_tests {
    use super::*;
    use crate::schema::types::CVRVersion;

    fn make_test_cvr() -> CVR {
        CVR {
            id: "cg-test".to_string(),
            version: CVRVersion {
                state_version: "v1".to_string(),
                config_version: None,
            },
            last_active: 0,
            ttl_clock: 0,
            replica_version: Some("r1".to_string()),
            clients: BTreeMap::new(),
            queries: BTreeMap::new(),
            client_schema: None,
            profile_id: None,
        }
    }

    fn make_shard() -> ShardID {
        ShardID {
            app_id: "test".to_string(),
            shard_num: 0,
        }
    }

    // ─── CVRConfigDrivenUpdater tests ──────────────────────────────────

    #[test]
    fn test_ensure_client_creates_client_and_internal_queries() {
        let cvr = make_test_cvr();
        let shard = make_shard();
        let mut updater = CVRConfigDrivenUpdater::new(cvr, shard);

        updater.ensure_client("client1");

        // Client should exist
        assert!(updater.base.cvr.clients.contains_key("client1"));

        // Internal queries should be created
        assert!(updater.base.cvr.queries.contains_key(CLIENT_LMID_QUERY_ID));
        assert!(
            updater
                .base
                .cvr
                .queries
                .contains_key(CLIENT_MUTATION_RESULTS_QUERY_ID)
        );

        // Version should be bumped
        assert!(
            cmp_versions(
                &Some(CVRVersion {
                    state_version: "v1".to_string(),
                    config_version: None,
                }),
                &Some(updater.base.cvr.version.clone())
            ) == Ordering::Less
        );

        // Store ops should have: InsertClient, PutQuery (lmids), PutQuery (mutationResults)
        let ops = updater.base.drain_store_ops();
        assert_eq!(ops.len(), 3); // InsertClient + 2 PutQuery
    }

    #[test]
    fn test_ensure_client_idempotent() {
        let cvr = make_test_cvr();
        let shard = make_shard();
        let mut updater = CVRConfigDrivenUpdater::new(cvr, shard);

        updater.ensure_client("client1");
        let _ops1 = updater.base.drain_store_ops();
        updater.ensure_client("client1");
        let ops2 = updater.base.drain_store_ops();

        // Second call should produce no new store ops
        assert!(ops2.is_empty());
    }

    #[test]
    fn test_put_desired_queries_new() {
        let cvr = make_test_cvr();
        let shard = make_shard();
        let mut updater = CVRConfigDrivenUpdater::new(cvr, shard);

        let queries = vec![DesiredQuerySpec {
            hash: "hash1".to_string(),
            ast: Some(serde_json::json!({"schema": "s", "table": "t"})),
            name: None,
            args: None,
            ttl: None,
        }];

        let patches = updater.put_desired_queries("client1", &queries);

        // Should produce 1 patch (put query for client1)
        assert_eq!(patches.len(), 1);

        // Client should have desiredQueryIDs
        let client = updater.base.cvr.clients.get("client1").unwrap();
        assert_eq!(client.desired_query_ids, vec!["hash1"]);

        // Query should exist with client state
        let query = updater.base.cvr.queries.get("hash1").unwrap();
        match query {
            QueryRecord::Client(r) => {
                assert!(r.client_state.contains_key("client1"));
                let state = r.client_state.get("client1").unwrap();
                assert!(state.inactivated_at.is_none());
            }
            _ => panic!("expected Client query"),
        }
    }

    #[test]
    fn test_put_desired_queries_no_change() {
        let cvr = make_test_cvr();
        let shard = make_shard();
        let mut updater = CVRConfigDrivenUpdater::new(cvr, shard);

        let queries = vec![DesiredQuerySpec {
            hash: "hash1".to_string(),
            ast: Some(serde_json::json!({"schema": "s", "table": "t"})),
            name: None,
            args: None,
            ttl: None,
        }];

        // First call — adds the query
        updater.put_desired_queries("client1", &queries);
        updater.base.drain_store_ops();

        // Second call with same query — should be no-op
        let patches = updater.put_desired_queries("client1", &queries);
        assert!(patches.is_empty());
    }

    #[test]
    fn test_delete_desired_queries() {
        let cvr = make_test_cvr();
        let shard = make_shard();
        let mut updater = CVRConfigDrivenUpdater::new(cvr, shard);

        let queries = vec![
            DesiredQuerySpec {
                hash: "hash1".to_string(),
                ast: Some(serde_json::json!({"schema": "s", "table": "t1"})),
                name: None,
                args: None,
                ttl: None,
            },
            DesiredQuerySpec {
                hash: "hash2".to_string(),
                ast: Some(serde_json::json!({"schema": "s", "table": "t2"})),
                name: None,
                args: None,
                ttl: None,
            },
        ];

        updater.put_desired_queries("client1", &queries);
        updater.base.drain_store_ops();

        // Delete hash1
        let patches = updater.delete_desired_queries("client1", &["hash1".to_string()]);
        assert_eq!(patches.len(), 1);

        // Client should only have hash2
        let client = updater.base.cvr.clients.get("client1").unwrap();
        assert_eq!(client.desired_query_ids, vec!["hash2"]);

        // hash1's client state for client1 should be removed
        let query = updater.base.cvr.queries.get("hash1").unwrap();
        assert!(!query.client_state().unwrap().contains_key("client1"));
    }

    /// Parity regression (BEHAVIORAL-SWEEP-FINDINGS.md, `delete_queries`):
    /// inactivating a query the client DESIRES but has NO clientState for (query
    /// never transformed) must NOT fabricate a clientState entry. TS
    /// (cvr.ts:463-476) guards the assignment with `if (clientState !== undefined)`;
    /// the old Rust inserted unconditionally.
    #[test]
    fn test_inactivate_missing_client_state_does_not_fabricate_entry() {
        let cvr = make_test_cvr();
        let shard = make_shard();
        let mut updater = CVRConfigDrivenUpdater::new(cvr, shard);

        let queries = vec![DesiredQuerySpec {
            hash: "hashX".to_string(),
            ast: Some(serde_json::json!({"schema": "s", "table": "t"})),
            name: None,
            args: None,
            ttl: None,
        }];
        updater.put_desired_queries("client1", &queries);

        // Simulate "query desired but never transformed": drop the clientState
        // entry while leaving "hashX" in the client's desiredQueryIDs.
        updater
            .base
            .cvr
            .queries
            .get_mut("hashX")
            .unwrap()
            .client_state_mut()
            .unwrap()
            .remove("client1");
        updater.base.drain_store_ops();

        // Inactivate it.
        let ttl_clock: TTLClock = 1000;
        updater.mark_desired_queries_as_inactive("client1", &["hashX".to_string()], ttl_clock);

        // TS leaves clientState absent; Rust must too (no fabricated entry).
        let q = updater.base.cvr.queries.get("hashX").unwrap();
        assert!(
            !q.client_state().unwrap().contains_key("client1"),
            "inactivating a query with no clientState must not fabricate a clientState entry (TS parity)"
        );
    }

    #[test]
    fn test_delete_client() {
        let cvr = make_test_cvr();
        let shard = make_shard();
        let mut updater = CVRConfigDrivenUpdater::new(cvr, shard);

        let queries = vec![DesiredQuerySpec {
            hash: "hash1".to_string(),
            ast: Some(serde_json::json!({"schema": "s", "table": "t"})),
            name: None,
            args: None,
            ttl: None,
        }];

        updater.put_desired_queries("client1", &queries);
        updater.base.drain_store_ops();

        let patches = updater.delete_client("client1", 1000);
        // Should produce 1 del patch for hash1
        assert_eq!(patches.len(), 1);

        // Client should be removed
        assert!(!updater.base.cvr.clients.contains_key("client1"));

        // Should have DeleteClient store op
        let ops = updater.base.drain_store_ops();
        assert!(ops.iter().any(|op| matches!(op, StoreOp::DeleteClient(_))));
    }

    #[test]
    fn test_delete_client_not_found() {
        let cvr = make_test_cvr();
        let shard = make_shard();
        let mut updater = CVRConfigDrivenUpdater::new(cvr, shard);

        let patches = updater.delete_client("nonexistent", 1000);
        assert!(patches.is_empty());
    }

    #[test]
    fn test_set_client_schema_new() {
        let cvr = make_test_cvr();
        let shard = make_shard();
        let mut updater = CVRConfigDrivenUpdater::new(cvr, shard);

        let schema = serde_json::json!({"version": 1});
        let result = updater.set_client_schema(schema.clone());
        assert!(result.is_ok());
        assert_eq!(updater.base.cvr.client_schema, Some(schema));
    }

    #[test]
    fn test_set_client_schema_mismatch() {
        let cvr = make_test_cvr();
        let shard = make_shard();
        let mut updater = CVRConfigDrivenUpdater::new(cvr, shard);

        let schema1 = serde_json::json!({"version": 1});
        let schema2 = serde_json::json!({"version": 2});

        updater.set_client_schema(schema1).unwrap();
        let result = updater.set_client_schema(schema2);
        assert!(result.is_err());
    }

    #[test]
    fn test_set_client_schema_same() {
        let cvr = make_test_cvr();
        let shard = make_shard();
        let mut updater = CVRConfigDrivenUpdater::new(cvr, shard);

        let schema = serde_json::json!({"version": 1});
        updater.set_client_schema(schema.clone()).unwrap();
        let result = updater.set_client_schema(schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_set_profile_id() {
        let cvr = make_test_cvr();
        let shard = make_shard();
        let mut updater = CVRConfigDrivenUpdater::new(cvr, shard);

        updater.set_profile_id("user123");
        assert_eq!(updater.base.cvr.profile_id, Some("user123".to_string()));

        // Setting same value — no-op
        updater.base.drain_store_ops();
        updater.set_profile_id("user123");
        let ops = updater.base.drain_store_ops();
        assert!(ops.is_empty());

        // Setting different value
        updater.set_profile_id("user456");
        assert_eq!(updater.base.cvr.profile_id, Some("user456".to_string()));
    }

    #[test]
    fn test_clear_desired_queries() {
        let cvr = make_test_cvr();
        let shard = make_shard();
        let mut updater = CVRConfigDrivenUpdater::new(cvr, shard);

        let queries = vec![
            DesiredQuerySpec {
                hash: "hash1".to_string(),
                ast: Some(serde_json::json!({"schema": "s", "table": "t1"})),
                name: None,
                args: None,
                ttl: None,
            },
            DesiredQuerySpec {
                hash: "hash2".to_string(),
                ast: Some(serde_json::json!({"schema": "s", "table": "t2"})),
                name: None,
                args: None,
                ttl: None,
            },
        ];

        updater.put_desired_queries("client1", &queries);
        updater.base.drain_store_ops();

        let patches = updater.clear_desired_queries("client1");
        assert_eq!(patches.len(), 2);

        let client = updater.base.cvr.clients.get("client1").unwrap();
        assert!(client.desired_query_ids.is_empty());
    }

    // ─── CVRQueryDrivenUpdater tests ───────────────────────────────────

    fn make_query_driven_updater(cvr: CVR, state_version: &str) -> CVRQueryDrivenUpdater {
        CVRQueryDrivenUpdater::new(cvr, state_version.to_string(), "r1".to_string(), None)
    }

    #[test]
    fn test_query_updater_bumps_version_on_new_state_version() {
        let cvr = make_test_cvr();
        let updater = make_query_driven_updater(cvr, "v2");
        assert_eq!(updater.base.cvr.version.state_version, "v2");
        assert!(updater.base.cvr.version.config_version.is_none());
    }

    #[test]
    fn test_query_updater_bumps_config_on_same_state_version() {
        let cvr = make_test_cvr();
        let updater = make_query_driven_updater(cvr, "v1");
        // Same stateVersion → config version should be bumped
        assert_eq!(updater.base.cvr.version.state_version, "v1");
        assert_eq!(updater.base.cvr.version.config_version, Some(1));
    }

    #[test]
    fn test_track_queries_executed() {
        let mut cvr = make_test_cvr();
        // Add a client query
        let query = QueryRecord::Client(ClientQueryRecord {
            base: BaseQueryRecord {
                id: "hash1".to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
            },
            ast: serde_json::json!({"schema": "s", "table": "t"}),
            client_state: BTreeMap::new(),
            patch_version: None,
        });
        cvr.queries.insert("hash1".to_string(), query);

        let mut updater = make_query_driven_updater(cvr, "v2");
        let (_version, patches) = updater.track_queries(&[("hash1", "th1")], &[]);

        // Should produce a got query patch
        assert_eq!(patches.len(), 1);
        match &patches[0].patch {
            Patch::Query(QueryPatch::Put { id, .. }) => {
                assert_eq!(id, "hash1");
            }
            _ => panic!("expected QueryPatch::Put"),
        }

        // Query should have transformationHash set
        let query = updater.base.cvr.queries.get("hash1").unwrap();
        assert_eq!(query.base().transformation_hash.as_deref(), Some("th1"));
        assert!(query.patch_version().is_some());
    }

    #[test]
    fn test_track_queries_removed() {
        let mut cvr = make_test_cvr();
        let query = QueryRecord::Client(ClientQueryRecord {
            base: BaseQueryRecord {
                id: "hash1".to_string(),
                transformation_hash: Some("th1".to_string()),
                transformation_version: None,
                row_set_signature: None,
            },
            ast: serde_json::json!({"schema": "s", "table": "t"}),
            client_state: BTreeMap::new(),
            patch_version: Some(CVRVersion {
                state_version: "v1".to_string(),
                config_version: None,
            }),
        });
        cvr.queries.insert("hash1".to_string(), query);

        let mut updater = make_query_driven_updater(cvr, "v2");
        let (_version, patches) = updater.track_queries(&[], &["hash1"]);

        // Should produce a del query patch
        assert_eq!(patches.len(), 1);
        match &patches[0].patch {
            Patch::Query(QueryPatch::Del { id, .. }) => {
                assert_eq!(id, "hash1");
            }
            _ => panic!("expected QueryPatch::Del"),
        }

        // Query should be removed
        assert!(!updater.base.cvr.queries.contains_key("hash1"));
    }

    #[test]
    fn test_received_new_row() {
        let cvr = make_test_cvr();
        let mut updater = make_query_driven_updater(cvr, "v2");
        updater.track_queries(&[], &[]); // Initiate tracking (no queries)

        let id = RowID {
            schema: "s".to_string(),
            table: "t".to_string(),
            row_key: serde_json::json!({"id": 1}).as_object().unwrap().clone(),
        };
        let id_str = crate::row_key::row_id_string(&id);
        let update = RowUpdate {
            version: Some("rv1".to_string()),
            contents: Some(std::sync::Arc::new(
                serde_json::json!({"id": 1, "name": "foo"}),
            )),
            ref_counts: [("hash1".to_string(), 1)].into_iter().collect(),
        };

        let mut rows = HashMap::new();
        rows.insert(id_str, (id, update));

        let existing = HashMap::new();
        let patches = updater.received(&rows, &existing);

        // Should produce a put row patch
        assert_eq!(patches.len(), 1);
        match &patches[0].patch {
            Patch::Row(RowPatch::Put { id, .. }) => {
                assert_eq!(id.schema, "s");
            }
            _ => panic!("expected RowPatch::Put"),
        }
    }

    #[test]
    fn test_received_unref_row() {
        let cvr = make_test_cvr();
        let mut updater = make_query_driven_updater(cvr, "v2");
        updater.track_queries(&[], &[]);

        let id = RowID {
            schema: "s".to_string(),
            table: "t".to_string(),
            row_key: serde_json::json!({"id": 1}).as_object().unwrap().clone(),
        };
        let id_str = crate::row_key::row_id_string(&id);

        // Existing row in the cache
        let mut existing = HashMap::new();
        existing.insert(
            id_str.clone(),
            RowRecord {
                id: id.clone(),
                row_version: "rv1".to_string(),
                patch_version: CVRVersion {
                    state_version: "v1".to_string(),
                    config_version: Some(1),
                },
                ref_counts: Some([("hash1".to_string(), 1)].into_iter().collect()),
            },
        );

        // Receive an unref (refCounts go to 0)
        let update = RowUpdate {
            version: None,
            contents: None,
            ref_counts: [("hash1".to_string(), -1)].into_iter().collect(),
        };

        let mut rows = HashMap::new();
        rows.insert(id_str, (id, update));

        let patches = updater.received(&rows, &existing);

        // Should produce a del row patch
        assert_eq!(patches.len(), 1);
        match &patches[0].patch {
            Patch::Row(RowPatch::Del { id: _ }) => {}
            _ => panic!("expected RowPatch::Del"),
        }
    }

    /// Cross-batch parity regression (BEHAVIORAL-SWEEP-FINDINGS.md, `received`):
    /// `received_rows` accumulates across batches within one pass. TS keys the
    /// merge on entry PRESENCE (`previouslyReceived !== undefined`), so a row that
    /// collapsed to `null` in an earlier batch re-merges as
    /// `mergeRefCounts(null, refCounts)` — the RAW received counts, dropping the
    /// stale `existing` refs. The old Rust flattened present-null → None and
    /// re-applied `existing.refCounts`, resurrecting a retracted ref (`qA`) in the
    /// persisted row. This asserts the re-referenced row carries ONLY the freshly
    /// received `qB`.
    #[test]
    fn test_received_null_then_reref_drops_stale_existing_refs() {
        let cvr = make_test_cvr();
        let mut updater = make_query_driven_updater(cvr, "v2");
        updater.track_queries(&[], &[]); // empty removed/executed filter

        let id = RowID {
            schema: "s".to_string(),
            table: "t".to_string(),
            row_key: serde_json::json!({"id": 1}).as_object().unwrap().clone(),
        };
        let id_str = crate::row_key::row_id_string(&id);

        // Existing row referenced only by qA.
        let mut existing = HashMap::new();
        existing.insert(
            id_str.clone(),
            RowRecord {
                id: id.clone(),
                row_version: "rv1".to_string(),
                patch_version: CVRVersion {
                    state_version: "v1".to_string(),
                    config_version: Some(1),
                },
                ref_counts: Some([("qA".to_string(), 1)].into_iter().collect()),
            },
        );

        // Batch 1: qA retracted → merged collapses to null → received_rows[R] = null.
        let mut rows1 = HashMap::new();
        rows1.insert(
            id_str.clone(),
            (
                id.clone(),
                RowUpdate {
                    version: None,
                    contents: None,
                    ref_counts: [("qA".to_string(), -1)].into_iter().collect(),
                },
            ),
        );
        updater.received(&rows1, &existing);
        assert!(
            updater
                .received_rows
                .get(&id_str)
                .is_some_and(|v| v.is_none()),
            "batch 1 should leave a present-but-null received_rows entry"
        );

        // Batch 2: same row re-referenced by a DIFFERENT query qB.
        let mut rows2 = HashMap::new();
        rows2.insert(
            id_str.clone(),
            (
                id.clone(),
                RowUpdate {
                    version: Some("rv2".to_string()),
                    contents: Some(std::sync::Arc::new(serde_json::json!({"id": 1}))),
                    ref_counts: [("qB".to_string(), 1)].into_iter().collect(),
                },
            ),
        );
        updater.received(&rows2, &existing);

        // The last PutRowRecord for R must carry ONLY qB — the retracted qA must
        // NOT be resurrected from `existing` (TS: mergeRefCounts(null, {qB:1})).
        let last = updater
            .base
            .store_ops
            .iter()
            .rev()
            .find_map(|op| match op {
                StoreOp::PutRowRecord(r) if r.id == id => Some(r.clone()),
                _ => None,
            })
            .expect("expected a PutRowRecord for the row");
        let rc = last.ref_counts.expect("row should be referenced by qB");
        let keys: std::collections::BTreeSet<&String> = rc.keys().collect();
        assert_eq!(
            keys,
            [&"qB".to_string()]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            "re-referenced row must carry only qB, not the retracted qA (null-vs-absent parity)"
        );
    }

    /// Regression for the `patchVersion` parity fix: when a row's refCounts
    /// collapse to null (`merged == None`), TS's `existing.rowVersion ===
    /// newRowVersion` compares against `undefined` and so ALWAYS bumps
    /// (`#assertNewVersion`). The old Rust used `new_row_version.unwrap_or("")`,
    /// which — for an existing row whose `row_version` is the empty string —
    /// wrongly matched and KEPT the stale `patch_version` (a client-visible
    /// stale cookie on the Del). This asserts the Del's `to_version` is the
    /// updater's bumped version, not the existing row's old one.
    #[test]
    fn test_unref_empty_row_version_bumps_patch_version() {
        let cvr = make_test_cvr(); // stateVersion "v1"
        let mut updater = make_query_driven_updater(cvr, "v2");
        updater.track_queries(&[], &[]);

        let id = RowID {
            schema: "s".to_string(),
            table: "t".to_string(),
            row_key: serde_json::json!({"id": 1}).as_object().unwrap().clone(),
        };
        let id_str = crate::row_key::row_id_string(&id);

        let mut existing = HashMap::new();
        existing.insert(
            id_str.clone(),
            RowRecord {
                id: id.clone(),
                row_version: String::new(), // the latent-bug trigger
                patch_version: CVRVersion {
                    state_version: "v1".to_string(),
                    config_version: Some(1),
                },
                ref_counts: Some([("hash1".to_string(), 1)].into_iter().collect()),
            },
        );

        // Retract the only ref → merged collapses to None.
        let update = RowUpdate {
            version: None,
            contents: None,
            ref_counts: [("hash1".to_string(), -1)].into_iter().collect(),
        };
        let mut rows = HashMap::new();
        rows.insert(id_str, (id, update));

        let patches = updater.received(&rows, &existing);
        assert_eq!(patches.len(), 1);
        match &patches[0].patch {
            Patch::Row(RowPatch::Del { .. }) => {}
            _ => panic!("expected RowPatch::Del"),
        }
        // Must be the BUMPED version ("v2"), not the stale existing patch_version ("v1").
        assert_eq!(
            patches[0].to_version.state_version, "v2",
            "Del must carry a bumped to_version, not the stale existing patch_version"
        );
    }

    #[test]
    fn test_delete_unreferenced_rows() {
        let mut cvr = make_test_cvr();

        // Track a removed query so removedOrExecutedQueryIDs is non-empty
        let query = QueryRecord::Client(ClientQueryRecord {
            base: BaseQueryRecord {
                id: "hash1".to_string(),
                transformation_hash: Some("th1".to_string()),
                transformation_version: None,
                row_set_signature: None,
            },
            ast: serde_json::json!({"schema": "s", "table": "t"}),
            client_state: BTreeMap::new(),
            patch_version: Some(CVRVersion {
                state_version: "v1".to_string(),
                config_version: None,
            }),
        });
        cvr.queries.insert("hash1".to_string(), query);

        let mut updater = make_query_driven_updater(cvr, "v2");
        updater.track_queries(&[], &["hash1"]);

        // Existing row referenced by the removed query
        let id = RowID {
            schema: "s".to_string(),
            table: "t".to_string(),
            row_key: serde_json::json!({"id": 1}).as_object().unwrap().clone(),
        };
        let existing = vec![RowRecord {
            id: id.clone(),
            row_version: "rv1".to_string(),
            patch_version: CVRVersion {
                state_version: "v1".to_string(),
                config_version: Some(1),
            },
            ref_counts: Some([("hash1".to_string(), 1)].into_iter().collect()),
        }];

        let patches = updater.delete_unreferenced_rows(&existing);

        // Should produce a del row patch (hash1 was removed)
        assert_eq!(patches.len(), 1);
        match &patches[0].patch {
            Patch::Row(RowPatch::Del { id: _ }) => {}
            _ => panic!("expected RowPatch::Del"),
        }
    }

    #[test]
    fn test_flush_with_signature_provider() {
        let mut cvr = make_test_cvr();
        let query = QueryRecord::Client(ClientQueryRecord {
            base: BaseQueryRecord {
                id: "hash1".to_string(),
                transformation_hash: Some("th1".to_string()),
                transformation_version: None,
                row_set_signature: None,
            },
            ast: serde_json::json!({"schema": "s", "table": "t"}),
            client_state: BTreeMap::new(),
            patch_version: Some(CVRVersion {
                state_version: "v1".to_string(),
                config_version: None,
            }),
        });
        cvr.queries.insert("hash1".to_string(), query);

        let provider: Box<RowSetSignatureProvider> = Box::new(|_id: &str| Some(12345u64));

        let mut updater =
            CVRQueryDrivenUpdater::new(cvr, "v2".to_string(), "r1".to_string(), Some(provider));

        let (_cvr, _stats) = updater.flush(0, 0, 0);

        // Signature should be updated
        let query = updater.base.cvr.queries.get("hash1").unwrap();
        assert!(query.base().row_set_signature.is_some());

        // Should have UpdateRowSetSignature store op
        let ops = updater.base.drain_store_ops();
        assert!(
            ops.iter()
                .any(|op| matches!(op, StoreOp::UpdateRowSetSignature { .. }))
        );
    }
}
