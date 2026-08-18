//! Port of `CVRUpdater`, `CVRConfigDrivenUpdater`, and `CVRQueryDrivenUpdater`
//! from `packages/zero-cache/src/services/view-syncer/cvr.ts`.
//!
//! ## Design
//!
//! The updaters manage a mutable working copy of the CVR (`cvr`) and collect
//! `StoreOp`s in a buffer. After each public method call, the caller can drain
//! the buffer via `drain_store_ops()` and replay the operations against the
//! real CVRStore (TS side). This mirrors the TS pattern where the updater calls
//! store methods inline as side effects.
//!
//! For `received()` and `deleteUnreferencedRows()`, which need the current row
//! records, the caller passes them in as a parameter (from the RowRecordCache).

use std::cmp::Ordering;
#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};

use crate::cvr::{
    assert_not_internal, get_mutation_results_query, merge_ref_counts, new_query_record,
};
use crate::row_key::RowID;
use crate::ttl::{DEFAULT_TTL_MS, TTL, clamp_ttl, compare_ttl};
use crate::types::*;
use crate::version::{CVRVersion, cmp_cvr, cmp_versions, max_version, one_after};

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
                ast: serde_json::json!({
                    "schema": "",
                    "table": format!("{}.clients", upstream_schema(&self.shard)),
                    "where": {
                        "type": "and",
                        "conditions": [{
                            "type": "simple",
                            "left": {"type": "column", "name": "clientGroupID"},
                            "op": "=",
                            "right": {"type": "literal", "value": self.base.cvr.id}
                        }]
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

        for id in &needed {
            let q = queries.iter().find(|q| &q.hash == id).unwrap();
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
                    // Inactivate: set inactivatedAt.
                    if let Some(cs) = query.client_state_mut() {
                        if let Some(state) = cs.get(client_id) {
                            assert!(
                                state.inactivated_at.is_none(),
                                "Query {} is already inactivated",
                                id
                            );
                            ttl = clamp_ttl(TTL::Ms(state.ttl));
                        }
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

            self.base.cvr.queries.insert(id.clone(), query.clone());
            self.base.store_ops.push(StoreOp::PutQuery(query.clone()));
            self.base.store_ops.push(StoreOp::PutDesiredQuery {
                version: new_version.clone(),
                query_id: id.clone(),
                client_id: client_id.to_string(),
                deleted: inactivated_at.is_none(),
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
        if crate::trace::enabled() {
            crate::trace::recv(
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

            // Merge refCounts.
            let merged = match &previously_received {
                Some(prev) => merge_ref_counts(Some(prev), Some(ref_counts), None),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version::CVRVersion;

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
            contents: Some(serde_json::json!({"id": 1, "name": "foo"})),
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
