//! Updater tests for `cvr.rs`.
//!
//! Kept out of line so the production file stays reviewable. Declared with
//! `#[path]` from `cvr.rs` under `#[cfg(test)]`, so `use super::*` sees
//! the same private items an inline `mod tests` would.

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

/// F-CVR-STORE-12: at the SAME stateVersion the constructor must NOT bump
/// (TS cvr.ts:599-601 has no else branch) — the bump comes lazily from
/// `track_queries`' `ensure_new_version`, and `received()` asserts it
/// happened when actually needed. This test previously pinned the buggy
/// eager bump (`config_version == Some(1)` straight from `new`); the
/// TS-golden fixture scenario "same-stateVersion pass ... must NOT bump"
/// pins the corrected behavior end-to-end.
#[test]
fn test_query_updater_does_not_bump_on_same_state_version() {
    let cvr = make_test_cvr();
    let mut updater = make_query_driven_updater(cvr, "v1");
    assert_eq!(updater.base.cvr.version.state_version, "v1");
    assert_eq!(
        updater.base.cvr.version.config_version, None,
        "constructor must not bump at stateVersion equality (TS has no else branch)"
    );
    // The bump arrives lazily — track_executed/track_removed call
    // _ensureNewVersion per query; `ensure_new_version` is TS's public
    // alias for the same deferred bump (cvr.ts:798-800).
    let new_version = updater.ensure_new_version();
    assert_eq!(new_version.state_version, "v1");
    assert_eq!(
        new_version.config_version,
        Some(1),
        "ensure_new_version provides the configVersion bump TS defers to"
    );
}

/// Port-parity for `updatedVersion()` (TS cvr.ts:789 — returns the LIVE
/// `_cvr.version`, not a snapshot): before `track_queries` it reflects the
/// constructor's stateVersion bump; after `track_queries` it equals the
/// version `track_queries` returned (which may carry the lazy
/// `ensure_new_version` minor bump). rust-syncer reads it for
/// `pokers_version` (view_syncer.rs), so a stale snapshot here would poke
/// clients at the wrong cookie.
#[test]
fn test_updated_version_tracks_live_cvr_version() {
    let mut cvr = make_test_cvr();
    cvr.queries.insert(
        "hash1".to_string(),
        QueryRecord::Client(ClientQueryRecord {
            base: BaseQueryRecord {
                id: "hash1".to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
            },
            ast: serde_json::json!({"schema": "s", "table": "t"}),
            client_state: BTreeMap::new(),
            patch_version: None,
        }),
    );

    let mut updater = make_query_driven_updater(cvr, "v2");
    let before = updater.updated_version();
    assert_eq!(before.state_version, "v2");
    assert!(before.config_version.is_none());

    let (tracked_version, _patches) = updater.track_queries(&[("hash1", "th1")], &[]);
    assert_eq!(
        updater.updated_version(),
        tracked_version,
        "updated_version must be the LIVE version track_queries advanced to"
    );
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
    let patches = updater.received(&rows, &existing).unwrap();

    // Should produce a put row patch
    assert_eq!(patches.len(), 1);
    match &patches[0].patch {
        Patch::Row(RowPatch::Put { id, .. }) => {
            assert_eq!(id.schema, "s");
        }
        _ => panic!("expected RowPatch::Put"),
    }
}

/// Regression (production CVR version-bump panic): `received` must NOT panic
/// when a changed row needs a new patchVersion but no version bump happened.
/// TS `#assertNewVersion` (cvr.ts:769) throws — recoverable, so the flush
/// aborts transactionally and the client re-hydrates; rust must return `Err`,
/// never `assert!`/panic. A panic poisons the CG's locks and wedges every
/// client on the group until a cache clear — a divergence TS-prod never
/// exhibits (0 occurrences over 7 days). Pins the TS-parity error semantics:
/// same message, recoverable, transactional (no `store_ops` buffered on the
/// failed pass, so nothing is persisted).
#[test]
fn test_received_no_bump_changed_row_returns_err_not_panic() {
    let cvr = make_test_cvr(); // stateVersion "v1"
    // SAME stateVersion → constructor must NOT bump (F-CVR-STORE-12), and no
    // executed queries means `track_queries` does not lazily bump either.
    let mut updater = make_query_driven_updater(cvr, "v1");
    updater.track_queries(&[], &[]);

    let id = RowID {
        schema: "s".to_string(),
        table: "t".to_string(),
        row_key: serde_json::json!({"id": 1}).as_object().unwrap().clone(),
    };
    let id_str = crate::row_key::row_id_string(&id);

    // Existing CVR record for the row at rowVersion "rv1".
    let mut existing = HashMap::new();
    existing.insert(
        id_str.clone(),
        RowRecord {
            id: id.clone(),
            row_version: "rv1".to_string(),
            patch_version: CVRVersion {
                state_version: "v1".to_string(),
                config_version: None,
            },
            ref_counts: Some([("hash1".to_string(), 1)].into_iter().collect()),
        },
    );

    // Receive the same row at a NEWER rowVersion "rv2" (a changed row) — this
    // needs a new patchVersion, so `received` reaches `assert_new_version`.
    let update = RowUpdate {
        version: Some("rv2".to_string()),
        contents: Some(std::sync::Arc::new(
            serde_json::json!({"id": 1, "name": "x"}),
        )),
        ref_counts: [("hash1".to_string(), 1)].into_iter().collect(),
    };
    let mut rows = HashMap::new();
    rows.insert(id_str, (id, update));

    // Must be a graceful Err (TS throw semantics), NOT a panic.
    let result = updater.received(&rows, &existing);
    assert_eq!(
        result.as_ref().err().map(String::as_str),
        Some("Expected CVR version to have been bumped above original"),
        "expected recoverable Err on a no-bump changed row (TS #assertNewVersion \
             throws); Ok/panic here is the prod wedge"
    );
    // Transactional: the failed pass buffered no persisted ops.
    assert!(
        updater.base.store_ops.is_empty(),
        "no store_ops may be buffered when the version-bump invariant fails"
    );
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

    let patches = updater.received(&rows, &existing).unwrap();

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
    updater.received(&rows1, &existing).unwrap();
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
    updater.received(&rows2, &existing).unwrap();

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

    let patches = updater.received(&rows, &existing).unwrap();
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

    let patches = updater.delete_unreferenced_rows(&existing).unwrap();

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

/// Non-vacuous guard for the row-set-signature PERSIST branch in flush (TS
/// `cvr.ts` flush, view-syncer/cvr.ts:808-825 — persist only, no drift
/// count; the `row-set-signature-drifts` counter lives in
/// `hydrate_unchanged_queries`). A CHANGED provider signature persists the
/// new value + emits the store op; an UNCHANGED one is a no-op (cvr.rs `if
/// stored == Some(sig) { continue }`). Reverting either branch fails an
/// assertion.
#[test]
fn test_flush_records_signature_drift_only_when_changed() {
    let prior = crate::row_set_signature::format_signature(11111u64);

    // ── Drift: stored signature (11111) differs from the provider (22222).
    {
        let mut cvr = make_test_cvr();
        cvr.queries.insert(
            "q1".to_string(),
            QueryRecord::Client(ClientQueryRecord {
                base: BaseQueryRecord {
                    id: "q1".to_string(),
                    transformation_hash: Some("th1".to_string()),
                    transformation_version: None,
                    row_set_signature: Some(prior.clone()),
                },
                ast: serde_json::json!({"schema": "s", "table": "t"}),
                client_state: BTreeMap::new(),
                patch_version: Some(CVRVersion {
                    state_version: "v1".to_string(),
                    config_version: None,
                }),
            }),
        );
        let provider: Box<RowSetSignatureProvider> = Box::new(|_id: &str| Some(22222u64));
        let mut updater =
            CVRQueryDrivenUpdater::new(cvr, "v2".to_string(), "r1".to_string(), Some(provider));
        updater.flush(0, 0, 0);
        // The drifted signature is persisted to the changed value.
        let q = updater.base.cvr.queries.get("q1").unwrap();
        assert_eq!(
            q.base().row_set_signature.as_deref(),
            Some(crate::row_set_signature::format_signature(22222u64).as_str()),
            "a changed signature (drift) must be persisted"
        );
        let ops = updater.base.drain_store_ops();
        assert!(
            ops.iter()
                .any(|op| matches!(op, StoreOp::UpdateRowSetSignature { .. })),
            "drift must emit an UpdateRowSetSignature store op"
        );
    }

    // ── No drift: stored signature equals the provider (11111) → no-op.
    {
        let mut cvr = make_test_cvr();
        cvr.queries.insert(
            "q1".to_string(),
            QueryRecord::Client(ClientQueryRecord {
                base: BaseQueryRecord {
                    id: "q1".to_string(),
                    transformation_hash: Some("th1".to_string()),
                    transformation_version: None,
                    row_set_signature: Some(prior.clone()),
                },
                ast: serde_json::json!({"schema": "s", "table": "t"}),
                client_state: BTreeMap::new(),
                patch_version: Some(CVRVersion {
                    state_version: "v1".to_string(),
                    config_version: None,
                }),
            }),
        );
        let provider: Box<RowSetSignatureProvider> = Box::new(|_id: &str| Some(11111u64));
        let mut updater =
            CVRQueryDrivenUpdater::new(cvr, "v2".to_string(), "r1".to_string(), Some(provider));
        updater.flush(0, 0, 0);
        let ops = updater.base.drain_store_ops();
        assert!(
            !ops.iter()
                .any(|op| matches!(op, StoreOp::UpdateRowSetSignature { .. })),
            "an unchanged signature must NOT emit an UpdateRowSetSignature op"
        );
    }
}
