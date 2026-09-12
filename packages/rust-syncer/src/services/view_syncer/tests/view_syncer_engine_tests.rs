//! Engine-level tests for `view_syncer.rs`.
//!
//! Kept out of line so the production file stays reviewable. Declared with
//! `#[path]` from `view_syncer.rs` under `#[cfg(test)]`, so `use super::*` sees
//! the same private items an inline `mod tests` would.

use super::*;

/// Port guard for TS `#trackRowSetSignatures` (pipeline-driver.ts:884-899),
/// which `IvmPipelines` applies to every change `hydrate` / `advance` yield:
/// the first row of a query seeds the entry (`?? 0n` ^ unit), later rows XOR
/// in, a Remove undoes its Add, queries are independent, EDITs are skipped,
/// and `remove_query` drops the entry (:843). The driver is the ONE owner of
/// the signature (TS `#rowSetSignatures`, :263) — the syncer reads it through
/// `row_set_signature` for the flush provider and the drift candidate.
#[test]
fn track_row_set_signature_folds_like_ts_track_row_set_signatures() {
    use crate::services::view_syncer::pipeline_driver::IvmPipelines;
    use rust_ivm::ivm::change::ChangeType;
    use rust_ivm::ivm::data::Value;
    use std::sync::Arc;

    // `Row` is `Arc<FxHashMap<Arc<str>, Value>>`; collecting into the alias
    // infers the hasher, so the test needs no `rustc_hash` dependency.
    let row_key = |id: &str| -> rust_ivm::ivm::data::Row {
        Arc::new(
            [("id".into(), Value::Str(Arc::from(id)))]
                .into_iter()
                .collect(),
        )
    };
    let change = |ct: ChangeType, qid: &str, id: &str| rust_ivm::streamer::RowChange {
        change_type: ct,
        query_id: qid.into(),
        table: "issues".into(),
        row_key: row_key(id),
        row: None,
        is_hidden: false,
    };

    let unit1 = rust_ivm::row_signature_unit("issues", &row_key("i1"));
    let unit2 = rust_ivm::row_signature_unit("issues", &row_key("i2"));
    assert_ne!(unit1, unit2, "the fixture rows must hash differently");

    let mut driver = IvmPipelines::new();
    assert_eq!(
        driver.row_set_signature("q1"),
        None,
        "no pipeline → undefined"
    );

    // First row of a query: the absent entry is seeded, TS's `?? 0n` ^ unit.
    driver.track_row_set_signature(&change(ChangeType::Add, "q1", "i1"));
    assert_eq!(driver.row_set_signature("q1"), Some(unit1));

    // Second row of the SAME query takes the present-key arm: XOR in, do
    // not overwrite.
    driver.track_row_set_signature(&change(ChangeType::Add, "q1", "i2"));
    assert_eq!(
        driver.row_set_signature("q1"),
        Some(unit1 ^ unit2),
        "a second row must XOR into the running signature, not replace it"
    );

    // A Remove of a previously-added row undoes it (XOR is its own inverse).
    driver.track_row_set_signature(&change(ChangeType::Remove, "q1", "i1"));
    assert_eq!(
        driver.row_set_signature("q1"),
        Some(unit2),
        "a Remove must undo the matching Add"
    );

    // Queries are independent accumulators.
    driver.track_row_set_signature(&change(ChangeType::Add, "q2", "i1"));
    assert_eq!(driver.row_set_signature("q2"), Some(unit1));
    assert_eq!(
        driver.row_set_signature("q1"),
        Some(unit2),
        "q2 must not disturb q1"
    );

    // TS skips EDIT entirely (`change.type !== ChangeType.EDIT`).
    driver.track_row_set_signature(&change(ChangeType::Edit, "q1", "i2"));
    assert_eq!(
        driver.row_set_signature("q1"),
        Some(unit2),
        "an Edit must not change the row-set signature"
    );

    // TS `removeQuery` → `this.#rowSetSignatures.delete(queryID)` (:843).
    driver.remove_query("q1", "remove-query");
    assert_eq!(
        driver.row_set_signature("q1"),
        None,
        "removing the query drops its signature"
    );
    assert_eq!(driver.row_set_signature("q2"), Some(unit1));
}
// The dissolved `SyncEngine` (1970feeb7): tests keep the old name.
use super::ViewSyncerService as SyncEngine;
// Auth-maintenance harness helpers live in the sibling `tests` module.
use super::tests::{pinned_params, revalidate_state, validate_test_connection};
use crate::services::view_syncer::pipeline_driver::{IvmColumnSchema, IvmTableSpec};
use crate::ws_sink::{DirectWebSocketSink, WsCommand};
use rust_cvr::cvr::CVR;
use rust_cvr::schema::types::CVRVersion;
use rust_cvr::schema::types::{BaseQueryRecord, ClientQueryRecord, QueryRecord};
use rust_cvr::shards::ShardID;
use std::collections::BTreeMap;

/// Non-vacuous port guard for the TS `#addAndRemoveQueries` force-bump +
/// reason (view-syncer.ts:2182-2214): a same-transformation-hash rehydrate
/// with no other bump trigger MUST force a `configVersion` bump (the mechanism
/// preventing the no-bump `#assertNewVersion` wedge), and the reason label is
/// keyed off `driftedQueryIDs` — `row-set-signature-drift` when the re-added
/// query drifted, `missing-pipeline` when merely reaped, `mixed` for both.
/// Reverting the guard to `None` makes the first assertion fail; the negative
/// branches pin every `trackQueriesWillBumpVersion` term.
#[test]
fn same_hash_rehydration_forces_bump_matches_ts_guard() {
    use std::collections::HashSet;
    let insert_q = |cvr: &mut CVR, id: &str, hash: &str| {
        cvr.queries.insert(
            id.to_string(),
            QueryRecord::Client(ClientQueryRecord {
                base: BaseQueryRecord {
                    id: id.to_string(),
                    transformation_hash: Some(hash.to_string()),
                    transformation_version: None,
                    row_set_signature: None,
                },
                ast: serde_json::json!({"table": "users"}),
                client_state: BTreeMap::new(),
                patch_version: None,
            }),
        );
    };
    // CVR already has q1 at hash "H", state version "05".
    let mut cvr = empty_cvr("cg1", "01");
    cvr.version = CVRVersion {
        state_version: "05".to_string(),
        config_version: None,
    };
    insert_q(&mut cvr, "q1", "H");
    let no_drift: HashSet<String> = HashSet::new();

    // Same hash, same stateVersion, no removals, no hash change, NOT drifted
    // → force bump with reason `missing-pipeline`.
    assert_eq!(
        same_hash_rehydration_bump_reason(
            &cvr,
            &[("q1".to_string(), "H".to_string())],
            &[],
            "05",
            &no_drift
        ),
        Some("missing-pipeline"),
        "same-hash rehydrate with no other bump trigger MUST force a bump"
    );
    // Same as above but q1 IS in the drifted set → reason `row-set-signature-drift`.
    let drifted: HashSet<String> = ["q1".to_string()].into_iter().collect();
    assert_eq!(
        same_hash_rehydration_bump_reason(
            &cvr,
            &[("q1".to_string(), "H".to_string())],
            &[],
            "05",
            &drifted
        ),
        Some("row-set-signature-drift"),
        "a drifted same-hash query bumps with the drift reason"
    );
    // Two same-hash queries, one drifted one not → reason `mixed`.
    insert_q(&mut cvr, "q2", "H");
    assert_eq!(
        same_hash_rehydration_bump_reason(
            &cvr,
            &[
                ("q1".to_string(), "H".to_string()),
                ("q2".to_string(), "H".to_string())
            ],
            &[],
            "05",
            &drifted
        ),
        Some("mixed"),
        "a mix of drifted + reaped same-hash queries → mixed reason"
    );
    // Changed transformation hash → track_queries bumps → no force.
    assert_eq!(
        same_hash_rehydration_bump_reason(
            &cvr,
            &[("q1".to_string(), "H2".to_string())],
            &[],
            "05",
            &no_drift
        ),
        None,
        "a changed transformation hash bumps via track_queries; no force"
    );
    // Advanced stateVersion → track_queries bumps → no force.
    assert_eq!(
        same_hash_rehydration_bump_reason(
            &cvr,
            &[("q1".to_string(), "H".to_string())],
            &[],
            "06",
            &no_drift
        ),
        None,
        "an advanced stateVersion bumps via track_queries; no force"
    );
    // A removal present → track_queries bumps → no force.
    assert_eq!(
        same_hash_rehydration_bump_reason(
            &cvr,
            &[("q1".to_string(), "H".to_string())],
            &["qZ".to_string()],
            "05",
            &no_drift
        ),
        None,
        "a removal bumps via track_queries; no force"
    );
    // No already-gotten same-hash query at all → nothing to force.
    assert_eq!(
        same_hash_rehydration_bump_reason(
            &cvr,
            &[("qX".to_string(), "H".to_string())],
            &[],
            "05",
            &no_drift
        ),
        None,
        "no already-gotten same-hash query → no force"
    );
}

/// `value_to_serde_json` REAL→JSON semantics (TS
/// `JSON.stringify` of a JS Number): an integral, in-i64-range REAL
/// serializes as an INTEGER token (JS `2` not `2.0`), a fractional REAL
/// keeps its fraction, and the non-finite fallbacks route through
/// `sqlite_real_to_json`'s sentinel object (JSON has no NaN/Infinity).
#[test]
fn real_to_json_matches_js_number_semantics() {
    use rust_ivm::ivm::data::Value as IvmValue;
    // Integral float → integer token, exactly as JS stringifies `2.0`.
    assert_eq!(
        serde_json::to_string(&value_to_serde_json(&IvmValue::F64(2.0))).unwrap(),
        "2"
    );
    assert_eq!(
        serde_json::to_string(&value_to_serde_json(&IvmValue::F64(-0.0))).unwrap(),
        "0"
    );
    // JS max safe integer round-trips as an integer.
    assert_eq!(
        serde_json::to_string(&value_to_serde_json(&IvmValue::F64(9007199254740991.0))).unwrap(),
        "9007199254740991"
    );
    // Fractional stays fractional.
    assert_eq!(
        serde_json::to_string(&value_to_serde_json(&IvmValue::F64(1.5))).unwrap(),
        "1.5"
    );
}

/// `sqlite_real_to_json` non-finite fallback: JSON cannot represent
/// NaN/±Infinity, so the value is wrapped in the `__rustIvmSqliteReal`
/// sentinel (Rust-only encoding for a value TS could never emit through
/// JSON.stringify — flagged, not silently nulled).
#[test]
fn sqlite_real_to_json_nonfinite_uses_sentinel() {
    assert_eq!(
        sqlite_real_to_json(f64::NAN),
        serde_json::json!({"__rustIvmSqliteReal": "NaN"})
    );
    assert_eq!(
        sqlite_real_to_json(f64::INFINITY),
        serde_json::json!({"__rustIvmSqliteReal": "Infinity"})
    );
    assert_eq!(
        sqlite_real_to_json(f64::NEG_INFINITY),
        serde_json::json!({"__rustIvmSqliteReal": "-Infinity"})
    );
    // A finite value passes through as a plain JSON number.
    assert_eq!(sqlite_real_to_json(2.5), serde_json::json!(2.5));
}

/// A censused type must return its live-object counter to baseline once it
/// drops — otherwise the census leaks and defeats the leak hunt. `SyncEngine`
/// carries a `live_count::Guard` on `SYNC_ENGINE`; construct one, assert the
/// counter went up, drop it, assert it came back down.
#[test]
fn sync_engine_census_returns_to_baseline_after_drop() {
    use crate::live_count::SYNC_ENGINE;
    use std::sync::atomic::Ordering;
    // The census counter is process-global and the harness runs tests on
    // parallel threads, so a sibling test constructing/dropping its own
    // SyncEngine mid-assertion makes an exact-count check flaky (it aborted
    // a release run). Retry a few times; a real Guard leak fails EVERY
    // attempt (the counter never returns to its snapshot), while transient
    // cross-test interference passes on a quiet retry.
    let mut last: Option<(i64, i64, i64)> = None;
    for _ in 0..8 {
        let base = SYNC_ENGINE.load(Ordering::Relaxed);
        let held = {
            let _engine = SyncEngine::new(IvmPipelines::new());
            SYNC_ENGINE.load(Ordering::Relaxed)
        };
        let after = SYNC_ENGINE.load(Ordering::Relaxed);
        if held == base + 1 && after == base {
            return;
        }
        last = Some((base, held, after));
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    panic!("SyncEngine census never returned to baseline: {last:?}");
}

/// A CVR with a single client query `q1` (mirrors rust-cvr's test helper),
/// so `track_queries` produces a got-query patch.
fn make_cvr() -> CVR {
    let mut cvr = CVR {
        id: "cg1".to_string(),
        version: CVRVersion {
            state_version: "00".to_string(),
            config_version: None,
        },
        last_active: 0,
        ttl_clock: 0,
        replica_version: Some("v1".to_string()),
        clients: BTreeMap::new(),
        queries: BTreeMap::new(),
        client_schema: None,
        profile_id: None,
    };
    let query = QueryRecord::Client(ClientQueryRecord {
        base: BaseQueryRecord {
            id: "q1".to_string(),
            transformation_hash: None,
            transformation_version: None,
            row_set_signature: None,
        },
        ast: serde_json::json!({"table": "users"}),
        client_state: BTreeMap::new(),
        patch_version: None,
    });
    cvr.queries.insert("q1".to_string(), query);
    cvr
}

/// `users` plus the internal `app_0.clients` / `app_0.mutations` tables the
/// engine under test (shard `app`/0) queries for every client group.
fn users_tables() -> Vec<IvmTableSpec> {
    let mut tables = crate::services::view_syncer::pipeline_driver::internal_table_specs("app_0");
    tables.push(users_spec());
    tables
}

fn users_spec() -> IvmTableSpec {
    IvmTableSpec {
        table: "users".into(),
        column_order: Vec::new(),
        columns: HashMap::from([(
            "id".to_string(),
            IvmColumnSchema {
                r#type: "string".to_string(),
                optional: false,
            },
        )]),
        primary_key: vec!["id".to_string()],
        unique_keys: None,
        all_potential_primary_keys: vec![vec!["id".to_string()]],
        min_row_version: None,
    }
}

/// Non-vacuous port guard for `hydrate_unchanged_queries` (TS
/// `#hydrateUnchangedQueries`, view-syncer.ts:1449): re-hydrate a gotten,
/// same-transformation-hash query and compare its freshly-computed row-set
/// signature to the CVR-stored one — a MISMATCH drifts (record the drift +
/// remove the pipeline so it re-executes), a MATCH does not. Reverting the
/// drift branch (never insert into `drifted`) fails the first assertion.
/// TS view-syncer.ts:1570-1577: the `hydrateUnchangedQueries:` summary
/// classifies EVERY got query — hydrated (same hash), other / custom hash
/// mismatch, custom transform error, inactivated. Mutation test: without the
/// line, or with a query in the wrong bucket, the exact-string assertion
/// fails (the pre-fix code logged nothing here).
#[tokio::test]
async fn hydrate_unchanged_queries_logs_the_ts_summary_line() {
    use rust_cvr::schema::types::{ClientState, CustomQueryRecord};
    let (buf, _guard) = capture_logs(tracing::Level::INFO);
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    let live = || {
        let mut cs = BTreeMap::new();
        cs.insert(
            "client1".to_string(),
            ClientState {
                inactivated_at: None,
                ttl: 1000,
                version: CVRVersion {
                    state_version: "00".to_string(),
                    config_version: None,
                },
            },
        );
        cs
    };
    let base = |id: &str, hash: &str| BaseQueryRecord {
        id: id.to_string(),
        transformation_hash: Some(hash.to_string()),
        transformation_version: None,
        row_set_signature: None,
    };
    let client = |id: &str, hash: &str, cs: BTreeMap<String, ClientState>| {
        QueryRecord::Client(ClientQueryRecord {
            base: base(id, hash),
            ast: serde_json::json!({"table": "users"}),
            client_state: cs,
            patch_version: None,
        })
    };
    let custom = |id: &str, hash: &str| {
        QueryRecord::Custom(CustomQueryRecord {
            base: base(id, hash),
            name: id.to_string(),
            args: vec![],
            client_state: live(),
            patch_version: None,
        })
    };
    let mut cvr = make_cvr();
    cvr.queries.clear();
    // q1: same hash → hydrated.
    cvr.queries.insert("q1".into(), client("q1", "H1", live()));
    // q2: hash changed → other hash mismatch.
    cvr.queries.insert("q2".into(), client("q2", "OLD", live()));
    // q3: every client state inactivated → inactivated.
    let mut gone = live();
    gone.get_mut("client1").unwrap().inactivated_at = Some(5);
    cvr.queries.insert("q3".into(), client("q3", "H3", gone));
    // q4: custom, errored in this pass → custom transform error.
    cvr.queries.insert("q4".into(), custom("q4", "C4"));
    // q5: custom, transformed to a different hash → custom hash mismatch.
    cvr.queries.insert("q5".into(), custom("q5", "C5"));
    // q6: no transformationHash → not a got query at all.
    let mut never = client("q6", "X", live());
    never.base_mut().transformation_hash = None;
    cvr.queries.insert("q6".into(), never);
    let ast = serde_json::json!({"table": "users"});
    let executed = vec![
        ("q1".to_string(), ast.clone(), "H1".to_string()),
        ("q2".to_string(), ast.clone(), "NEW".to_string()),
        ("q3".to_string(), ast.clone(), "H3".to_string()),
        ("q5".to_string(), ast.clone(), "C5x".to_string()),
        ("q6".to_string(), ast.clone(), "X".to_string()),
    ];
    let drifted = engine
        .hydrate_unchanged_queries(&cvr, &executed, &["q4".to_string()], "00")
        .await
        .unwrap();
    assert!(drifted.is_empty(), "{drifted:?}");
    // TS view-syncer.ts:1638: `#hydrations.add(1)` per rehydrated query —
    // the one same-hash survivor here. Mutation test: drop the
    // `record_hydration(elapsed)` in `hydrate_unchanged_queries` → 0.
    assert_eq!(
        engine.metrics.snapshot()["hydrations"],
        1,
        "one query rehydrated → one hydration"
    );
    let logged = captured(&buf);
    assert!(
        logged.contains(
            "hydrateUnchangedQueries: 5 got queries, 1 inactivated, \
                 1 custom transform errors, 1 custom hash mismatches, \
                 1 other hash mismatches, 1 hydrated"
        ),
        "TS view-syncer.ts:1570-1577 summary, verbatim; got:\n{logged}"
    );
    assert_eq!(
        engine.pipelines.query_transformation_hash("q1"),
        Some("H1"),
        "only the same-hash query is hydrated here"
    );
    assert!(
        engine.pipelines.query_transformation_hash("q2").is_none(),
        "a changed-hash query is left to #syncQueryPipelineSet"
    );
}

#[tokio::test]
async fn hydrate_unchanged_queries_detects_drift() {
    // Build a fresh engine over an EMPTY users source (so the re-hydrated
    // row-set signature is 0), with q1 gotten at hash "H", the given stored
    // signature, and one LIVE (non-inactivated) client.
    let build = |stored_sig: u64| {
        let mut pipelines = IvmPipelines::new();
        pipelines.init(users_tables(), None, "zero").unwrap();
        let engine = SyncEngine::new(pipelines);
        let mut cvr = make_cvr();
        let q = cvr.queries.get_mut("q1").unwrap();
        q.base_mut().transformation_hash = Some("H".to_string());
        q.base_mut().row_set_signature =
            Some(rust_cvr::row_set_signature::format_signature(stored_sig));
        if let Some(cs) = q.client_state_mut() {
            cs.insert(
                "client1".to_string(),
                rust_cvr::schema::types::ClientState {
                    inactivated_at: None,
                    ttl: 1000,
                    version: CVRVersion {
                        state_version: "00".to_string(),
                        config_version: None,
                    },
                },
            );
        }
        (engine, cvr)
    };
    let executed = vec![(
        "q1".to_string(),
        serde_json::json!({"table": "users"}),
        "H".to_string(),
    )];

    // Drift: stored (999) != candidate (0) → q1 drifts + pipeline removed.
    let (mut engine, cvr) = build(999);
    let drifted = engine
        .hydrate_unchanged_queries(&cvr, &executed, &[], "00")
        .await
        .unwrap();
    assert!(
        drifted.contains("q1"),
        "a mismatched stored signature must drift"
    );
    assert!(
        engine.pipelines.query_transformation_hash("q1").is_none(),
        "a drifted query's pipeline is removed for full re-execution"
    );

    // No drift: stored (0) == candidate (0) → q1 kept, not drifted.
    let (mut engine, cvr) = build(0);
    let drifted = engine
        .hydrate_unchanged_queries(&cvr, &executed, &[], "00")
        .await
        .unwrap();
    assert!(
        !drifted.contains("q1"),
        "a matching stored signature must NOT drift"
    );
    assert_eq!(
        engine.pipelines.query_transformation_hash("q1"),
        Some("H"),
        "a non-drifted query keeps its rebuilt pipeline"
    );
}

/// Mutation test: the
/// `quiet commit discarded a version bump` diagnostic exists to name the
/// path that closes a client with `Patches were sent but finalVersion ...
/// is not greater than baseVersion`. TS raises that ONLY on the
/// `pokeStarted` branch (client-handler.ts:327-334), so a discarded bump
/// with nothing sent cannot close anyone. Ungated, the line fired on that
/// benign case: 54 times in 3.5 min of GKE sandbox traffic, against ZERO
/// started pokes and ZERO closes (2 of 187 poke frames ever ended off their
/// opening cookie, both on a different path).
///
/// Both arms below take the SAME quiet-commit branch with the SAME discarded
/// bump; only whether a patch went out differs. Drop the `pokers
/// .any_started() &&` guard → the silent arm logs and FAILS.
#[tokio::test]
async fn quiet_commit_bump_discard_logs_only_when_patches_were_sent() {
    // `sent`: register a poke target, so the got-query patch opens its poke
    // (pokeStart) before the flush is forced to report a quiet commit.
    async fn run(sent: bool) -> (String, bool) {
        let mut pipelines = IvmPipelines::new();
        pipelines.init(users_tables(), None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);
        let mut ws_ids: Vec<String> = Vec::new();
        // Held for the whole call: dropping the receiver closes the sink, and
        // `add_patch` then fails the poker instead of starting it.
        let _rx_keepalive;
        if sent {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
            _rx_keepalive = rx;
            let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
            engine.register_client(
                "client1",
                "ws1",
                "cg1",
                &ShardID {
                    app_id: "app".to_string(),
                    shard_num: 0,
                },
                None,
                sink,
            );
            ws_ids.push("ws1".to_string());
        }
        // Force the store flush to report the QUIET-COMMIT outcome, so the
        // updater's bumped version is discarded and `orig` is restored.
        engine.forced_flush_outcomes.borrow_mut().push_back(false);

        let (buf, guard) = capture_logs(tracing::Level::WARN);
        let (_result, pokers) = engine
            .hydrate_and_sync(
                make_cvr(),
                "00".to_string(),
                "v1".to_string(),
                &[("q1".to_string(), "hash1".to_string())],
                &[],
                &ws_ids,
                &[("q1".to_string(), r#"{"table":"users"}"#.to_string())],
                FlushTimes {
                    last_connect_time: 0,
                    last_active: 0,
                    ttl_clock: 0,
                },
                &std::collections::HashSet::new(),
            )
            .await
            .unwrap();
        let started = pokers.any_started();
        drop(guard);
        (captured(&buf), started)
    }

    let (silent, started_none) = run(false).await;
    assert!(
        !started_none,
        "control: with no poke target nothing is sent, so TS `pokeStarted` stays false"
    );
    assert!(
        !silent.contains("quiet commit discarded a version bump"),
        "a discarded bump with NOTHING sent cannot close a client — TS raises only on \
             the pokeStarted branch — so it must not be logged; got:\n{silent}"
    );

    let (loud, started_some) = run(true).await;
    assert!(
        started_some,
        "control: the got-query patch must open the poke, or the arms are not comparable"
    );
    assert!(
        loud.contains("hydrate quiet commit discarded a version bump"),
        "a discarded bump AFTER patches went out is the dangerous shape and must be \
             logged; got:\n{loud}"
    );
    assert!(
        loud.contains("WARN"),
        "it predicts a client close, so it is a warning, not info; got:\n{loud}"
    );
}

#[tokio::test]
async fn hydrate_and_sync_emits_poke_frames() {
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();

    let mut engine = SyncEngine::new(pipelines);

    // Wire a client whose sink drains into a channel (buffer large enough
    // that blocking_send never blocks for the few poke frames).
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    engine.register_client(
        "client1",
        "ws1",
        "cg1",
        &ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        },
        None,
        sink,
    );

    let (result, pokers) = engine
        .hydrate_and_sync(
            make_cvr(),
            "00".to_string(),
            "v1".to_string(),
            &[("q1".to_string(), "hash1".to_string())],
            &[],
            &["ws1".to_string()],
            &[("q1".to_string(), r#"{"table":"users"}"#.to_string())],
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
            &std::collections::HashSet::new(),
        )
        .await
        .unwrap();

    // Store is None → no flush; the got-query patch still produces a poke.
    assert!(result.reset_reason.is_none());
    assert!(
        !result.query_patches.is_empty(),
        "expected a got-query patch"
    );

    // Reconnect-catch-up regression: the poke must still be OPEN after
    // `hydrate_and_sync` returns, so catch-up patches appended here ride the
    // SAME poke and are delivered. (Previously `end()` ran inside, advancing
    // every client's base to the new version — a separate catch-up poke then
    // NOOP-dropped every patch a reconnecting client missed while away.)
    let mut del_key = serde_json::Map::new();
    del_key.insert("id".to_string(), serde_json::json!("stale-row"));
    pokers.add_patch(&PatchToVersion {
        patch: Patch::Row(RowPatch::Del {
            id: RowID {
                schema: String::new(),
                table: "users".into(),
                row_key: del_key,
            },
        }),
        to_version: result.cvr.version.clone(),
    });
    pokers.end(result.cvr.version.clone());

    let mut frames = Vec::new();
    while let Ok(cmd) = rx.try_recv() {
        // `frame_value`, not `Send { msg }`: a poke part is serialized by
        // the writer task, so matching the variant dropped every one of
        // them and this test's `has_del` assertion saw nothing.
        if let Some(v) = cmd.frame_value() {
            frames.push(v);
        }
    }
    assert!(
        frames.len() >= 2,
        "expected at least pokeStart + pokeEnd, got {}",
        frames.len()
    );
    assert_eq!(frames.first().unwrap()[0], "pokeStart");
    assert_eq!(frames.last().unwrap()[0], "pokeEnd");
    // Exactly ONE poke (no nested pokeStart), and the late catch-up del is in it.
    let starts = frames.iter().filter(|f| f[0] == "pokeStart").count();
    assert_eq!(starts, 1, "hydrate + catch-up must share one poke");
    let has_del = frames.iter().any(|f| {
        f[0] == "pokePart"
            && f[1]["rowsPatch"]
                .as_array()
                .is_some_and(|ps| ps.iter().any(|p| p["op"] == "del"))
    });
    assert!(has_del, "late catch-up patch must be delivered in the poke");
}

/// `hydrate_and_sync` records the per-query inspector server metrics —
/// `query-materialization-server` (from the engine's hydration timing) and
/// the queryID→AST map (`add_query`), both keyed by the queryID, exactly as
/// TS `#syncQueryPipelineSet` (view-syncer.ts:2297-2298). Mutation test: after
/// hydrating q1, the delegate reports the AST and a `query-hydration-server-ms`
/// for q1; removing the recording loop makes both `get_ast_for_query` and
/// `get_metrics_json_for_query` return `None`, failing the asserts.
#[tokio::test]
async fn hydrate_and_sync_records_inspector_materialization_and_ast() {
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    engine.register_client(
        "client1",
        "ws1",
        "cg1",
        &ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        },
        None,
        sink,
    );

    let ast = r#"{"table":"users"}"#;
    engine
        .hydrate_and_sync(
            make_cvr(),
            "00".to_string(),
            "v1".to_string(),
            &[("q1".to_string(), "hash1".to_string())],
            &[],
            &["ws1".to_string()],
            &[("q1".to_string(), ast.to_string())],
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
            &std::collections::HashSet::new(),
        )
        .await
        .unwrap();

    // The AST is stored for the `queries` op fallback (`getASTForQuery`).
    assert_eq!(
        engine.inspector_delegate().borrow().get_ast_for_query("q1"),
        Some(&serde_json::json!({"table": "users"})),
        "hydrate must record the query AST in the inspector delegate"
    );
    // A per-query materialization metric was recorded (hydration ms present).
    let per_query = engine
        .inspector_delegate()
        .borrow_mut()
        .get_metrics_json_for_query("q1")
        .expect("q1 must have per-query metrics after hydrate");
    assert!(
        per_query.get("query-hydration-server-ms").is_some(),
        "the hydration ms must be recorded for q1; got {per_query}"
    );
    // The global materialization aggregate carries exactly one sample.
    let global = engine.inspector_delegate().borrow_mut().get_metrics_json();
    let mat = global["query-materialization-server"].as_array().unwrap();
    // `[compression, mean, weight]` — one centroid of weight 1.
    assert_eq!(mat.len(), 3, "one materialization sample: {mat:?}");
    assert_eq!(mat[2], serde_json::json!(1), "weight 1");
}

/// Regression for the advance-path panic: `advance_and_sync` must construct
/// the query-driven updater with the REAL post-advance version, not an empty
/// placeholder. The old code passed `String::new()`, and `new()` asserts
/// `stateVersion >= cvr.version.stateVersion` — false for any non-empty CVR
/// version (`"" >= "00"` is false in Rust) → panic on the FIRST advance after
/// hydration. `make_cvr()` has stateVersion "00", so the old code panicked
/// here; the fix advances first and uses the header version. Needs a
/// snapshotter-backed pipeline (advance is unavailable on MemorySource).
#[tokio::test]
async fn advance_and_sync_uses_header_version_not_empty() {
    use rusqlite::Connection;

    let db_path = "/tmp/rust-syncer-advance-and-sync-test.db";
    let cleanup = || {
        for suffix in ["", "-wal", "-wal2", "-shm"] {
            let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
        }
    };
    cleanup();
    {
        let conn = Connection::open(db_path).unwrap();
        let _ = conn.pragma_update(None, "journal_mode", "wal2");
        conn.execute_batch(
            r#"
                CREATE TABLE "_zero.replicationConfig" (
                    lock TEXT PRIMARY KEY DEFAULT 'singleton',
                    replicaVersion TEXT NOT NULL,
                    publications TEXT NOT NULL
                );
                CREATE TABLE "_zero.replicationState" (
                    lock TEXT PRIMARY KEY DEFAULT 'singleton',
                    stateVersion TEXT NOT NULL
                );
                CREATE TABLE "_zero.changeLog2" (
                    "stateVersion" TEXT NOT NULL,
                    "table"        TEXT NOT NULL,
                    "rowKey"       TEXT NOT NULL,
                    "op"           TEXT NOT NULL,
                    "pos"          INTEGER NOT NULL,
                    PRIMARY KEY ("stateVersion", "pos")
                );
                CREATE TABLE users (id TEXT PRIMARY KEY, "_0_version" TEXT NOT NULL);
                INSERT INTO "_zero.replicationConfig" (lock, replicaVersion, publications)
                    VALUES ('singleton', 'v1', '[]');
                INSERT INTO "_zero.replicationState" (lock, stateVersion)
                    VALUES ('singleton', 'v1');
                "#,
        )
        .unwrap();
    }

    let mut pipelines = IvmPipelines::new();
    pipelines
        .init(users_tables(), Some(db_path), "app")
        .unwrap();
    let mut engine = SyncEngine::new(pipelines);

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    engine.register_client(
        "client1",
        "ws1",
        "cg1",
        &ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        },
        None,
        sink,
    );

    // make_cvr() has stateVersion "00" and replicaVersion "v1"; advancing a
    // snapshot pinned at "v1" MUST NOT panic (it did before the fix).
    //
    // ALSO PINS that this zero-change advance never reads
    // the CVR row map. TS materialises no row map for an advance:
    // `#advancePipelines` hands `#processChanges` only the changed-row batch
    // (view-syncer.ts:2472-2505 -> `updater.received(lc, rows)`), and the
    // store reads row records solely inside `#flush` under
    // `if (this.#pendingRowRecordUpdates.size)` (cvr-store.ts:1066-1067).
    // Rust loaded it in `on_notification` before EVERY advance, so each
    // notification paid an `offload()` hop plus a store-mutex acquire even
    // when the commit touched nothing this CG queries — a cost that scales
    // with (live CGs x commit rate) and produced 1.5M no-op advance cycles
    // at ~900 CGs against ~3K on the TS arm. Drop the `collected.is_empty()`
    // guard in `advance_and_sync` and the assertion below fails with 1.
    engine.existing_rows_calls.set(0);
    engine
        .last_row_count
        .store(7, std::sync::atomic::Ordering::Relaxed);
    *crate::metrics::LAST_ADVANCE_MS.lock().unwrap() = None;
    let result = engine
        .advance_and_sync(
            make_cvr(),
            "v1".to_string(),
            &["ws1".to_string()],
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await;
    let row_map_reads = engine.existing_rows_calls.get();
    let row_count_after = engine.row_count();

    cleanup();
    let result = result.expect("advance_and_sync must not error/panic");
    // `zero.sync.advance-time` — TS records `timer.totalElapsed()` (the
    // process clock) at the end of `#advancePipelines` (view-syncer.ts:2628-
    // 2632); rust records it here, inside `advance_and_sync`, with the same
    // value the outcome carries. Mutation test: an earlier version recorded
    // this in `on_notification` on the WALL clock — this direct call left
    // the seam `None`.
    assert_eq!(
        crate::metrics::LAST_ADVANCE_MS.lock().unwrap().take(),
        Some(result.process_time_ms),
        "advance-time is the outcome's process time"
    );
    assert!(result.reset_reason.is_none(), "unexpected reset");
    assert!(
        !result.version.is_empty(),
        "advance produced an empty version — the updater was built without the header version"
    );
    assert_eq!(
        row_map_reads, 0,
        "an advance with no collected row changes must not touch the CVR row \
             map — TS never materialises one for an advance"
    );
    // TS `rowCount` is `#cvrStore.rowCount`, written only by `#flush`
    // (cvr-store.ts:1068, 1217); a zero-change advance flushes no rows, so
    // the count must be exactly what it was. 4453a0f91 assigned the (now
    // empty) lazily-read map's length here, zeroing the `zero.sync.rows`
    // gauge after every no-op advance. Mutation test: restore that
    // assignment and this reads 0.
    assert_eq!(
        row_count_after, 7,
        "a zero-change advance must leave rowCount untouched (TS reads \
             #cvrStore.rowCount, which only #flush writes)"
    );
}

/// Mutation test: the config poke must be opened AFTER the
/// store flush and ONLY when the CVR version actually advanced. TS:
///
/// ```text
/// this.#cvr = await this.#flushUpdater(lc, updater);
/// if (cmpVersions(cvr.version, this.#cvr.version) < 0) {
///   const pokers = startPoke(this.#getClients(cvr.version), newCVR.version);
///   for (const patch of patches) { await pokers.addPatch(patch); }
///   await pokers.end(newCVR.version);
/// }
/// ```
///
/// (view-syncer.ts:1138-1159). Rust opened the pokers BEFORE the flush at the
/// optimistically bumped version, admitted the config patches there, then
/// discovered the flush was a QUIET COMMIT (`flushed: false` — nothing
/// material to persist), reverted `cfg_cvr` to `orig` underneath the open
/// poke, and called `end(orig)`. That found `base == final` with patches
/// pending and CLOSED the client:
/// `Patches were sent but finalVersion ... is not greater than baseVersion`.
/// 256 of 256 such closes in a prod-replay came from this one site, against
/// ZERO on the TS arm running the same trace.
///
/// Revert `handle_config_update` to poke-before-flush and the first case
/// FAILS (the client receives `WsCommand::Fail`); the second case pins that
/// the guard does not over-suppress a poke that legitimately advanced.

#[tokio::test]
async fn config_update_no_op_flush_does_not_close_client() {
    async fn run(flush_outcome: bool) -> (usize, usize, usize) {
        let mut pipelines = IvmPipelines::new();
        pipelines.init(users_tables(), None, "zero").unwrap();
        let mut engine = SyncEngine::new(pipelines);

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
        let shard = ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        };
        // The client is AT the CVR's version ("00", `empty_cvr`), so it is a
        // `#getClients(cvr.version)` poke target AND its poke base equals the
        // version a reverted flush falls back to — the exact production
        // shape. (A `None` cookie means "no floor" in `add_patch`/`end`, so
        // it cannot express the base == final collision.)
        engine.register_client("client1", "ws1", "cg1", &shard, Some("00"), sink);

        // Force the store flush to report the quiet-commit outcome.
        engine
            .forced_flush_outcomes
            .borrow_mut()
            .push_back(flush_outcome);

        let cvr = super::empty_cvr("cg1", "v1");
        let puts = vec![DesiredQuerySpec {
            hash: "q1".to_string(),
            ast: Some(serde_json::json!({"table": "users"})),
            name: None,
            args: None,
            ttl: None,
        }];

        engine
            .handle_config_update(
                cvr,
                "client1",
                &["ws1".to_string()],
                &shard,
                puts,
                Vec::new(),
                false,
                None,
                None,
                FlushTimes {
                    last_connect_time: 0,
                    last_active: 0,
                    ttl_clock: 0,
                },
            )
            .await
            .unwrap();

        let (mut starts, mut ends, mut fails) = (0, 0, 0);
        while let Ok(cmd) = rx.try_recv() {
            match cmd {
                WsCommand::Send { msg, .. } => match msg[0].as_str() {
                    Some("pokeStart") => starts += 1,
                    Some("pokeEnd") => ends += 1,
                    _ => {}
                },
                WsCommand::Fail(_) => fails += 1,
                _ => {}
            }
        }
        (starts, ends, fails)
    }

    // Quiet commit: the version never advanced, so TS opens no poke at all
    // and the client stays connected.
    let (starts, ends, fails) = run(false).await;
    assert_eq!(
        fails, 0,
        "a no-op CVR flush must not close the client — this is the \
             finalVersion-not-greater-than-baseVersion close"
    );
    assert_eq!(
        (starts, ends),
        (0, 0),
        "TS pokes nothing when cmpVersions(cvr.version, newCVR.version) is not < 0"
    );

    // Positive control: a flush that DID persist advances the version, so the
    // guard opens the poke exactly as before.
    let (starts, ends, fails) = run(true).await;
    assert_eq!(fails, 0, "a successful flush must not close the client");
    assert!(
        starts >= 1 && ends >= 1,
        "an advancing config flush must still poke: {starts} starts, {ends} ends"
    );
}

/// Mutation test: the SECOND port of TS `#updateCVRConfig`.
/// `deleteClients` routes through `#handleConfigUpdate` → `#updateCVRConfig`
/// (view-syncer.ts:1046), so it must flush first and poke only when the
/// version advanced — exactly like `handle_config_update`. Rust had the same
/// poke-before-flush ordering here, so a quiet commit on a delete pass closed
/// the client the same way. Revert `delete_clients` to poke-before-flush and
/// this FAILS with the `finalVersion ... is not greater than baseVersion`
/// close.
#[tokio::test]
async fn delete_clients_no_op_flush_does_not_close_client() {
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };

    let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    engine.register_client(
        "client1",
        "ws1",
        "cg1",
        &shard,
        Some("00"),
        Arc::new(DirectWebSocketSink::new(tx1)),
    );
    let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    engine.register_client(
        "client2",
        "ws2",
        "cg1",
        &shard,
        Some("00"),
        Arc::new(DirectWebSocketSink::new(tx2)),
    );

    // Give client2 a desired query so its deletion produces real patches
    // (`delete_client` → `mark_desired_queries_as_inactive`). This pass
    // flushes for real, advancing the CVR past "00".
    let cvr = engine
        .handle_config_update(
            super::empty_cvr("cg1", "v1"),
            "client2",
            &["ws1".to_string(), "ws2".to_string()],
            &shard,
            vec![DesiredQuerySpec {
                hash: "q1".to_string(),
                ast: Some(serde_json::json!({"table": "users"})),
                name: None,
                args: None,
                ttl: None,
            }],
            Vec::new(),
            false,
            None,
            None,
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    while rx1.try_recv().is_ok() {}

    // Now delete client2 with a QUIET COMMIT: patches are produced, but the
    // store reports nothing material persisted, so the CVR reverts to the
    // pre-delete version that client1 is already at.
    engine.forced_flush_outcomes.borrow_mut().push_back(false);
    engine
        .delete_clients(
            cvr,
            &shard,
            "client1",
            "ws1",
            &["client2".to_string()],
            &["client2".to_string()],
            &[],
            &["ws1".to_string()],
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();

    let mut fails = 0;
    while let Ok(cmd) = rx1.try_recv() {
        if matches!(cmd, WsCommand::Fail(_)) {
            fails += 1;
        }
    }
    assert_eq!(
        fails, 0,
        "a no-op delete-clients flush must not close the client — TS pokes \
             only when cmpVersions(cvr.version, this.#cvr.version) < 0"
    );
}

#[tokio::test]
async fn config_and_hydrate_from_desired_queries_pokes_client() {
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

    // A fresh CVR + a single desired query (as an initConnection would carry).
    let cvr = super::empty_cvr("cg1", "v1");
    let puts = vec![DesiredQuerySpec {
        hash: "q1".to_string(),
        ast: Some(serde_json::json!({"table": "users"})),
        name: None,
        args: None,
        ttl: None,
    }];

    let result_cvr = engine
        .config_and_hydrate(
            cvr,
            "client1",
            &["ws1".to_string()],
            &shard,
            puts,
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();

    // The client group now tracks the desired query, and the client got
    // both a config poke and a hydrate poke.
    assert!(result_cvr.clients.contains_key("client1"));
    assert!(result_cvr.queries.contains_key("q1"));

    let mut starts = 0;
    let mut ends = 0;
    while let Ok(Some(v)) = rx.try_recv().map(|c| c.frame_value()) {
        match v[0].as_str() {
            Some("pokeStart") => starts += 1,
            Some("pokeEnd") => ends += 1,
            _ => {}
        }
    }
    assert!(
        starts >= 1 && ends >= 1,
        "expected poke frames: {starts} starts, {ends} ends"
    );
}

/// Capture the `tracing` output emitted on THIS thread while the returned
/// guard lives. `set_default` (thread-local dispatcher) rather than
/// `with_default` so an `async` test can hold it across `.await`s on the
/// current-thread runtime `#[tokio::test]` provides.
pub(super) fn capture_logs(
    level: tracing::Level,
) -> (Arc<Mutex<Vec<u8>>>, tracing::subscriber::DefaultGuard) {
    #[derive(Clone)]
    struct CapWriter(Arc<Mutex<Vec<u8>>>);
    struct CapGuard(Arc<Mutex<Vec<u8>>>);
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapWriter {
        type Writer = CapGuard;
        fn make_writer(&'a self) -> CapGuard {
            CapGuard(self.0.clone())
        }
    }
    impl std::io::Write for CapGuard {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(CapWriter(buf.clone()))
        .with_ansi(false)
        .with_max_level(level)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (buf, guard)
}

pub(super) fn captured(buf: &Arc<Mutex<Vec<u8>>>) -> String {
    String::from_utf8(buf.lock().unwrap().clone()).unwrap()
}

fn desired(hash: &str) -> DesiredQuerySpec {
    DesiredQuerySpec {
        hash: hash.to_string(),
        ast: Some(serde_json::json!({"table": "users"})),
        name: None,
        args: None,
        ttl: None,
    }
}

/// Mutation test: TS's slow-hydrate warning is PER
/// QUERY — `if (elapsed > slowHydrateThreshold) queryLC.warn?.('Slow query
/// materialization', elapsed, q.ast)` on the query's own process time, with
/// `hash`/`queryHash`/`transformationHash` contexts and the transformed AST
/// as payload (view-syncer.ts:2265-2271, 2305-2307). Rust used to warn ONCE
/// per `config_and_hydrate` pass on the whole pass's wall time (flush
/// included), so a 250 ms query inside a fast pass never got a line and a
/// fast query inside a slow pass did. With the threshold pinned below any
/// hydration time: revert to the aggregate warn → no per-query line (fails);
/// re-add the aggregate → the `config_and_hydrate took` assert fails.
#[tokio::test]
async fn slow_query_materialization_warns_per_query_with_its_ast() {
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

    set_slow_hydrate_threshold_for_test(Some(-1.0));
    let (buf, guard) = capture_logs(tracing::Level::WARN);
    let result = engine
        .config_and_hydrate(
            super::empty_cvr("cg1", "v1"),
            "client1",
            &["ws1".to_string()],
            &shard,
            vec![desired("q1"), desired("q2")],
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await;
    drop(guard);
    set_slow_hydrate_threshold_for_test(None);
    result.unwrap();
    let logged = captured(&buf);
    let lines: Vec<&str> = logged
        .lines()
        .filter(|l| l.contains("Slow query materialization"))
        .collect();
    // TS's loop covers every query in `addQueries` — the internal `lmidsQuery` /
    // `mutationResultsQuery` (cvr.ts:234-262) included — so an
    // initConnection carrying q1 + q2 yields four per-query lines.
    assert_eq!(
        lines.len(),
        4,
        "one WARN per slow query (TS view-syncer.ts:2305); got:\n{logged}"
    );
    let mut hashes: Vec<&str> = lines
        .iter()
        .map(|l| {
            let start = l.find("query_hash=").unwrap() + "query_hash=".len();
            l[start..].split(' ').next().unwrap()
        })
        .collect();
    hashes.sort_unstable();
    assert_eq!(hashes, ["lmids", "mutationResults", "q1", "q2"]);
    for line in &lines {
        let hash = {
            let start = line.find("query_hash=").unwrap() + "query_hash=".len();
            line[start..].split(' ').next().unwrap()
        };
        assert!(line.contains("WARN"), "TS `queryLC.warn`; got: {line}");
        assert!(
            line.contains(&format!("query_hash={hash}")),
            "TS `queryHash` context; got: {line}"
        );
        assert!(
            line.contains("transformation_hash="),
            "TS `transformationHash` context; got: {line}"
        );
        assert!(
            line.contains(r#"ast={""#),
            "TS attaches `q.ast` as the payload; got: {line}"
        );
        if hash == "q1" || hash == "q2" {
            assert!(
                line.contains(r#""table":"users""#),
                "the payload is the query's transformed AST; got: {line}"
            );
        }
        assert!(
            !line.contains("query_name="),
            "TS adds `queryName` only when defined; got: {line}"
        );
    }
    assert!(
        !logged.contains("config_and_hydrate took"),
        "no rust-only whole-pass warn (TS has none); got:\n{logged}"
    );
}

/// Mutation test: the threshold is TS's
/// `log.slowHydrateThreshold` — env `ZERO_LOG_SLOW_HYDRATE_THRESHOLD`,
/// default 100 (otel/src/log-options.ts:24-29). Rust read a rust-only name
/// with a 10x default (1000), so a bare rust binary warned on almost nothing
/// while the TS process beside it warned at 100 ms. Revert the default → the
/// first assert fails; drop the TS name → the second fails.
#[test]
fn slow_hydrate_threshold_resolves_the_ts_env_name_and_default() {
    let env = |vars: &'static [(&'static str, &'static str)]| {
        move |k: &str| {
            vars.iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| v.to_string())
        }
    };
    assert_eq!(slow_hydrate_threshold_from_env(env(&[])), 100.0);
    assert_eq!(
        slow_hydrate_threshold_from_env(env(&[("ZERO_LOG_SLOW_HYDRATE_THRESHOLD", "50")])),
        50.0
    );
    // The earlier bridge name still applies (deprecated alias)…
    assert_eq!(
        slow_hydrate_threshold_from_env(env(&[("ZERO_SLOW_HYDRATE_THRESHOLD_MS", "250")])),
        250.0
    );
    // …but the TS name wins when both are present.
    assert_eq!(
        slow_hydrate_threshold_from_env(env(&[
            ("ZERO_SLOW_HYDRATE_THRESHOLD_MS", "250"),
            ("ZERO_LOG_SLOW_HYDRATE_THRESHOLD", "50"),
        ])),
        50.0
    );
    assert_eq!(
        slow_hydrate_threshold_from_env(env(&[("ZERO_LOG_SLOW_HYDRATE_THRESHOLD", "abc")])),
        100.0
    );
}

/// Mutation test: the TTL expiry tick syncs the pipeline set
/// ONLY when pipelines are synced. TS:
/// ```text
/// // #syncQueryPipelineSet() will remove the expired queries.
/// if (this.#pipelinesSynced) {
///   await this.#syncQueryPipelineSet(lc, cvr, 'missing', undefined);
/// }
/// ```
/// (view-syncer.ts:642-645), with the eviction timer rescheduled either way.
/// Rust ran `remove_expired_queries` unconditionally. Revert the
/// `if !self.pipelines_synced` early return in `on_expiry_tick` → the
/// "unsynced tick must not remove" assert fails.
#[tokio::test]
async fn expiry_tick_removes_nothing_until_pipelines_are_synced() {
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    engine.replica_version = "v1".to_string(); // CVRs below are at replica v1

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);
    let ws = vec!["ws1".to_string()];

    // Subscribe q1 with a 1000ms TTL, then unsubscribe it at ttl_clock=0 so
    // it is INACTIVE (still hydrated) and expires at ttl_clock >= 1000.
    let cvr = engine
        .config_and_hydrate(
            super::empty_cvr("cg1", "v1"),
            "client1",
            &ws,
            &shard,
            vec![DesiredQuerySpec {
                hash: "q1".to_string(),
                ast: Some(serde_json::json!({"table": "users"})),
                name: None,
                args: None,
                ttl: Some(1000),
            }],
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    let cvr = engine
        .config_and_hydrate(
            cvr,
            "client1",
            &ws,
            &shard,
            Vec::new(),
            vec!["q1".to_string()],
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    assert!(engine.pipelines().has_query("q1"), "inactive query lingers");

    // Park the TTL clock past the query's deadline so the tick would expire
    // it if it ran (`get_ttl_clock` advances by `now - ttl_clock_base`).
    engine.cvr = Some(cvr);
    engine.ttl_clock = 2000;
    engine.ttl_clock_base = now_ms();

    // Pipelines NOT synced → TS skips the sync entirely; the query stays.
    engine.pipelines_synced = false;
    engine.on_expiry_tick().await;
    assert!(
        engine.pipelines().has_query("q1"),
        "an unsynced expiry tick must not remove queries (TS view-syncer.ts:643)"
    );
    assert!(
        engine.cvr.is_some(),
        "the early return must put the CVR back, not drop it"
    );
    assert!(
        engine.expired_queries_timer.is_some(),
        "TS reschedules the eviction timer even when it skips the sync"
    );

    // Pipelines synced → the same tick expires it.
    engine.ttl_clock = 2000;
    engine.ttl_clock_base = now_ms();
    engine.pipelines_synced = true;
    engine.on_expiry_tick().await;
    assert!(
        !engine.pipelines().has_query("q1"),
        "a synced expiry tick must remove the expired query"
    );
}

/// Port parity for TS `deleteClients` → `#handleConfigUpdate(…, 'missing')`
/// → `#updateCVRConfig` (view-syncer.ts:1032-1049, 1160-1167): once the
/// config change is flushed and poked, the pipeline set is RE-SYNCED from
/// the CVR. A query only the deleted client desired with `ttl: 0` is
/// expired by that sync (`removeQueriesQueryIds`): its pipeline goes, its
/// rows leave the CVR and the remaining clients get the del patches NOW —
/// not on a later expiry tick. Rust's dedicated `delete_clients` flushed and
/// poked but never re-synced, so the orphaned pipeline kept running.
#[tokio::test]
async fn delete_clients_resyncs_the_pipeline_set_like_update_cvr_config() {
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    // The test CVRs are at replica "v1"; the sync-set updater asserts the
    // engine's replica version is not older (TS `#pipelines.replicaVersion`).
    engine.replica_version = "v1".to_string();
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink1: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx1));
    let sink2: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx2));
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink1);
    engine.register_client("client2", "ws2", "cg1", &shard, None, sink2);
    let both = vec!["ws1".to_string(), "ws2".to_string()];
    let desire = |hash: &str, ttl: Option<i64>| DesiredQuerySpec {
        hash: hash.to_string(),
        ast: Some(serde_json::json!({"table": "users"})),
        name: None,
        args: None,
        ttl,
    };
    let cvr = engine
        .config_and_hydrate(
            super::empty_cvr("cg1", "v1"),
            "client1",
            &both,
            &shard,
            vec![desire("q1", Some(0))],
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    let cvr = engine
        .config_and_hydrate(
            cvr,
            "client2",
            &both,
            &shard,
            vec![desire("q2", None)],
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    assert!(engine.pipelines().has_query("q1") && engine.pipelines().has_query("q2"));

    // client2 deletes client1: q1 (ttl 0, desired only by client1) expires.
    let cvr = engine
        .delete_clients(
            cvr,
            &shard,
            "client2",
            "ws2",
            &["client1".to_string()],
            &["client1".to_string()],
            &[],
            &["ws2".to_string()],
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    assert!(!cvr.clients.contains_key("client1"));
    assert!(
        !engine.pipelines().has_query("q1"),
        "TS re-syncs the pipeline set after deleteClients (#updateCVRConfig): the \
             expired q1 must be gone, not left running until an expiry tick"
    );
    assert!(
        engine.pipelines().has_query("q2"),
        "q2 is still desired by client2"
    );
}

/// TS `#removeExpiredQueries` (view-syncer.ts:635-652) is a FULL
/// `#syncQueryPipelineSet(lc, cvr, 'missing')`, not a targeted removal:
/// besides dropping the expired queries it re-adds any CVR query absent
/// from the pipelines (e.g. one whose last transform errored and was
/// removed). Rust's targeted `hydrate_and_sync(dels = expired)` never
/// re-added such a query.
#[tokio::test]
async fn remove_expired_queries_re_adds_a_cvr_query_missing_from_the_pipelines() {
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    // The test CVRs are at replica "v1"; the sync-set updater asserts the
    // engine's replica version is not older (TS `#pipelines.replicaVersion`).
    engine.replica_version = "v1".to_string();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);
    let ws = vec!["ws1".to_string()];
    let desire = |hash: &str, ttl: Option<i64>| DesiredQuerySpec {
        hash: hash.to_string(),
        ast: Some(serde_json::json!({"table": "users"})),
        name: None,
        args: None,
        ttl,
    };
    let cvr = engine
        .config_and_hydrate(
            super::empty_cvr("cg1", "v1"),
            "client1",
            &ws,
            &shard,
            vec![desire("q1", Some(1000)), desire("q2", None)],
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    // Inactivate q1 (ttl 1000 starts ticking).
    let cvr = engine
        .config_and_hydrate(
            cvr,
            "client1",
            &ws,
            &shard,
            Vec::new(),
            vec!["q1".to_string()],
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    // A pipeline that lost q2 while the CVR still desires it (TS: an
    // errored transform removes the pipeline; the CVR keeps the query).
    engine
        .pipelines
        .remove_query("q2", "test: simulate a dropped pipeline");
    assert!(!engine.pipelines().has_query("q2"));

    let (_cvr, removed) = engine
        .remove_expired_queries(
            cvr,
            &ws,
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 2000,
            },
        )
        .await
        .unwrap();
    assert_eq!(removed, 1, "q1 expired");
    assert!(!engine.pipelines().has_query("q1"));
    assert!(
        engine.pipelines().has_query("q2"),
        "the 'missing' sync must re-add a CVR query absent from the pipelines (TS #syncQueryPipelineSet)"
    );
}

#[tokio::test]
async fn expired_query_is_removed_after_ttl_elapses() {
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    // The test CVRs are at replica "v1"; the sync-set updater asserts the
    // engine's replica version is not older (TS `#pipelines.replicaVersion`).
    engine.replica_version = "v1".to_string();

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

    let ws = vec!["ws1".to_string()];

    // 1) Subscribe q1 with a 1000ms TTL.
    let puts = vec![DesiredQuerySpec {
        hash: "q1".to_string(),
        ast: Some(serde_json::json!({"table": "users"})),
        name: None,
        args: None,
        ttl: Some(1000),
    }];
    let cvr = engine
        .config_and_hydrate(
            super::empty_cvr("cg1", "v1"),
            "client1",
            &ws,
            &shard,
            puts,
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    assert!(engine.pipelines().has_query("q1"));

    // 2) Unsubscribe (del) at ttl_clock=0 → q1 marked inactive, still running.
    let cvr = engine
        .config_and_hydrate(
            cvr,
            "client1",
            &ws,
            &shard,
            Vec::new(),
            vec!["q1".to_string()],
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    assert!(engine.pipelines().has_query("q1"), "inactive query lingers");

    // 3) Not yet expired at ttl_clock=500 (< inactivated_at 0 + ttl 1000).
    let (cvr, removed) = engine
        .remove_expired_queries(
            cvr,
            &ws,
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 500,
            },
        )
        .await
        .unwrap();
    assert_eq!(removed, 0);
    assert!(engine.pipelines().has_query("q1"));

    // 4) Expired at ttl_clock=2000 → removed from pipeline + CVR.
    let (cvr, removed) = engine
        .remove_expired_queries(
            cvr,
            &ws,
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 2000,
            },
        )
        .await
        .unwrap();
    assert_eq!(removed, 1);
    assert!(!engine.pipelines().has_query("q1"));
    assert!(
        !cvr.queries.contains_key("q1"),
        "expired query removed from CVR"
    );
}

#[tokio::test]
async fn clear_op_drops_all_desired_queries() {
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

    // Subscribe q1.
    let cvr = engine
        .config_and_hydrate(
            super::empty_cvr("cg1", "v1"),
            "client1",
            &["ws1".to_string()],
            &shard,
            vec![DesiredQuerySpec {
                hash: "q1".to_string(),
                ast: Some(serde_json::json!({"table": "users"})),
                name: None,
                args: None,
                ttl: None,
            }],
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    assert!(
        cvr.clients["client1"]
            .desired_query_ids
            .contains(&"q1".to_string())
    );

    // A `clear` op removes all of the client's desired queries.
    let cvr = engine
        .config_and_hydrate(
            cvr,
            "client1",
            &["ws1".to_string()],
            &shard,
            Vec::new(),
            Vec::new(),
            true,
            // clear
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    assert!(
        !cvr.clients["client1"]
            .desired_query_ids
            .contains(&"q1".to_string()),
        "clear should drop the client's desired q1"
    );
}

#[tokio::test]
async fn config_and_hydrate_reissue_takes_catchup_branch_without_store() {
    // A second config_and_hydrate for an already-hydrated query has an empty
    // add set, so it takes the catchup branch. With no CVR store wired,
    // catchup is a clean no-op and the call still returns the CVR intact.
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

    let put = || {
        vec![DesiredQuerySpec {
            hash: "q1".to_string(),
            ast: Some(serde_json::json!({"table": "users"})),
            name: None,
            args: None,
            ttl: None,
        }]
    };

    // First call hydrates q1.
    let cvr = engine
        .config_and_hydrate(
            super::empty_cvr("cg1", "v1"),
            "client1",
            &["ws1".to_string()],
            &shard,
            put(),
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    assert!(engine.pipelines().has_query("q1"));

    // Second call: q1 already in the pipeline → empty add set → catchup path.
    let cvr = engine
        .config_and_hydrate(
            cvr,
            "client1",
            &["ws1".to_string()],
            &shard,
            put(),
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    assert!(cvr.queries.contains_key("q1"));
    assert!(cvr.clients.contains_key("client1"));
}

/// Mutation test: `customQueryTransformMode` — a `Missing`
/// pass must submit ONLY custom queries that are not already hydrated, an
/// `All` pass must submit every one. Port of TS `customQueriesToTransform`
/// (view-syncer.ts:1954-1959).
///
/// Oracle: a `userQueryURL` outside the allow-list fails the transform
/// SYNCHRONOUSLY (no network) and the failure body carries `queryIDs` — the
/// exact batch that was submitted — which reaches the client as an
/// `["error", body]` frame. So the frame IS the "what did we re-transform"
/// observable.
///
/// Before the fix rust had no mode: every sync re-submitted the whole custom
/// set. Revert the `custom_query_transform_mode` filter and the `Missing`
/// pass below emits an error frame again → `assert!(missing_err.is_none())`
/// fails.
#[tokio::test]
async fn custom_query_transform_mode_missing_skips_already_hydrated_queries() {
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    // A custom (named) desired query — `name` set is what makes the CVR
    // record a `QueryRecord::Custom`.
    let custom = |h: &str| DesiredQuerySpec {
        hash: h.to_string(),
        ast: None,
        name: Some("myNamedQuery".to_string()),
        args: Some(vec![]),
        ttl: None,
    };
    // Disallowed URL => `transform` fails without a network
    // round-trip, carrying the submitted batch in `queryIDs`.
    let ctx = crate::custom_queries::transform_query::CustomQueryContext {
        url: "https://mode-filter.example/query".to_string(),
        allowed_urls: vec!["https://allowed.example/*".to_string()],
        ..Default::default()
    };
    // The submitted batch, read off the client's `["error", body]` frame.
    let submitted = |rx: &mut tokio::sync::mpsc::UnboundedReceiver<WsCommand>| {
        std::iter::from_fn(|| rx.try_recv().ok()).find_map(|command| match command.frame_value() {
            Some(msg) if msg.get(0).and_then(serde_json::Value::as_str) == Some("error") => msg
                .get(1)
                .and_then(|b| b.get("queryIDs"))
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect::<Vec<_>>()
                }),
            _ => None,
        })
    };

    // ── `All`: the custom query IS submitted even though it is hydrated. ──
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    // Pretend `q1` is already running (what a steady-state CG looks like).
    pipelines.set_query_transformation_hash("q1", "hash-q1");
    let mut engine = SyncEngine::new(pipelines);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    engine.register_client(
        "client1",
        "ws1",
        "cg1",
        &shard,
        None,
        Arc::new(DirectWebSocketSink::new(tx)),
    );
    let _ = engine
        .config_and_hydrate(
            super::empty_cvr("cg1", "v1"),
            "client1",
            &["ws1".to_string()],
            &shard,
            vec![custom("q1")],
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            Some(&ctx),
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await;
    assert_eq!(
        submitted(&mut rx).as_deref(),
        Some(&["q1".to_string()][..]),
        "'all' must re-transform an already-hydrated custom query \
             (TS: \"re transform all on new connections\", view-syncer.ts:949)"
    );

    // ── `Missing`: the same hydrated query is NOT submitted. ──
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    pipelines.set_query_transformation_hash("q1", "hash-q1");
    let mut engine = SyncEngine::new(pipelines);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    engine.register_client(
        "client1",
        "ws1",
        "cg1",
        &shard,
        None,
        Arc::new(DirectWebSocketSink::new(tx)),
    );
    // Pipelines are already synced in steady state — otherwise
    // `hydrate_unchanged_queries` runs and muddies the observable.
    engine.pipelines_synced = true;
    let _ = engine
        .config_and_hydrate(
            super::empty_cvr("cg1", "v1"),
            "client1",
            &["ws1".to_string()],
            &shard,
            vec![custom("q1")],
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::Missing,
            None,
            &serde_json::json!({}),
            Some(&ctx),
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await;
    assert!(
        submitted(&mut rx).is_none(),
        "'missing' must NOT re-transform a custom query already in the \
             pipeline (TS `filter(q => !this.#pipelines.queries().has(q.id))`, \
             view-syncer.ts:1958)"
    );

    // ── `Missing` still transforms a custom query that is NOT hydrated. ──
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    engine.register_client(
        "client1",
        "ws1",
        "cg1",
        &shard,
        None,
        Arc::new(DirectWebSocketSink::new(tx)),
    );
    engine.pipelines_synced = true;
    let _ = engine
        .config_and_hydrate(
            super::empty_cvr("cg1", "v1"),
            "client1",
            &["ws1".to_string()],
            &shard,
            vec![custom("q2")],
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::Missing,
            None,
            &serde_json::json!({}),
            Some(&ctx),
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await;
    assert_eq!(
        submitted(&mut rx).as_deref(),
        Some(&["q2".to_string()][..]),
        "'missing' must still transform a custom query absent from the pipeline"
    );
}

/// Mutation test: `hydrate_unchanged_queries` — the proactive
/// re-materialize of every already-gotten pipeline — must run ONCE per pipeline
/// init (TS `#hydrateUnchangedQueries` gated by `#pipelinesSynced`,
/// view-syncer.ts:568-606), NOT on every connect/config-change. Before the fix
/// it ran inside `sync_query_pipeline_set` on every call, re-materializing a big
/// CG's whole query set on each reconnect (whale-CG 20-88s hydrates). Revert the
/// `pipelines_synced` gate → the counter reaches 2 across two syncs and the
/// `== 1` assert fails.
#[tokio::test]
async fn hydrate_unchanged_runs_once_per_pipeline_init() {
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

    let ws = ["ws1".to_string()];
    let empty_auth = serde_json::json!({});
    let put = || {
        vec![DesiredQuerySpec {
            hash: "q1".to_string(),
            ast: Some(serde_json::json!({"table": "users"})),
            name: None,
            args: None,
            ttl: None,
        }]
    };

    assert!(!engine.pipelines_synced, "starts un-synced");
    let cvr = engine
        .config_and_hydrate(
            super::empty_cvr("cg1", "v1"),
            "client1",
            &ws,
            &shard,
            put(),
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &empty_auth,
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    assert!(engine.pipelines_synced, "first sync marks pipelines synced");
    assert_eq!(
        engine.hydrate_unchanged_runs, 1,
        "hydrate_unchanged_queries runs on the first (init) sync"
    );

    // Second sync (reconnect / config re-issue): must NOT re-run it.
    let cvr = engine
        .config_and_hydrate(
            cvr,
            "client1",
            &ws,
            &shard,
            put(),
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &empty_auth,
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        engine.hydrate_unchanged_runs, 1,
        "a second sync on synced pipelines must NOT re-run the proactive re-hydrate"
    );

    // A pipeline reset re-arms it (TS `#pipelinesSynced = false` on
    // `#pipelines.reset()`): simulate the flag reset and re-sync.
    engine.pipelines_synced = false;
    let _ = engine
        .config_and_hydrate(
            cvr,
            "client1",
            &ws,
            &shard,
            put(),
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &empty_auth,
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        engine.hydrate_unchanged_runs, 2,
        "after a reset the init re-hydrate runs again"
    );
}

#[tokio::test]
async fn changed_transformation_hash_rehydrates_query() {
    // Simulates the updateAuth re-transform path: a query already hydrated
    // with one transformation hash is re-hydrated when the recomputed hash
    // differs (as it would when authData changes the permission expansion).
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

    let put = || {
        vec![DesiredQuerySpec {
            hash: "q1".to_string(),
            ast: Some(serde_json::json!({"table": "users"})),
            name: None,
            args: None,
            ttl: None,
        }]
    };

    let cvr = engine
        .config_and_hydrate(
            super::empty_cvr("cg1", "v1"),
            "client1",
            &["ws1".to_string()],
            &shard,
            put(),
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    let real_hash = engine
        .pipelines()
        .query_transformation_hash("q1")
        .unwrap()
        .to_string();
    assert!(!real_hash.is_empty());

    // Force a stale recorded hash (as if the previous transform used a
    // different authData), then re-run: q1 must be torn down + re-hydrated,
    // restoring the correct hash.
    engine
        .pipelines()
        .set_query_transformation_hash("q1", "STALE-HASH");
    assert_eq!(
        engine.pipelines().query_transformation_hash("q1"),
        Some("STALE-HASH")
    );

    engine
        .config_and_hydrate(
            cvr,
            "client1",
            &["ws1".to_string()],
            &shard,
            put(),
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    assert!(engine.pipelines().has_query("q1"));
    assert_eq!(
        engine.pipelines().query_transformation_hash("q1"),
        Some(real_hash.as_str()),
        "drifted query should be re-hydrated back to the correct transform hash"
    );
}

/// TS `#catchupClients` creates the pokers BEFORE gathering any patches and
/// ends them unconditionally (view-syncer.ts:2400 + 2464-2467), so a client
/// with NOTHING to catch up on still receives the forced empty initial poke
/// — client-handler.ts:123: "We will send a poke on connect even if the
/// client is already caught up, so that it can learn its got-queries state
/// has been reconciled with the server" — and the version is marked served.
///
/// rust used to `return Ok(())` the moment the patch set came back empty,
/// which skipped BOTH the poke and `mark_version_served`. The 60-minute dual
/// replay's frame-sequence gate caught it as 4 client groups with
/// rust=1 frame (`connected`) against ts=3 (`connected`, `pokeStart`,
/// `pokeEnd`) — a client that never learns its queries were reconciled.
///
/// The predecessor of this test (`catchup_clients_without_store_is_noop`)
/// only asserted that the call did not panic, so it passed both before and
/// after the fix; it is tightened here to pin the frames themselves.
#[tokio::test]
async fn catchup_clients_pokes_even_when_there_are_no_patches() {
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    engine.register_client(
        "client1",
        "ws1",
        "cg1",
        &shard,
        None, // baseCookie None => never poked => forceInitialPoke
        Arc::new(DirectWebSocketSink::new(tx)),
    );
    let cvr = super::empty_cvr("cg1", "v1");
    // No store → `gather_catchup_patches` yields an EMPTY patch set, the
    // same branch a live CG takes when it has nothing to catch up on.
    engine
        .catchup_clients(
            &cvr,
            &cvr.version.clone(),
            &[],
            &["ws1".to_string()],
            &std::collections::HashMap::new(),
        )
        .await
        .unwrap();

    let mut frames = Vec::new();
    while let Ok(cmd) = rx.try_recv() {
        if let Some(msg) = cmd.frame_value() {
            frames.push(
                msg.get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("?")
                    .to_string(),
            );
        }
    }
    assert_eq!(
        frames,
        vec!["pokeStart".to_string(), "pokeEnd".to_string()],
        "an empty catch-up must still emit the forced initial poke (TS \
             #catchupClients starts the pokers before gathering patches)"
    );
    assert_eq!(
        engine.served_version.as_deref(),
        Some(cvr.version.state_version.as_str()),
        "TS marks the version served after pokers.end(), even with no patches"
    );
}

/// TS `#catchupClients` has no client-count guard: with `#getClients()`
/// empty it still builds the (target-less) pokers, gathers the patches and
/// runs `#markVersionServed(cvr.version)` (view-syncer.ts:2399-2400,
/// 2464-2467). Rust returned early on an empty client set, skipping the
/// served mark — the e2e serving-lag observation and the cross-CG
/// `servedVersion` TS records for a group whose last client just dropped.
/// Mutation test: restore the `clients.is_empty()` early return and
/// `served_version` stays `None`.
#[tokio::test]
async fn catchup_clients_marks_the_version_served_with_no_clients_like_ts() {
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    let cvr = super::empty_cvr("cg1", "v1");
    engine
        .catchup_clients(
            &cvr,
            &cvr.version.clone(),
            &[],
            &[],
            &std::collections::HashMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        engine.served_version.as_deref(),
        Some(cvr.version.state_version.as_str()),
        "TS marks the version served even when no client is connected"
    );
}

/// Regression for reconnect catch-up: the floor must be each client's cookie
/// as of the START of the config/hydrate cycle — NOT its live `version()`,
/// which the config & hydrate pokes' `end()` have already advanced to the new
/// CVR version. Feeding the live version collapses the interval to
/// `[current, current]` and a reconnecting client loses everything it missed.
#[test]
fn catchup_floor_uses_original_cookie_not_advanced_version() {
    use rust_cvr::schema::types::version_from_string;

    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    // Client connected at cookie "01".
    engine.register_client("c1", "ws1", "cg1", &shard, Some("01"), sink);

    // Cycle-start snapshot (captured before any poke advanced base_version).
    let original: std::collections::HashMap<String, NullableCVRVersion> =
        std::collections::HashMap::from([("ws1".to_string(), Some(version_from_string("01")))]);

    // Simulate the config/hydrate pokes advancing base_version to the new "05".
    let clients = engine.get_clients(&["ws1".to_string()]);
    clients[0].set_base_version_for_test(version_from_string("05"));

    let cvr_version = version_from_string("05");

    // With the snapshot the floor is the ORIGINAL "01" — catch-up replays the
    // whole [01, 05] interval the reconnecting client missed.
    let floor = SyncEngine::catchup_floor(&cvr_version, &clients, &original);
    assert_eq!(floor, Some(version_from_string("01")));

    // Guard: the OLD behavior (reading the already-advanced live version)
    // collapses the floor to "05" == current → an empty catch-up interval.
    let buggy =
        SyncEngine::catchup_floor(&cvr_version, &clients, &std::collections::HashMap::new());
    assert_eq!(buggy, Some(version_from_string("05")));
    assert_ne!(
        floor, buggy,
        "the fix must not collapse the catch-up interval"
    );
}

/// One handler per socket, however often a ws_id repeats in the request.
///
/// TS cannot produce a duplicate: `#getClients()` reads the values of a Map
/// keyed by clientID. Rust resolves handlers from a LIST of ws_ids (built
/// from `registered_ws.values()`, which is keyed by client_id — so a
/// repeated ws_id is representable), and a repeat put the same
/// `Arc<ClientHandler>` into one `MultiPoker` twice. Both pokers then share
/// that client's `poke_chain`: the first takes it, the second cannot, and
/// `acquire_chain` used to spin on `std::thread::yield_now()` forever
/// because the holder is on this very thread and only an `.await` could let
/// it run. One duplicated entry wedged the whole client-group thread.
///
/// Mutation test: drop the `seen.insert(...)` filter in `get_clients` and the
/// length assertion fails with 2.
#[test]
fn get_clients_returns_one_handler_per_socket_for_a_repeated_ws_id() {
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    let shard = ShardID {
        app_id: "zero".to_string(),
        shard_num: 0,
    };
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn rust_cvr::client_handler::WebSocketSink> =
        Arc::new(DirectWebSocketSink::new(tx));
    engine.register_client("c1", "ws1", "cg1", &shard, None, sink);

    let clients = engine.get_clients(&["ws1".to_string(), "ws1".to_string(), "ws1".to_string()]);
    assert_eq!(
        clients.len(),
        1,
        "a repeated ws_id must yield ONE poker, not one per repeat"
    );

    // The duplicate is what made this fatal: two pokers over one client
    // share its chain, so the second could never acquire it.
    let refs: Vec<&rust_cvr::client_handler::ClientHandler> =
        clients.iter().map(|c| c.as_ref()).collect();
    let pokers = rust_cvr::client_handler::MultiPoker::new(
        &refs,
        rust_cvr::schema::types::version_from_string("02"),
        "test",
    );
    // Reaching this line at all is the point: with a duplicate present,
    // the first `add_patch` never returned.
    pokers.cancel();
}

/// An advance may only poke clients that are AT the pre-advance cvr.version;
/// lagging clients (behind it) and never-poked clients are excluded and get
/// caught up on reconnect instead. Port of TS `#getClients(cvr.version)`.
#[test]
fn advance_poke_targets_excludes_lagging_clients() {
    use rust_cvr::schema::types::version_from_string;

    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    let mk = || -> Arc<dyn WebSocketSink> {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        Arc::new(DirectWebSocketSink::new(tx))
    };
    engine.register_client("cA", "wsA", "cg1", &shard, Some("05"), mk()); // at cvr.version
    engine.register_client("cB", "wsB", "cg1", &shard, Some("03"), mk()); // lagging
    engine.register_client("cC", "wsC", "cg1", &shard, None, mk()); // never poked

    let all = engine.get_clients(&["wsA".to_string(), "wsB".to_string(), "wsC".to_string()]);
    let targets = SyncEngine::advance_poke_targets(all, &version_from_string("05"));
    let ids: Vec<String> = targets.iter().map(|c| c.ws_id.clone()).collect();
    assert_eq!(
        ids,
        vec!["wsA".to_string()],
        "only the client at cvr.version may receive the advance delta"
    );
}

#[test]
fn config_poke_targets_include_new_but_exclude_lagging_clients() {
    use rust_cvr::schema::types::version_from_string;

    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    let mk = || -> Arc<dyn WebSocketSink> {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        Arc::new(DirectWebSocketSink::new(tx))
    };
    engine.register_client("new", "ws-new", "cg1", &shard, None, mk());
    engine.register_client("current", "ws-current", "cg1", &shard, Some("02"), mk());
    engine.register_client("lagging", "ws-lagging", "cg1", &shard, Some("01"), mk());

    let new_targets = SyncEngine::config_poke_targets(
        engine.get_clients(&["ws-new".to_string()]),
        &version_from_string("00"),
    );
    assert_eq!(new_targets.len(), 1, "no cookie is TS empty version 00");

    let targets = SyncEngine::config_poke_targets(
        engine.get_clients(&["ws-current".to_string(), "ws-lagging".to_string()]),
        &version_from_string("02"),
    );
    let ids: Vec<_> = targets.iter().map(|client| client.ws_id.as_str()).collect();
    assert_eq!(ids, vec!["ws-current"]);
}

#[tokio::test]
async fn delete_clients_removes_client_and_acks() {
    let mut pipelines = IvmPipelines::new();
    pipelines.init(users_tables(), None, "zero").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };

    let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    engine.register_client(
        "client1",
        "ws1",
        "cg1",
        &shard,
        None,
        Arc::new(DirectWebSocketSink::new(tx1)),
    );
    let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    engine.register_client(
        "client2",
        "ws2",
        "cg1",
        &shard,
        None,
        Arc::new(DirectWebSocketSink::new(tx2)),
    );

    let q = |h: &str| DesiredQuerySpec {
        hash: h.to_string(),
        ast: Some(serde_json::json!({"table": "users"})),
        name: None,
        args: None,
        ttl: None,
    };

    let cvr = engine
        .config_and_hydrate(
            super::empty_cvr("cg1", "v1"),
            "client1",
            &["ws1".to_string()],
            &shard,
            vec![q("q1")],
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    let cvr = engine
        .config_and_hydrate(
            cvr,
            "client2",
            &["ws2".to_string()],
            &shard,
            vec![q("q2")],
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "v1".to_string(),
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    assert!(cvr.clients.contains_key("client1"));
    assert!(cvr.clients.contains_key("client2"));

    // Delete client2, poking both connected clients + acking.
    let cvr = engine
        .delete_clients(
            cvr,
            &shard,
            "client1",
            "ws1",
            &["client2".to_string()],
            &["client2".to_string()],
            &[],
            &["ws1".to_string(), "ws2".to_string()],
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        )
        .await
        .unwrap();
    assert!(cvr.clients.contains_key("client1"));
    assert!(
        !cvr.clients.contains_key("client2"),
        "client2 removed from CVR"
    );

    // client1 received a deleteClients ack naming client2.
    let mut saw_ack = false;
    while let Ok(Some(v)) = rx1.try_recv().map(|c| c.frame_value()) {
        if v[0] == "deleteClients"
            && let Some(ids) = v[1]["clientIDs"].as_array()
            && ids.iter().any(|x| x == "client2")
        {
            saw_ack = true;
        }
    }
    assert!(saw_ack, "expected deleteClients ack naming client2");
}

/// Mutation test: a failed custom-query transform must
/// emit the TS WARN AND still forward the error to clients. Port of TS
/// `#processTransformedCustomQueries` (view-syncer.ts:1715-1719,
/// `lc.warn?.(errorMessage, q)`). Before the fix rust forwarded to clients
/// but logged nothing — silent in ops. Revert the `tracing::warn!` in
/// `record_transform_error` and the emission assertion fails; break the
/// message format and the `format_transform_error_message` asserts fail.
#[test]
fn record_transform_error_emits_ts_warn_and_forwards() {
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct BufWriter(Arc<Mutex<Vec<u8>>>);
    struct BufGuard(Arc<Mutex<Vec<u8>>>);
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
        type Writer = BufGuard;
        fn make_writer(&'a self) -> BufGuard {
            BufGuard(self.0.clone())
        }
    }
    impl std::io::Write for BufGuard {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    // Exact shape the API server returns for an InputValidationError, as seen
    // in prod TS logs: {id, name, error:"app", details:{...}}.
    let err = serde_json::json!({
        "id": "392e943e2358d54f",
        "name": "ticketsByIds",
        "error": "app",
        "details": {"type": "InputValidationError"}
    });

    // Pure-format parity (pins the exact TS wording, view-syncer.ts:1716):
    assert_eq!(
        format_transform_error_message(&err),
        "Error transforming custom query ticketsByIds: app {\"type\":\"InputValidationError\"}"
    );
    // details absent → no trailing segment (TS `q.details ? ... : ''`).
    assert_eq!(
        format_transform_error_message(&serde_json::json!({"name": "q2", "error": "http"})),
        "Error transforming custom query q2: http"
    );

    // Emission + forwarding parity:
    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(BufWriter(buf.clone()))
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .finish();
    let mut forwarded: Vec<serde_json::Value> = Vec::new();
    crate::ensure_permissive_global_subscriber();
    tracing::subscriber::with_default(subscriber, || {
        record_transform_error(err.clone(), &mut forwarded);
    });

    let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(
        logged.contains(
            "Error transforming custom query ticketsByIds: app {\"type\":\"InputValidationError\"}"
        ),
        "expected TS-parity transform-error WARN; got: {logged}"
    );
    assert!(logged.contains("WARN"), "must be WARN level; got: {logged}");
    // Client forwarding is preserved (the pre-fix behavior).
    assert_eq!(forwarded, vec![err]);
}

/// Capture WARN-level logs emitted while running `f`. Used to pin the exact
/// TS-parity auth-maintenance warnings.
fn capture_warns<F: FnOnce()>(f: F) -> String {
    use std::sync::{Arc, Mutex};
    #[derive(Clone)]
    struct BufWriter(Arc<Mutex<Vec<u8>>>);
    struct BufGuard(Arc<Mutex<Vec<u8>>>);
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
        type Writer = BufGuard;
        fn make_writer(&'a self) -> BufGuard {
            BufGuard(self.0.clone())
        }
    }
    impl std::io::Write for BufGuard {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(BufWriter(buf.clone()))
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .finish();
    crate::ensure_permissive_global_subscriber();
    tracing::subscriber::with_default(subscriber, f);
    String::from_utf8(buf.lock().unwrap().clone()).unwrap()
}

/// Pure classifier parity (TS `#runBackgroundRetransform` catch dispatch,
/// view-syncer.ts:2700-2723): an auth error body → `AuthError`, a transient
/// transform-failed body → `TransformFailed`, and `None` (no throw) →
/// `Success`. Mutation test: mis-map any arm (e.g. treat every failure as
/// transient) and the corresponding assert fails.
#[test]
fn classify_retransform_failure_splits_auth_transient_success() {
    assert!(matches!(
        classify_retransform_failure(None),
        RetransformOutcome::Success
    ));
    // {kind: Unauthorized} and http 401/403 are auth (auth.ts isAuthErrorBody).
    assert!(matches!(
        classify_retransform_failure(Some(serde_json::json!({"kind": "Unauthorized"}))),
        RetransformOutcome::AuthError(_)
    ));
    assert!(matches!(
        classify_retransform_failure(Some(serde_json::json!({
            "kind": "TransformFailed", "reason": "http", "status": 401
        }))),
        RetransformOutcome::AuthError(_)
    ));
    // A 5xx / non-auth transform failure is transient → deferred, not fatal.
    assert!(matches!(
        classify_retransform_failure(Some(serde_json::json!({
            "kind": "TransformFailed", "reason": "http", "status": 503
        }))),
        RetransformOutcome::TransformFailed(_)
    ));
    assert!(matches!(
        classify_retransform_failure(Some(serde_json::json!({
            "kind": "TransformFailed", "reason": "internal", "message": "boom"
        }))),
        RetransformOutcome::TransformFailed(_)
    ));
}

/// Mutation test: a background retransform whose re-hydrate
/// hits an AUTH error must NOT mark success — it must WARN, fail the stale
/// connection, and retry under a replacement (TS `#runBackgroundRetransform`
/// view-syncer.ts:2700-2709,2726-2745). The pre-fix code ran the re-hydrate
/// and marked success UNCONDITIONALLY (the stale-auth outage
/// class): no warn, no fail, no retry. Revert `run_background_retransform` to
/// that unconditional mark and every assert below fails.
#[test]
fn background_retransform_auth_error_fails_connection_and_retries() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);

    // Two validated connections in the group, both pinned to user-1, so a
    // failed background connection has a replacement to retry under.
    let (tx1, _d1) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx1),
    ));
    let (tx2, _d2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c2", "ws2", "user-1"),
        DirectWebSocketSink::new(tx2),
    ));
    // "Two VALIDATED connections": TS validates at initConnection
    // (`#validateConnection`, view-syncer.ts:942), not at socket accept;
    // connection-context-manager.ts:267 registers `revalidateAt: undefined`,
    // and the CCM only promotes a
    // background connection from a validated one.
    validate_test_connection(&rt, &mut state, "c1", "ws1");
    validate_test_connection(&rt, &mut state, "c2", "ws2");
    assert_eq!(state.registered_ws.len(), 2);

    // First attempt: auth error. Second (on the replacement): success.
    state
        .forced_retransform_outcomes
        .push_back(RetransformOutcome::AuthError(
            serde_json::json!({"kind": "Unauthorized"}),
        ));
    state
        .forced_retransform_outcomes
        .push_back(RetransformOutcome::Success);

    let logged = capture_warns(|| rt.block_on(state.run_background_retransform()));

    assert!(
        logged.contains(
            "Background retransform auth failed; failing connection and searching for replacement"
        ),
        "expected the TS auth-fail WARN; got: {logged}"
    );
    assert_eq!(
        state.registered_ws.len(),
        1,
        "the auth-failed background connection must be dropped, its replacement kept"
    );
    assert!(
        state.forced_retransform_outcomes.is_empty(),
        "both attempts must run — the retry under the replacement connection"
    );
}

/// Mutation test: a background retransform whose re-hydrate hits a
/// TRANSIENT transform failure must WARN + defer maintenance and KEEP the
/// connection — never mark success, never close the socket (TS
/// `#runBackgroundRetransform` view-syncer.ts:2710-2719). Revert to the
/// unconditional mark and the WARN assert fails (the old path was silent and
/// marked success).
#[test]
fn background_retransform_transform_failed_defers_and_keeps_connection() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);

    let (tx, _d) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    // TS validates at initConnection, not at socket accept
    // (view-syncer.ts:942); the CCM only promotes a background connection
    // from a validated one, and without one this retransform is a no-op.
    validate_test_connection(&rt, &mut state, "c1", "ws1");
    assert_eq!(state.registered_ws.len(), 1);

    state
        .forced_retransform_outcomes
        .push_back(RetransformOutcome::TransformFailed(
            serde_json::json!({"kind": "TransformFailed", "reason": "http", "status": 503}),
        ));

    let logged = capture_warns(|| rt.block_on(state.run_background_retransform()));

    assert!(
        logged.contains("Background retransform failed; deferring auth maintenance"),
        "expected the TS transform-failed defer WARN; got: {logged}"
    );
    assert_eq!(
        state.registered_ws.len(),
        1,
        "a transient transform failure must NOT close the connection"
    );
    assert!(
        state.forced_retransform_outcomes.is_empty(),
        "the single attempt must run"
    );
}

/// A successful background retransform takes the success branch silently:
/// no maintenance WARN, connection retained. (The `markBackgroundRetransform
/// Success` call itself has no observable deadline effect here because the
/// test CCM is built with no retransform interval; the auth/transform-failed
/// tests above carry the non-vacuous weight of the fix.)
#[test]
fn background_retransform_success_is_silent_and_keeps_connection() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);

    let (tx, _d) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    // The CCM only promotes a BACKGROUND connection once one is validated,
    // and TS validates at initConnection, not at socket accept
    // (view-syncer.ts:942 / connection-context-manager.ts:267). Without this
    // the maintenance plan has no background connection and the retransform
    // is a silent no-op.
    validate_test_connection(&rt, &mut state, "c1", "ws1");

    state
        .forced_retransform_outcomes
        .push_back(RetransformOutcome::Success);

    let logged = capture_warns(|| rt.block_on(state.run_background_retransform()));

    assert!(
        !logged.contains("Background retransform"),
        "success path must emit no retransform WARN; got: {logged}"
    );
    assert_eq!(state.registered_ws.len(), 1);
    assert!(state.forced_retransform_outcomes.is_empty());
}
