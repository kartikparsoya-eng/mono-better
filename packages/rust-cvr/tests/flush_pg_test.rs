//! Live-Postgres, seed-parameterized flush differential (query-driven WRITE path).
//!
//! Replays the trackQueries -> received -> deleteUnreferencedRows -> flush the TS
//! golden generator ran (`agentic/parity/generate-flush-fixture.mjs`) through the
//! REAL Rust CVRStore + CVRQueryDrivenUpdater, for each scenario (single/multi-
//! column keys, a POISONED non-PK rowKey, multi-query shared refCounts), then
//! asserts the persisted `rows` (and queries / instances / rowsVersion) match the
//! TS-written golden byte-for-byte. Each scenario carries its own `baseSeedSql`,
//! `tracked`, and `received`, so the inputs cannot drift between the two
//! languages. Pins the DB-row SERIALIZATION — rowKey / refCounts / versions — the
//! layer where the prior poisoned-rowKey-in-PG corruption happened.
//!
//! All scenarios seed the base instance at version '01' and advance to '02', so
//! the flush CAS (expectedCurrentVersion) is the loaded '01'.
//!
//! Regenerate the golden with:
//!   TEST_CVR_PG_URI=... npx tsx agentic/parity/generate-flush-fixture.mjs
//!
//! Gated on `TEST_CVR_PG_URI`; skips (passes) when unset.

use rust_cvr::cvr::CVRQueryDrivenUpdater;
use rust_cvr::cvr::{RefCounts, RowUpdate};
use rust_cvr::cvr_store::CVRStoreHandle;
use rust_cvr::schema::types::CVRVersion;
use rust_cvr::schema::types::RowID;
use serde_json::Value;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::Arc;

const SCHEMA: &str = "roze_1/cvr";

/// Both tests in this file DROP and re-CREATE `SCHEMA`, and cargo runs them on
/// separate threads. They cannot simply use different schemas — the DDL comes
/// from `flush-schema.sql`, which hardcodes the schema name the TS golden fixture
/// generator writes to — so they take turns owning it instead.
static PG_SCHEMA: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));
const CVR_ID: &str = "cg-flush";
const TASK_ID: &str = "flush-task";

fn row_id_from_json(v: &Value) -> RowID {
    RowID {
        schema: v["schema"].as_str().unwrap().to_string(),
        table: v["table"].as_str().unwrap().to_string(),
        row_key: v["rowKey"].as_object().unwrap().clone(),
    }
}

#[tokio::test]
async fn flush_matches_ts_golden() {
    let _schema_guard = PG_SCHEMA.lock().await;
    let uri = match std::env::var("TEST_CVR_PG_URI") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!("SKIP flush_matches_ts_golden: TEST_CVR_PG_URI unset");
            return;
        }
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&uri)
        .await
        .expect("connect to TEST_CVR_PG_URI");

    let golden: Value = serde_json::from_str(include_str!("../agentic/parity/flush-fixture.json"))
        .expect("flush-fixture.json");
    let connect_time = golden["connectTime"].as_f64().expect("connectTime");
    let now = golden["now"].as_i64().expect("now");
    let scenarios = golden["scenarios"].as_array().expect("scenarios array");
    assert!(!scenarios.is_empty(), "no flush scenarios in golden");

    let mut checked = 0;
    for sc in scenarios {
        let name = sc["name"].as_str().unwrap_or("?");

        // Fresh schema from the exact TS DDL, then the scenario's own base seed.
        sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE"#))
            .execute(&pool)
            .await
            .expect("drop schema");
        sqlx::raw_sql(include_str!("../agentic/parity/flush-schema.sql"))
            .execute(&pool)
            .await
            .expect("create schema");
        sqlx::raw_sql(sc["baseSeedSql"].as_str().expect("baseSeedSql"))
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("seed scenario {name}: {e}"));

        let mut store = CVRStoreHandle::new(
            pool.clone(),
            SCHEMA.to_string(),
            CVR_ID.to_string(),
            TASK_ID.to_string(),
        );
        let loaded = store.load(connect_time).await.expect("load");
        let mut updater =
            CVRQueryDrivenUpdater::new(loaded.cvr, "02".to_string(), "01".to_string(), None);

        // tracked.executed: [[id, transformationHash], ...]; tracked.removed: [id, ...]
        let executed: Vec<(String, String)> = sc["tracked"]["executed"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| {
                (
                    e[0].as_str().unwrap().to_string(),
                    e[1].as_str().unwrap().to_string(),
                )
            })
            .collect();
        let executed_refs: Vec<(&str, &str)> = executed
            .iter()
            .map(|(a, b)| (a.as_str(), b.as_str()))
            .collect();
        let removed: Vec<String> = sc["tracked"]["removed"]
            .as_array()
            .map(|a| a.iter().map(|x| x.as_str().unwrap().to_string()).collect())
            .unwrap_or_default();
        let removed_refs: Vec<&str> = removed.iter().map(|s| s.as_str()).collect();
        updater.track_queries(&executed_refs, &removed_refs);

        let mut rows: HashMap<String, (RowID, RowUpdate)> = HashMap::new();
        for r in sc["received"].as_array().unwrap() {
            let id = row_id_from_json(&r["id"]);
            let id_str = rust_cvr::row_key::row_id_string(&id);
            let ref_counts: RefCounts =
                serde_json::from_value(r["refCounts"].clone()).expect("refCounts");
            rows.insert(
                id_str,
                (
                    id,
                    RowUpdate {
                        version: Some("02".to_string()),
                        contents: Some(Arc::new(r["contents"].clone())),
                        ref_counts,
                    },
                ),
            );
        }
        let existing: HashMap<String, rust_cvr::schema::types::RowRecord> = HashMap::new();
        updater.received(&rows, &existing).unwrap();
        updater.delete_unreferenced_rows(existing.values()).unwrap();

        let (cvr_final, _stats) = updater.flush(connect_time as i64, now, now);
        let ops = updater.base.drain_store_ops();
        store.apply_store_ops(ops);
        let expected_version = CVRVersion {
            state_version: "01".to_string(),
            config_version: None,
        };
        store
            .flush(&expected_version, &cvr_final, connect_time)
            .await
            .expect("flush");

        // ── assert persisted rows match the TS golden ──
        let persisted = sqlx::query(&format!(
            r#"SELECT "schema","table","rowKey","rowVersion","patchVersion","refCounts"
               FROM "{SCHEMA}".rows ORDER BY "table","rowKey"::text"#
        ))
        .fetch_all(&pool)
        .await
        .expect("select rows");
        let actual_rows: Vec<Value> = persisted
            .iter()
            .map(|r| {
                serde_json::json!({
                    "schema": r.get::<String, _>("schema"),
                    "table": r.get::<String, _>("table"),
                    "rowKey": r.get::<Value, _>("rowKey"),
                    "rowVersion": r.get::<String, _>("rowVersion"),
                    "patchVersion": r.get::<String, _>("patchVersion"),
                    "refCounts": r.get::<Option<Value>, _>("refCounts"),
                })
            })
            .collect();
        assert_eq!(
            &Value::Array(actual_rows),
            &sc["expected"]["rows"],
            "scenario `{name}`: persisted rows differ from TS golden"
        );

        // instances (version / replicaVersion / ttlClock) + rowsVersion.
        let inst = sqlx::query(&format!(
            r#"SELECT version, "replicaVersion", "ttlClock" FROM "{SCHEMA}".instances"#
        ))
        .fetch_one(&pool)
        .await
        .expect("select instances");
        let g_inst = &sc["expected"]["instances"][0];
        assert_eq!(
            inst.get::<String, _>("version"),
            g_inst["version"].as_str().unwrap(),
            "scenario `{name}`: instance version"
        );
        assert_eq!(
            inst.get::<Option<String>, _>("replicaVersion").as_deref(),
            g_inst["replicaVersion"].as_str(),
            "scenario `{name}`: replicaVersion"
        );
        assert_eq!(
            inst.get::<f64, _>("ttlClock") as i64,
            g_inst["ttlClock"].as_i64().unwrap(),
            "scenario `{name}`: ttlClock"
        );

        let rv = sqlx::query(&format!(r#"SELECT version FROM "{SCHEMA}"."rowsVersion""#))
            .fetch_one(&pool)
            .await
            .expect("select rowsVersion");
        assert_eq!(
            rv.get::<String, _>("version"),
            sc["expected"]["rowsVersion"][0]["version"]
                .as_str()
                .unwrap(),
            "scenario `{name}`: rowsVersion"
        );
        checked += 1;
    }
    eprintln!("flush differential: {checked} scenarios matched the TS golden");
}

/// The write-back wiring: a row batch over the `RowRecordCache` deferred
/// threshold must NOT be written by the flush transaction — not `cvr.rows`, not
/// `cvr.rowsVersion` — so the client poke that follows the flush is not stuck
/// behind the row commit. TS does this via
/// `executeRowUpdates(..., 'allow-defer')` (cvr-store.ts:1166 ->
/// row-record-cache.ts:418-427); this port's store takes the cache as a
/// parameter and asks it at the same call site.
///
/// Non-vacuous: before the wiring the store always wrote every row inline
/// (`stats.rows_flushed` did not exist and `cvr.rows` held all 150 rows after
/// the flush), so every assertion below fails.
///
/// The second half pins that deferral is not data loss: `apply(flushed=false)`
/// queues the rows and the background flush lands them, advancing `rowsVersion`.
///
/// Gated on `TEST_CVR_PG_URI`; skips (passes) when unset.
#[tokio::test]
async fn flush_defers_large_row_batches_to_the_write_back_cache() {
    let _schema_guard = PG_SCHEMA.lock().await;
    let uri = match std::env::var("TEST_CVR_PG_URI") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!("SKIP flush_defers_large_row_batches: TEST_CVR_PG_URI unset");
            return;
        }
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&uri)
        .await
        .expect("connect to TEST_CVR_PG_URI");

    let golden: Value = serde_json::from_str(include_str!("../agentic/parity/flush-fixture.json"))
        .expect("flush-fixture.json");
    let connect_time = golden["connectTime"].as_f64().expect("connectTime");
    let now = golden["now"].as_i64().expect("now");
    let sc = &golden["scenarios"][0];

    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE"#))
        .execute(&pool)
        .await
        .expect("drop schema");
    sqlx::raw_sql(include_str!("../agentic/parity/flush-schema.sql"))
        .execute(&pool)
        .await
        .expect("create schema");
    sqlx::raw_sql(sc["baseSeedSql"].as_str().expect("baseSeedSql"))
        .execute(&pool)
        .await
        .expect("seed");

    let mut store = CVRStoreHandle::new(
        pool.clone(),
        SCHEMA.to_string(),
        CVR_ID.to_string(),
        TASK_ID.to_string(),
    );
    let loaded = store.load(connect_time).await.expect("load");
    let mut updater =
        CVRQueryDrivenUpdater::new(loaded.cvr, "02".to_string(), "01".to_string(), None);
    let executed: Vec<(String, String)> = sc["tracked"]["executed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e[0].as_str().unwrap().to_string(),
                e[1].as_str().unwrap().to_string(),
            )
        })
        .collect();
    let executed_refs: Vec<(&str, &str)> = executed
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    updater.track_queries(&executed_refs, &[]);

    // 150 rows — comfortably over DEFAULT_DEFERRED_THRESHOLD (100).
    const N: i64 = 150;
    let query_id = executed[0].0.clone();
    let mut rows: HashMap<String, (RowID, RowUpdate)> = HashMap::new();
    for i in 0..N {
        let mut row_key = serde_json::Map::new();
        row_key.insert("id".to_string(), Value::Number(i.into()));
        let id = RowID {
            schema: "public".to_string(),
            table: "issue".to_string(),
            row_key,
        };
        rows.insert(
            rust_cvr::row_key::row_id_string(&id),
            (
                id,
                RowUpdate {
                    version: Some("02".to_string()),
                    contents: Some(Arc::new(serde_json::json!({"id": i}))),
                    ref_counts: RefCounts::from([(query_id.clone(), 1)]),
                },
            ),
        );
    }
    let existing: HashMap<String, rust_cvr::schema::types::RowRecord> = HashMap::new();
    updater.received(&rows, &existing).unwrap();

    let (cvr_final, _stats) = updater.flush(connect_time as i64, now, now);
    let ops = updater.base.drain_store_ops();
    let row_op_count = ops
        .iter()
        .filter(|op| {
            matches!(
                op,
                rust_cvr::cvr::StoreOp::PutRowRecord(_) | rust_cvr::cvr::StoreOp::DelRowRecord(_)
            )
        })
        .count();
    assert_eq!(row_op_count, N as usize, "all {N} rows are pending");
    store.apply_store_ops(ops);

    let expected_version = CVRVersion {
        state_version: "01".to_string(),
        config_version: None,
    };
    let stats = store
        .flush(&expected_version, &cvr_final, connect_time)
        .await
        .expect("flush")
        .expect("material flush");

    assert!(
        !stats.rows_flushed,
        "a {N}-row batch must defer, not write inline"
    );
    assert_eq!(stats.rows, 0, "no row statements ran in the flush tx");
    assert_eq!(stats.rows_deferred, N as usize);

    let count: i64 = sqlx::query_scalar(&format!(r#"SELECT count(*) FROM "{SCHEMA}".rows"#))
        .fetch_one(&pool)
        .await
        .expect("count rows");
    assert_eq!(
        count, 0,
        "cvr.rows must be untouched by the deferred flush — writing it inline is \
         exactly the ~1900-upsert stall in front of a large hydrate's pokeEnd"
    );
    let rows_version: Option<String> = sqlx::query_scalar(&format!(
        r#"SELECT "version" FROM "{SCHEMA}"."rowsVersion""#
    ))
    .fetch_optional(&pool)
    .await
    .expect("select rowsVersion");
    assert_ne!(
        rows_version.as_deref(),
        Some("02"),
        "rowsVersion must lag instances.version while rows are pending (that lag \
         is what `load()` retries on)"
    );

    // The deferred rows are not lost: `flush` already handed them to the cache
    // with `flushed=false`, which queued them and spawned the background flush.
    // `store.flushed()` is TS's `await this.#cvrStore.flushed(lc)`.
    tokio::time::timeout(std::time::Duration::from_secs(20), store.flushed())
        .await
        .expect("background flush timed out")
        .expect("background flush failed");

    let count: i64 = sqlx::query_scalar(&format!(r#"SELECT count(*) FROM "{SCHEMA}".rows"#))
        .fetch_one(&pool)
        .await
        .expect("count rows");
    assert_eq!(
        count, N,
        "the background flush persisted every deferred row"
    );
    let rows_version: String = sqlx::query_scalar(&format!(
        r#"SELECT "version" FROM "{SCHEMA}"."rowsVersion""#
    ))
    .fetch_one(&pool)
    .await
    .expect("select rowsVersion");
    assert_eq!(
        rows_version, "02",
        "the background flush advances rowsVersion to the CVR version"
    );
}

/// Pins the ATOMIC snapshot-and-clear of the pending write-back rows — port of
/// the synchronous block inside TS `#flush`'s `runTx` callback
/// (row-record-cache.ts:270-284): `rows = #pending.size`, `executeRowUpdates(tx,
/// rowsVersion, #pending, 'force')`, `#pending.clear()`. In TS that block cannot
/// interleave with `apply()`, so a batch applied WHILE the transaction is in
/// flight (the `'allow-defer'`-while-flushing path, and TS's own comment: "apply()
/// may have called while the transaction was committing") stays pending and is
/// written by the next loop iteration.
///
/// Non-vacuous: the previous rust loop cloned `pending`, ran the tx, and only
/// THEN cleared `pending` — wiping batch 2 without ever writing it, then bumping
/// `rowsVersion` to '03' over an empty map. With that shape this test fails on
/// the row-set assertion (`["a","b"]`, not `["a","b","c","d"]`).
///
/// The in-flight transaction is held open by a side transaction that row-locks
/// the CVR's `rowsVersion` row: the write-back's FIRST statement is the
/// `rowsVersion` upsert, so it parks on that lock until the side tx rolls back.
///
/// Gated on `TEST_CVR_PG_URI`; skips (passes) when unset.
#[tokio::test]
async fn write_back_keeps_rows_applied_while_a_flush_transaction_is_in_flight() {
    use rust_cvr::row_record_cache::RowRecordCache;
    use rust_cvr::schema::types::RowRecord;

    let _schema_guard = PG_SCHEMA.lock().await;
    let uri = match std::env::var("TEST_CVR_PG_URI") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!("SKIP write_back_keeps_rows_applied_while_in_flight: TEST_CVR_PG_URI unset");
            return;
        }
    };
    // write-back tx (parked) + side tx + load + test queries.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(6)
        .connect(&uri)
        .await
        .expect("connect to TEST_CVR_PG_URI");

    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE"#))
        .execute(&pool)
        .await
        .expect("drop schema");
    sqlx::raw_sql(include_str!("../agentic/parity/flush-schema.sql"))
        .execute(&pool)
        .await
        .expect("create schema");
    // The row the side transaction locks must exist before the write-back
    // upserts it.
    sqlx::query(&format!(
        r#"INSERT INTO "{SCHEMA}"."rowsVersion" ("clientGroupID", "version") VALUES ($1, '01')"#
    ))
    .bind(CVR_ID)
    .execute(&pool)
    .await
    .expect("seed rowsVersion");

    let cache = RowRecordCache::new(
        pool.clone(),
        SCHEMA.to_string(),
        CVR_ID.to_string(),
        rust_cvr::row_record_cache::DEFAULT_DEFERRED_THRESHOLD,
        Arc::new(|e: String| panic!("fail_service: {e}")),
        None,
    );
    cache.load().await.expect("load empty cache");

    let version = |s: &str| CVRVersion {
        state_version: s.to_string(),
        config_version: None,
    };
    let record = |id: &str, patch: &str| {
        let row_id = RowID {
            schema: String::new(),
            table: "t".to_string(),
            row_key: serde_json::json!({"id": id}).as_object().unwrap().clone(),
        };
        let rec = RowRecord {
            id: row_id.clone(),
            row_version: "r1".to_string(),
            patch_version: version(patch),
            ref_counts: Some(serde_json::from_value(serde_json::json!({"q1": 1})).unwrap()),
        };
        (row_id, Some(rec))
    };

    // Side transaction: hold the rowsVersion row lock.
    let mut side = pool.begin().await.expect("side tx");
    sqlx::query(&format!(
        r#"SELECT 1 FROM "{SCHEMA}"."rowsVersion" WHERE "clientGroupID" = $1 FOR UPDATE"#
    ))
    .bind(CVR_ID)
    .execute(&mut *side)
    .await
    .expect("lock rowsVersion row");

    // Batch 1: spawns the write-back, whose transaction parks on the lock.
    cache
        .apply(
            vec![record("a", "02"), record("b", "02")],
            version("02"),
            false,
        )
        .await
        .expect("apply batch 1");
    assert!(
        cache.has_pending_updates().await,
        "write-back must be in flight"
    );

    // Wait until the write-back backend is actually parked on the row lock, so
    // batch 2 provably lands mid-transaction rather than before it started.
    let parked_sql = r#"SELECT count(*) FROM pg_stat_activity
        WHERE wait_event_type = 'Lock' AND state = 'active' AND query ILIKE '%rowsVersion%'"#;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let parked: i64 = sqlx::query_scalar(parked_sql)
            .fetch_one(&pool)
            .await
            .expect("pg_stat_activity");
        if parked >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "write-back never parked on the rowsVersion lock"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    // Batch 2 lands while batch 1's transaction is in flight.
    cache
        .apply(
            vec![record("c", "03"), record("d", "03")],
            version("03"),
            false,
        )
        .await
        .expect("apply batch 2");

    // Release the lock; the loop commits batch 1, then batch 2.
    side.rollback().await.expect("release lock");
    cache.flushed().await.expect("write-back completes");

    let ids: Vec<String> = sqlx::query_scalar(&format!(
        r#"SELECT "rowKey"->>'id' FROM "{SCHEMA}".rows WHERE "clientGroupID" = $1 ORDER BY 1"#
    ))
    .bind(CVR_ID)
    .fetch_all(&pool)
    .await
    .expect("select rows");
    assert_eq!(
        ids,
        vec!["a", "b", "c", "d"],
        "rows applied during the in-flight write-back transaction must be persisted"
    );
    let rv: String = sqlx::query_scalar(&format!(
        r#"SELECT version FROM "{SCHEMA}"."rowsVersion" WHERE "clientGroupID" = $1"#
    ))
    .bind(CVR_ID)
    .fetch_one(&pool)
    .await
    .expect("select rowsVersion");
    assert_eq!(rv, "03", "rowsVersion advances to the last applied version");
}

/// TS `runTx` (run-transaction.ts:37-47) runs `SET LOCAL statement_timeout =
/// 0` inside every transaction, so a provider-level `statement_timeout` never
/// cancels a CVR flush that is waiting on the instance row lock (the
/// `#checkVersionAndOwnership` `SELECT ... FOR UPDATE`). This pins the same
/// for `flush_internal`: with a 50 ms session timeout on the store's pool and
/// the instance row locked by a side transaction for ~400 ms, the flush must
/// wait and succeed instead of failing with "canceling statement due to
/// statement timeout" (which it did before the `SET LOCAL` was ported).
///
/// Gated on `TEST_CVR_PG_URI`; skips (passes) when unset.
#[tokio::test]
async fn config_flush_disables_the_session_statement_timeout_like_ts_run_tx() {
    let _schema_guard = PG_SCHEMA.lock().await;
    let uri = match std::env::var("TEST_CVR_PG_URI") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!(
                "SKIP config_flush_disables_the_session_statement_timeout: TEST_CVR_PG_URI unset"
            );
            return;
        }
    };
    // Setup + side transaction: no timeout.
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(3)
        .connect(&uri)
        .await
        .expect("connect to TEST_CVR_PG_URI");
    // The store's pool: every session starts with a 50 ms statement timeout,
    // as a managed provider might configure at the database level.
    let store_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET statement_timeout = 50")
                    .execute(conn)
                    .await
                    .map(|_| ())
            })
        })
        .connect(&uri)
        .await
        .expect("connect store pool");

    let golden: Value = serde_json::from_str(include_str!("../agentic/parity/flush-fixture.json"))
        .expect("flush-fixture.json");
    let connect_time = golden["connectTime"].as_f64().expect("connectTime");
    let now = golden["now"].as_i64().expect("now");
    let sc = &golden["scenarios"].as_array().expect("scenarios")[0];

    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE"#))
        .execute(&admin)
        .await
        .expect("drop schema");
    sqlx::raw_sql(include_str!("../agentic/parity/flush-schema.sql"))
        .execute(&admin)
        .await
        .expect("create schema");
    sqlx::raw_sql(sc["baseSeedSql"].as_str().expect("baseSeedSql"))
        .execute(&admin)
        .await
        .expect("seed");

    let mut store = CVRStoreHandle::new(
        store_pool.clone(),
        SCHEMA.to_string(),
        CVR_ID.to_string(),
        TASK_ID.to_string(),
    );
    let loaded = store.load(connect_time).await.expect("load");
    let mut updater =
        CVRQueryDrivenUpdater::new(loaded.cvr, "02".to_string(), "01".to_string(), None);
    let executed: Vec<(String, String)> = sc["tracked"]["executed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e[0].as_str().unwrap().to_string(),
                e[1].as_str().unwrap().to_string(),
            )
        })
        .collect();
    let executed_refs: Vec<(&str, &str)> = executed
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    updater.track_queries(&executed_refs, &[]);
    // The scenario's received rows make the flush material (a config-only
    // no-op flush never opens a transaction, cvr-store.ts:1088-1097).
    let mut rows: HashMap<String, (RowID, RowUpdate)> = HashMap::new();
    for r in sc["received"].as_array().unwrap() {
        let id = row_id_from_json(&r["id"]);
        let id_str = rust_cvr::row_key::row_id_string(&id);
        let ref_counts: RefCounts =
            serde_json::from_value(r["refCounts"].clone()).expect("refCounts");
        rows.insert(
            id_str,
            (
                id,
                RowUpdate {
                    version: Some("02".to_string()),
                    contents: Some(Arc::new(r["contents"].clone())),
                    ref_counts,
                },
            ),
        );
    }
    let existing: HashMap<String, rust_cvr::schema::types::RowRecord> = HashMap::new();
    updater.received(&rows, &existing).unwrap();
    let (cvr_final, _stats) = updater.flush(connect_time as i64, now, now);
    let ops = updater.base.drain_store_ops();
    store.apply_store_ops(ops);
    let expected_version = CVRVersion {
        state_version: "01".to_string(),
        config_version: None,
    };

    // Side transaction: hold the instance row lock for ~400 ms, then release.
    let mut side = admin.begin().await.expect("side tx");
    sqlx::query(&format!(
        r#"SELECT 1 FROM "{SCHEMA}".instances WHERE "clientGroupID" = $1 FOR UPDATE"#
    ))
    .bind(CVR_ID)
    .execute(&mut *side)
    .await
    .expect("lock instance row");
    let release = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        side.rollback().await.expect("release lock");
    });

    let started = std::time::Instant::now();
    let result = store
        .flush(&expected_version, &cvr_final, connect_time)
        .await;
    let waited = started.elapsed();
    release.await.unwrap();
    assert!(
        waited >= std::time::Duration::from_millis(300),
        "the flush must have blocked on the locked instance row (waited {waited:?}) — otherwise this test proves nothing"
    );
    assert!(
        result.is_ok(),
        "flush must wait for the lock instead of being cancelled by the session statement_timeout: {:?}",
        result.err()
    );
}
