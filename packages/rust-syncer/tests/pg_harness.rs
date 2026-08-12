//! Postgres-backed integration tests for the CVR store round-trip.
//!
//! Gated on `TEST_CVR_PG_URI` (e.g.
//! `postgres://user@localhost:5432/rust_syncer_test`); when unset the tests
//! print a skip notice and pass, so `cargo test` stays green without a DB.
//!
//! These exercise `CVRStoreHandle` flush + reload against a live Postgres using
//! the authoritative CVR schema (ported from
//! `zero-cache/src/services/view-syncer/schema/cvr.ts`). This harness already
//! surfaced and fixed three real store bugs that were invisible without PG:
//!   1. pool creation needs an ambient Tokio runtime (set_cvr_store enters it),
//!   2. `lastActive`/`grantedAt` are TIMESTAMPTZ — the store now converts
//!      epoch-ms ↔ timestamp instead of binding raw f64,
//!   3. the `rows` FK to `rowsVersion` requires a `rowsVersion` upsert (added),
//!      and `apply(flushed=true)` must advance `flushed_rows_version`.
//!
//! CATCHUP DEADLOCK — ROOT-CAUSED AND FIXED. The reported hang in the catchup
//! READ path was NOT a threading/`block_on` issue. Two chained bugs in the
//! row-record-cache write-back flush caused it:
//!   1. `flush_one_iteration` bulk insert used `json_to_recordset($1)`, but sqlx
//!      binds `serde_json::Value` as JSONB, so Postgres raised
//!      "function json_to_recordset(jsonb) does not exist" and the flush failed.
//!      Fixed by casting the param: `json_to_recordset($1::json)` (keeps the TS
//!      function name; jsonb→json cast is valid).
//!   2. On a flush failure the background task set `is_flushing=false` and
//!      returned WITHOUT waking `flushed()` awaiters, so
//!      `catchup_row_patches`'s `await flushed()` blocked forever
//!      (`pending_rows_version` stayed ahead of `flushed_rows_version`). Fixed by
//!      recording `flush_error` and notifying the watch channel, so `flushed()`
//!      returns the error instead of hanging.
//! Regression coverage: `pg_repro_catchup_from_cg_thread` (the exact CG-thread
//! model: non-worker thread + `Handle::block_on` + real flush + catchup) and
//! `pg_repro_failed_flush_does_not_hang` (liveness on flush failure).

use std::collections::BTreeMap;

use rust_cvr::store::CVRStoreHandle;
use rust_cvr::types::{CVR, DesiredQuerySpec, ShardID};
use rust_cvr::updater::CVRConfigDrivenUpdater;
use rust_cvr::version::CVRVersion;

fn pg_uri() -> Option<String> {
    std::env::var("TEST_CVR_PG_URI")
        .ok()
        .filter(|s| !s.is_empty())
}

/// The CVR schema DDL for `schema`, ported from `cvr.ts` (column-faithful to
/// what `CVRStoreHandle` reads/writes).
fn cvr_ddl(schema: &str) -> String {
    format!(
        r#"
DROP SCHEMA IF EXISTS "{schema}" CASCADE;
CREATE SCHEMA "{schema}";

CREATE TABLE "{schema}".instances (
  "clientGroupID"  TEXT PRIMARY KEY,
  "version"        TEXT NOT NULL,
  "lastActive"     TIMESTAMPTZ NOT NULL,
  "ttlClock"       DOUBLE PRECISION NOT NULL DEFAULT 0,
  "replicaVersion" TEXT,
  "owner"          TEXT,
  "grantedAt"      TIMESTAMPTZ,
  "clientSchema"   JSONB,
  "profileID"      TEXT,
  "deleted"        BOOL DEFAULT FALSE
);

CREATE TABLE "{schema}".clients (
  "clientGroupID" TEXT,
  "clientID"      TEXT,
  PRIMARY KEY ("clientGroupID", "clientID"),
  CONSTRAINT fk_clients_client_group FOREIGN KEY ("clientGroupID")
    REFERENCES "{schema}".instances ("clientGroupID") ON DELETE CASCADE
);

CREATE TABLE "{schema}".queries (
  "clientGroupID"         TEXT,
  "queryHash"             TEXT,
  "clientAST"             JSONB,
  "queryName"             TEXT,
  "queryArgs"             JSON,
  "patchVersion"          TEXT,
  "transformationHash"    TEXT,
  "transformationVersion" TEXT,
  "internal"              BOOL,
  "deleted"               BOOL,
  "rowSetSignature"       TEXT,
  PRIMARY KEY ("clientGroupID", "queryHash"),
  CONSTRAINT fk_queries_client_group FOREIGN KEY ("clientGroupID")
    REFERENCES "{schema}".instances ("clientGroupID") ON DELETE CASCADE
);

CREATE TABLE "{schema}".desires (
  "clientGroupID"   TEXT,
  "clientID"        TEXT,
  "queryHash"       TEXT,
  "patchVersion"    TEXT NOT NULL,
  "deleted"         BOOL,
  "ttl"             INTERVAL,
  "ttlMs"           DOUBLE PRECISION,
  "inactivatedAt"   TIMESTAMPTZ,
  "inactivatedAtMs" DOUBLE PRECISION,
  PRIMARY KEY ("clientGroupID", "clientID", "queryHash"),
  CONSTRAINT fk_desires_query FOREIGN KEY ("clientGroupID", "queryHash")
    REFERENCES "{schema}".queries ("clientGroupID", "queryHash") ON DELETE CASCADE
);

CREATE TABLE "{schema}"."rowsVersion" (
  "clientGroupID" TEXT PRIMARY KEY,
  "version"       TEXT NOT NULL
);

CREATE TABLE "{schema}".rows (
  "clientGroupID" TEXT,
  "schema"        TEXT,
  "table"         TEXT,
  "rowKey"        JSONB,
  "rowVersion"    TEXT NOT NULL,
  "patchVersion"  TEXT NOT NULL,
  "refCounts"     JSONB,
  PRIMARY KEY ("clientGroupID", "schema", "table", "rowKey"),
  CONSTRAINT fk_rows_client_group FOREIGN KEY ("clientGroupID")
    REFERENCES "{schema}"."rowsVersion" ("clientGroupID") ON DELETE CASCADE
);
"#
    )
}

fn empty_cvr(id: &str) -> CVR {
    CVR {
        id: id.to_string(),
        version: CVRVersion {
            state_version: "00".to_string(),
            config_version: None,
        },
        last_active: 0,
        ttl_clock: 0,
        replica_version: Some("01".to_string()),
        clients: BTreeMap::new(),
        queries: BTreeMap::new(),
        client_schema: None,
        profile_id: None,
    }
}

/// Flushing a config-driven CVR to Postgres persists the instance + client +
/// desired query, and reloading returns them. Exercises the `lastActive`
/// TIMESTAMPTZ conversion + the runtime-context pool creation end to end.
#[test]
fn pg_cvr_store_flush_and_reload_roundtrip() {
    let Some(uri) = pg_uri() else {
        eprintln!("SKIP pg_cvr_store_flush_and_reload_roundtrip: TEST_CVR_PG_URI not set");
        return;
    };
    let schema = "cvr_store_roundtrip";
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };

    rt.block_on(async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&uri)
            .await
            .expect("connect");
        sqlx::raw_sql(&cvr_ddl(schema))
            .execute(&pool)
            .await
            .expect("ddl");

        // Config pass: record client1 + a desired query q1.
        let mut cfg = CVRConfigDrivenUpdater::new(empty_cvr("cg1"), shard.clone());
        cfg.ensure_client("client1");
        let _ = cfg.put_desired_queries(
            "client1",
            &[DesiredQuerySpec {
                hash: "q1".to_string(),
                ast: Some(serde_json::json!({"table": "issue"})),
                name: None,
                args: None,
                ttl: None,
            }],
        );
        let (cfg_cvr, _stats) = cfg.flush(0, 0, 0);
        let ops = cfg.base.drain_store_ops();

        // Persist to PG through the store.
        let mut store = CVRStoreHandle::new(
            pool.clone(),
            schema.to_string(),
            "cg1".to_string(),
            "task-0".to_string(),
        );
        store.apply_store_ops(ops);
        store
            .flush(&cfg_cvr.version, &cfg_cvr, 0.0)
            .await
            .expect("store flush");

        // The instance persisted with a real timestamp (the epoch-ms → TIMESTAMPTZ
        // conversion round-trips, no type error).
        let inst: (i64,) = sqlx::query_as(&format!(
            r#"SELECT count(*) FROM "{schema}".instances WHERE "clientGroupID" = 'cg1'"#
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(inst.0, 1);

        // Reload via a fresh store handle — the client + desired query survive.
        let mut store2 = CVRStoreHandle::new(
            pool.clone(),
            schema.to_string(),
            "cg1".to_string(),
            "task-0".to_string(),
        );
        let loaded = store2.load(0.0).await.expect("load");
        assert!(!loaded.is_new, "reloaded an existing CVR");
        assert!(
            loaded.cvr.clients.contains_key("client1"),
            "client reloaded"
        );
        assert!(
            loaded.cvr.queries.contains_key("q1"),
            "desired query reloaded"
        );

        sqlx::raw_sql(&format!(r#"DROP SCHEMA IF EXISTS "{schema}" CASCADE;"#))
            .execute(&pool)
            .await
            .unwrap();
    });
}

/// A row leaves the CVR either as an explicit del OR as a put with
/// `refCounts = null` (the tombstone form). BOTH must DELETE the row from the
/// `rows` table. The old store skipped `None` deletes entirely and wrote
/// tombstones as `refCounts = NULL` upserts, leaking dead rows and dropping
/// real deletions. Mirrors TS `executeRowUpdates`.
#[test]
fn pg_cvr_store_deletes_rows() {
    use rust_cvr::row_key::RowID;
    use rust_cvr::types::{RowRecord, StoreOp};

    let Some(uri) = pg_uri() else {
        eprintln!("SKIP pg_cvr_store_deletes_rows: TEST_CVR_PG_URI not set");
        return;
    };
    let schema = "cvr_store_deletes";
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };

    let mk_row = |key: &str, refs: Option<i64>| -> RowRecord {
        let mut row_key = serde_json::Map::new();
        row_key.insert(
            "id".to_string(),
            serde_json::Value::String(key.to_string()),
        );
        RowRecord {
            id: RowID {
                schema: "public".to_string(),
                table: "issue".to_string(),
                row_key,
            },
            row_version: "rv1".to_string(),
            patch_version: CVRVersion {
                state_version: "01".to_string(),
                config_version: None,
            },
            ref_counts: refs.map(|n| BTreeMap::from([("q1".to_string(), n)])),
        }
    };

    rt.block_on(async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&uri)
            .await
            .expect("connect");
        sqlx::raw_sql(&cvr_ddl(schema)).execute(&pool).await.unwrap();

        // Create the CVR instance so the rows can be persisted.
        let mut cfg = CVRConfigDrivenUpdater::new(empty_cvr("cg1"), shard.clone());
        cfg.ensure_client("client1");
        let (cvr, _) = cfg.flush(0, 0, 0);
        let ops = cfg.base.drain_store_ops();
        let mut store = CVRStoreHandle::new(
            pool.clone(),
            schema.to_string(),
            "cg1".to_string(),
            "task-0".to_string(),
        );
        store.apply_store_ops(ops);
        store.flush(&cvr.version, &cvr, 0.0).await.expect("flush");

        let row_count = |pool: sqlx::PgPool| async move {
            let c: (i64,) = sqlx::query_as(&format!(
                r#"SELECT count(*) FROM "{schema}".rows WHERE "clientGroupID" = 'cg1'"#
            ))
            .fetch_one(&pool)
            .await
            .unwrap();
            c.0
        };

        // Put two referenced rows A and B.
        store.apply_store_ops(vec![
            StoreOp::PutRowRecord(mk_row("A", Some(1))),
            StoreOp::PutRowRecord(mk_row("B", Some(1))),
        ]);
        store.flush(&cvr.version, &cvr, 0.0).await.expect("flush puts");
        assert_eq!(row_count(pool.clone()).await, 2, "both rows persisted");

        // Delete A via a tombstone (put refCounts = None); delete B via an
        // explicit del. Both must remove the row from the `rows` table.
        store.apply_store_ops(vec![
            StoreOp::PutRowRecord(mk_row("A", None)),
            StoreOp::DelRowRecord(mk_row("B", None).id),
        ]);
        store
            .flush(&cvr.version, &cvr, 0.0)
            .await
            .expect("flush deletes");
        assert_eq!(
            row_count(pool.clone()).await,
            0,
            "both the tombstone and the explicit del removed their rows"
        );

        sqlx::raw_sql(&format!(r#"DROP SCHEMA IF EXISTS "{schema}" CASCADE;"#))
            .execute(&pool)
            .await
            .unwrap();
    });
}

/// Optimistic concurrency + ownership guard (port of TS `#checkVersionAndOwnership`):
/// a flush must be REJECTED if the on-disk CVR version moved since this store
/// last saw it, or if another task now owns the CVR. Without it two syncers
/// clobber each other's CVR.
#[test]
fn pg_cvr_store_guard_rejects_concurrent_and_owned() {
    use rust_cvr::store::CVRStoreError;

    let Some(uri) = pg_uri() else {
        eprintln!("SKIP pg_cvr_store_guard: TEST_CVR_PG_URI not set");
        return;
    };
    let schema = "cvr_store_guard";
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };

    rt.block_on(async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&uri)
            .await
            .expect("connect");
        sqlx::raw_sql(&cvr_ddl(schema)).execute(&pool).await.unwrap();

        // Store A creates the instance (version X).
        let mut cfg = CVRConfigDrivenUpdater::new(empty_cvr("cg1"), shard.clone());
        cfg.ensure_client("client1");
        let (cvr_x, _) = cfg.flush(0, 0, 0);
        let ops = cfg.base.drain_store_ops();
        let mut a = CVRStoreHandle::new(
            pool.clone(),
            schema.to_string(),
            "cg1".to_string(),
            "task-A".to_string(),
        );
        a.apply_store_ops(ops);
        a.flush(&cvr_x.version, &cvr_x, 0.0).await.expect("A create");

        // Store B loads the CVR at version X.
        let mut b = CVRStoreHandle::new(
            pool.clone(),
            schema.to_string(),
            "cg1".to_string(),
            "task-A".to_string(),
        );
        assert!(!b.load(0.0).await.expect("B load").is_new);

        // Store A advances the CVR to a higher version Y.
        let mut cvr_y = cvr_x.clone();
        cvr_y.version = CVRVersion {
            state_version: "09".to_string(),
            config_version: None,
        };
        a.flush(&cvr_y.version, &cvr_y, 0.0).await.expect("A advance");

        // Store B, still at X, must be rejected as concurrently modified.
        let err = b
            .flush(&cvr_y.version, &cvr_y, 0.0)
            .await
            .expect_err("B flush must be rejected");
        assert!(
            matches!(err, CVRStoreError::ConcurrentModification { .. }),
            "expected ConcurrentModification, got {err:?}"
        );

        // Ownership: hand the CVR to another task, granted just now (> A's last
        // connect time of 0). A's next flush must be rejected as not-owner.
        sqlx::raw_sql(&format!(
            r#"UPDATE "{schema}".instances
               SET "owner" = 'task-OTHER', "grantedAt" = now()
               WHERE "clientGroupID" = 'cg1'"#
        ))
        .execute(&pool)
        .await
        .unwrap();
        // A is at version Y (matches DB), so only the ownership check can fire.
        let err = a
            .flush(&cvr_y.version, &cvr_y, 0.0)
            .await
            .expect_err("A flush must be rejected on ownership");
        assert!(
            matches!(err, CVRStoreError::OwnershipError { .. }),
            "expected OwnershipError, got {err:?}"
        );

        sqlx::raw_sql(&format!(r#"DROP SCHEMA IF EXISTS "{schema}" CASCADE;"#))
            .execute(&pool)
            .await
            .unwrap();
    });
}

/// REPRODUCTION: isolate the catchup read path exactly as the CG thread drives
/// it — a non-worker OS thread calling `Handle::block_on`. If this hangs, the
/// deadlock is inside `RowRecordCache` (flush/flushed/catchup), independent of
/// the full engine. Uses the multi-thread runtime like `main.rs`.
#[test]
fn pg_repro_catchup_from_cg_thread() {
    use rust_cvr::row_key::RowID;
    use rust_cvr::row_record_cache::{RowRecord, RowRecordCache};
    use std::collections::HashMap;
    use std::sync::Arc;

    let Some(uri) = pg_uri() else {
        eprintln!("SKIP pg_repro_catchup_from_cg_thread: TEST_CVR_PG_URI not set");
        return;
    };
    let schema = "cvr_repro_catchup";

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = rt.handle().clone();

    // DDL + a matching instances row (catchup's checkVersion reads it).
    let v1 = CVRVersion {
        state_version: "01".to_string(),
        config_version: None,
    };
    let v1s = rust_cvr::version::version_string(&v1);
    rt.block_on(async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(&uri)
            .await
            .unwrap();
        sqlx::raw_sql(&cvr_ddl(schema)).execute(&pool).await.unwrap();
        sqlx::query(&format!(
            r#"INSERT INTO "{schema}".instances ("clientGroupID","version","lastActive") VALUES ('cg1',$1, NOW())"#
        ))
        .bind(&v1s)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(&format!(
            r#"INSERT INTO "{schema}"."rowsVersion" ("clientGroupID","version") VALUES ('cg1',$1)"#
        ))
        .bind(&v1s)
        .execute(&pool)
        .await
        .unwrap();
    });

    // Build the cache on a runtime-context (pool reaper needs it).
    let cache = {
        let _g = handle.enter();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy(&uri)
            .unwrap();
        let fail: rust_cvr::row_record_cache::FailCallback =
            Arc::new(|e: String| eprintln!("cache: {e}"));
        Arc::new(RowRecordCache::new(
            pool,
            schema.to_string(),
            "cg1".to_string(),
            100,
            fail,
            None,
        ))
    };

    // Drive load + apply(flushed=false) + catchup from a NON-worker std::thread,
    // exactly like the CG thread. A 30s watchdog turns a deadlock into a failure.
    let cache2 = cache.clone();
    let h2 = handle.clone();
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done2 = done.clone();
    let worker = std::thread::spawn(move || {
        h2.block_on(async {
            cache2.load().await.unwrap();
            let mut key = serde_json::Map::new();
            key.insert("id".to_string(), serde_json::json!(1));
            let rec = RowRecord {
                id: RowID {
                    schema: "public".to_string(),
                    table: "issue".to_string(),
                    row_key: key.clone(),
                },
                row_version: "r1".to_string(),
                patch_version: v1.clone(),
                ref_counts: Some(HashMap::from([("q1".to_string(), 1)])),
            };
            let id = rec.id.clone();
            // flushed=false → spawns the background flush task.
            cache2
                .apply(vec![(id, Some(rec))], v1.clone(), false)
                .await
                .unwrap();

            // Now catch up — this awaits flushed() then streams rows.
            let mut cursor = cache2
                .catchup_row_patches(None, &v1, &v1, &[])
                .await
                .expect("catchup begin");
            let mut n = 0;
            while let Some(page) = cursor.next_page().await.expect("page") {
                n += page.len();
            }
            n
        })
    });

    let watchdog = std::thread::spawn(move || {
        for _ in 0..300 {
            if done2.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        eprintln!("DEADLOCK: catchup did not finish within 30s");
        std::process::abort();
    });

    let n = worker.join().unwrap();
    done.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = watchdog.join();
    assert_eq!(n, 1, "expected 1 catchup row");

    rt.block_on(async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(&uri)
            .await
            .unwrap();
        sqlx::raw_sql(&format!(r#"DROP SCHEMA IF EXISTS "{schema}" CASCADE;"#))
            .execute(&pool)
            .await
            .unwrap();
    });
}

/// LIVENESS: a background flush that FAILS must surface as an error from
/// `flushed()` / `catchup_row_patches`, not block forever. Regression for the
/// deadlock where a failed flush left `pending_rows_version > flushed_rows_version`
/// with no wakeup. Here the `rows` table has an extra NOT NULL column the bulk
/// INSERT never populates, so the flush fails while `load()` still succeeds.
#[test]
fn pg_repro_failed_flush_does_not_hang() {
    use rust_cvr::row_key::RowID;
    use rust_cvr::row_record_cache::{RowRecord, RowRecordCache};
    use std::collections::HashMap;
    use std::sync::Arc;

    let Some(uri) = pg_uri() else {
        eprintln!("SKIP pg_repro_failed_flush_does_not_hang: TEST_CVR_PG_URI not set");
        return;
    };
    let schema = "cvr_repro_failflush";
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = rt.handle().clone();

    let v1 = CVRVersion {
        state_version: "01".to_string(),
        config_version: None,
    };

    // Full schema, but `rows` gets an extra NOT NULL column the bulk INSERT
    // never populates → flush fails; `load()` (SELECT of the 7 known columns)
    // still succeeds.
    rt.block_on(async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(&uri)
            .await
            .unwrap();
        sqlx::raw_sql(&cvr_ddl(schema))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql(&format!(
            r#"ALTER TABLE "{schema}".rows ADD COLUMN "mustFail" TEXT NOT NULL;"#
        ))
        .execute(&pool)
        .await
        .unwrap();
    });

    let cache = {
        let _g = handle.enter();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy(&uri)
            .unwrap();
        let fail: rust_cvr::row_record_cache::FailCallback = Arc::new(|_e: String| {});
        Arc::new(RowRecordCache::new(
            pool,
            schema.to_string(),
            "cg1".to_string(),
            100,
            fail,
            None,
        ))
    };

    let cache2 = cache.clone();
    let h2 = handle.clone();
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done2 = done.clone();
    let worker = std::thread::spawn(move || {
        h2.block_on(async {
            cache2.load().await.unwrap();
            let mut key = serde_json::Map::new();
            key.insert("id".to_string(), serde_json::json!(1));
            let rec = RowRecord {
                id: RowID {
                    schema: "public".to_string(),
                    table: "issue".to_string(),
                    row_key: key,
                },
                row_version: "r1".to_string(),
                patch_version: v1.clone(),
                ref_counts: Some(HashMap::from([("q1".to_string(), 1)])),
            };
            let id = rec.id.clone();
            cache2
                .apply(vec![(id, Some(rec))], v1.clone(), false)
                .await
                .unwrap();
            // flushed() must return an Err (flush failed), not hang.
            cache2.flushed().await
        })
    });

    let watchdog = std::thread::spawn(move || {
        for _ in 0..300 {
            if done2.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        eprintln!("DEADLOCK: flushed() did not return after a failed flush within 30s");
        std::process::abort();
    });

    let res = worker.join().unwrap();
    done.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = watchdog.join();
    assert!(
        res.is_err(),
        "flushed() must surface the flush failure, got {res:?}"
    );

    rt.block_on(async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(&uri)
            .await
            .unwrap();
        sqlx::raw_sql(&format!(r#"DROP SCHEMA IF EXISTS "{schema}" CASCADE;"#))
            .execute(&pool)
            .await
            .unwrap();
    });
}

/// Full engine hydrate + catchup over PG. The catchup deadlock this once
/// tracked is root-caused and fixed (see module docs + `pg_repro_catchup_from_cg_thread`
/// / `pg_repro_failed_flush_does_not_hang`, which drive the exact
/// `catchup_row_patches` → `flushed()` → `flush_loop` machinery through the same
/// non-worker-thread + `Handle::block_on` model the CG thread uses).
///
/// Remaining as a follow-up: a full `SyncEngine::config_and_hydrate` test that
/// also seeds the IVM pipeline with rows (so `get_row` returns catchup content)
/// needs a test-only source-seeding hook or a real SQLite replica fixture —
/// scaffolding beyond the catchup-deadlock fix. Left `#[ignore]`d until that
/// fixture exists.
#[test]
#[ignore = "needs IVM source-seeding fixture; catchup deadlock itself fixed + covered by pg_repro_* tests"]
fn pg_engine_hydrate_and_catchup() {}
