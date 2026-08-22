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
//!
//! Regression coverage: `pg_repro_catchup_from_cg_thread` (the exact CG-thread
//! model: non-worker thread + `Handle::block_on` + real flush + catchup) and
//! `pg_repro_failed_flush_does_not_hang` (liveness on flush failure).

use std::collections::BTreeMap;

use rust_cvr::store::CVRStoreHandle;
use rust_cvr::types::{CVR, DesiredQuerySpec, ShardID};
use rust_cvr::updater::CVRConfigDrivenUpdater;
use rust_cvr::version::{CVRVersion, EMPTY_CVR_VERSION};

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

/// A minimal MATERIAL buffered write (a client insert). `store.flush` only opens
/// its transaction — and thus only runs the version/ownership guard — when there
/// is a material change to write (TS `#flush` short-circuits an empty flush).
/// Tests that exercise the guard must therefore queue a real change first, as a
/// production flush always would.
fn insert_client_op(id: &str) -> rust_cvr::types::StoreOp {
    rust_cvr::types::StoreOp::InsertClient(rust_cvr::types::ClientRecord {
        id: id.to_string(),
        desired_query_ids: vec![],
    })
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
            .flush(&EMPTY_CVR_VERSION, &cfg_cvr, 0.0)
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

/// Row-flush semantics, mirroring TS `executeRowUpdates`:
/// - an explicit del (`None`) hard-DELETEs the row from the `rows` table;
/// - a tombstone (put with `refCounts = null`) is UPSERTED and KEPT — it
///   carries the deletion's patchVersion, which is what catch-up reads to emit
///   row DELs to reconnecting clients. (Hard-deleting tombstones starved
///   catch-up of DELs: a reconnecting client never learned a row was removed
///   while it was away.) Tombstones stay invisible to the row-record cache,
///   whose load filters `refCounts IS NOT NULL`.
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
        row_key.insert("id".to_string(), serde_json::Value::String(key.to_string()));
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
        sqlx::raw_sql(&cvr_ddl(schema))
            .execute(&pool)
            .await
            .unwrap();

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
        store
            .flush(&EMPTY_CVR_VERSION, &cvr, 0.0)
            .await
            .expect("flush");

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
        store
            .flush(&cvr.version, &cvr, 0.0)
            .await
            .expect("flush puts");
        assert_eq!(row_count(pool.clone()).await, 2, "both rows persisted");

        // Tombstone A (put refCounts = None); explicitly del B. B's row must be
        // gone; A's row must REMAIN as a refCounts-NULL tombstone (the catch-up
        // DEL source), invisible to the cache's refCounts-IS-NOT-NULL load.
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
            1,
            "explicit del removed B; A remains as a tombstone"
        );
        let tombstones: (i64,) = sqlx::query_as(&format!(
            r#"SELECT count(*) FROM "{schema}".rows
               WHERE "clientGroupID" = 'cg1' AND "refCounts" IS NULL"#
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(tombstones.0, 1, "A's tombstone carries refCounts = NULL");

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
        sqlx::raw_sql(&cvr_ddl(schema))
            .execute(&pool)
            .await
            .unwrap();

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
        a.flush(&EMPTY_CVR_VERSION, &cvr_x, 0.0)
            .await
            .expect("A create");

        // Store B loads the CVR at version X.
        let mut b = CVRStoreHandle::new(
            pool.clone(),
            schema.to_string(),
            "cg1".to_string(),
            "task-A".to_string(),
        );
        assert!(!b.load(0.0).await.expect("B load").is_new);

        // Store A advances the CVR to a higher version Y (with a material write).
        let mut cvr_y = cvr_x.clone();
        cvr_y.version = CVRVersion {
            state_version: "09".to_string(),
            config_version: None,
        };
        a.apply_store_ops(vec![insert_client_op("client2")]);
        a.flush(&cvr_x.version, &cvr_y, 0.0)
            .await
            .expect("A advance");

        // Store B, still at X, must be rejected as concurrently modified (it too
        // has a material write, so it reaches the version guard inside the tx).
        b.apply_store_ops(vec![insert_client_op("client3")]);
        let err = b
            .flush(&cvr_x.version, &cvr_y, 0.0)
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
        // A is at version Y (matches DB), so only the ownership check can fire
        // (again with a material write so the flush reaches the guard).
        a.apply_store_ops(vec![insert_client_op("client4")]);
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

/// Rows-behind retry (port of TS `CVRStore.load`'s retry loop): if the rows
/// table lags the CVR instance version at load time (the previous owner hasn't
/// flushed its pending row writes yet), `load` must WAIT and retry rather than
/// return a CVR whose row records are inconsistent with its version. Once the
/// rows table catches up, the load succeeds.
#[test]
fn pg_cvr_store_load_retries_until_rows_catch_up() {
    let Some(uri) = pg_uri() else {
        eprintln!("SKIP pg_cvr_store_load_retries: TEST_CVR_PG_URI not set");
        return;
    };
    let schema = "cvr_store_rows_behind";
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
            .unwrap();

        // Create the CVR at version "01" (flush upserts rowsVersion = "01" too).
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
        a.flush(&EMPTY_CVR_VERSION, &cvr_x, 0.0)
            .await
            .expect("A create");

        // Simulate an in-flight advance: the instance version jumps to "05" but
        // the rows table still lags at "01" (pending row writes not flushed).
        sqlx::raw_sql(&format!(
            r#"UPDATE "{schema}".instances SET "version" = '05' WHERE "clientGroupID" = 'cg1'"#
        ))
        .execute(&pool)
        .await
        .unwrap();

        // Background: after ~700ms (past the first retry sleep) the rows table
        // catches up to "05". The load below must retry and then succeed.
        let bg_pool = pool.clone();
        let bg_schema = schema.to_string();
        let catcher = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(700)).await;
            sqlx::raw_sql(&format!(
                r#"UPDATE "{bg_schema}"."rowsVersion" SET "version" = '05' WHERE "clientGroupID" = 'cg1'"#
            ))
            .execute(&bg_pool)
            .await
            .unwrap();
        });

        // task-A owns the CVR, so no ownership error — only the rows-behind
        // check gates this load. It should block through ≥1 retry then return
        // the CVR at the caught-up version "05".
        let mut b = CVRStoreHandle::new(
            pool.clone(),
            schema.to_string(),
            "cg1".to_string(),
            "task-A".to_string(),
        );
        let loaded = b.load(0.0).await.expect("B load must eventually succeed");
        assert!(!loaded.is_new);
        assert_eq!(loaded.cvr.version.state_version, "05");
        catcher.await.unwrap();

        sqlx::raw_sql(&format!(r#"DROP SCHEMA IF EXISTS "{schema}" CASCADE;"#))
            .execute(&pool)
            .await
            .unwrap();
    });
}

/// Config catch-up must emit GOT-query patches (from the `queries` table, no
/// clientID) as well as per-client desire patches, so a reconnecting client can
/// rebuild its `gotQueriesPatch`. The old code read only `desires`.
#[test]
fn pg_cvr_store_catchup_includes_got_query_patches() {
    use rust_cvr::types::{Patch, QueryPatch};

    let Some(uri) = pg_uri() else {
        eprintln!("SKIP pg_cvr_store_catchup_got_query: TEST_CVR_PG_URI not set");
        return;
    };
    let schema = "cvr_store_catchup_got";
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&uri)
            .await
            .expect("connect");
        sqlx::raw_sql(&cvr_ddl(schema))
            .execute(&pool)
            .await
            .unwrap();

        // Seed: instance @ "05", a GOT query q1 @ "03", a desire for q1 @ "04".
        sqlx::raw_sql(&format!(
            r#"
            INSERT INTO "{schema}".instances ("clientGroupID","version","lastActive")
                VALUES ('cg1','05', now());
            INSERT INTO "{schema}".clients ("clientGroupID","clientID") VALUES ('cg1','c1');
            INSERT INTO "{schema}".queries ("clientGroupID","queryHash","patchVersion","deleted")
                VALUES ('cg1','q1','03', false);
            INSERT INTO "{schema}".desires
                ("clientGroupID","clientID","queryHash","patchVersion","deleted")
                VALUES ('cg1','c1','q1','04', false);
            "#
        ))
        .execute(&pool)
        .await
        .unwrap();

        let store = CVRStoreHandle::new(
            pool.clone(),
            schema.to_string(),
            "cg1".to_string(),
            "task-0".to_string(),
        );
        let up_to = CVRVersion {
            state_version: "05".to_string(),
            config_version: None,
        };
        let patches = store
            .catchup_config_patches(None, &up_to, &up_to)
            .await
            .expect("catchup");

        let got = patches.iter().any(|p| {
            matches!(&p.patch, Patch::Query(QueryPatch::Put { id, client_id })
                if id == "q1" && client_id.is_none())
        });
        let desire = patches.iter().any(|p| {
            matches!(&p.patch, Patch::Query(QueryPatch::Put { id, client_id })
                if id == "q1" && client_id.as_deref() == Some("c1"))
        });
        assert!(got, "expected a GOT-query patch (clientID null) for q1");
        assert!(desire, "expected a per-client desire patch for q1");

        sqlx::raw_sql(&format!(r#"DROP SCHEMA IF EXISTS "{schema}" CASCADE;"#))
            .execute(&pool)
            .await
            .unwrap();
    });
}

/// Loading a CVR grants the loading task ownership (gated on `lastConnectTime`),
/// so a task that connected earlier is refused and a task that connects later
/// takes over — which then makes the stale ex-owner's flush fail. Port of TS
/// `load`'s ownership grant + `#checkVersionAndOwnership`.
#[test]
fn pg_cvr_store_load_grants_and_transfers_ownership() {
    use rust_cvr::store::CVRStoreError;

    let Some(uri) = pg_uri() else {
        eprintln!("SKIP pg_cvr_store_load_ownership: TEST_CVR_PG_URI not set");
        return;
    };
    let schema = "cvr_store_ownership";
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    let store = |pool: &sqlx::PgPool, task: &str| {
        CVRStoreHandle::new(
            pool.clone(),
            schema.to_string(),
            "cg1".to_string(),
            task.to_string(),
        )
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
            .unwrap();

        // task-A creates the instance.
        let mut cfg = CVRConfigDrivenUpdater::new(empty_cvr("cg1"), shard.clone());
        cfg.ensure_client("c1");
        let (cvr, _) = cfg.flush(0, 0, 0);
        let ops = cfg.base.drain_store_ops();
        let mut a = store(&pool, "task-A");
        a.apply_store_ops(ops);
        // A connects at time 1000 → its flush grants ownership @1000.
        a.flush(&EMPTY_CVR_VERSION, &cvr, 1000.0)
            .await
            .expect("A create");
        a.load(1000.0).await.expect("A reload (still owner)");

        // task-B connected EARLIER (500) → A's live lease refuses it.
        let mut b = store(&pool, "task-B");
        let err = b.load(500.0).await.expect_err("B must be refused");
        assert!(
            matches!(err, CVRStoreError::OwnershipError { .. }),
            "expected OwnershipError for the earlier task, got {err:?}"
        );

        // task-C connected LATER (2000) → A's lease has lapsed → C takes over.
        let mut c = store(&pool, "task-C");
        c.load(2000.0).await.expect("C takes over");
        let owner: (Option<String>,) = sqlx::query_as(&format!(
            r#"SELECT "owner" FROM "{schema}".instances WHERE "clientGroupID" = 'cg1'"#
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(owner.0.as_deref(), Some("task-C"), "C now owns the CVR");

        // The stale ex-owner A can no longer flush (C's lease @2000 > A's 1000).
        // Queue a material write so the flush reaches the ownership guard.
        a.apply_store_ops(vec![insert_client_op("c2")]);
        let err = a
            .flush(&cvr.version, &cvr, 1000.0)
            .await
            .expect_err("stale ex-owner must be rejected");
        assert!(
            matches!(err, CVRStoreError::OwnershipError { .. }),
            "expected OwnershipError for the stale ex-owner, got {err:?}"
        );

        sqlx::raw_sql(&format!(r#"DROP SCHEMA IF EXISTS "{schema}" CASCADE;"#))
            .execute(&pool)
            .await
            .unwrap();
    });
}

/// On reload, each client's per-query desire state (ttl + inactivatedAt) must be
/// reconstructed onto the query's `client_state`. The old load populated only
/// `desired_query_ids`, so an INACTIVE (TTL-pending) desire reloaded as fully
/// active — the TTL scheduler could never see it to expire it. Port of TS
/// `loadCVR`, which rebuilds `clientState` from the desires rows.
#[test]
fn pg_cvr_store_reloads_desire_state_and_inactivation() {
    let Some(uri) = pg_uri() else {
        eprintln!("SKIP pg_cvr_store_reloads_desire_state: TEST_CVR_PG_URI not set");
        return;
    };
    let schema = "cvr_store_desire_state";
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
            .unwrap();

        // Client c1 desires q1 with an explicit 60s ttl.
        let mut cfg = CVRConfigDrivenUpdater::new(empty_cvr("cg1"), shard.clone());
        cfg.ensure_client("c1");
        let _ = cfg.put_desired_queries(
            "c1",
            &[DesiredQuerySpec {
                hash: "q1".to_string(),
                ast: Some(serde_json::json!({"table": "issue"})),
                name: None,
                args: None,
                ttl: Some(60_000),
            }],
        );
        let (cvr1, _) = cfg.flush(0, 0, 0);
        let ops = cfg.base.drain_store_ops();
        let mut store = CVRStoreHandle::new(
            pool.clone(),
            schema.to_string(),
            "cg1".to_string(),
            "task-0".to_string(),
        );
        store.apply_store_ops(ops);
        store
            .flush(&EMPTY_CVR_VERSION, &cvr1, 0.0)
            .await
            .expect("flush 1");

        // Reload: the desire state (ttl, active) is reconstructed on q1.
        let mut store2 = CVRStoreHandle::new(
            pool.clone(),
            schema.to_string(),
            "cg1".to_string(),
            "task-0".to_string(),
        );
        let loaded = store2.load(0.0).await.expect("load 1");
        let cs = loaded
            .cvr
            .queries
            .get("q1")
            .and_then(|q| q.client_state())
            .and_then(|m| m.get("c1"))
            .expect("q1 has c1 desire state");
        assert_eq!(cs.ttl, 60_000, "ttl round-trips");
        assert_eq!(cs.inactivated_at, None, "still active");

        // Mark q1 inactive (TTL-pending) at ttlClock 1234, flush.
        let mut cfg2 = CVRConfigDrivenUpdater::new(loaded.cvr, shard.clone());
        let _ = cfg2.mark_desired_queries_as_inactive("c1", &["q1".to_string()], 1234);
        let (cvr2, _) = cfg2.flush(0, 0, 1234);
        let ops2 = cfg2.base.drain_store_ops();
        store.apply_store_ops(ops2);
        store
            .flush(&cvr1.version, &cvr2, 0.0)
            .await
            .expect("flush 2");

        // Reload: the inactivation timestamp survives (not reloaded as active).
        let mut store3 = CVRStoreHandle::new(
            pool.clone(),
            schema.to_string(),
            "cg1".to_string(),
            "task-0".to_string(),
        );
        let loaded2 = store3.load(0.0).await.expect("load 2");
        let cs2 = loaded2
            .cvr
            .queries
            .get("q1")
            .and_then(|q| q.client_state())
            .and_then(|m| m.get("c1"))
            .expect("q1 still has c1 desire state");
        assert_eq!(
            cs2.inactivated_at,
            Some(1234),
            "inactivation timestamp reloaded — desire is NOT resurrected as active"
        );
        assert!(
            !loaded2.cvr.clients["c1"]
                .desired_query_ids
                .iter()
                .any(|id| id == "q1"),
            "inactive desires must not be reconstructed as active desiredQueryIDs"
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
                ref_counts: Some(std::collections::BTreeMap::from([("q1".to_string(), 1)])),
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
                ref_counts: Some(std::collections::BTreeMap::from([("q1".to_string(), 1)])),
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

/// Advance-driven `lastMutationID` change with NO client queries — bug 3628
/// (view-syncer.pg.test.ts "process advancement with lmid change, client has no
/// queries"). A client with only the internal `lmids` query still receives a
/// `lastMutationIDChanges` poke when an advance bumps its `clients` row, proving
/// mutation acks flow without any user query subscribed. Gated on TEST_CVR_PG_URI
/// (needs the PG CVR store + a wal2 replica for advance).
#[test]
fn pg_advance_lmid_change_with_no_queries() {
    use std::sync::Arc;

    use rusqlite::Connection;
    use rust_cvr::client_handler::WebSocketSink;
    use rust_cvr::updater::RowRecordMap;
    use rust_syncer::pipeline_driver::IvmPipelines;
    use rust_syncer::sync_engine::{SyncEngine, empty_cvr as empty_engine_cvr};
    use rust_syncer::ws_sink::{DirectWebSocketSink, WsCommand};

    let Some(uri) = pg_uri() else {
        eprintln!("SKIP pg_advance_lmid_change_with_no_queries: TEST_CVR_PG_URI not set");
        return;
    };

    let schema = "cvr_lmid_advance";
    let db_path = format!("/tmp/rust-syncer-pg-lmid-{}.db", std::process::id());
    let cleanup_sqlite = || {
        for suffix in ["", "-wal", "-wal2", "-shm"] {
            let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
        }
    };
    cleanup_sqlite();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = rt.handle().clone();
    rt.block_on(async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(&uri)
            .await
            .expect("connect");
        sqlx::raw_sql(&cvr_ddl(schema))
            .execute(&pool)
            .await
            .unwrap();
    });

    // Replica with only the `clients` table (shard app/0 → "app_0.clients"),
    // holding one client row at lastMutationID = 42. No user data tables.
    {
        let conn = Connection::open(&db_path).unwrap();
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
            CREATE TABLE "app_0.clients" (
                "clientGroupID"  TEXT NOT NULL,
                "clientID"       TEXT NOT NULL,
                "lastMutationID" INTEGER NOT NULL,
                "userID"         TEXT,
                "_0_version"     TEXT NOT NULL,
                PRIMARY KEY ("clientGroupID", "clientID")
            );
            INSERT INTO "_zero.replicationConfig" (lock, replicaVersion, publications)
                VALUES ('singleton', 'replica-1', '[]');
            INSERT INTO "_zero.replicationState" (lock, stateVersion)
                VALUES ('singleton', '01');
            INSERT INTO "app_0.clients"
                ("clientGroupID","clientID","lastMutationID","userID","_0_version")
                VALUES ('cg1', 'c1', 42, NULL, '01');
            "#,
        )
        .unwrap();
    }

    let specs = rust_syncer::compute_table_specs_from_path(&db_path).unwrap();
    let mut pipelines = IvmPipelines::new();
    pipelines.init(specs, Some(&db_path), "app").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    let pool = {
        let _g = handle.enter();
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect_lazy(&uri)
            .unwrap()
    };
    engine.set_tokio_handle(handle);
    engine
        .set_cvr_store(
            pool,
            schema.to_string(),
            "cg1".to_string(),
            "task-0".to_string(),
        )
        .unwrap();

    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink1: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx1));
    engine.register_client("c1", "ws1", "cg1", &shard, None, sink1);

    // Hydrate with NO client queries — only the internal lmids query. It should
    // poke lastMutationIDChanges.c1 == 42.
    let hydrated = rt
        .block_on(engine.config_and_hydrate(
            empty_engine_cvr("cg1", "replica-1"),
            "c1",
            &["ws1".to_string()],
            &shard,
            Vec::new(),
            Vec::new(),
            false,
            None,
            None,
            &serde_json::json!({}),
            None,
            "01".to_string(),
            "replica-1".to_string(),
            &RowRecordMap::new(),
            0,
            0,
            0,
        ))
        .expect("initial hydrate");

    let mut hydrate_wire = String::new();
    while let Ok(WsCommand::Send { msg: frame, .. }) = rx1.try_recv() {
        hydrate_wire.push_str(&frame.to_string());
    }
    assert!(
        hydrate_wire.contains("lastMutationIDChanges") && hydrate_wire.contains("42"),
        "hydrate must poke lastMutationIDChanges c1=42; wire={hydrate_wire}"
    );

    // Advance: bump the client's lastMutationID 42 → 43 on the replica.
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            r#"
            BEGIN;
            UPDATE "app_0.clients"
               SET "lastMutationID" = 43, "_0_version" = '02'
             WHERE "clientGroupID" = 'cg1' AND "clientID" = 'c1';
            INSERT INTO "_zero.changeLog2" ("stateVersion","table","rowKey","op","pos")
                VALUES ('02', 'app_0.clients', '{"clientGroupID":"cg1","clientID":"c1"}', 's', 0);
            UPDATE "_zero.replicationState" SET stateVersion = '02';
            COMMIT;
            "#,
        )
        .unwrap();
    }

    let existing = rt.block_on(engine.existing_rows());
    let advanced = rt
        .block_on(engine.advance_and_sync(
            hydrated,
            "replica-1".to_string(),
            // c1 is still online, so the advance must poke it directly (unlike
            // the offline-catchup template, which passes no poke targets).
            &["ws1".to_string()],
            &existing,
            0,
            0,
            0,
        ))
        .expect("lmid advance");
    assert_eq!(advanced.num_changes, 1, "one clients row changed");
    assert!(advanced.reset_reason.is_none(), "advance must not reset");

    let mut advance_wire = String::new();
    while let Ok(WsCommand::Send { msg: frame, .. }) = rx1.try_recv() {
        advance_wire.push_str(&frame.to_string());
    }
    assert!(
        advance_wire.contains("lastMutationIDChanges") && advance_wire.contains("43"),
        "advance must poke lastMutationIDChanges c1=43 with no client queries; wire={advance_wire}"
    );

    drop(engine);
    cleanup_sqlite();
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

/// Production lifecycle gate: connect → hydrate from the SQLite replica →
/// persist CVR/rows in Postgres → disconnect → advance while offline → reconnect
/// with the pre-advance cookie → catch up the missed row from Postgres.
///
/// This deliberately drives the syncer from a normal OS thread with an injected
/// Tokio handle, matching the real per-client-group execution model. It covers
/// the entire read path that the smaller store and in-memory engine tests cannot
/// cover together.
#[test]
fn pg_engine_hydrate_advance_reconnect_and_catchup() {
    use std::sync::Arc;

    use rusqlite::Connection;
    use rust_cvr::client_handler::WebSocketSink;
    use rust_cvr::updater::RowRecordMap;
    use rust_cvr::version::version_string;
    use rust_syncer::pipeline_driver::IvmPipelines;
    use rust_syncer::sync_engine::{SyncEngine, empty_cvr as empty_engine_cvr};
    use rust_syncer::ws_sink::{DirectWebSocketSink, WsCommand};

    let Some(uri) = pg_uri() else {
        eprintln!("SKIP pg_engine_hydrate_advance_reconnect_and_catchup: TEST_CVR_PG_URI not set");
        return;
    };

    let schema = "cvr_engine_lifecycle";
    let db_path = format!("/tmp/rust-syncer-pg-lifecycle-{}.db", std::process::id());
    let cleanup_sqlite = || {
        for suffix in ["", "-wal", "-wal2", "-shm"] {
            let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
        }
    };
    cleanup_sqlite();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = rt.handle().clone();
    rt.block_on(async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(&uri)
            .await
            .expect("connect");
        sqlx::raw_sql(&cvr_ddl(schema))
            .execute(&pool)
            .await
            .unwrap();
    });

    {
        let conn = Connection::open(&db_path).unwrap();
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
            CREATE TABLE issue (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                "_0_version" TEXT NOT NULL
            );
            INSERT INTO "_zero.replicationConfig" (lock, replicaVersion, publications)
                VALUES ('singleton', 'replica-1', '[]');
            INSERT INTO "_zero.replicationState" (lock, stateVersion)
                VALUES ('singleton', '01');
            INSERT INTO issue (id, title, "_0_version")
                VALUES ('i1', 'before advance', '01');
            "#,
        )
        .unwrap();
    }

    let specs = rust_syncer::compute_table_specs_from_path(&db_path).unwrap();
    let mut pipelines = IvmPipelines::new();
    pipelines.init(specs, Some(&db_path), "app").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    // Build the shared CVR pool (the store no longer creates its own — it takes a
    // clone of the one process-wide pool). Create it inside the runtime context.
    let pool = {
        let _g = handle.enter();
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect_lazy(&uri)
            .unwrap()
    };
    engine.set_tokio_handle(handle);
    engine
        .set_cvr_store(
            pool,
            schema.to_string(),
            "cg1".to_string(),
            "task-0".to_string(),
        )
        .unwrap();

    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink1: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx1));
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink1);

    let puts = vec![DesiredQuerySpec {
        hash: "q_issue".to_string(),
        ast: Some(serde_json::json!({"table": "issue"})),
        name: None,
        args: None,
        ttl: None,
    }];
    let hydrated = rt
        .block_on(engine.config_and_hydrate(
            empty_engine_cvr("cg1", "replica-1"),
            "client1",
            &["ws1".to_string()],
            &shard,
            puts,
            Vec::new(),
            false,
            None,
            None,
            &serde_json::json!({}),
            None,
            "01".to_string(),
            "replica-1".to_string(),
            &RowRecordMap::new(),
            0,
            0,
            0,
        ))
        .expect("initial hydrate");
    let hydrate_cookie = version_string(&hydrated.version);

    let mut hydrate_wire = String::new();
    while let Ok(WsCommand::Send { msg: frame, .. }) = rx1.try_recv() {
        hydrate_wire.push_str(&frame.to_string());
    }
    assert!(
        hydrate_wire.contains("before advance"),
        "initial connection must receive the hydrated SQLite row"
    );

    // The client goes offline before the replica advances, so no delta poke can
    // reach it. Its last durable cookie is the post-hydration cookie above.
    engine.unregister_client("ws1");
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            r#"
            BEGIN;
            UPDATE issue
               SET title = 'after advance', "_0_version" = '02'
             WHERE id = 'i1';
            INSERT INTO "_zero.changeLog2" ("stateVersion", "table", "rowKey", "op", "pos")
                VALUES ('02', 'issue', '{"id":"i1"}', 's', 0);
            UPDATE "_zero.replicationState" SET stateVersion = '02';
            COMMIT;
            "#,
        )
        .unwrap();
    }

    let existing_before_advance = rt.block_on(engine.existing_rows());
    let advanced = rt
        .block_on(engine.advance_and_sync(
            hydrated,
            "replica-1".to_string(),
            &[],
            &existing_before_advance,
            0,
            0,
            0,
        ))
        .expect("offline advance");
    assert_eq!(advanced.num_changes, 1, "one replica row changed");
    assert!(advanced.reset_reason.is_none(), "advance must not reset");
    assert_ne!(
        version_string(&advanced.cvr.version),
        hydrate_cookie,
        "advance must move the CVR beyond the disconnected client's cookie"
    );

    // Reconnect the same logical client with its old cookie. No query needs a
    // new hydrate; config_and_hydrate must take the catch-up branch and rebuild
    // the missed row contents from the advanced IVM state.
    let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink2: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx2));
    engine.register_client(
        "client1",
        "ws2",
        "cg1",
        &shard,
        Some(&hydrate_cookie),
        sink2,
    );
    let existing_after_advance = rt.block_on(engine.existing_rows());
    let caught_up = rt
        .block_on(engine.config_and_hydrate(
            advanced.cvr,
            "client1",
            &["ws2".to_string()],
            &shard,
            Vec::new(),
            Vec::new(),
            false,
            None,
            None,
            &serde_json::json!({}),
            None,
            "02".to_string(),
            "replica-1".to_string(),
            &existing_after_advance,
            0,
            0,
            0,
        ))
        .expect("reconnect catchup");

    let mut catchup_wire = String::new();
    while let Ok(WsCommand::Send { msg: frame, .. }) = rx2.try_recv() {
        catchup_wire.push_str(&frame.to_string());
    }
    assert!(
        catchup_wire.contains("after advance"),
        "reconnecting client must receive the row change missed while offline; wire={catchup_wire}"
    );
    assert!(
        !catchup_wire.contains("before advance"),
        "catchup must rebuild contents from the current IVM row"
    );

    let reloaded = rt
        .block_on(engine.load_cvr(0.0))
        .unwrap()
        .expect("persisted CVR");
    assert_eq!(
        reloaded.version, caught_up.version,
        "Postgres must hold the exact CVR delivered after catchup"
    );

    drop(engine);
    cleanup_sqlite();
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

/// Quiet-commit store contract (1.9 review Fix 1). An advance that produced
/// zero IVM output for a CG (the commit only touched other groups' rows, or a
/// duplicate `/notify` re-advanced at the same version) drains ZERO store ops,
/// so `store.flush` must (a) return `None` and write nothing, and (b) the next
/// material flush must succeed with the ORIGINAL version as
/// `expectedCurrentVersion`. Before the sync_engine fallback fix, the caller
/// adopted the bumped in-memory CVR after the no-op, so the next material
/// flush passed the bumped (never-persisted) version and died on
/// `ConcurrentModification` — tearing down the whole client group. The
/// counter-factual assertion at the end pins that failure shape.
#[test]
fn pg_quiet_commit_noop_flush_contract() {
    let Some(uri) = pg_uri() else {
        eprintln!("SKIP pg_quiet_commit_noop_flush_contract: TEST_CVR_PG_URI not set");
        return;
    };
    let schema = "cvr_quiet_commit";
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

        // Establish v1 in the store: a material config flush (client1).
        let mut cfg = CVRConfigDrivenUpdater::new(empty_cvr("cg1"), shard.clone());
        cfg.ensure_client("client1");
        let (cvr_v1, _stats) = cfg.flush(0, 0, 0);
        let ops = cfg.base.drain_store_ops();
        let mut store = CVRStoreHandle::new(
            pool.clone(),
            schema.to_string(),
            "cg1".to_string(),
            "task-0".to_string(),
        );
        store.apply_store_ops(ops);
        store
            .flush(&EMPTY_CVR_VERSION, &cvr_v1, 0.0)
            .await
            .expect("initial material flush")
            .expect("initial flush is material");

        // Quiet advance: bump the state version with ZERO tracked changes. The
        // updater drains no ops, and the store flush is a no-op that must not
        // advance the on-disk version.
        let mut quiet = rust_cvr::updater::CVRQueryDrivenUpdater::new(
            cvr_v1.clone(),
            "01".to_string(),
            "01".to_string(),
            None,
        );
        let (quiet_cvr, _stats) = quiet.flush(0, 0, 0);
        let quiet_ops = quiet.base.drain_store_ops();
        assert!(
            quiet_ops.is_empty(),
            "a zero-change advance must drain no store ops, got {quiet_ops:?}"
        );
        let flushed = store
            .flush(&cvr_v1.version, &quiet_cvr, 0.0)
            .await
            .expect("quiet flush must not error");
        assert!(
            flushed.is_none(),
            "a zero-op flush must be a no-op (return None)"
        );
        let db_version: (String,) = sqlx::query_as(&format!(
            r#"SELECT "version" FROM "{schema}".instances WHERE "clientGroupID" = 'cg1'"#
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            db_version.0,
            rust_cvr::version::version_string(&cvr_v1.version),
            "no-op flush must not advance the stored version"
        );

        // FIXED caller behavior: after the no-op it stays on cvr_v1 (orig), so
        // the next material advance passes cvr_v1.version as expected — and
        // succeeds.
        let mut material = rust_cvr::updater::CVRQueryDrivenUpdater::new(
            cvr_v1.clone(),
            "02".to_string(),
            "01".to_string(),
            None,
        );
        let (material_cvr, _stats) = material.flush(0, 0, 0);
        let mut ops = material.base.drain_store_ops();
        ops.push(insert_client_op("client2"));
        store.apply_store_ops(ops);
        store
            .flush(&cvr_v1.version, &material_cvr, 0.0)
            .await
            .expect("material flush after quiet no-op must succeed")
            .expect("material flush is material");

        // COUNTER-FACTUAL (the pre-fix bug): a caller that adopted the bumped
        // quiet CVR would pass the never-persisted version as expected and die
        // on the version guard.
        let mut store2 = CVRStoreHandle::new(
            pool.clone(),
            schema.to_string(),
            "cg1".to_string(),
            "task-0".to_string(),
        );
        store2.apply_store_ops(vec![insert_client_op("client3")]);
        let err = store2
            .flush(&quiet_cvr.version, &material_cvr, 0.0)
            .await
            .expect_err("stale expected version must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.to_lowercase().contains("concurrent"),
            "expected ConcurrentModification, got: {msg}"
        );

        sqlx::raw_sql(&format!(r#"DROP SCHEMA IF EXISTS "{schema}" CASCADE;"#))
            .execute(&pool)
            .await
            .unwrap();
    });
}

/// Advance-path regression guard for the client-PK row-key convergence
/// (commits d6c1dd80b + 2a75395f9). On a table whose client PK differs from the
/// IVM `keyCmp[0]` (compound client PK [channelId,userId] + a shorter surrogate
/// unique index on `id`), an upstream UPDATE that changes a CLIENT-PK column but
/// NOT the surrogate must produce REMOVE(old rowKey) + ADD(new rowKey) — NOT a
/// single EDIT. Before the fix the advance `same_pk` check used `keyCmp[0]`
/// (=`id`, unchanged) → single EDIT → the old CVR row `{channelId,userId=old}`
/// is orphaned and the client phantom-edits a rowKey it never received. After
/// the fix `same_pk` uses the client PK → `userId` changed → REMOVE + ADD.
#[test]
fn pg_advance_client_pk_col_update_emits_remove_add() {
    use std::sync::Arc;

    use rusqlite::Connection;
    use rust_cvr::client_handler::WebSocketSink;
    use rust_cvr::updater::RowRecordMap;
    use rust_syncer::pipeline_driver::IvmPipelines;
    use rust_syncer::sync_engine::{SyncEngine, empty_cvr as empty_engine_cvr};
    use rust_syncer::ws_sink::{DirectWebSocketSink, WsCommand};

    let Some(uri) = pg_uri() else {
        eprintln!("SKIP pg_advance_client_pk_col_update_emits_remove_add: TEST_CVR_PG_URI not set");
        return;
    };

    let schema = "cvr_advance_clientpk";
    let db_path = format!(
        "/tmp/rust-syncer-pg-advance-clientpk-{}.db",
        std::process::id()
    );
    let cleanup_sqlite = || {
        for suffix in ["", "-wal", "-wal2", "-shm"] {
            let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
        }
    };
    cleanup_sqlite();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = rt.handle().clone();
    rt.block_on(async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(&uri)
            .await
            .expect("connect");
        sqlx::raw_sql(&cvr_ddl(schema))
            .execute(&pool)
            .await
            .unwrap();
    });

    // Replica with a junction table: NO SQL PRIMARY KEY, a compound unique index
    // (the app's client PK) AND a shorter surrogate unique index on `id`.
    // compute_table_specs' keyCmp[0] therefore picks ["id"] (fewest columns).
    {
        let conn = Connection::open(&db_path).unwrap();
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
            CREATE TABLE "channel_user_status" (
                "channelId"  "text|NOT_NULL",
                "userId"     "text|NOT_NULL",
                "id"         "text|NOT_NULL",
                "_0_version" "text|NOT_NULL"
            );
            CREATE UNIQUE INDEX "cus_client_pk" ON "channel_user_status" ("channelId", "userId");
            CREATE UNIQUE INDEX "cus_surrogate" ON "channel_user_status" ("id");
            INSERT INTO "_zero.replicationConfig" (lock, replicaVersion, publications)
                VALUES ('singleton', 'replica-1', '[]');
            INSERT INTO "_zero.replicationState" (lock, stateVersion)
                VALUES ('singleton', '01');
            INSERT INTO "channel_user_status" ("channelId", "userId", "id", "_0_version")
                VALUES ('c1', 'u1', 'cus1', '01');
            "#,
        )
        .unwrap();
    }

    let specs = rust_syncer::compute_table_specs_from_path(&db_path).unwrap();
    // Precondition: the IVM keyCmp[0] is the surrogate ["id"], diverging from the
    // client's declared PK [channelId, userId].
    let cus = specs
        .iter()
        .find(|s| s.table == "channel_user_status")
        .expect("channel_user_status must be syncable");
    assert_eq!(
        cus.primary_key,
        vec!["id".to_string()],
        "precondition: keyCmp[0] must be the shortest unique key (surrogate id)"
    );

    let mut pipelines = IvmPipelines::new();
    pipelines.init(specs, Some(&db_path), "app").unwrap();
    let mut engine = SyncEngine::new(pipelines);
    let pool = {
        let _g = handle.enter();
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect_lazy(&uri)
            .unwrap()
    };
    engine.set_tokio_handle(handle);
    engine
        .set_cvr_store(
            pool,
            schema.to_string(),
            "cg1".to_string(),
            "task-0".to_string(),
        )
        .unwrap();

    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink1: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx1));
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink1);

    // Client schema declares the compound PK [channelId, userId] — this is what
    // config_and_hydrate installs on the pipelines for rowKey emission + identity.
    let client_schema = serde_json::json!({
        "tables": {
            "channel_user_status": {
                "columns": {
                    "channelId": {"type": "string"},
                    "userId": {"type": "string"},
                    "id": {"type": "string"}
                },
                "primaryKey": ["channelId", "userId"]
            }
        }
    });
    let puts = vec![DesiredQuerySpec {
        hash: "q_cus".to_string(),
        ast: Some(serde_json::json!({"table": "channel_user_status"})),
        name: None,
        args: None,
        ttl: None,
    }];
    let hydrated = rt
        .block_on(engine.config_and_hydrate(
            empty_engine_cvr("cg1", "replica-1"),
            "client1",
            &["ws1".to_string()],
            &shard,
            puts,
            Vec::new(),
            false,
            Some(client_schema),
            None,
            &serde_json::json!({}),
            None,
            "01".to_string(),
            "replica-1".to_string(),
            &RowRecordMap::new(),
            0,
            0,
            0,
        ))
        .expect("initial hydrate");

    let mut hydrate_wire = String::new();
    while let Ok(WsCommand::Send { msg: frame, .. }) = rx1.try_recv() {
        hydrate_wire.push_str(&frame.to_string());
    }
    // Emission must be keyed by the client PK: the hydrated row carries both
    // client-PK columns (channelId + userId=u1).
    assert!(
        hydrate_wire.contains("channelId") && hydrate_wire.contains("u1"),
        "hydrate must emit the row keyed by the client PK; wire={hydrate_wire}"
    );

    // Upstream UPDATE: change a CLIENT-PK column (userId u1 -> u2) while the
    // surrogate `id` stays 'cus1'. The changelog rowKey is the replicator key
    // (keyCmp[0] = id), exactly as replication writes it.
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            r#"
            BEGIN;
            UPDATE "channel_user_status"
               SET "userId" = 'u2', "_0_version" = '02'
             WHERE "id" = 'cus1';
            INSERT INTO "_zero.changeLog2" ("stateVersion", "table", "rowKey", "op", "pos")
                VALUES ('02', 'channel_user_status', '{"id":"cus1"}', 's', 0);
            UPDATE "_zero.replicationState" SET stateVersion = '02';
            COMMIT;
            "#,
        )
        .unwrap();
    }

    let existing = rt.block_on(engine.existing_rows());
    let _advanced = rt
        .block_on(engine.advance_and_sync(
            hydrated,
            "replica-1".to_string(),
            // Poke the still-connected client (at the pre-advance version) so we
            // can observe the delta the advance produces.
            &["ws1".to_string()],
            &existing,
            0,
            0,
            0,
        ))
        .expect("advance");

    let mut adv_wire = String::new();
    while let Ok(WsCommand::Send { msg: frame, .. }) = rx1.try_recv() {
        adv_wire.push_str(&frame.to_string());
    }

    // THE REGRESSION ASSERTION: a client-PK-column change must produce a REMOVE
    // (del) of the old rowKey plus an ADD (put) of the new — NOT a single EDIT.
    // Pre-fix (same_pk on keyCmp[0]=id, unchanged) there is NO `del`; the old
    // row {channelId:c1,userId:u1} is orphaned.
    assert!(
        adv_wire.contains(r#""op":"del""#),
        "advance must REMOVE the old client-PK rowKey (not a single EDIT); wire={adv_wire}"
    );
    assert!(
        adv_wire.contains("u2"),
        "advance must ADD the row under the new client PK (userId=u2); wire={adv_wire}"
    );

    rt.block_on(async {
        sqlx::raw_sql(&format!(r#"DROP SCHEMA IF EXISTS "{schema}" CASCADE;"#))
            .execute(
                &sqlx::postgres::PgPoolOptions::new()
                    .connect(&uri)
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
    });
    cleanup_sqlite();
}
