//! REGRESSION GATE — CVR flush round-trip cost (sandbox hydrate-stall repro).
//!
//! Times the two `rust_cvr` `CVRStoreHandle::flush` row paths for growing N:
//!   * INSERT path — all referenced rows go into ONE `json_to_recordset` bulk
//!     upsert → ~constant round-trips regardless of N.
//!   * DELETE path — now ALSO one batched `DELETE ... USING json_to_recordset`
//!     statement (was one awaited `DELETE` per row = N SEQUENTIAL round-trips).
//!
//! History: the per-row DELETE loop's wall-clock scaled LINEARLY with N × RTT;
//! on the sandbox's latent CVR DB (`zero-playground-db-v2`) a large-board hydrate
//! churn awaited inline on the single-threaded CG starved the socket → reconnect
//! storm → the ~20s "Loading conversations…" stall. After batching, DELETE is
//! flat in N like INSERT (5000-del @ +3ms RTT: 22.8s → ~67ms). This test guards
//! against a regression back to the per-row loop — run it with injected latency
//! (toxiproxy) and DELETE-per-row should stay within a small multiple of INSERT.
//!
//! Gated on TEST_CVR_PG_URI. Run:
//!   TEST_CVR_PG_URI=postgres://postgres:postgres@localhost:55432/cvr_repro \
//!   cargo test -p rust-syncer --test cvr_flush_bench --no-default-features \
//!     -- --nocapture
//!
//! Optional latency knob for the injected-latency run: CVR_BENCH_LABEL=<text>.

use std::collections::BTreeMap;

use rust_cvr::schema::types::RowID;
use rust_cvr::cvr_store::CVRStoreHandle;
use rust_cvr::cvr::{CVR, StoreOp};
use rust_cvr::schema::types::{RowRecord};
use rust_cvr::shards::{ShardID};
use rust_cvr::cvr::CVRConfigDrivenUpdater;
use rust_cvr::schema::types::{CVRVersion, EMPTY_CVR_VERSION};

fn pg_uri() -> Option<String> {
    std::env::var("TEST_CVR_PG_URI")
        .ok()
        .filter(|s| !s.is_empty())
}

fn cvr_ddl(schema: &str) -> String {
    format!(
        r#"
DROP SCHEMA IF EXISTS "{schema}" CASCADE;
CREATE SCHEMA "{schema}";
CREATE TABLE "{schema}".instances (
  "clientGroupID" TEXT PRIMARY KEY, "version" TEXT NOT NULL,
  "lastActive" TIMESTAMPTZ NOT NULL, "ttlClock" DOUBLE PRECISION NOT NULL DEFAULT 0,
  "replicaVersion" TEXT, "owner" TEXT, "grantedAt" TIMESTAMPTZ,
  "clientSchema" JSONB, "profileID" TEXT, "deleted" BOOL DEFAULT FALSE
);
CREATE TABLE "{schema}".clients (
  "clientGroupID" TEXT, "clientID" TEXT,
  PRIMARY KEY ("clientGroupID", "clientID"),
  CONSTRAINT fk_clients_cg FOREIGN KEY ("clientGroupID")
    REFERENCES "{schema}".instances ("clientGroupID") ON DELETE CASCADE
);
CREATE TABLE "{schema}".queries (
  "clientGroupID" TEXT, "queryHash" TEXT, "clientAST" JSONB, "queryName" TEXT,
  "queryArgs" JSON, "patchVersion" TEXT, "transformationHash" TEXT,
  "transformationVersion" TEXT, "internal" BOOL, "deleted" BOOL, "rowSetSignature" TEXT,
  PRIMARY KEY ("clientGroupID", "queryHash"),
  CONSTRAINT fk_queries_cg FOREIGN KEY ("clientGroupID")
    REFERENCES "{schema}".instances ("clientGroupID") ON DELETE CASCADE
);
CREATE TABLE "{schema}".desires (
  "clientGroupID" TEXT, "clientID" TEXT, "queryHash" TEXT,
  "patchVersion" TEXT NOT NULL, "deleted" BOOL, "ttl" INTERVAL, "ttlMs" DOUBLE PRECISION,
  "inactivatedAt" TIMESTAMPTZ, "inactivatedAtMs" DOUBLE PRECISION,
  PRIMARY KEY ("clientGroupID", "clientID", "queryHash"),
  CONSTRAINT fk_desires_query FOREIGN KEY ("clientGroupID", "queryHash")
    REFERENCES "{schema}".queries ("clientGroupID", "queryHash") ON DELETE CASCADE
);
CREATE TABLE "{schema}"."rowsVersion" (
  "clientGroupID" TEXT PRIMARY KEY, "version" TEXT NOT NULL
);
CREATE TABLE "{schema}".rows (
  "clientGroupID" TEXT, "schema" TEXT, "table" TEXT, "rowKey" JSONB,
  "rowVersion" TEXT NOT NULL, "patchVersion" TEXT NOT NULL, "refCounts" JSONB,
  PRIMARY KEY ("clientGroupID", "schema", "table", "rowKey"),
  CONSTRAINT fk_rows_cg FOREIGN KEY ("clientGroupID")
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

#[test]
fn cvr_flush_roundtrip_bench() {
    let Some(uri) = pg_uri() else {
        eprintln!("SKIP cvr_flush_roundtrip_bench: TEST_CVR_PG_URI not set");
        return;
    };
    let label = std::env::var("CVR_BENCH_LABEL").unwrap_or_else(|_| "local".to_string());
    let schema = "cvr_flush_bench";
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

        // Create the CVR instance so rows can be persisted (FK to rowsVersion/instance).
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
            .expect("create instance");

        // Warm one round-trip (connection/prepare) so the table isn't polluted by
        // first-connection cost.
        let _: (i64,) = sqlx::query_as("SELECT 1::bigint")
            .fetch_one(&pool)
            .await
            .unwrap();

        eprintln!("\n=== CVR flush round-trip bench [label={label}] ===");
        eprintln!("uri = {uri}");
        eprintln!(
            "{:>7} | {:>16} | {:>18} | {:>12} | {:>12} | {:>8}",
            "N", "insert_bulk(ms)", "delete_perrow(ms)", "ins us/row", "del us/row", "del/ins"
        );
        eprintln!("{}", "-".repeat(92));

        for &n in &[100usize, 1000, 5000] {
            let pfx = format!("n{n}_");

            // INSERT path: N referenced rows -> ONE json_to_recordset bulk upsert.
            let puts: Vec<StoreOp> = (0..n)
                .map(|i| StoreOp::PutRowRecord(mk_row(&format!("{pfx}r{i}"), Some(1))))
                .collect();
            store.apply_store_ops(puts);
            let t = std::time::Instant::now();
            store
                .flush(&cvr.version, &cvr, 0.0)
                .await
                .expect("flush inserts");
            let insert_ms = t.elapsed().as_secs_f64() * 1000.0;

            // DELETE path: N explicit dels -> ONE awaited DELETE per row (N round-trips).
            let dels: Vec<StoreOp> = (0..n)
                .map(|i| StoreOp::DelRowRecord(mk_row(&format!("{pfx}r{i}"), None).id))
                .collect();
            store.apply_store_ops(dels);
            let t = std::time::Instant::now();
            store
                .flush(&cvr.version, &cvr, 0.0)
                .await
                .expect("flush deletes");
            let delete_ms = t.elapsed().as_secs_f64() * 1000.0;

            eprintln!(
                "{:>7} | {:>16.1} | {:>18.1} | {:>12.2} | {:>12.2} | {:>8.1}x",
                n,
                insert_ms,
                delete_ms,
                insert_ms * 1000.0 / n as f64,
                delete_ms * 1000.0 / n as f64,
                delete_ms / insert_ms.max(0.001)
            );
        }
        eprintln!("{}", "-".repeat(92));
        eprintln!(
            "INSERT = 1 bulk stmt (flat in N); DELETE = N sequential round-trips (linear in N).\n\
             Multiply del us/row by real CVR-DB RTT/local-RTT to project sandbox latency.\n"
        );

        sqlx::raw_sql(&format!(r#"DROP SCHEMA IF EXISTS "{schema}" CASCADE;"#))
            .execute(&pool)
            .await
            .unwrap();
    });
}
