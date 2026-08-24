//! Live-Postgres TS-vs-Rust differential for `CVRStore::inspect_queries` (port
//! of TS `CVRStore.inspectQueries`, cvr-store.ts). Seeds desires / queries / rows
//! byte-identically to `agentic/parity/generate-inspect-fixture.mjs` (which drives
//! the REAL TS impl → `inspect-fixture.json`), then asserts the Rust output equals
//! that golden — pinning the SQL semantics (LEFT JOIN desires→queries, rowCount via
//! `refCounts ? queryHash`, got flag, COALESCE(ttlMs, DEFAULT), TTL-expiry filter,
//! client filter, `(clientID, queryHash)` ordering) against actual TS output.
//!
//! Regenerate the golden with:
//!   TEST_CVR_PG_URI=... npx tsx agentic/parity/generate-inspect-fixture.mjs
//!
//! Gated on `TEST_CVR_PG_URI`; skips (passes) when unset.

use rust_cvr::cvr_store::{CVRStoreHandle, InspectQueryRow};
use rust_cvr::ttl_clock::TTLClock;
use serde_json::Value;

const SCHEMA: &str = "roze_1/cvr";
const CVR_ID: &str = "cg-inspect";
const TASK_ID: &str = "inspect-task";
const TTL_CLOCK: TTLClock = 5_000;

#[tokio::test]
async fn inspect_queries_matches_ts_sql() {
    let uri = match std::env::var("TEST_CVR_PG_URI") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!("SKIP inspect_queries_matches_ts_sql: TEST_CVR_PG_URI unset");
            return;
        }
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
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

    // Seed instances → queries → desires → rows (respecting FKs).
    sqlx::raw_sql(&format!(
        r#"
        INSERT INTO "{SCHEMA}".instances ("clientGroupID", version, "lastActive", "ttlClock", "replicaVersion")
          VALUES ('{CVR_ID}', '01', to_timestamp(0), 0, '01');
        INSERT INTO "{SCHEMA}"."rowsVersion" ("clientGroupID", version) VALUES ('{CVR_ID}', '01');

        INSERT INTO "{SCHEMA}".queries ("clientGroupID", "queryHash", "clientAST", "queryName", "queryArgs", "patchVersion", internal, deleted)
          VALUES
          ('{CVR_ID}', 'q1', '{{"table":"issues"}}', NULL, NULL, '01', false, false),   -- client query, got
          ('{CVR_ID}', 'q2', NULL, 'myQuery', '[42]', NULL, false, false),               -- custom query, not got
          ('{CVR_ID}', 'q3', '{{"table":"labels"}}', NULL, NULL, NULL, false, false);    -- client query, will be TTL-expired

        INSERT INTO "{SCHEMA}".desires ("clientGroupID", "clientID", "queryHash", "patchVersion", deleted, "ttlMs", "inactivatedAtMs")
          VALUES
          ('{CVR_ID}', 'c1', 'q1', '01', false, 300000, NULL),   -- active, ttl default-ish
          ('{CVR_ID}', 'c1', 'q2', '01', false, 2000,   4000),   -- inactivated 4000+2000=6000 > 5000 → kept
          ('{CVR_ID}', 'c1', 'q3', '01', false, 1000,   1000),   -- inactivated 1000+1000=2000 <= 5000 → filtered
          ('{CVR_ID}', 'c2', 'q1', '01', false, 300000, NULL);   -- second client, for the client filter

        INSERT INTO "{SCHEMA}".rows ("clientGroupID", schema, "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
          VALUES
          ('{CVR_ID}', 'public', 'issues', '{{"id":"1"}}', '01', '01', '{{"q1":1}}'),
          ('{CVR_ID}', 'public', 'issues', '{{"id":"2"}}', '01', '01', '{{"q1":1}}');
        "#
    ))
    .execute(&pool)
    .await
    .expect("seed");

    let store = CVRStoreHandle::new(
        pool.clone(),
        SCHEMA.to_string(),
        CVR_ID.to_string(),
        TASK_ID.to_string(),
    );

    // TS golden from generate-inspect-fixture.mjs (real CVRStore.inspectQueries
    // over the byte-identical seed above).
    let golden: Value =
        serde_json::from_str(include_str!("../agentic/parity/inspect-fixture.json"))
            .expect("inspect-fixture.json");

    // Structural comparison (parsed Value): the TS SQL emits columns in SELECT
    // order, the Rust struct in protocol order — object equality is order-
    // independent, so this pins values/shape without depending on key order.
    let to_value = |rows: Vec<InspectQueryRow>| -> Value {
        Value::Array(
            rows.iter()
                .map(|r| serde_json::to_value(r).unwrap())
                .collect(),
        )
    };

    let all = store
        .inspect_queries(TTL_CLOCK, None)
        .await
        .expect("inspect all");
    assert_eq!(
        to_value(all),
        golden["all"],
        "inspect_queries(None) differs from the TS golden"
    );

    let filtered = store
        .inspect_queries(TTL_CLOCK, Some("c2"))
        .await
        .expect("inspect c2");
    assert_eq!(
        to_value(filtered),
        golden["filtered"],
        "inspect_queries(\"c2\") differs from the TS golden"
    );
}
