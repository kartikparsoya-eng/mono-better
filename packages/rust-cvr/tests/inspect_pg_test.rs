//! Live-Postgres test for `CVRStore.inspect_queries` — the port of TS
//! `CVRStore.inspectQueries` (cvr-store.ts). Seeds the desires / queries / rows
//! tables directly, then asserts the SQL produces the exact `InspectQueryRow`
//! set: LEFT JOIN of desires→queries, per-query `rowCount` (`refCounts ? hash`),
//! the `got` flag, `COALESCE(ttlMs, DEFAULT)`, the TTL-expiry filter, the
//! optional client filter, and `(clientID, queryHash)` ordering.
//!
//! Gated on `TEST_CVR_PG_URI`; skips (passes) when unset.

use rust_cvr::cvr_store::CVRStoreHandle;
use rust_cvr::ttl_clock::TTLClock;

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

    // ── all clients ──
    let rows = store
        .inspect_queries(TTL_CLOCK, None)
        .await
        .expect("inspect");
    // Ordered by (clientID, queryHash); q3 (expired) is filtered out.
    let ids: Vec<(&str, &str)> = rows
        .iter()
        .map(|r| (r.client_id.as_str(), r.query_id.as_str()))
        .collect();
    assert_eq!(
        ids,
        vec![("c1", "q1"), ("c1", "q2"), ("c2", "q1")],
        "expected c1/q1, c1/q2, c2/q1 in order (q3 TTL-expired, filtered)"
    );

    // c1/q1 — client query, got, active, rowCount 2 from the two q1-referencing rows.
    let q1 = &rows[0];
    assert!(q1.got, "q1 has a patchVersion → got");
    assert!(!q1.deleted);
    assert_eq!(q1.ttl, 300000);
    assert_eq!(q1.inactivated_at, None);
    assert_eq!(q1.row_count, 2);
    assert_eq!(q1.ast, Some(serde_json::json!({"table": "issues"})));
    assert_eq!(q1.name, None);
    assert_eq!(q1.args, None);

    // c1/q2 — custom query, not got, inactivated but not yet expired, rowCount 0.
    let q2 = &rows[1];
    assert!(!q2.got, "q2 has no patchVersion → not got");
    assert_eq!(q2.ttl, 2000);
    assert_eq!(q2.inactivated_at, Some(4000));
    assert_eq!(q2.row_count, 0);
    assert_eq!(q2.ast, None);
    assert_eq!(q2.name, Some("myQuery".to_string()));
    assert_eq!(q2.args, Some(serde_json::json!([42])));

    // ── client filter ──
    let filtered = store
        .inspect_queries(TTL_CLOCK, Some("c2"))
        .await
        .expect("inspect c2");
    let fids: Vec<(&str, &str)> = filtered
        .iter()
        .map(|r| (r.client_id.as_str(), r.query_id.as_str()))
        .collect();
    assert_eq!(fids, vec![("c2", "q1")], "client filter keeps only c2");
}
