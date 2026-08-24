//! Live-Postgres, seed-parameterized TS-vs-Rust differential for
//! `CVRStore::inspect_queries` (port of TS `CVRStore.inspectQueries`).
//!
//! The golden (`agentic/parity/generate-inspect-fixture.mjs`) defines several
//! scenarios — got/not-got, custom vs crud, TTL-expiry boundaries (`<=`), a
//! client with no rows, and client filters that match nothing — and drives the
//! REAL TS impl over each. Every scenario is SELF-CONTAINED: it carries its own
//! `seedSql` plus expected results per filter. This test replays that exact
//! `seedSql` (so the seed data cannot drift between the two languages), runs the
//! Rust `inspect_queries`, and asserts each filter's output equals the TS golden —
//! pinning the SQL semantics (LEFT JOIN desires→queries, rowCount via
//! `refCounts ? queryHash`, got flag, COALESCE(ttlMs, DEFAULT), the TTL-expiry
//! filter, client filter, ordering).
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

    let golden: Value =
        serde_json::from_str(include_str!("../agentic/parity/inspect-fixture.json"))
            .expect("inspect-fixture.json");
    let scenarios = golden["scenarios"].as_array().expect("scenarios array");
    assert!(!scenarios.is_empty(), "no inspect scenarios in golden");

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

    let mut checked = 0;
    for sc in scenarios {
        let name = sc["name"].as_str().unwrap_or("?");
        let seed_sql = sc["seedSql"].as_str().expect("seedSql");
        let ttl_clock = sc["ttlClock"].as_i64().expect("ttlClock") as TTLClock;

        // Fresh schema from the exact TS DDL, then the scenario's own seed.
        sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE"#))
            .execute(&pool)
            .await
            .expect("drop schema");
        sqlx::raw_sql(include_str!("../agentic/parity/flush-schema.sql"))
            .execute(&pool)
            .await
            .expect("create schema");
        sqlx::raw_sql(seed_sql)
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("seed scenario {name}: {e}"));

        let store = CVRStoreHandle::new(
            pool.clone(),
            SCHEMA.to_string(),
            CVR_ID.to_string(),
            TASK_ID.to_string(),
        );

        for f in sc["filters"].as_array().expect("filters") {
            let filter: Option<&str> = f.as_str();
            let key = filter.unwrap_or("null");
            let actual = store
                .inspect_queries(ttl_clock, filter)
                .await
                .unwrap_or_else(|e| panic!("inspect scenario {name} filter {key}: {e}"));
            assert_eq!(
                to_value(actual),
                sc["results"][key],
                "inspect scenario `{name}` filter `{key}` differs from the TS golden"
            );
            checked += 1;
        }
    }
    eprintln!("inspect differential: {checked} scenario/filter cases matched the TS golden");
}
