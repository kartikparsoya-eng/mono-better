//! Live-Postgres flush differential (write path).
//!
//! Replays the same trackQueries -> received -> flush the TS golden generator ran
//! (`agentic/parity/generate-flush-fixture.mjs`) through the REAL Rust CVRStore +
//! CVRQueryDrivenUpdater against a fresh schema, then asserts the persisted `rows`
//! (and queries / instances / rowsVersion) match the TS-written golden byte-for-byte.
//! Pins the DB-row SERIALIZATION — rowKey / refCounts / versions — the layer where
//! the prior poisoned-rowKey-in-PG corruption happened. The schema DDL is the exact
//! TS DDL captured into `flush-schema.sql`.
//!
//! Gated on `TEST_CVR_PG_URI`; skips (passes) when unset so the suite still runs
//! without a database. Regenerate the golden with:
//!   TEST_CVR_PG_URI=... npx tsx packages/rust-cvr/agentic/parity/generate-flush-fixture.mjs

use rust_cvr::store::CVRStoreHandle;
use rust_cvr::types::{RefCounts, RowID, RowUpdate};
use rust_cvr::updater::CVRQueryDrivenUpdater;
use rust_cvr::version::CVRVersion;
use serde_json::Value;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::Arc;

const SCHEMA: &str = "roze_1/cvr";
const CVR_ID: &str = "cg-flush";
const TASK_ID: &str = "flush-task";
const CONNECT_TIME: f64 = 1_725_408_000_000.0; // Date.UTC(2024, 8, 4)
const NOW: i64 = 1_725_494_400_000; // Date.UTC(2024, 8, 5)

fn row_id_from_json(v: &Value) -> RowID {
    RowID {
        schema: v["schema"].as_str().unwrap().to_string(),
        table: v["table"].as_str().unwrap().to_string(),
        row_key: v["rowKey"].as_object().unwrap().clone(),
    }
}

#[tokio::test]
async fn flush_matches_ts_golden() {
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

    // Fresh schema from the exact TS DDL, plus the same base state the generator seeded.
    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE"#))
        .execute(&pool)
        .await
        .expect("drop schema");
    sqlx::raw_sql(include_str!("../agentic/parity/flush-schema.sql"))
        .execute(&pool)
        .await
        .expect("create schema");
    sqlx::raw_sql(&format!(
        r#"
        INSERT INTO "{SCHEMA}".instances ("clientGroupID", version, "lastActive", "ttlClock", "replicaVersion")
          VALUES ('{CVR_ID}', '01', to_timestamp({CONNECT_TIME} / 1000.0), {CONNECT_TIME}, '01');
        INSERT INTO "{SCHEMA}"."rowsVersion" ("clientGroupID", version) VALUES ('{CVR_ID}', '01');
        INSERT INTO "{SCHEMA}".queries ("clientGroupID", "queryHash", "clientAST", "patchVersion", "transformationHash", "transformationVersion")
          VALUES ('{CVR_ID}', 'foo', '{{"table":"issues"}}', '01', 'foo-t', '01');
        "#
    ))
    .execute(&pool)
    .await
    .expect("seed base state");

    let golden: Value = serde_json::from_str(include_str!("../agentic/parity/flush-fixture.json"))
        .expect("flush-fixture.json");

    // Drive the real store + query-driven updater exactly as the TS generator did.
    let mut store = CVRStoreHandle::new(
        pool.clone(),
        SCHEMA.to_string(),
        CVR_ID.to_string(),
        TASK_ID.to_string(),
    );
    let loaded = store.load(CONNECT_TIME).await.expect("load");
    let mut updater =
        CVRQueryDrivenUpdater::new(loaded.cvr, "02".to_string(), "01".to_string(), None);
    updater.track_queries(&[("foo", "foo-t")], &[]);

    let mut rows: HashMap<String, (RowID, RowUpdate)> = HashMap::new();
    for r in golden["received"].as_array().unwrap() {
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
    let existing: HashMap<String, rust_cvr::types::RowRecord> = HashMap::new();
    updater.received(&rows, &existing);
    updater.delete_unreferenced_rows(existing.values());

    let (cvr_final, _stats) = updater.flush(CONNECT_TIME as i64, NOW, NOW);
    let ops = updater.base.drain_store_ops();
    store.apply_store_ops(ops);
    let expected_version = CVRVersion {
        state_version: "01".to_string(),
        config_version: None,
    };
    store
        .flush(&expected_version, &cvr_final, CONNECT_TIME)
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
        &golden["rows"],
        "persisted rows differ from TS golden"
    );

    // instances (version / replicaVersion / ttlClock) + rowsVersion.
    let inst = sqlx::query(&format!(
        r#"SELECT version, "replicaVersion", "ttlClock" FROM "{SCHEMA}".instances"#
    ))
    .fetch_one(&pool)
    .await
    .expect("select instances");
    let g_inst = &golden["instances"][0];
    assert_eq!(
        inst.get::<String, _>("version"),
        g_inst["version"].as_str().unwrap()
    );
    assert_eq!(
        inst.get::<Option<String>, _>("replicaVersion").as_deref(),
        g_inst["replicaVersion"].as_str()
    );
    assert_eq!(
        inst.get::<f64, _>("ttlClock") as i64,
        g_inst["ttlClock"].as_i64().unwrap()
    );

    let rv = sqlx::query(&format!(r#"SELECT version FROM "{SCHEMA}"."rowsVersion""#))
        .fetch_one(&pool)
        .await
        .expect("select rowsVersion");
    assert_eq!(
        rv.get::<String, _>("version"),
        golden["rowsVersion"][0]["version"].as_str().unwrap()
    );
}
