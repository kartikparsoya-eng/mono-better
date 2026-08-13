//! Live-Postgres differential for `catchup_row_patches`.
//!
//! Seeds a disposable Postgres with the shared `catchup-seed.sql` (identical
//! CVR row-set the TS golden generator used) and asserts the Rust catchup emits
//! the exact same rows as the TS `catchupRowPatches` SQL captured in
//! `catchup-fixture.json`. Exercises the landmines: base<head partial catchup
//! (rows emitted at their STORED patchVersion, not promoted), poisoned rowKey
//! replay (a rowKey carrying a non-PK column passed through verbatim), the
//! `?|` exclude-hashes filter (tombstones kept), and the checkVersion guard
//! (current != head => error).
//!
//! Gated on `TEST_CVR_PG_URI`; skips (passes) when unset so the suite still
//! runs without a database.

use rust_cvr::row_record_cache::RowRecordCache;
use rust_cvr::version::{NullableCVRVersion, version_from_string};
use serde_json::Value;
use std::sync::Arc;

fn sort_key(v: &Value) -> String {
    serde_json::to_string(&serde_json::json!([v["patchVersion"], v["rowKey"]])).unwrap()
}

#[tokio::test]
async fn catchup_matches_ts_golden() {
    let uri = match std::env::var("TEST_CVR_PG_URI") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!("SKIP catchup_matches_ts_golden: TEST_CVR_PG_URI unset");
            return;
        }
    };

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&uri)
        .await
        .expect("connect to TEST_CVR_PG_URI");

    // Seed the exact same state the TS golden was generated from.
    sqlx::raw_sql(include_str!("../agentic/parity/catchup-seed.sql"))
        .execute(&pool)
        .await
        .expect("run catchup-seed.sql");

    let golden: Value =
        serde_json::from_str(include_str!("../agentic/parity/catchup-fixture.json"))
            .expect("catchup-fixture.json");

    for scen in golden["scenarios"].as_array().expect("scenarios") {
        let name = scen["name"].as_str().unwrap();
        let after: NullableCVRVersion = scen["after"].as_str().map(version_from_string);
        let up_to = version_from_string(scen["upTo"].as_str().unwrap());
        let current = version_from_string(scen["current"].as_str().unwrap());
        let exclude: Vec<String> = scen["exclude"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();

        let cache = RowRecordCache::new(
            pool.clone(),
            "cvr_parity".to_string(),
            "cg1".to_string(),
            100,
            Arc::new(|_: String| {}),
            None,
        );
        let started = cache
            .catchup_row_patches(after, &up_to, &current, &exclude)
            .await;

        // checkVersion runs inside the streaming task, so the mismatch surfaces
        // on the first page pull, not on the call itself.
        if scen
            .get("expectError")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            let mut cursor = started.expect("catchup should start");
            let page = cursor.next_page().await;
            // Assert the SPECIFIC checkVersion error, not merely any Err — a
            // connection/setup failure would also be Err and pass a blanket
            // is_err() check for the wrong reason.
            let err = page.expect_err(&format!("[{name}] expected an error"));
            assert!(
                err.contains("version mismatch"),
                "[{name}] expected a checkVersion mismatch, got a different error: {err}"
            );
            continue;
        }

        let mut cursor = started.expect("catchup ok");
        let mut rows: Vec<Value> = Vec::new();
        while let Some(page) = cursor.next_page().await.expect("catchup page") {
            for r in &page {
                rows.push(serde_json::to_value(r).unwrap());
            }
        }

        let mut expected: Vec<Value> = scen["rows"].as_array().unwrap().clone();
        rows.sort_by_key(sort_key);
        expected.sort_by_key(sort_key);
        assert_eq!(
            rows, expected,
            "[{name}] catchup rows differ from TS golden"
        );
    }
}
