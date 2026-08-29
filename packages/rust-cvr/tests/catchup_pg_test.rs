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
use rust_cvr::schema::types::{NullableCVRVersion, version_from_string};
use serde_json::Value;
use std::sync::Arc;

/// L2 triage item 3: a catchup spanning MORE than one cursor page
/// (CATCHUP_PAGE_SIZE = 10000, the TS `.cursor(10000)` twin). The golden
/// scenarios above are all single-page, so the multi-page pull loop and its
/// page-boundary closures had FNDA=0. Seeds 25_000 qualifying rows → the
/// cursor must yield 10000/10000/5000 and every seeded key exactly once.
#[tokio::test]
async fn catchup_pages_through_large_row_sets() {
    let uri = match std::env::var("TEST_CVR_PG_URI") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!("SKIP catchup_pages_through_large_row_sets: TEST_CVR_PG_URI unset");
            return;
        }
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&uri)
        .await
        .expect("connect to TEST_CVR_PG_URI");

    const SCHEMA: &str = "cvr_paging";
    const TOTAL: usize = 25_000;
    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE"#))
        .execute(&pool)
        .await
        .expect("drop schema");
    let ddl = include_str!("../agentic/parity/flush-schema.sql").replace("roze_1/cvr", SCHEMA);
    sqlx::raw_sql(&ddl).execute(&pool).await.expect("ddl");
    sqlx::raw_sql(&format!(
        r#"
        INSERT INTO "{SCHEMA}".instances
            ("clientGroupID", "version", "lastActive", "replicaVersion", "ttlClock")
        VALUES ('cgP', '03', now(), '00', 0);
        INSERT INTO "{SCHEMA}"."rowsVersion" ("clientGroupID", "version") VALUES ('cgP', '03');
        INSERT INTO "{SCHEMA}".rows
            ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
        SELECT 'cgP', '', 't', jsonb_build_object('id', g), '02', '02',
               jsonb_build_object('q1', 1)
        FROM generate_series(1, {TOTAL}) g;
        "#
    ))
    .execute(&pool)
    .await
    .expect("seed paging rows");

    let cache = RowRecordCache::new(
        pool.clone(),
        SCHEMA.to_string(),
        "cgP".to_string(),
        100,
        Arc::new(|_: String| {}),
        None,
    );
    let mut cursor = cache
        .catchup_row_patches(
            Some(version_from_string("01")),
            &version_from_string("03"),
            &version_from_string("03"),
            &[],
        )
        .await
        .expect("catchup starts");

    let mut page_sizes: Vec<usize> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    while let Some(page) = cursor.next_page().await.expect("page") {
        page_sizes.push(page.len());
        for r in &page {
            let v = serde_json::to_value(r).unwrap();
            assert_eq!(v["patchVersion"], "02");
            let id = v["rowKey"]["id"].as_i64().expect("id key");
            assert!(seen.insert(id), "row id {id} emitted twice across pages");
        }
    }
    assert_eq!(
        seen.len(),
        TOTAL,
        "every seeded row exactly once (pages: {page_sizes:?})"
    );
    assert!(
        page_sizes.len() >= 3 && page_sizes[0] == 10_000,
        "expected 10000-row pages then a remainder, got {page_sizes:?}"
    );
}

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
