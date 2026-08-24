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
        updater.received(&rows, &existing);
        updater.delete_unreferenced_rows(existing.values());

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
