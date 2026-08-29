//! Live-Postgres pin of `CVRStoreHandle::load_with_retries` — the port of the
//! TS `CVRStore.load` retry loop (cvr-store.ts:279-296): a CVR whose `rows`
//! version lags `instances.version` (the previous owner's pending row writes
//! not yet flushed) is retried every LOAD_ATTEMPT_INTERVAL_MS up to
//! MAX_LOAD_ATTEMPTS, then surfaced as `ClientNotFoundError` ("max attempts
//! exceeded…"), which makes the syncer spawn a FRESH client group. A CVR that
//! catches up MID-retry must load normally — the retry loop is a wait, not a
//! failure path.
//!
//! L2 triage item 1 (parity/coverage/rust-cvr/triage.md): this loop had no
//! test at all; `lcov` showed `load_with_retries` FNDA=0.
//!
//! Each test uses its OWN schema (cargo runs the two tests concurrently).
//! Gated on `TEST_CVR_PG_URI`; skips (passes) when unset.

use rust_cvr::cvr_store::{CVRStoreError, CVRStoreHandle};

const CVR_ID: &str = "cg-load-retry";
const TASK_ID: &str = "load-retry-task";

fn pg_uri() -> Option<String> {
    match std::env::var("TEST_CVR_PG_URI") {
        Ok(u) if !u.is_empty() => Some(u),
        _ => None,
    }
}

async fn fresh_schema(pool: &sqlx::PgPool, schema: &str) {
    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{schema}" CASCADE"#))
        .execute(pool)
        .await
        .expect("drop schema");
    // The checked-in DDL is the exact TS schema, written for "roze_1/cvr";
    // re-target it so concurrent tests don't drop each other's tables.
    let ddl = include_str!("../agentic/parity/flush-schema.sql").replace("roze_1/cvr", schema);
    sqlx::raw_sql(&ddl)
        .execute(pool)
        .await
        .expect("create schema");
}

/// Seed an instance at version '02' whose rowsVersion is stuck at '01' —
/// the exact rows-behind shape TS detects via `RowsVersionBehindError`.
async fn seed_rows_behind(pool: &sqlx::PgPool, schema: &str) {
    sqlx::raw_sql(&format!(
        r#"
        INSERT INTO "{schema}".instances
            ("clientGroupID", "version", "lastActive", "replicaVersion", "ttlClock")
        VALUES ('{CVR_ID}', '02', now(), '00', 0);
        INSERT INTO "{schema}"."rowsVersion" ("clientGroupID", "version")
        VALUES ('{CVR_ID}', '01');
        "#
    ))
    .execute(pool)
    .await
    .expect("seed rows-behind");
}

async fn connect(uri: &str) -> sqlx::PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(uri)
        .await
        .expect("connect to TEST_CVR_PG_URI")
}

/// rowsVersion permanently behind → every attempt sees RowsVersionBehind →
/// after MAX_LOAD_ATTEMPTS the error is ClientNotFound "max attempts
/// exceeded…" carrying the behind detail — NOT a bare RowsVersionBehind
/// (TS throws ClientNotFoundError so the client group is respawned fresh).
#[tokio::test(flavor = "multi_thread")]
async fn load_exhausts_retries_to_client_not_found_when_rows_stay_behind() {
    let Some(uri) = pg_uri() else {
        eprintln!("SKIP load_exhausts_retries: TEST_CVR_PG_URI unset");
        return;
    };
    let schema = "roze_1/cvr_lr_exhaust";
    let pool = connect(&uri).await;
    fresh_schema(&pool, schema).await;
    seed_rows_behind(&pool, schema).await;

    let mut store = CVRStoreHandle::new(
        pool.clone(),
        schema.to_string(),
        CVR_ID.to_string(),
        TASK_ID.to_string(),
    );
    let started = std::time::Instant::now();
    let err = store.load(0.0).await.expect_err("rows stay behind");
    match &err {
        CVRStoreError::ClientNotFound(msg) => {
            assert!(
                msg.contains("max attempts exceeded"),
                "TS message shape ('max attempts exceeded…'), got: {msg}"
            );
            assert!(
                msg.contains("01") && msg.contains("02"),
                "detail must carry the behind versions, got: {msg}"
            );
        }
        other => panic!("expected ClientNotFound, got: {other:?}"),
    }
    // 10 attempts with 500ms sleeps BETWEEN them (TS sleeps before attempts
    // 2..N, not before the first) ⇒ at least 9 intervals of wall clock.
    assert!(
        started.elapsed() >= std::time::Duration::from_millis(9 * 500),
        "retry loop must actually wait between attempts (elapsed {:?})",
        started.elapsed()
    );
}

/// rowsVersion catches up while load is retrying → load SUCCEEDS with the
/// caught-up CVR (version '02'); the retry loop is a wait, not a fast-fail.
#[tokio::test(flavor = "multi_thread")]
async fn load_succeeds_when_rows_catch_up_mid_retry() {
    let Some(uri) = pg_uri() else {
        eprintln!("SKIP load_succeeds_mid_retry: TEST_CVR_PG_URI unset");
        return;
    };
    let schema = "roze_1/cvr_lr_recover";
    let pool = connect(&uri).await;
    fresh_schema(&pool, schema).await;
    seed_rows_behind(&pool, schema).await;

    // Catch the rows table up after ~2 attempt intervals.
    let catchup_pool = pool.clone();
    let catchup = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        sqlx::query(&format!(
            r#"UPDATE "{schema}"."rowsVersion" SET "version" = '02' WHERE "clientGroupID" = $1"#
        ))
        .bind(CVR_ID)
        .execute(&catchup_pool)
        .await
        .expect("bump rowsVersion");
    });

    let mut store = CVRStoreHandle::new(
        pool.clone(),
        schema.to_string(),
        CVR_ID.to_string(),
        TASK_ID.to_string(),
    );
    let loaded = store.load(0.0).await.expect("load succeeds after catchup");
    assert_eq!(loaded.cvr.version.state_version, "02");
    catchup.await.unwrap();
}
