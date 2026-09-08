//! Live-Postgres test for the standalone ttlClock persistence pair —
//! `CVRStoreHandle::update_ttl_clock` / `get_ttl_clock`, ports of TS
//! `CVRStore.updateTTLClock` / `getTTLClock` (cvr-store.ts:555-583).
//! F-CVR-STORE-8: TS refreshes `instances.ttlClock` + `lastActive` every
//! `TTL_CLOCK_INTERVAL` (60s) OUTSIDE any flush; before the port these columns
//! went stale between flushes (restart/rehome deferred TTL expiry, lastActive
//! skewed CVR-purge GC). This pins the SQL (to_timestamp ms conversion,
//! DOUBLE PRECISION ttlClock round-trip) against the exact TS DDL.
//!
//! Gated on `TEST_CVR_PG_URI`; skips (passes) when unset.

const SCHEMA: &str = "roze_1/cvr";
const CVR_ID: &str = "cg-ttl-clock";

#[tokio::test]
async fn update_and_get_ttl_clock_match_ts_contract() {
    let uri = match std::env::var("TEST_CVR_PG_URI") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!("SKIP update_and_get_ttl_clock_match_ts_contract: TEST_CVR_PG_URI unset");
            return;
        }
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&uri)
        .await
        .expect("connect to TEST_CVR_PG_URI");

    // Fresh schema from the exact TS DDL.
    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE"#))
        .execute(&pool)
        .await
        .expect("drop schema");
    sqlx::raw_sql(include_str!("../agentic/parity/flush-schema.sql"))
        .execute(&pool)
        .await
        .expect("create schema");
    sqlx::query(&format!(
        r#"INSERT INTO "{SCHEMA}".instances
           ("clientGroupID", "version", "lastActive", "ttlClock")
           VALUES ($1, '01', to_timestamp(0), 0)"#,
    ))
    .bind(CVR_ID)
    .execute(&pool)
    .await
    .expect("seed instance");

    let store = rust_cvr::cvr_store::CVRStoreHandle::new(
        pool.clone(),
        SCHEMA.to_string(),
        CVR_ID.to_string(),
        "ttl-task".to_string(),
    );

    // TS getTTLClock: existing instance → its ttlClock.
    assert_eq!(
        store.get_ttl_clock().await.expect("get"),
        Some(0),
        "seeded ttlClock must read back as 0"
    );

    // TS updateTTLClock(ttlClock, lastActive): standalone UPDATE outside flush.
    let last_active_ms = 1_724_500_000_123.0f64;
    store
        .update_ttl_clock(654_321, last_active_ms)
        .await
        .expect("update_ttl_clock");

    assert_eq!(
        store.get_ttl_clock().await.expect("get after update"),
        Some(654_321),
        "ttlClock must round-trip through instances.ttlClock"
    );
    // lastActive is TIMESTAMPTZ: verify the ms → to_timestamp conversion.
    let (ms,): (f64,) = sqlx::query_as(&format!(
        r#"SELECT (extract(epoch from "lastActive") * 1000.0)::double precision
           FROM "{SCHEMA}".instances WHERE "clientGroupID" = $1"#,
    ))
    .bind(CVR_ID)
    .fetch_one(&pool)
    .await
    .expect("read lastActive");
    assert!(
        (ms - last_active_ms).abs() < 1.0,
        "lastActive must persist the update's wall-clock ms (got {ms}, want {last_active_ms})"
    );

    // TS getTTLClock: uninitialized CVR → undefined (None).
    let missing = rust_cvr::cvr_store::CVRStoreHandle::new(
        pool.clone(),
        SCHEMA.to_string(),
        "cg-never-initialized".to_string(),
        "ttl-task".to_string(),
    );
    assert_eq!(
        missing.get_ttl_clock().await.expect("get missing"),
        None,
        "uninitialized CVR must yield None (TS undefined)"
    );
}

/// F-CVR-SCHEMA-1 regression: `desires.deleted` is a NULLABLE BOOL
/// (schema/cvr.ts:164 `deleted: boolean | null`) and TS reads NULL as falsy.
/// Pre-fix, Rust's non-optional `bool` in `DesireLoadRow` made a NULL fail
/// sqlx decode → the whole CVR `load()` errored on a legacy row. Proven by
/// temp-revert (ColumnDecode "UNEXPECTED_NULL").
#[tokio::test]
async fn load_treats_null_desire_deleted_as_falsy_like_ts() {
    let uri = match std::env::var("TEST_CVR_PG_URI") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!(
                "SKIP load_treats_null_desire_deleted_as_falsy_like_ts: TEST_CVR_PG_URI unset"
            );
            return;
        }
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&uri)
        .await
        .expect("connect to TEST_CVR_PG_URI");

    // Own schema — this test runs in parallel with the ttlClock test above,
    // which drops/recreates the shared parity schema.
    const SCHEMA2: &str = "roze_null_deleted/cvr";
    const CG: &str = "cg-null-deleted";
    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{SCHEMA2}" CASCADE"#))
        .execute(&pool)
        .await
        .expect("drop schema");
    sqlx::raw_sql(
        &include_str!("../agentic/parity/flush-schema.sql").replace("roze_1/cvr", SCHEMA2),
    )
    .execute(&pool)
    .await
    .expect("create schema");
    sqlx::raw_sql(&format!(
        r#"
        INSERT INTO "{SCHEMA2}".instances ("clientGroupID", "version", "lastActive", "ttlClock")
          VALUES ('{CG}', '01', to_timestamp(0), 0);
        INSERT INTO "{SCHEMA2}"."rowsVersion" ("clientGroupID", "version")
          VALUES ('{CG}', '01');
        INSERT INTO "{SCHEMA2}".clients ("clientGroupID", "clientID")
          VALUES ('{CG}', 'c1');
        INSERT INTO "{SCHEMA2}".queries ("clientGroupID", "queryHash", "clientAST", "patchVersion")
          VALUES ('{CG}', 'q1', '{{"table":"issue"}}', '01');
        -- Legacy row shape: deleted left NULL.
        INSERT INTO "{SCHEMA2}".desires ("clientGroupID", "clientID", "queryHash", "patchVersion", "deleted")
          VALUES ('{CG}', 'c1', 'q1', '01', NULL);
        "#
    ))
    .execute(&pool)
    .await
    .expect("seed");

    let mut store = rust_cvr::cvr_store::CVRStoreHandle::new(
        pool.clone(),
        SCHEMA2.to_string(),
        CG.to_string(),
        "ttl-task".to_string(),
    );
    let loaded = store.load(0.0).await.expect(
        "load must succeed with a NULL desires.deleted (TS reads it falsy, not a decode error)",
    );
    let client = loaded.cvr.clients.get("c1").expect("client c1 loaded");
    assert_eq!(
        client.desired_query_ids,
        vec!["q1".to_string()],
        "NULL deleted must count as an ACTIVE desire (TS falsy), not drop the row"
    );
}

/// F-CVR-STORE-9 regression — `catchup_config_patches` must mirror TS
/// `catchupConfigPatches` (cvr-store.ts:725-745) on two branches:
/// 1. Early return `[]` when `afterVersion >= upToCVR.version` — BEFORE any
///    SQL or version check (pre-fix: no early return, so the version check ran
///    and spuriously errored on a stale `up_to`).
/// 2. The `checkVersion` guard compares the DB version against the caller's
///    CURRENT snapshot, not against `up_to` (pre-fix: a diverged `current`
///    passed silently as long as `up_to` matched the DB).
/// Both proven by temp-revert.
#[tokio::test]
async fn catchup_config_patches_version_semantics_match_ts() {
    let uri = match std::env::var("TEST_CVR_PG_URI") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!(
                "SKIP catchup_config_patches_version_semantics_match_ts: TEST_CVR_PG_URI unset"
            );
            return;
        }
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&uri)
        .await
        .expect("connect to TEST_CVR_PG_URI");

    const SCHEMA3: &str = "roze_catchup_cfg/cvr";
    const CG: &str = "cg-catchup-cfg";
    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{SCHEMA3}" CASCADE"#))
        .execute(&pool)
        .await
        .expect("drop schema");
    sqlx::raw_sql(
        &include_str!("../agentic/parity/flush-schema.sql").replace("roze_1/cvr", SCHEMA3),
    )
    .execute(&pool)
    .await
    .expect("create schema");
    sqlx::raw_sql(&format!(
        r#"
        INSERT INTO "{SCHEMA3}".instances ("clientGroupID", "version", "lastActive", "ttlClock")
          VALUES ('{CG}', '02', to_timestamp(0), 0);
        INSERT INTO "{SCHEMA3}".queries ("clientGroupID", "queryHash", "clientAST", "patchVersion")
          VALUES ('{CG}', 'q1', '{{"table":"issue"}}', '02');
        "#
    ))
    .execute(&pool)
    .await
    .expect("seed");

    let store = rust_cvr::cvr_store::CVRStoreHandle::new(
        pool.clone(),
        SCHEMA3.to_string(),
        CG.to_string(),
        "catchup-task".to_string(),
    );
    use rust_cvr::schema::types::version_from_string;

    // 1. after == up_to ('01') while the DB is at '02': TS returns [] BEFORE
    //    checkVersion, so no ConcurrentModification even though '01' != '02'.
    let same = store
        .catchup_config_patches(
            Some(version_from_string("01")),
            &version_from_string("01"),
            &version_from_string("02"),
        )
        .await
        .expect("after >= upTo must early-return Ok([]) before the version check");
    assert!(same.is_empty(), "early return must yield no patches");

    // 2. up_to matches the DB ('02') but the caller's CURRENT snapshot ('03')
    //    does not: TS checkVersion(current) throws ConcurrentModification.
    let err = store
        .catchup_config_patches(None, &version_from_string("02"), &version_from_string("03"))
        .await;
    assert!(
        matches!(
            err,
            Err(rust_cvr::cvr_store::CVRStoreError::ConcurrentModification { .. })
        ),
        "a diverged `current` must fail checkVersion like TS; got {err:?}"
    );

    // Control: consistent versions succeed and surface the q1 patch.
    let ok = store
        .catchup_config_patches(None, &version_from_string("02"), &version_from_string("02"))
        .await
        .expect("consistent versions must succeed");
    assert_eq!(ok.len(), 1, "the q1 got-query patch is in range");
}
