//! B-POOL regression (task #112): CVR load under pool-acquire contention must
//! RETRY and succeed — never surface a transient `PoolTimedOut` as a terminal
//! load failure (which the router escalates to `fail_group` → the client
//! group's clients reconnect and cold-rehydrate, adding MORE pool demand — the
//! self-amplifying storm behind the ART G25 latency FAIL: 548 pool timeouts,
//! 314 CG kills).
//!
//! TS twin of the behavior: postgres.js has NO acquire timeout — pool
//! contention QUEUES unboundedly and degrades to latency, never to an error
//! (see main.rs pool comment + `load_with_retries` PoolTimedOut arm, both
//! labeled Rust-only adaptations).
//!
//! Gated on `TEST_CVR_PG_URI`; skips (passes) when unset.

const SCHEMA: &str = "roze_1/cvr";
const CVR_ID: &str = "cg-pool-contention";

#[tokio::test]
async fn load_survives_pool_acquire_contention() {
    let uri = match std::env::var("TEST_CVR_PG_URI") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!("SKIP load_survives_pool_acquire_contention: TEST_CVR_PG_URI unset");
            return;
        }
    };
    // Setup pool (separate from the contended one) for DDL + seeding.
    let setup = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&uri)
        .await
        .expect("connect to TEST_CVR_PG_URI");
    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE"#))
        .execute(&setup)
        .await
        .expect("drop schema");
    sqlx::raw_sql(include_str!("../agentic/parity/flush-schema.sql"))
        .execute(&setup)
        .await
        .expect("create schema");
    sqlx::query(&format!(
        r#"INSERT INTO "{SCHEMA}".instances
           ("clientGroupID", "version", "lastActive", "ttlClock")
           VALUES ($1, '01', to_timestamp(0), 0)"#,
    ))
    .bind(CVR_ID)
    .execute(&setup)
    .await
    .expect("seed instance");
    sqlx::query(&format!(
        r#"INSERT INTO "{SCHEMA}"."rowsVersion" ("clientGroupID", "version")
           VALUES ($1, '01')"#,
    ))
    .bind(CVR_ID)
    .execute(&setup)
    .await
    .expect("seed rowsVersion");

    // The CONTENDED pool: one connection, aggressive acquire timeout so the
    // convoy is felt immediately (production default is 120s; 200ms here keeps
    // the test fast while exercising the same PoolTimedOut error path).
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_millis(200))
        .connect(&uri)
        .await
        .expect("connect contended pool");

    // Hold the ONLY connection for ~1.6s — long enough that the first load
    // attempts time out acquiring, short enough that the 500ms-interval
    // retries (MAX_LOAD_ATTEMPTS=10) find a free connection afterwards.
    // The oneshot guarantees the holder OWNS the connection before load()
    // runs — otherwise load can win the race and the test proves nothing.
    let holder_pool = pool.clone();
    let (held_tx, held_rx) = tokio::sync::oneshot::channel::<()>();
    let holder = tokio::spawn(async move {
        let conn = holder_pool.acquire().await.expect("hold the connection");
        let _ = held_tx.send(());
        tokio::time::sleep(std::time::Duration::from_millis(1600)).await;
        drop(conn);
    });
    held_rx.await.expect("holder signalled");

    let mut store = rust_cvr::cvr_store::CVRStoreHandle::new(
        pool.clone(),
        SCHEMA.to_string(),
        CVR_ID.to_string(),
        "pool-task".to_string(),
    );
    let loaded = store.load(0.0).await.expect(
        "load must ride out pool-acquire contention by retrying \
         (PoolTimedOut is back-pressure, not a terminal CVR failure)",
    );
    assert_eq!(loaded.cvr.id, CVR_ID);
    holder.await.expect("holder task");
}

/// NON-VACUOUS (fix, 2026-09-05): a flush that fails ACQUIRING its connection
/// must leave the pending write set intact, so the caller's retry actually
/// re-sends it.
///
/// `CVRStoreHandle::flush` consumes `self.pending` with `std::mem::take`. That
/// take used to run BEFORE `pool.begin()`, so a pool-acquire timeout ate the
/// writes: the retry in `flush_ops_to_store` then found an empty pending set,
/// took the `is_empty()` early return, and reported a QUIET COMMIT — a success.
/// The writes were silently dropped and no error ever reached the client.
///
/// Observed in a 60-minute prod-trace replay: 192 of 192 CVR flush failures
/// logged `retry 1/3` and then "succeeded", with ZERO escalations to attempt
/// 2 or 3 and ZERO group failures — against a payload PG rejects
/// deterministically, which could never have succeeded on a genuine re-send.
///
/// Move the `take` back above `pool.begin()` and this test FAILS: the second
/// flush returns `Ok(None)` (quiet commit) and the instance row never advances.
#[tokio::test]
async fn flush_pool_acquire_failure_preserves_pending_writes() {
    use rust_cvr::cvr::{CVRConfigDrivenUpdater, DesiredQuerySpec};
    use rust_cvr::shards::ShardID;

    let uri = match std::env::var("TEST_CVR_PG_URI") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!(
                "SKIP flush_pool_acquire_failure_preserves_pending_writes: TEST_CVR_PG_URI unset"
            );
            return;
        }
    };
    // Own schema: the sibling test in this file uses `roze_1/cvr`, and cargo
    // runs tests in a binary concurrently.
    const S: &str = "cvr_flush_retry";
    const CG: &str = "cg-flush-retry";

    let setup = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&uri)
        .await
        .expect("connect to TEST_CVR_PG_URI");
    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{S}" CASCADE"#))
        .execute(&setup)
        .await
        .expect("drop schema");
    sqlx::raw_sql(&include_str!("../agentic/parity/flush-schema.sql").replace("roze_1/cvr", S))
        .execute(&setup)
        .await
        .expect("create schema");
    sqlx::query(&format!(
        r#"INSERT INTO "{S}".instances
             ("clientGroupID","version","lastActive","replicaVersion","ttlClock")
           VALUES ('{CG}', '01', now(), '00', 0)"#
    ))
    .execute(&setup)
    .await
    .expect("seed instance");
    sqlx::query(&format!(
        r#"INSERT INTO "{S}"."rowsVersion" ("clientGroupID","version") VALUES ('{CG}', '01')"#
    ))
    .execute(&setup)
    .await
    .expect("seed rowsVersion");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_millis(200))
        .connect(&uri)
        .await
        .expect("connect contended pool");

    let mut store = rust_cvr::cvr_store::CVRStoreHandle::new(
        pool.clone(),
        S.to_string(),
        CG.to_string(),
        "flush-retry-task".to_string(),
    );
    // Load BEFORE starving the pool — this test is about the flush, not the load.
    let loaded = store.load(0.0).await.expect("load");
    let expected_version = loaded.cvr.version.clone();

    // A config change with real pending ops (client + desired query).
    let mut cfg = CVRConfigDrivenUpdater::new(
        loaded.cvr,
        ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        },
    );
    cfg.ensure_client("c1");
    let patches = cfg.put_desired_queries(
        "c1",
        &[DesiredQuerySpec {
            hash: "q1".to_string(),
            ast: Some(serde_json::json!({"table": "users"})),
            name: None,
            args: None,
            ttl: None,
        }],
    );
    assert!(
        !patches.is_empty(),
        "the config change must produce patches"
    );
    let (cvr_final, _stats) = cfg.flush(0, 0, 0);
    assert_ne!(
        cvr_final.version, expected_version,
        "the config change must bump the version"
    );
    store.apply_store_ops(cfg.base.drain_store_ops());

    // Starve the pool so `pool.begin()` cannot acquire.
    let holder_pool = pool.clone();
    let (held_tx, held_rx) = tokio::sync::oneshot::channel::<()>();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let holder = tokio::spawn(async move {
        let conn = holder_pool.acquire().await.expect("hold the connection");
        let _ = held_tx.send(());
        let _ = release_rx.await;
        drop(conn);
    });
    held_rx.await.expect("holder signalled");

    let first = store.flush(&expected_version, &cvr_final, 0.0).await;
    assert!(
        matches!(
            first,
            Err(rust_cvr::cvr_store::CVRStoreError::Sqlx(
                sqlx::Error::PoolTimedOut
            ))
        ),
        "expected a pool-acquire timeout, got {first:?}"
    );

    let _ = release_tx.send(());
    holder.await.expect("holder task");

    // THE ASSERTION: the retry must actually re-send. A quiet commit here means
    // the writes were eaten by the failed attempt.
    let second = store.flush(&expected_version, &cvr_final, 0.0).await;
    match second {
        Ok(Some(_)) => {}
        Ok(None) => panic!(
            "retry reported a QUIET COMMIT — the failed acquire consumed the \
             pending writes, so the retry silently discarded them"
        ),
        Err(e) => panic!("retry failed: {e}"),
    }

    let persisted: (String,) = sqlx::query_as(&format!(
        r#"SELECT "version" FROM "{S}".instances WHERE "clientGroupID" = $1"#
    ))
    .bind(CG)
    .fetch_one(&setup)
    .await
    .expect("read back instance");
    let want = rust_cvr::schema::types::version_string(&cvr_final.version);
    assert_eq!(
        persisted.0, want,
        "the CVR must actually be at the post-config version after the retry"
    );
}
