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
