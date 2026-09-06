//! The `flushed cvr@…` line must carry TS's LogContext.
//!
//! TS emits it through the view-syncer's `LogContext`, so every flush line
//! carries `appID`, `shardNum`, `clientGroupID`, `instance`, `lock`,
//! `stateVersion` and `cvrFlushID` — the last stamped per call by
//! `lc.withContext('cvrFlushID', flushCounter++)` (cvr-store.ts:1238, logged at
//! :1249). A production TS line reads:
//!
//! ```text
//! {"appID":"sandbox_rust_test_ts","shardNum":0,
//!  "clientGroupID":"artdiff-0fed54d2de","stateVersion":"76j4ts6o0",
//!  "cvrFlushID":553868,"message":"flushed cvr@76j4ts6o0:01 {…} in (20.6 ms)"}
//! ```
//!
//! Rust emitted the same message with NO fields at all. That is a real parity
//! loss, not cosmetics: on the 2026-09-06 G8 diff-oracle run rust's terminal
//! cookie carried one more config-version bump than TS's on 2 of 4 pairs, and
//! the flush log was the only record of which config update did it — but with
//! no `clientGroupID` on the line, the rust flushes could not be attributed to
//! a client group at all, while the TS ones could.
//!
//! `instance` and `lock` have no rust twin (no `#lock` — the CG thread is
//! serial, INVENTIONS.md I-12 — and no per-service instance id), so they are
//! not asserted.
//!
//! NON-VACUOUS: drop the fields from the `tracing::info!` at the flush site and
//! every assertion below fails — the captured line is the bare message.
//!
//! PG-gated on `TEST_CVR_PG_URI`: the flush log only fires when a real store
//! flushed something (`store_flushed.is_some()`), so a storeless engine never
//! reaches the line.

mod common;
use common::{cvr_ddl, pg_uri};

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use rust_cvr::client_handler::WebSocketSink;
use rust_cvr::cvr::DesiredQuerySpec;
use rust_cvr::shards::ShardID;
use rust_syncer::services::view_syncer::pipeline_driver::IvmPipelines;
use rust_syncer::services::view_syncer::view_syncer::{
    CustomQueryTransformMode, ViewSyncerService as SyncEngine, empty_cvr,
};

const SCHEMA: &str = "cvr_flush_log_ctx";
const CG: &str = "cg-flush-log-ctx";
const APP: &str = "flushlogapp";

struct NullSink;
impl WebSocketSink for NullSink {
    fn push(&self, _msg: serde_json::Value) -> Result<(), String> {
        Ok(())
    }
    fn fail(&self, _e: String) {}
    fn cancel(&self) {}
}

#[derive(Clone)]
struct Cap(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for Cap {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Cap {
    type Writer = Cap;
    fn make_writer(&'a self) -> Cap {
        self.clone()
    }
}

#[test]
fn flushed_cvr_line_carries_the_ts_log_context() {
    let Some(uri) = pg_uri() else {
        eprintln!("SKIP flushed_cvr_line_carries_the_ts_log_context: TEST_CVR_PG_URI unset");
        return;
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let pool = rt.block_on(async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&uri)
            .await
            .expect("connect to TEST_CVR_PG_URI");
        sqlx::raw_sql(&cvr_ddl(SCHEMA))
            .execute(&pool)
            .await
            .expect("create cvr schema");
        sqlx::query(&format!(
            r#"INSERT INTO "{SCHEMA}".instances
                 ("clientGroupID","version","lastActive","replicaVersion","ttlClock")
               VALUES ('{CG}', '00', now(), '01', 0)"#
        ))
        .execute(&pool)
        .await
        .expect("seed instance");
        sqlx::query(&format!(
            r#"INSERT INTO "{SCHEMA}"."rowsVersion" ("clientGroupID","version")
               VALUES ('{CG}', '00')"#
        ))
        .execute(&pool)
        .await
        .expect("seed rowsVersion");
        pool
    });

    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE "issue" (
            "id"    "text|NOT_NULL",
            "title" "text",
            "_0_version" "text",
            PRIMARY KEY ("id")
        );
        INSERT INTO "issue" ("id", "title", "_0_version") VALUES ('i1', 'first', '01');
        "#,
    )
    .unwrap();
    let specs =
        rust_syncer::compute_zql_specs(&conn, &rust_syncer::ZqlSpecOptions::default(), None)
            .unwrap();
    let mut pipelines = IvmPipelines::new();
    pipelines
        .init_from_connection(specs, Rc::new(RefCell::new(conn)))
        .unwrap();

    let mut engine = SyncEngine::new(pipelines);
    let shard = ShardID {
        app_id: APP.to_string(),
        shard_num: 7,
    };
    // `set_cvr_store` builds a pool-backed store; it enters the ambient runtime.
    let _guard = rt.enter();
    // Production sets these in `new_with_accepting` from the CG's config; the
    // bare test constructor leaves them blank, so the seam supplies the same
    // identity TS carries in its LogContext.
    engine.set_identity_for_tests(APP, shard.clone(), CG);
    engine
        .set_cvr_store(
            pool.clone(),
            SCHEMA.to_string(),
            CG.to_string(),
            "flush-log-ctx-task".to_string(),
        )
        .expect("set_cvr_store");
    engine.register_client("client1", "ws1", CG, &shard, None, Arc::new(NullSink));

    let anyone_can = serde_json::json!({
        "tables": {"issue": {"row": {"select": [["allow", {"type": "and", "conditions": []}]]}}}
    });
    let puts = vec![DesiredQuerySpec {
        hash: "q_issue".to_string(),
        ast: Some(serde_json::json!({"table": "issue"})),
        name: None,
        args: None,
        ttl: None,
    }];

    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(Cap(buf.clone()))
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        rt.block_on(engine.config_and_hydrate(
            empty_cvr(CG, "01"),
            "client1",
            &["ws1".to_string()],
            &shard,
            puts,
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            Some(&anyone_can),
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "01".to_string(),
            0,
            0,
            0,
        ))
        .expect("config_and_hydrate");
    });

    let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let line = logged
        .lines()
        .find(|l| l.contains("flushed cvr@"))
        .unwrap_or_else(|| panic!("expected a `flushed cvr@` line; got:\n{logged}"));

    // Every field TS's LogContext puts on this line and rust has a twin for.
    for (field, value) in [
        ("appID", APP),
        ("shardNum", "7"),
        ("clientGroupID", CG),
        ("stateVersion", "00"),
    ] {
        assert!(
            line.contains(&format!("{field}={value}")),
            "TS's `flushed cvr@` line carries {field}={value} through the \
             view-syncer LogContext (cvr-store.ts:1249); without it a flush \
             cannot be attributed to a client group. Line: {line}"
        );
    }
    assert!(
        line.contains("cvrFlushID="),
        "TS stamps each flush with `lc.withContext('cvrFlushID', flushCounter++)` \
         (cvr-store.ts:1238). Line: {line}"
    );

    // `flushCounter++` is per-flush, so two flushes never share an id.
    let ids: Vec<&str> = logged
        .lines()
        .filter(|l| l.contains("flushed cvr@"))
        .filter_map(|l| l.split("cvrFlushID=").nth(1))
        .filter_map(|r| r.split(|c: char| !c.is_ascii_digit()).next())
        .collect();
    let mut uniq = ids.clone();
    uniq.sort_unstable();
    uniq.dedup();
    assert_eq!(
        ids.len(),
        uniq.len(),
        "cvrFlushID must be unique per flush (TS `flushCounter++`); got {ids:?}"
    );

    rt.block_on(async {
        let _ = sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE"#))
            .execute(&pool)
            .await;
    });
}
