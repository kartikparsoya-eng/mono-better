//! `finished processing queries (process: … ms, wall: … ms)` must report TS's
//! span, not a narrower one.
//!
//! TS emits this line at the end of `#addAndRemoveQueries`, timed from that
//! method's OWN `const start = performance.now()` (view-syncer.ts:2168, logged
//! at :2364-2366). The span therefore covers hydrate, `deleteUnreferencedRows`,
//! `#flushUpdater` (the CVR write), `#catchupClients` and `pokers.end`. It is a
//! SECOND clock, distinct from `#syncQueryPipelineSet`'s `start` (:1880), which
//! feeds the `viewSyncerHydration` histogram (:2109) instead.
//!
//! Rust emitted the same message from inside `hydrate_and_sync`, reporting
//! `fetch_started.elapsed()` — the initial-fetch loop ALONE. So the identical
//! log line meant a different thing on each engine, and the one line built to
//! expose post-fetch cost hid exactly the CVR flush, catch-up and poke it was
//! supposed to show. On an A/B replay that read as rust `wall == process`
//! against TS `wall >> process`, which would have been read as "rust never
//! yields" if taken at face value.
//!
//! Mutation test: the sink below stalls the `pokeEnd` frame, which is inside TS's
//! span and outside the old rust one. Move the log back into `hydrate_and_sync`
//! and the reported `wall` drops to the fetch time, failing the assertion.
use rust_syncer::services::view_syncer::view_syncer::FlushTimes;
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

/// Time the `pokeEnd` frame is held. Comfortably above the fetch time of a
/// two-row table, so the two spans cannot be confused.
const POKE_END_STALL_MS: u64 = 120;

struct StallingPokeSink {
    frames: Mutex<Vec<serde_json::Value>>,
}

impl WebSocketSink for StallingPokeSink {
    fn push(&self, msg: serde_json::Value) -> Result<(), String> {
        if msg[0] == "pokeEnd" {
            std::thread::sleep(std::time::Duration::from_millis(POKE_END_STALL_MS));
        }
        self.frames.lock().unwrap().push(msg);
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
fn processing_wall_covers_the_poke_not_just_the_fetch() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE "issue" (
            "id"    "text|NOT_NULL",
            "title" "text",
            "_0_version" "text",
            PRIMARY KEY ("id")
        );
        INSERT INTO "issue" ("id", "title", "_0_version") VALUES
            ('i1', 'first issue', '01'),
            ('i2', 'second issue', '01');
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
    let sink: Arc<dyn WebSocketSink> = Arc::new(StallingPokeSink {
        frames: Mutex::new(Vec::new()),
    });
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

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
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(engine.config_and_hydrate(
            empty_cvr("cg1", "01"),
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
            FlushTimes {
                last_connect_time: 0,
                last_active: 0,
                ttl_clock: 0,
            },
        ))
        .unwrap();
    });

    let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let line = logged
        .lines()
        .find(|l| l.contains("finished processing queries"))
        .unwrap_or_else(|| {
            panic!("expected the TS `finished processing queries` line; got:\n{logged}")
        });

    let nums: Vec<f64> = line
        .split("process: ")
        .nth(1)
        .and_then(|rest| {
            let (p, tail) = rest.split_once(" ms, wall: ")?;
            let (w, _) = tail.split_once(" ms)")?;
            Some(vec![p.trim().parse().ok()?, w.trim().parse().ok()?])
        })
        .unwrap_or_else(|| panic!("could not parse process/wall out of: {line}"));
    let (process, wall) = (nums[0], nums[1]);

    // `wall` spans the poke (TS: hydrate .. pokers.end), so the stall lands in it.
    assert!(
        wall >= (POKE_END_STALL_MS as f64) * 0.8,
        "wall must span through pokeEnd like TS's #addAndRemoveQueries clock \
         (view-syncer.ts:2168 -> :2364); a {POKE_END_STALL_MS}ms pokeEnd stall \
         reported wall={wall}ms, which is the FETCH span, not the TS span. \
         Line: {line}"
    );
    // ...and `process` is hydrate process time only, so it does NOT.
    assert!(
        process < (POKE_END_STALL_MS as f64) * 0.8,
        "process is TS `totalProcessTime` — hydrate process time, excluding the \
         poke; got process={process}ms. Line: {line}"
    );
}
