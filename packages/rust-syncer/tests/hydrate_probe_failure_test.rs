//! A hydrate failure must reach the client as an ERROR FRAME, never as a dead
//! CG task.
//!
//! TS `#addQueryImpl` runs the scanstatus cost model by PREPARING the probe SQL
//! (`const stmt = db.prepare(sql)`, sqlite-cost-model.ts:78). An unpreparable
//! probe — a query argument carrying a character SQLite's tokenizer rejects —
//! makes better-sqlite3 THROW a `SqliteError`. That throw is caught by
//! `#addQueryImpl` (`'query-pipeline-hydrate-failed'`), RETHROWN
//! (pipeline-driver.ts:794-812), and surfaces at the view-syncer as
//! `#cleanup(err)` → `client.fail(err)`: every client of the group gets an
//! `Internal` error frame and a clean close.
//!
//! Rust's cost-model closure cannot return a `Result` (the signature is TS's),
//! so a probe failure unwinds — that part is faithful. What was NOT faithful is
//! where the unwind LANDED: `HydrateChanges` re-threw it past the driver, so it
//! killed the client-group task (`cg_executor.rs:299` "CG … task panicked",
//! `fail_group("panic")`). A task panic bypasses `fail_group` entirely, so the
//! clients' sockets just died with NO error frame — a different observable
//! outcome than TS for the same input (AGENTS.md rules 1 and 10).
//!
//! Measured on the 60-minute prod-replay of image 37c1908dd (20 cores / 50 GB):
//! 160 `CG … task panicked: probe SQL contains NUL byte: SELECT …` lines (the
//! rust-only NUL-guard text of the time; SQLite's own `unrecognized token` is
//! what TS and rust report now), one
//! per client group killed. The TS arm of the same replay logged ~230
//! `unrecognized token: "'￿ "` errors — the SAME queries, surfaced as error
//! frames, with the groups still serving afterwards.
//!
//! Mutation test: restore `std::panic::resume_unwind(payload)` in
//! `pipeline_driver::hydrate` / `HydrateChanges::next` and both tests below
//! abort with that panic instead of reporting an `Err`.
//!
//! LOG SURFACE (third test): the carrier unwind is caught, but the DEFAULT
//! panic hook still reported it — three raw non-JSON stderr lines per probe
//! failure (`thread 'cg-exec-N' panicked at …/sqlite_cost_model.rs:632`,
//! `Box<dyn Any>`, the `RUST_BACKTRACE` note), 229 times in a 60-minute
//! production-trace replay, while the TS arm's caught throw printed nothing beyond its
//! JSON log lines. `install_typed_unwind_panic_hook` (INVENTIONS.md I-22)
//! silences exactly the two carrier payloads. Mutation test: make that installer a
//! no-op and the child process's stderr carries `sqlite_cost_model.rs` again.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use rusqlite::Connection;
use rust_cvr::client_handler::WebSocketSink;
use rust_cvr::cvr::DesiredQuerySpec;
use rust_cvr::shards::ShardID;
use rust_ivm::sqlite::sqlite_cost_model::scanstatus_available;
use rust_syncer::services::view_syncer::pipeline_driver::{IvmPipelines, Timer};
use rust_syncer::services::view_syncer::view_syncer::{
    CustomQueryTransformMode, TimeSliceTimer, ViewSyncerService as SyncEngine, empty_cvr,
};
use rust_syncer::ws_sink::{DirectWebSocketSink, WsCommand};

/// A replica-shaped schema (the `"text|NOT_NULL"` column types `compute_zql_specs`
/// parses) with a parent/child pair, so the planner has a correlated subquery to
/// cost — i.e. so it actually PROBES.
fn seeded_pipelines() -> IvmPipelines {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE "issue" (
            "id"    "text|NOT_NULL",
            "title" "text",
            "_0_version" "text",
            PRIMARY KEY ("id")
        );
        CREATE TABLE "comment" (
            "id"      "text|NOT_NULL",
            "issueId" "text",
            "body"    "text",
            "_0_version" "text",
            PRIMARY KEY ("id")
        );
        INSERT INTO "issue" ("id", "title", "_0_version") VALUES ('i1', 'first', '01');
        INSERT INTO "comment" ("id", "issueId", "body", "_0_version")
            VALUES ('c1', 'i1', 'hello', '01');
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
    pipelines
}

/// An `EXISTS` whose child filter inlines a literal SQLite cannot tokenize.
/// Both characters below come straight from the prod-replay query args that
/// produced the panics; the NUL is what `CString::new` rejects when the probe
/// is prepared.
fn unprobeable_ast() -> serde_json::Value {
    let nasty = format!("{}{}needle", '\u{FFFF}', '\u{0}');
    serde_json::json!({
        "table": "issue",
        "where": {
            "type": "correlatedSubquery",
            "op": "EXISTS",
            "related": {
                "correlation": {"parentField": ["id"], "childField": ["issueId"]},
                "subquery": {
                    "table": "comment",
                    "alias": "comments",
                    "where": {
                        "type": "simple",
                        "op": "=",
                        "left": {"type": "column", "name": "body"},
                        "right": {"type": "literal", "value": nasty}
                    }
                }
            }
        }
    })
}

/// Driver level: `hydrate` REPORTS the failure. TS's `db.prepare` throw is a
/// catchable error at its call site; rust's equivalent must not escape the
/// driver as an unwind.
#[test]
fn unpreparable_cost_probe_fails_the_hydrate_instead_of_unwinding() {
    if !scanstatus_available() {
        eprintln!(
            "SKIP: linked SQLite lacks SQLITE_ENABLE_STMT_SCANSTATUS, so no \
             cost-model probe runs and there is nothing to fail"
        );
        return;
    }
    let mut pipelines = seeded_pipelines();
    // `HydrateChanges` borrows the driver and is not `Debug`, so match rather
    // than `expect_err`.
    let err = match pipelines.hydrate(
        &[("q1".to_string(), unprobeable_ast().to_string())],
        Rc::new(TimeSliceTimer::new()) as Rc<dyn Timer>,
    ) {
        Err(e) => e,
        Ok(_) => panic!(
            "an unpreparable cost-model probe must FAIL the hydrate (TS: \
             db.prepare throws a SqliteError), not unwind past the driver"
        ),
    };
    assert_eq!(
        err.name, "SqliteError",
        "TS `db.prepare` throws better-sqlite3's SqliteError (sqlite-cost-model.ts:78); \
         its name is what `String(e)` prints on the fail line"
    );
    assert!(
        err.message.contains("unrecognized token"),
        "the reported error must carry the probe failure so the client's \
         Internal error body is diagnostic, like TS's SqliteError message; got: {err}"
    );

    // The driver stays usable: TS's throw leaves the PipelineDriver intact
    // (`#hydrateContext = null` in the `finally`), and a well-formed query
    // hydrates afterwards.
    let mut ok = pipelines
        .hydrate(
            &[("q2".to_string(), r#"{"table":"issue"}"#.to_string())],
            Rc::new(TimeSliceTimer::new()) as Rc<dyn Timer>,
        )
        .expect("the driver must still hydrate after a failed probe");
    let rows = ok.by_ref().count();
    ok.finish().unwrap();
    assert!(rows > 0, "expected the follow-up hydrate to stream rows");
}

/// Client-observable level: the same query drives `config_and_hydrate` to an
/// `Err`, which is what the CG dispatch turns into `fail_group(e)` →
/// `wrap_with_protocol_error` → an `Internal` frame on every connection
/// (view_syncer.rs:2876-2879) — TS `#cleanup(err)` → `client.fail(err)`.
/// Pre-fix this call ABORTED the task, so `fail_group` never ran.
#[test]
fn unpreparable_cost_probe_fails_the_group_with_an_error_not_a_dead_task() {
    if !scanstatus_available() {
        eprintln!("SKIP: linked SQLite lacks SQLITE_ENABLE_STMT_SCANSTATUS");
        return;
    }
    let mut engine = SyncEngine::new(seeded_pipelines());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

    let anyone_can = serde_json::json!({
        "tables": {
            "issue": {"row": {"select": [["allow", {"type": "and", "conditions": []}]]}},
            "comment": {"row": {"select": [["allow", {"type": "and", "conditions": []}]]}}
        }
    });
    let puts = vec![DesiredQuerySpec {
        hash: "q_bad".to_string(),
        ast: Some(unprobeable_ast()),
        name: None,
        args: None,
        ttl: None,
    }];

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let err = rt
        .block_on(engine.config_and_hydrate(
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
            0,
            0,
            0,
        ))
        .expect_err(
            "a hydrate whose cost probe cannot be prepared must return an ERROR \
             the caller can fail_group with (TS throws to #cleanup), not abort \
             the client-group task",
        );
    assert!(
        err.message.contains("unrecognized token"),
        "the group must be failed WITH the underlying error (it becomes the \
         Internal error body the client sees); got: {err}"
    );

    // The config pass legitimately poked its desired-query patches first (TS
    // `#updateCVRConfig` flushes and pokes before `#syncQueryPipelineSet`), but
    // the failed hydrate must have delivered NO rows: TS throws out of
    // `#addQueryImpl` before a single change reaches `#processChanges`.
    let mut rows_patches = 0;
    while let Ok(cmd) = rx.try_recv() {
        if let Some(msg) = cmd.frame_value()
            && msg[0] == "pokePart"
            && msg.get(1).and_then(|b| b.get("rowsPatch")).is_some()
        {
            rows_patches += 1;
        }
    }
    assert_eq!(
        rows_patches, 0,
        "a hydrate that failed its cost probe must not have streamed rows"
    );
}

/// Log-surface level, in a CHILD PROCESS (a panic hook is process-global and
/// libtest captures per-thread, so only a real process boundary observes what
/// reaches stderr). The child does what `main.rs` does at entry — install the
/// hook — then fails the real probe, then raises an ordinary panic for
/// contrast. The parent asserts the probe's carrier left NO trace on stderr
/// while the ordinary panic still got the default report: the hook must be
/// silent for carriers only, never for defects.
#[test]
fn typed_probe_unwind_is_silent_on_stderr_while_a_defect_panic_still_reports() {
    if !scanstatus_available() {
        eprintln!("SKIP: linked SQLite lacks SQLITE_ENABLE_STMT_SCANSTATUS");
        return;
    }
    const CHILD_ENV: &str = "HYDRATE_PROBE_PANIC_HOOK_CHILD";
    const TEST_NAME: &str =
        "typed_probe_unwind_is_silent_on_stderr_while_a_defect_panic_still_reports";
    if std::env::var_os(CHILD_ENV).is_some() {
        rust_ivm::sqlite::sqlite_cost_model::install_typed_unwind_panic_hook();
        let mut pipelines = seeded_pipelines();
        let failed = pipelines
            .hydrate(
                &[("q1".to_string(), unprobeable_ast().to_string())],
                Rc::new(TimeSliceTimer::new()) as Rc<dyn Timer>,
            )
            .is_err();
        assert!(
            failed,
            "the unprobeable query must fail the hydrate in the child"
        );
        let defect = std::panic::catch_unwind(|| panic!("DEFECT_PANIC_MARKER"));
        assert!(defect.is_err());
        println!("CHILD_OK");
        std::process::exit(0);
    }

    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([TEST_NAME, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, "1")
        .output()
        .expect("spawn the test binary as the child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success() && stdout.contains("CHILD_OK"),
        "child did not complete: status={:?}\nstdout={stdout}\nstderr={stderr}",
        out.status
    );
    assert!(
        stderr.contains("DEFECT_PANIC_MARKER"),
        "a defect panic must still reach the default hook's stderr report \
         (the hook may silence carriers only); stderr={stderr}"
    );
    assert!(
        !stderr.contains("sqlite_cost_model.rs") && !stderr.contains("Box<dyn Any>"),
        "the probe's typed unwind must leave NO panic report on stderr — TS's \
         caught SqliteError prints nothing beyond its JSON log lines; stderr={stderr}"
    );
}
