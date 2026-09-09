//! `zero.sync.ivm.advance-time` records the PROCESS time of one change — TS
//! `#advanceTime.recordMs(elapsed, {table})` with `elapsed = timer.totalElapsed()
//! - start` (pipeline-driver.ts:981/1034-1038): a delta of the view-syncer's
//! TimeSliceTimer, which the engine reads through the advance gate's clock.
//!
//! NON-VACUOUS: the gate clock here is a constant, so the TS-parity value is
//! exactly 0.0 for every change; the pre-2026-09-09 site recorded
//! `Instant::elapsed()` wall-clock, which is never exactly 0.0.
//!
//! Run: cargo test --test ivm_advance_time_metric_test

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use rusqlite::Connection;
use rust_ivm::builder::ast::Ast;
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::stream::StreamItem;
use rust_ivm::snapshotter::Snapshotter;
use rust_ivm::snapshotter::spec::{ColumnSchema, LiteAndZqlSpec, TableSpec};
use rust_ivm::sqlite::table_source::TableSource;

fn ver(n: usize) -> String {
    format!("v{n:08}")
}

fn clean(db: &str) {
    for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
        let _ = std::fs::remove_file(format!("{db}{suffix}"));
    }
}

fn seed(db: &str) {
    clean(db);
    let conn = Connection::open(db).unwrap();
    let _ = conn.pragma_update(None, "journal_mode", "wal2");
    let _ = conn.pragma_update(None, "journal_mode", "wal");
    conn.execute_batch(&format!(
        r#"
        CREATE TABLE "_zero.replicationConfig" (lock TEXT PRIMARY KEY DEFAULT 'singleton',
            replicaVersion TEXT NOT NULL, publications TEXT NOT NULL);
        CREATE TABLE "_zero.replicationState" (lock TEXT PRIMARY KEY DEFAULT 'singleton',
            stateVersion TEXT NOT NULL);
        CREATE TABLE "_zero.changeLog2" ("stateVersion" TEXT NOT NULL, "table" TEXT NOT NULL,
            "rowKey" TEXT NOT NULL, "op" TEXT NOT NULL, "pos" INTEGER NOT NULL,
            PRIMARY KEY ("stateVersion","pos"));
        CREATE TABLE issues (id TEXT PRIMARY KEY, ownerId TEXT NOT NULL, _0_version TEXT NOT NULL);
        INSERT INTO "_zero.replicationConfig" VALUES ('singleton','{v1}','[]');
        INSERT INTO "_zero.replicationState"  VALUES ('singleton','{v1}');
        INSERT INTO issues VALUES ('i100','Alice','{v1}');
        "#,
        v1 = ver(1),
    ))
    .unwrap();
}

fn issues_spec() -> LiteAndZqlSpec {
    let mut columns = HashMap::new();
    for c in ["id", "ownerId", "_0_version"] {
        columns.insert(
            c.to_string(),
            ColumnSchema {
                r#type: "TEXT".to_string(),
                optional: false,
            },
        );
    }
    LiteAndZqlSpec {
        table_spec: TableSpec {
            name: "issues".to_string(),
            columns: columns.clone(),
            unique_keys: vec![vec!["id".to_string()]],
            min_row_version: None,
        },
        zql_spec: columns,
    }
}

fn issues_ast() -> Ast {
    Ast {
        schema: None,
        table: "issues".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: Some(vec![rust_ivm::builder::ast::OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    }
}

/// One committed change (an insert) at version `n`, logged in changeLog2.
fn write_step(w: &Connection, n: usize) {
    let v = ver(n);
    let id_new = format!("i{}", n + 100);
    w.execute(
        "INSERT INTO issues (id,ownerId,_0_version) VALUES (?,?,?)",
        rusqlite::params![id_new, "Alice", v],
    )
    .unwrap();
    w.execute(
        r#"INSERT INTO "_zero.changeLog2" ("stateVersion","table","rowKey","op","pos") VALUES (?,?,?,?,?)"#,
        rusqlite::params![v, "issues", format!(r#"{{"id":"{id_new}"}}"#), "s", 0i64],
    )
    .unwrap();
    w.execute(
        r#"UPDATE "_zero.replicationState" SET stateVersion=? WHERE lock='singleton'"#,
        rusqlite::params![v],
    )
    .unwrap();
}

#[test]
fn ivm_advance_time_is_the_gate_clock_delta_not_wall_time() {
    let db = format!(
        "{}/rust-ivm-advance-time-metric-{}.db",
        std::env::temp_dir().display(),
        std::process::id()
    );
    seed(&db);
    let mut snap = Snapshotter::new(&db, "", None);
    snap.init().unwrap();
    let curr = snap.current_conn().unwrap();
    let icols: HashMap<String, ColumnType> = ["id", "ownerId", "_0_version"]
        .iter()
        .map(|n| (n.to_string(), ColumnType::String { optional: false }))
        .collect();
    let its = TableSource::new(curr.clone(), "issues", icols, vec!["id".to_string()]);
    let mut eng = Engine::new(HashMap::from([(
        "issues".to_string(),
        vec!["id".to_string()],
    )]));
    eng.register_source(Rc::new(RefCell::new(its)));
    eng.set_unique_keys("issues", vec![vec!["id".to_string()]]);
    let _ = eng.add_queries(&[QuerySpec {
        query_id: "q".into(),
        ast: issues_ast(),
    }]);
    let syncable = HashMap::from([("issues".to_string(), issues_spec())]);
    let all_tables: HashSet<String> = HashSet::from(["issues".to_string()]);

    let writer = Connection::open(&db).unwrap();
    write_step(&writer, 2);

    *rust_ivm::otel_metrics::LAST_IVM_ADVANCE_MS.lock().unwrap() = None;
    // The view-syncer's TimeSliceTimer, frozen: no process time elapses.
    let frozen_clock: Rc<dyn Fn() -> f64> = Rc::new(|| 0.0);
    let mut stream = eng
        .start_advance(&mut snap, &syncable, &all_tables, None, Some(frozen_clock))
        .unwrap();
    assert_eq!(stream.num_changes(), 1, "fixture must produce one change");
    for item in stream.by_ref() {
        if let StreamItem::Yield = item {
            // A yield is exactly the time TS's timer does not charge.
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
    let _ = eng.finish_advance(stream);
    let recorded = rust_ivm::otel_metrics::LAST_IVM_ADVANCE_MS
        .lock()
        .unwrap()
        .take();
    clean(&db);
    assert_eq!(
        recorded,
        Some(0.0),
        "ivm.advance-time is the gate-clock delta (TS timer.totalElapsed() - start), not wall time"
    );
}
