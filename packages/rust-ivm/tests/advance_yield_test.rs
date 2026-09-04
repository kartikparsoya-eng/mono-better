//! D1 phase 2 — IVM time slicing for ADVANCE (port of TS
//! `PipelineDriver.#advance` as a generator that yields between changes,
//! pipeline-driver.ts:948-1000 + `#shouldAdvanceYieldMaybeAbortAdvance`
//! :975-977). Real path: Snapshotter + TableSource + `_zero.changeLog2`
//! writes, exactly as the replication stream feeds prod.
//!
//! Non-vacuous pins (each was proven to FAIL on the pre-phase-2 behaviour):
//!   * a `should_yield` hook that is always true surfaces one `Yield` before
//!     EVERY change and never re-asks for the same change after resuming;
//!   * no hook (`None`) → zero yields on the identical diff;
//!   * the row changes delivered are byte-identical with and without yields;
//!   * while the stream is suspended at a `Yield` the thread-local advance
//!     gate is DISARMED (it is armed only for the duration of each `next()`).

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use rusqlite::Connection;

use rust_ivm::builder::ast::Ast;
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::change::ChangeType;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::stream::StreamItem;
use rust_ivm::snapshotter::Snapshotter;
use rust_ivm::snapshotter::spec::{ColumnSchema, LiteAndZqlSpec, TableSpec};
use rust_ivm::sqlite::table_source::TableSource;
use rust_ivm::streamer::RowChange;

fn ver(n: usize) -> String {
    format!("v{n:08}")
}

fn db_path(tag: &str) -> String {
    format!(
        "{}/rust-ivm-advance-yield-{tag}-{}.db",
        std::env::temp_dir().display(),
        std::process::id()
    )
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
        INSERT INTO issues VALUES ('i101','Alice','{v1}');
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

/// One replication step: add issue `i{n+100}`, remove `i{n+98}` (n ≥ 3), bump
/// stateVersion — the same shape as `advance_leak_realpath.rs`.
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
    if n >= 3 {
        let id_old = format!("i{}", n + 100 - 2);
        w.execute("DELETE FROM issues WHERE id=?", rusqlite::params![id_old])
            .unwrap();
        w.execute(
            r#"INSERT INTO "_zero.changeLog2" ("stateVersion","table","rowKey","op","pos") VALUES (?,?,?,?,?)"#,
            rusqlite::params![v, "issues", format!(r#"{{"id":"{id_old}"}}"#), "d", 1i64],
        )
        .unwrap();
    }
    w.execute(
        r#"UPDATE "_zero.replicationState" SET stateVersion=? WHERE lock='singleton'"#,
        rusqlite::params![v],
    )
    .unwrap();
}

struct Fixture {
    db: String,
    snap: Snapshotter,
    eng: Engine,
    syncable: HashMap<String, LiteAndZqlSpec>,
    all_tables: HashSet<String>,
    writer: Connection,
}

impl Fixture {
    fn new(tag: &str) -> Self {
        let db = db_path(tag);
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
        eng.add_queries_streaming(
            &[QuerySpec {
                query_id: "q".into(),
                ast: issues_ast(),
            }],
            |_rc: &RowChange| {},
        );
        let writer = Connection::open(&db).unwrap();
        Self {
            db,
            snap,
            eng,
            syncable: HashMap::from([("issues".to_string(), issues_spec())]),
            all_tables: HashSet::from(["issues".to_string()]),
            writer,
        }
    }

    /// Three changes across two versions: +i102 (v2), +i103 and −i101 (v3).
    fn write_three_changes(&self) {
        write_step(&self.writer, 2);
        write_step(&self.writer, 3);
    }

    /// Run one advance to head, returning (yields, rows, version).
    fn advance(
        &mut self,
        should_yield: Option<Rc<dyn Fn() -> bool>>,
        mut at_yield: impl FnMut(),
    ) -> (usize, Vec<(ChangeType, String)>, String) {
        let mut stream = self
            .eng
            .start_advance(
                &mut self.snap,
                &self.syncable,
                &self.all_tables,
                should_yield,
            )
            .unwrap();
        assert_eq!(stream.num_changes(), 3, "fixture must produce 3 changes");
        let version = stream.version().to_string();
        let mut yields = 0;
        let mut rows = Vec::new();
        for item in stream.by_ref() {
            match item {
                StreamItem::Yield => {
                    yields += 1;
                    at_yield();
                }
                StreamItem::Data(rc) => {
                    let id = match rc.row_key.get("id") {
                        Some(rust_ivm::ivm::data::Value::Str(s)) => s.to_string(),
                        other => panic!("row key id: {other:?}"),
                    };
                    rows.push((rc.change_type, id));
                }
            }
        }
        let outcome = self.eng.finish_advance(stream).unwrap();
        assert!(
            !outcome.aborted,
            "advance must commit: {:?} {:?}",
            outcome.reset_reason, outcome.reset_msg
        );
        (yields, rows, version)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.snap.destroy();
        clean(&self.db);
    }
}

fn expected_rows() -> Vec<(ChangeType, String)> {
    vec![
        (ChangeType::Add, "i102".to_string()),
        (ChangeType::Add, "i103".to_string()),
        (ChangeType::Remove, "i101".to_string()),
    ]
}

#[test]
fn advance_yields_once_before_every_change_when_the_slice_is_up() {
    let mut fx = Fixture::new("always");
    fx.write_three_changes();
    let asked = Rc::new(Cell::new(0usize));
    let hook = {
        let asked = asked.clone();
        Rc::new(move || {
            asked.set(asked.get() + 1);
            true
        }) as Rc<dyn Fn() -> bool>
    };
    let (yields, rows, version) = fx.advance(Some(hook), || {});
    // TS: `if (this.#shouldAdvanceYieldMaybeAbortAdvance()) yield 'yield';`
    // runs once per change BEFORE it is pushed; after resuming, the same
    // change is processed without asking again.
    assert_eq!(yields, 3, "one yield before each of the 3 changes");
    assert_eq!(
        asked.get(),
        3,
        "the hook is consulted exactly once per change"
    );
    assert_eq!(rows, expected_rows());
    assert_eq!(version, ver(3));
}

#[test]
fn advance_never_yields_without_a_should_yield_hook() {
    let mut fx = Fixture::new("none");
    fx.write_three_changes();
    let (yields, rows, version) = fx.advance(None, || {});
    assert_eq!(yields, 0);
    assert_eq!(rows, expected_rows());
    assert_eq!(version, ver(3));
}

#[test]
fn a_hook_that_declines_never_yields() {
    let mut fx = Fixture::new("declines");
    fx.write_three_changes();
    let (yields, rows, _) = fx.advance(Some(Rc::new(|| false)), || {});
    assert_eq!(yields, 0);
    assert_eq!(rows, expected_rows());
}

#[test]
fn row_changes_are_identical_with_and_without_yields() {
    let mut with = Fixture::new("with");
    with.write_three_changes();
    let (_, rows_with, v_with) = with.advance(Some(Rc::new(|| true)), || {});

    let mut without = Fixture::new("without");
    without.write_three_changes();
    let (_, rows_without, v_without) = without.advance(None, || {});

    assert_eq!(rows_with, rows_without);
    assert_eq!(v_with, v_without);
}

#[test]
fn the_advance_gate_is_disarmed_while_the_stream_is_suspended_at_a_yield() {
    let mut fx = Fixture::new("gate");
    fx.write_three_changes();
    // Inside `next()` (where the hook runs) the per-fetch gate must be armed
    // so the row-read loop can consult the budget…
    let armed_inside = Rc::new(Cell::new(true));
    let hook = {
        let armed_inside = armed_inside.clone();
        Rc::new(move || {
            armed_inside.set(armed_inside.get() && rust_ivm::advance_gate::is_armed());
            true
        }) as Rc<dyn Fn() -> bool>
    };
    // …and at every suspension point it must be DISARMED, so a neighbouring
    // client group hydrating on this shard thread never reads this advance's
    // budget (the contract in INVENTIONS.md I-11/I-12).
    let armed_outside = Rc::new(Cell::new(false));
    let (yields, _, _) = {
        let armed_outside = armed_outside.clone();
        fx.advance(Some(hook), move || {
            armed_outside.set(armed_outside.get() || rust_ivm::advance_gate::is_armed());
        })
    };
    assert_eq!(yields, 3);
    assert!(
        armed_inside.get(),
        "gate must be armed while a pull is in progress"
    );
    assert!(
        !armed_outside.get(),
        "gate must be disarmed while the stream is suspended at a Yield"
    );
    assert!(
        !rust_ivm::advance_gate::is_armed(),
        "gate must be disarmed after the stream is exhausted"
    );
}

/// TS parity for WHAT THE ADVANCE BUDGET COUNTS.
///
/// `#advancePipelines` builds ONE `TimeSliceTimer` and hands it both to
/// `pipelines.advance(timer)` — whose budget arms read it as
/// `advanceTimer.totalElapsed()` (pipeline-driver.ts:1102) — and to
/// `#processChanges` (view-syncer.ts:2579-2585). `#processChanges` stops that
/// timer in exactly one place, `await timer.yieldProcess(…)`
/// (view-syncer.ts:2508-2512). So consumer work between pulls counts against
/// the budget, and only the awaited time slice does not.
///
/// rust excluded ALL time between two pulls (`AdvanceStream::next` →
/// `exclude(last_return.elapsed())`) plus each row's delivery callback
/// (`push_source_change` → `exclude_current`), justified by a NAPI boundary
/// deleted in a5e502ad9. rust therefore measured a smaller `elapsed` than TS
/// for identical work and shed LESS eagerly.
///
/// Both directions are pinned: a stall while holding a DATA item must be
/// charged (the advance aborts), the same stall while yielded must not be.
/// Reverting `next()` to exclude unconditionally makes the first case stop
/// aborting and FAILS this test.
#[test]
fn consumer_stall_counts_against_the_budget_but_a_yielded_slice_does_not() {
    // > MIN_ADVANCEMENT_TIME_LIMIT_MS (50) so the timeout arm can fire at all.
    let stall = std::time::Duration::from_millis(140);

    // Drive the fixture's 3-change advance, stalling once — either while
    // holding a data row (consumer work) or while yielded. Returns `aborted`.
    let run = |tag: &str, yield_always: bool, stall_on_yield: bool| -> bool {
        let mut fx = Fixture::new(tag);
        fx.write_three_changes();
        let hook: Option<Rc<dyn Fn() -> bool>> = if yield_always {
            Some(Rc::new(|| true))
        } else {
            None
        };
        let mut stream = fx
            .eng
            .start_advance(&mut fx.snap, &fx.syncable, &fx.all_tables, hook)
            .unwrap();
        let mut stalled = false;
        for item in stream.by_ref() {
            let is_yield = matches!(item, StreamItem::Yield);
            if !stalled && is_yield == stall_on_yield {
                stalled = true;
                std::thread::sleep(stall);
            }
        }
        assert!(stalled, "{tag}: the stall must have happened");
        fx.eng.finish_advance(stream).unwrap().aborted
    };

    // Stalling while holding a data row is consumer work: TS charges it.
    assert!(
        run("stall-data", false, false),
        "a consumer stall between pulls must count against the advance budget \
         (TS stops its TimeSliceTimer only inside yieldProcess), so this \
         advance must abort",
    );

    // Stalling while yielded is the awaited time slice: TS excludes it.
    assert!(
        !run("stall-yield", true, true),
        "time spent awaiting a yielded slice must NOT count against the budget \
         (TS `timer.yieldProcess()` stops the timer), so this advance must commit",
    );
}
