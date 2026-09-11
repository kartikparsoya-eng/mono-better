//! D1 phase 2 — IVM time slicing for ADVANCE (port of TS
//! `PipelineDriver.#advance` as a generator that yields between changes,
//! pipeline-driver.ts:948-1000 + `#shouldAdvanceYieldMaybeAbortAdvance`
//! :975-977). Real path: Snapshotter + TableSource + `_zero.changeLog2`
//! writes, exactly as the replication stream feeds prod.
//!
//! Mutation-tested pins (each was proven to FAIL on the earlier behaviour):
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
                None,
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
/// The economic budget's clock is the view-syncer's ONE `TimeSliceTimer`.
///
/// `#advancePipelines` builds it, hands it to `pipelines.advance(timer)` —
/// whose budget arms read `advanceTimer.totalElapsed()`
/// (pipeline-driver.ts:1101) — and to `#processChanges`
/// (view-syncer.ts:2579-2585). Three properties follow from
/// `TimeSliceTimer` (view-syncer.ts:2943-3001):
///
/// 1. `await timer.start()` yields to the time-slice queue and only THEN zeroes
///    and starts the clock, so the queue turn before the first pull is not
///    charged.
/// 2. `await timer.yieldProcess()` stops the lap, so a yielded slice is not
///    charged.
/// 3. Everything else — `updater.received()`, `pokers.addPatch()` — runs with
///    the timer RUNNING and IS charged.
///
/// Rust used a private `Instant` started inside `start_advance` minus an
/// `exclude()` that fired only after a `Yield`. That got (2) and (3) roughly
/// right but (1) badly wrong: the `timer.start().await` queue turn landed
/// inside the budget. On a shard hosting ~1000 client groups that turn costs
/// tens of ms, so advances blew the 50ms floor at `pos: 0` — before processing
/// a single change. Measured on a 60-minute production-trace replay: 5,934
/// `advancement-timeout` resets (27% of them against a budget of exactly 0ms)
/// versus ~45 on TS, each destroying every pipeline in the group and forcing a
/// full re-hydrate — 2.9x TS's total hydrations.
///
/// All three are pinned below; each fails on its own if the clock regresses to
/// `start.elapsed()`.
#[test]
fn the_advance_budget_charges_only_time_slice_timer_process_time() {
    // > MIN_ADVANCEMENT_TIME_LIMIT_MS (50) so the timeout arm can fire at all.
    let stall = std::time::Duration::from_millis(140);

    /// Stand-in for the view-syncer's `TimeSliceTimer` (which lives in
    /// rust-syncer): accumulates wall time except while yielded, exactly as
    /// TS's `#startLap`/`#stopLap` do.
    #[derive(Default)]
    struct ProcessClock {
        total: std::cell::Cell<f64>,
        lap: std::cell::Cell<Option<std::time::Instant>>,
    }
    impl ProcessClock {
        /// TS `startWithoutYielding()`: zero the total, start a lap.
        fn start(&self) {
            self.total.set(0.0);
            self.lap.set(Some(std::time::Instant::now()));
        }
        fn stop_lap(&self) {
            if let Some(l) = self.lap.take() {
                self.total
                    .set(self.total.get() + l.elapsed().as_secs_f64() * 1000.0);
            }
        }
        fn start_lap(&self) {
            self.lap.set(Some(std::time::Instant::now()));
        }
        fn total_elapsed(&self) -> f64 {
            self.total.get()
                + self
                    .lap
                    .get()
                    .map_or(0.0, |l| l.elapsed().as_secs_f64() * 1000.0)
        }
    }

    // `stall_before_start`: sleep before `timer.start()` — the time-slice queue
    // turn (property 1). Otherwise stall once mid-stream, either while holding
    // a data row (property 3) or while yielded (property 2). Returns `aborted`.
    let run = |tag: &str, yield_always: bool, stall_on_yield: bool, stall_before_start: bool| {
        let mut fx = Fixture::new(tag);
        fx.write_three_changes();
        let hook: Option<Rc<dyn Fn() -> bool>> = if yield_always {
            Some(Rc::new(|| true))
        } else {
            None
        };
        let clock = Rc::new(ProcessClock::default());
        let read = Rc::clone(&clock);
        let mut stream = fx
            .eng
            .start_advance(
                &mut fx.snap,
                &fx.syncable,
                &fx.all_tables,
                hook,
                Some(Rc::new(move || read.total_elapsed())),
            )
            .unwrap();
        // TS: the stream exists BEFORE `await timer.start()`, so anything here
        // is the queue turn.
        if stall_before_start {
            std::thread::sleep(stall);
        }
        clock.start();
        let mut stalled = stall_before_start;
        for item in stream.by_ref() {
            let is_yield = matches!(item, StreamItem::Yield);
            if is_yield {
                clock.stop_lap();
            }
            if !stall_before_start && !stalled && is_yield == stall_on_yield {
                stalled = true;
                std::thread::sleep(stall);
            }
            if is_yield {
                clock.start_lap();
            }
        }
        assert!(stalled, "{tag}: the stall must have happened");
        fx.eng.finish_advance(stream).unwrap().aborted
    };

    // (1) The queue turn before `timer.start()` is NOT charged — the regression.
    assert!(
        !run("stall-before-start", false, false, true),
        "time before `await timer.start()` is the time-slice QUEUE TURN: TS \
         zeroes the timer after it (view-syncer.ts:2952-2957), so it must not \
         be charged and this advance must commit. Charging it is what produced \
         5,934 pos-0 `advancement-timeout` resets in an hour.",
    );

    // (3) A consumer stall while holding a data row IS charged.
    assert!(
        run("stall-data", false, false, false),
        "a consumer stall between pulls must count against the advance budget \
         (TS stops its TimeSliceTimer only inside yieldProcess), so this \
         advance must abort",
    );

    // (2) A stall while yielded is the awaited slice: NOT charged.
    assert!(
        !run("stall-yield", true, true, false),
        "time spent awaiting a yielded slice must NOT count against the budget \
         (TS `timer.yieldProcess()` stops the timer), so this advance must commit",
    );
}
