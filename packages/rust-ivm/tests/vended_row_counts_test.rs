//! Non-vacuous test for the `VENDED` per-query vended-row counting — the
//! engine half of the port of TS `PipelineDriver.#addQueryImpl`'s
//! `runtimeDebugFlags.trackRowsVended` path (pipeline-driver.ts:616) +
//! `zqlite/table-source.ts` `#fetch` `debug?.rowVended` (table-source.ts:398).
//!
//! When `runtimeDebugFlags.trackRowsVended` is ON, the engine must attach a
//! `Debug` delegate to each query's pipeline so the SQLite `TableSource`
//! records, per (table, SQL), how many rows it VENDED (scanned). Those counts
//! surface on `QueryResult.vended_row_counts`, which the pipeline driver logs
//! as the `VENDED` diagnostic. When OFF (prod default), no delegate is created
//! and `vended_row_counts` is `None` — the hot read path pays nothing.
//!
//! Lives in its OWN integration binary because `runtimeDebugFlags` is a
//! process-global `AtomicBool`: a lib/co-located test toggling it could race
//! parallel tests. A dedicated binary gets a clean process.
//!
//! NON-VACUOUS: the source is seeded with exactly 3 rows and a full scan, so a
//! reverted wiring fails distinctly — dropping the engine's per-query `Debug`
//! (delegate `debug: None`) yields `None` (not `Some`), and dropping the
//! `debug?.rowVended` call in `#fetch` yields a count of 0 (not 3).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rusqlite::Connection;

use rust_ivm::builder::ast::Ast;
use rust_ivm::builder::debug_delegate::{Debug, runtime_debug_flags};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::operator::FetchRequest;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::stream::skip_yields;
use rust_ivm::sqlite::sqlite_cost_model::scanstatus_available;
use rust_ivm::sqlite::table_source::TableSource;
use rust_ivm::streamer::RowChange;

fn seed_three_rows() -> Rc<RefCell<Connection>> {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY);
         INSERT INTO t (id) VALUES (1), (2), (3);",
    )
    .unwrap();
    Rc::new(RefCell::new(conn))
}

fn basic_ast(table: &str) -> Ast {
    Ast {
        schema: None,
        table: table.to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: None,
        start: None,
    }
}

fn build_engine() -> Engine {
    let src = TableSource::new(
        seed_three_rows(),
        "t",
        HashMap::from([("id".to_string(), ColumnType::Number { optional: false })]),
        vec!["id".to_string()],
    );
    let mut eng = Engine::new(HashMap::from([("t".to_string(), vec!["id".to_string()])]));
    eng.register_source(Rc::new(RefCell::new(src)));
    eng.set_unique_keys("t", vec![vec!["id".to_string()]]);
    eng
}

fn total_vended_for_table(
    counts: &rust_ivm::builder::debug_delegate::RowCountsBySource,
    table: &str,
) -> u64 {
    counts
        .get(table)
        .map(|by_sql| by_sql.values().copied().sum())
        .unwrap_or(0)
}

#[test]
fn vended_row_counts_track_rows_scanned_only_when_flag_on() {
    let f = runtime_debug_flags();
    let prev_rows = f.track_rows_vended();

    // --- Flag ON: the engine attaches a Debug, the TableSource vends 3 rows. ---
    f.set_track_rows_vended(true);
    let mut eng = build_engine();
    let results = eng.add_queries_streaming(
        &[QuerySpec {
            query_id: "q1".into(),
            ast: basic_ast("t"),
        }],
        |_rc: &RowChange| {},
    );
    let vended = results[0]
        .vended_row_counts
        .as_ref()
        .expect("trackRowsVended ON ⇒ QueryResult carries Some(vended_row_counts)");
    assert_eq!(
        total_vended_for_table(vended, "t"),
        3,
        "the source VENDED (scanned) exactly the 3 seeded rows for this full-scan query; \
         got {vended:?}"
    );

    // --- Flag OFF: prod default — no delegate, so no counts. ---
    f.set_track_rows_vended(false);
    let mut eng_off = build_engine();
    let results_off = eng_off.add_queries_streaming(
        &[QuerySpec {
            query_id: "q1".into(),
            ast: basic_ast("t"),
        }],
        |_rc: &RowChange| {},
    );
    assert!(
        results_off[0].vended_row_counts.is_none(),
        "trackRowsVended OFF ⇒ vended_row_counts is None (hot path pays nothing)"
    );

    f.set_track_rows_vended(prev_rows);
}

/// End-to-end for the analyzeQuery engine hook: when `set_analyze_debug` attaches
/// an explicit delegate (the port of TS `runAst`'s `host.debug = new Debug()`),
/// a normal `add_queries_streaming` hydrate must populate that SAME delegate with
/// the vended-row counts (during hydrate) AND the nvisit + plans (on fetch drop),
/// so `run_ast` can read all three back off it. Non-vacuous: reverting the
/// `analyze_debug` precedence in the engine build leaves the caller's delegate
/// empty and the vended assertion fails.
#[test]
fn set_analyze_debug_populates_the_callers_delegate() {
    // Table with a non-PK column so the scan genuinely visits rows (NVISIT>1).
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
        .unwrap();
    for i in 0..8i64 {
        conn.execute("INSERT INTO t (id, v) VALUES (?1, ?2)", [i, i * 10])
            .unwrap();
    }
    let src = TableSource::new(
        std::rc::Rc::new(std::cell::RefCell::new(conn)),
        "t",
        HashMap::from([
            ("id".to_string(), ColumnType::Number { optional: false }),
            ("v".to_string(), ColumnType::Number { optional: false }),
        ]),
        vec!["id".to_string()],
    );
    let mut eng = Engine::new(HashMap::from([("t".to_string(), vec!["id".to_string()])]));
    eng.register_source(Rc::new(RefCell::new(src)));
    eng.set_unique_keys("t", vec![vec!["id".to_string()]]);

    let debug = Debug::new_shared();
    eng.set_analyze_debug(Some(debug.clone()));
    let _ = eng.add_queries_streaming(
        &[QuerySpec {
            query_id: "analyze".into(),
            ast: basic_ast("t"),
        }],
        |_rc: &RowChange| {},
    );
    eng.set_analyze_debug(None);

    let d = debug.borrow();
    // Vended-row counts populate during hydrate regardless of scanstatus.
    let vended = d.get_vended_row_counts();
    assert!(
        vended.contains_key("t") && vended["t"].values().copied().sum::<u64>() == 8,
        "the caller's delegate carries the 8 vended rows; got {vended:?}"
    );
    if scanstatus_available() {
        assert!(
            d.get_nvisit_counts().contains_key("t"),
            "the caller's delegate carries the nvisit (dbScansByQuery source)"
        );
        assert!(
            !d.get_sqlite_plans().is_empty(),
            "the caller's delegate carries the SQLite plans"
        );
    }
}

/// Non-vacuous (when the linked SQLite has SQLITE_ENABLE_STMT_SCANSTATUS): a
/// `TableSource` fetch carrying a `Debug` must, on drop, record NVISIT (rows
/// visited) + EXPLAIN via `record_nvisit`/`record_explain` — the port of TS
/// `#fetch`'s `finally` scanstatus block (zqlite/table-source.ts:343-372) that
/// feeds analyzeQuery's `dbScansByQuery`/`sqlitePlans`. Reverting the
/// `LazyRows::drop` record calls empties `get_nvisit_counts()` and this fails.
///
/// When scanstatus is unavailable in the test's linked SQLite (the
/// `engine_planner_wiring_test` SKIP condition), the read degrades to (0, [])
/// and nothing is recorded — assert that graceful path instead.
#[test]
fn fetch_records_nvisit_and_explain_from_scanstatus() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
        .unwrap();
    for i in 0..12i64 {
        conn.execute("INSERT INTO t (id, v) VALUES (?1, ?2)", [i, i * 10])
            .unwrap();
    }
    let db = std::rc::Rc::new(std::cell::RefCell::new(conn));

    // Include a non-PK column `v` so the fetch is a genuine full table scan
    // (a pure `SELECT id` is answerable from the rowid index without visiting
    // rows, which would report NVISIT=1).
    let mut src = TableSource::new(
        db,
        "t",
        HashMap::from([
            ("id".to_string(), ColumnType::Number { optional: false }),
            ("v".to_string(), ColumnType::Number { optional: false }),
        ]),
        vec!["id".to_string()],
    );
    let debug = Debug::new_shared();
    let input = src.connect(None, None, None, None, Some(debug.clone()));

    // Drain a full fetch; the stream (and its LazyRows) drops at the end of this
    // statement, running `LazyRows::drop` -> the scanstatus record calls.
    let rows: Vec<_> = skip_yields(input.borrow().fetch(&FetchRequest::default())).collect();
    assert_eq!(
        rows.len(),
        12,
        "the fetch itself returns the 12 seeded rows"
    );

    let d = debug.borrow();
    if scanstatus_available() {
        let nvisit = d.get_nvisit_counts();
        assert!(
            nvisit.contains_key("t"),
            "NVISIT recorded for table `t`; got {nvisit:?}"
        );
        let total: u64 = nvisit["t"].values().copied().sum();
        // Proves the re-execution DRAINS the scan (not a single step): a 12-row
        // full scan visits well more than 1 row.
        assert!(
            total >= 12,
            "the full 12-row scan is visited; got {total} (nvisit={nvisit:?})"
        );
        // A full table scan populates an EXPLAIN plan line.
        assert!(
            !d.get_sqlite_plans().is_empty(),
            "an EXPLAIN plan is recorded for the scanned query"
        );
    } else {
        // No scanstatus in the linked SQLite -> best-effort read records nothing.
        assert!(
            d.get_nvisit_counts().is_empty(),
            "without SQLITE_ENABLE_STMT_SCANSTATUS the read degrades to no-op"
        );
    }
}
