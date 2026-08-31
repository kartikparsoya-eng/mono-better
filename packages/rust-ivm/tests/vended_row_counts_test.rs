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
use rust_ivm::builder::debug_delegate::runtime_debug_flags;
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::schema::ColumnType;
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
