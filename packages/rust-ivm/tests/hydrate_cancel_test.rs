//! Hydrate cancellation (the consumer-abandonment path).
//!
//! When the view-syncer abandons a hydrate mid-stream (client disconnect /
//! teardown), the driver flips the engine's cancellation token via the
//! out-of-band `cancel()`. `add_queries_streaming` must then (1) stop producing
//! rows promptly and (2) register NOTHING — a partially-fetched pipeline is left
//! in an inconsistent operator state, so it must be discarded rather than left
//! registered for a later advance to run on.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::Ast;
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;

fn make_source(name: &str, n_rows: usize) -> Rc<RefCell<MemorySource>> {
    let mut columns: HashMap<String, ColumnType> = HashMap::new();
    columns.insert("id".to_string(), ColumnType::Number { optional: false });
    columns.insert("v".to_string(), ColumnType::Number { optional: false });
    let src = Rc::new(RefCell::new(MemorySource::new(
        name,
        columns,
        vec!["id".to_string()],
    )));
    for i in 0..n_rows {
        let mut row: FxHashMap<String, Value> = FxHashMap::default();
        row.insert("id".to_string(), Value::F64(i as f64));
        row.insert("v".to_string(), Value::F64((i * 10) as f64));
        src.borrow_mut().add_row(row);
    }
    src
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

#[test]
fn cancel_mid_hydrate_stops_producing_and_registers_nothing() {
    let source = make_source("users", 50);
    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let cancel = engine.cancellation_token();
    let mut produced = 0usize;

    // Cancel from inside the row callback after the first row (simulates the
    // driver's out-of-band cancel() while the consumer abandons the stream).
    let results = engine.add_queries_streaming(
        &[QuerySpec {
            query_id: "q1".to_string(),
            ast: basic_ast("users"),
        }],
        |_rc| {
            produced += 1;
            if produced == 1 {
                cancel.cancel();
            }
        },
    );

    // Stopped promptly — nowhere near all 50 rows streamed.
    assert!(
        produced < 50,
        "cancellation should stop production early, got {produced}/50",
    );
    // Registered NOTHING — no partial pipeline left behind.
    assert!(
        results.is_empty(),
        "a cancelled hydrate must return no results",
    );
    assert!(
        engine.pipeline_query_ids().is_empty(),
        "a cancelled hydrate must register no pipeline, got {:?}",
        engine.pipeline_query_ids(),
    );
}

#[test]
fn normal_hydrate_still_registers_after_a_prior_cancel() {
    // A cancel is per-call: the token resets at the start of the next hydrate,
    // so a fresh hydrate after a cancelled one behaves normally.
    let source = make_source("users", 5);
    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let cancel = engine.cancellation_token();
    let mut produced = 0usize;
    engine.add_queries_streaming(
        &[QuerySpec {
            query_id: "q1".to_string(),
            ast: basic_ast("users"),
        }],
        |_rc| {
            produced += 1;
            if produced == 1 {
                cancel.cancel();
            }
        },
    );
    assert!(engine.pipeline_query_ids().is_empty());

    // Second hydrate: no cancellation → all rows stream and it registers.
    let mut produced2 = 0usize;
    let results = engine.add_queries_streaming(
        &[QuerySpec {
            query_id: "q2".to_string(),
            ast: basic_ast("users"),
        }],
        |_rc| {
            produced2 += 1;
        },
    );
    assert_eq!(produced2, 5, "all rows should stream on the fresh hydrate");
    assert_eq!(results.len(), 1);
    assert_eq!(engine.pipeline_query_ids(), vec!["q2".to_string()]);
}
