//! Pins the engine accessor surface flagged truly-uncovered by the L2
//! coverage triage (parity/coverage/rust-ivm/triage.md, engine/mod.rs rows):
//! `transformed_ast`, `initialized`, `cancel`/`cancellation_token`, and
//! `get_row`. rust-syncer reads all four at runtime (pipeline_driver /
//! interrupt wiring / row lookups), so an accessor drifting from the state it
//! mirrors would surface as wrong syncer behavior, not a local test failure.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::Ast;
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;

fn make_source(name: &str, cols: &[&str], pk: &[&str]) -> Rc<RefCell<MemorySource>> {
    let columns: HashMap<String, ColumnType> = cols
        .iter()
        .map(|c| (c.to_string(), ColumnType::Number { optional: false }))
        .collect();
    Rc::new(RefCell::new(MemorySource::new(
        name,
        columns,
        pk.iter().map(|s| s.to_string()).collect(),
    )))
}

fn add_row(source: &Rc<RefCell<MemorySource>>, pairs: &[(&str, Value)]) {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    source.borrow_mut().add_row(map);
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

fn make_engine() -> Engine {
    let users = make_source("users", &["id"], &["id"]);
    add_row(&users, &[("id", Value::F64(1.0))]);
    let mut engine = Engine::new(HashMap::new());
    engine.register_source(users);
    engine
}

/// `initialized()` = "has sources registered": false on a bare engine, true
/// after register_source, false again after `reset()` clears sources (TS
/// `reset()` empties the source map).
#[test]
fn initialized_reflects_source_registration_and_reset() {
    let mut engine = Engine::new(HashMap::new());
    assert!(!engine.initialized(), "bare engine has no sources");

    let users = make_source("users", &["id"], &["id"]);
    engine.register_source(users);
    assert!(engine.initialized());

    engine.reset();
    assert!(
        !engine.initialized(),
        "reset() clears sources (TS reset empties the source map)"
    );
}

/// `transformed_ast(id)` returns the pipeline's stored logical AST for a live
/// query, `None` for unknown ids, and `None` again after `remove_query` —
/// rust-syncer keys re-transform decisions off this (`query_transformation_
/// hash` sibling), so a stale entry would suppress a needed re-hydrate.
#[test]
fn transformed_ast_tracks_pipeline_lifecycle() {
    let mut engine = make_engine();
    assert!(engine.transformed_ast("q1").is_none());

    engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast: basic_ast("users"),
    }]);
    let ast = engine.transformed_ast("q1").expect("live query has an AST");
    assert_eq!(ast.table, "users");

    engine.remove_query("q1");
    assert!(
        engine.transformed_ast("q1").is_none(),
        "removed query must not leave a stale AST behind"
    );
}

/// `cancel()` trips the shared token that `cancellation_token()` hands to the
/// SQLite progress-handler wiring: clones observe the same cancellation.
#[test]
fn cancel_trips_the_shared_cancellation_token() {
    let engine = make_engine();
    let token = engine.cancellation_token();
    assert!(!token.is_cancelled());

    engine.cancel();
    assert!(token.is_cancelled(), "clone must observe engine.cancel()");
    assert!(engine.cancellation_token().is_cancelled());
}

/// `get_row` (port of TS `getRow()`): present PK → the row; absent PK →
/// None; unknown table → None (TS optional-chains the missing source).
#[test]
fn get_row_by_primary_key() {
    let engine = make_engine();

    let row = engine
        .get_row("users", &[("id".to_string(), Value::F64(1.0))])
        .expect("row exists");
    assert_eq!(row.get("id"), Some(&Value::F64(1.0)));

    assert!(
        engine
            .get_row("users", &[("id".to_string(), Value::F64(999.0))])
            .is_none(),
        "absent PK returns None"
    );
    assert!(
        engine
            .get_row("nope", &[("id".to_string(), Value::F64(1.0))])
            .is_none(),
        "unknown table returns None, not a panic"
    );
}
