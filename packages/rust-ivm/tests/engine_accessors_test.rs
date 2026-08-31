//! Tests for the Engine accessors — ports of the pipeline-driver accessors in
//! `zero-cache` (getRow, transformedAst, initialized, cancel, setTableSpec).
//! These were untested (triage #25: engine/mod.rs L438/1347/1355/1365/1387).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::Ast;
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::data::{Row, Value};
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::schema::ColumnType;

fn make_source(name: &str, pk: &[&str]) -> Rc<RefCell<MemorySource>> {
    let columns: HashMap<String, ColumnType> = pk
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

// Port of TS `initialized`: false with no sources, true once a source is
// registered.
#[test]
fn initialized_reflects_registered_sources() {
    let mut engine = Engine::new(HashMap::new());
    assert!(!engine.initialized(), "fresh engine is not initialized");
    engine.register_source(make_source("users", &["id"]));
    assert!(engine.initialized(), "registering a source initializes it");
}

// Port of TS `getRow`: look a row up by table + primary key. Hit returns the
// row; missing PK and unknown table both return None.
#[test]
fn get_row_by_table_and_pk() {
    let source = make_source("users", &["id"]);
    add_row(
        &source,
        &[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("Alice".into())),
        ],
    );
    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let got: Option<Row> = engine.get_row("users", &[("id".to_string(), Value::F64(1.0))]);
    assert_eq!(
        got.and_then(|r| r.get("name").cloned()),
        Some(Value::Str("Alice".into()))
    );

    // Missing PK => None.
    assert!(
        engine
            .get_row("users", &[("id".to_string(), Value::F64(999.0))])
            .is_none(),
        "absent PK returns None"
    );
    // Unknown table => None (source lookup misses).
    assert!(
        engine
            .get_row("nope", &[("id".to_string(), Value::F64(1.0))])
            .is_none(),
        "unknown table returns None"
    );
}

// Port of TS `transformedAst`: the scalar-resolved logical AST is exposed per
// query after add_queries; an unknown query id returns None.
#[test]
fn transformed_ast_present_after_add_queries() {
    let source = make_source("users", &["id"]);
    add_row(&source, &[("id", Value::F64(1.0))]);
    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);
    engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast: basic_ast("users"),
    }]);

    let ast = engine
        .transformed_ast("q1")
        .expect("q1 has a transformed AST");
    assert_eq!(ast.table, "users");
    assert!(
        engine.transformed_ast("does-not-exist").is_none(),
        "unknown query id returns None"
    );
}

// Port of TS `cancel`: flips the cancellation token, observable via
// cancellation_token().is_cancelled().
#[test]
fn cancel_sets_the_cancellation_token() {
    let engine = Engine::new(HashMap::new());
    assert!(
        !engine.cancellation_token().is_cancelled(),
        "token starts uncancelled"
    );
    engine.cancel();
    assert!(
        engine.cancellation_token().is_cancelled(),
        "cancel flips the token"
    );
}

// `set_table_spec` records a per-table spec (min_row_version). It has no public
// getter — its effect is via the streamer's version gating — so this pins that
// the setter integrates without disturbing get_row/hydrate on the same table.
#[test]
fn set_table_spec_coexists_with_hydrate() {
    let source = make_source("users", &["id"]);
    add_row(&source, &[("id", Value::F64(1.0))]);
    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    engine.set_table_spec("users", Some("00".to_string()));

    // Row still retrievable and the query still hydrates through the spec'd table.
    assert!(
        engine
            .get_row("users", &[("id".to_string(), Value::F64(1.0))])
            .is_some()
    );
    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast: basic_ast("users"),
    }]);
    assert_eq!(results[0].changes.len(), 1);
}
