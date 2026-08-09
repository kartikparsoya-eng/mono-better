//! Pins the source-connection lifecycle to TS semantics.
//!
//! TS zqlite table-source.ts `destroy()` SPLICES the connection out of
//! `#connections`; the rust port originally only cleared `output`, so every
//! removed query permanently retained one connection per touched source.
//! Under production addQuery/removeQuery churn this grew per-CG memory
//! linearly at a flat client-group count (the prod syncer leak signature)
//! and made every push scan an ever-growing connection list.
//!
//! These tests fail against the pre-fix code (counts accumulate per cycle)
//! and pass with the splice-on-destroy fix.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{Ast, RelatedSubquery};
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

fn joined_ast() -> Ast {
    let mut ast = basic_ast("users");
    ast.related = vec![RelatedSubquery {
        subquery: Box::new(basic_ast("posts")),
        relationship_name: "posts".to_string(),
        parent_key: vec!["id".to_string()],
        child_key: vec!["author_id".to_string()],
        hidden: false,
        system: None,
    }];
    ast
}

fn make_engine() -> Engine {
    let users = make_source("users", &["id"], &["id"]);
    let posts = make_source("posts", &["id", "author_id"], &["id"]);
    add_row(&users, &[("id", Value::F64(1.0))]);
    add_row(
        &posts,
        &[("id", Value::F64(10.0)), ("author_id", Value::F64(1.0))],
    );
    let mut engine = Engine::new(HashMap::new());
    engine.register_source(users);
    engine.register_source(posts);
    engine
}

/// remove_query must return every touched source to its pre-add connection
/// count (TS: destroy() splices the connection out of #connections).
#[test]
fn remove_query_splices_source_connections() {
    let mut engine = make_engine();
    let baseline = engine.source_connection_checkpoint();

    engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast: joined_ast(),
    }]);
    let hydrated = engine.source_connection_checkpoint();
    assert!(
        hydrated.values().sum::<usize>() > baseline.values().sum::<usize>(),
        "hydrate must register connections"
    );

    engine.remove_query("q1");
    assert_eq!(
        engine.source_connection_checkpoint(),
        baseline,
        "remove_query left connections registered on sources \
         (TS destroy() splices; rust must too)"
    );
}

/// The prod leak shape: steady addQuery/removeQuery churn at a stable live
/// query count must not accumulate source connections.
#[test]
fn add_remove_churn_does_not_accumulate_connections() {
    let mut engine = make_engine();

    // One persistent query, like a CG's stable view set.
    engine.add_queries(&[QuerySpec {
        query_id: "stable".to_string(),
        ast: joined_ast(),
    }]);
    let steady = engine.source_connection_checkpoint();

    for i in 0..100 {
        engine.add_queries(&[QuerySpec {
            query_id: format!("churn-{i}"),
            ast: joined_ast(),
        }]);
        engine.remove_query(&format!("churn-{i}"));
        assert_eq!(
            engine.source_connection_checkpoint(),
            steady,
            "cycle {i}: churned query leaked source connections"
        );
    }
}

/// Re-adding the same query id (engine removes the old pipeline first) must
/// also stay flat — this is the changeDesiredQueries re-transform path.
#[test]
fn same_id_readd_does_not_accumulate_connections() {
    let mut engine = make_engine();

    engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast: joined_ast(),
    }]);
    let steady = engine.source_connection_checkpoint();

    for i in 0..50 {
        engine.add_queries(&[QuerySpec {
            query_id: "q1".to_string(),
            ast: joined_ast(),
        }]);
        assert_eq!(
            engine.source_connection_checkpoint(),
            steady,
            "re-add cycle {i}: replaced pipeline leaked source connections"
        );
    }
}

// ---------------------------------------------------------------------------
// Destroy-forwarding parity for the fan/filter adapter operators. TS forwards
// destroy through FanIn (fan-in.ts:49) and FilterEnd (filter-operators.ts:135);
// a non-forwarding port strands everything below them — including the source
// connection — exactly like the connection-splice leak.
// ---------------------------------------------------------------------------

use rust_ivm::ivm::filter_operators::build_filter_pipeline;
use rust_ivm::ivm::operator::InputBase;
use rust_ivm::ivm::source::Source;

#[test]
fn filter_end_destroy_reaches_source() {
    let source = make_source("users", &["id"], &["id"]);
    let input = source.borrow_mut().connect(None, None, None, None);
    assert_eq!(source.borrow().connection_count(), 1);

    let (_start, end) = build_filter_pipeline(input);
    end.borrow_mut().destroy();

    assert_eq!(
        source.borrow().connection_count(),
        0,
        "FilterEnd::destroy must forward through FilterStart to the source \
         input (TS filter-operators.ts:135)"
    );
}

#[test]
fn fan_in_destroy_reaches_source() {
    let source = make_source("users", &["id"], &["id"]);
    let branch_a = source.borrow_mut().connect(None, None, None, None);
    let branch_b = source.borrow_mut().connect(None, None, None, None);
    assert_eq!(source.borrow().connection_count(), 2);

    let schema = branch_a.borrow().get_schema();
    let fan_in = rust_ivm::ivm::fan_in::FanIn::new(schema);
    fan_in.borrow_mut().add_input(branch_a);
    fan_in.borrow_mut().add_input(branch_b);

    fan_in.borrow_mut().destroy();

    assert_eq!(
        source.borrow().connection_count(),
        0,
        "FanIn::destroy must forward to every branch input (TS fan-in.ts:49)"
    );
}
