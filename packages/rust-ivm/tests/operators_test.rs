//! Tests for individual operators — Take, Skip, Cap, Exists, FlippedJoin, FanOut/FanIn.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{Ast, Bound, Condition, OrderPart, SimpleCondition, ValuePosition};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::change::make_source_change_edit;
use rust_ivm::ivm::data::{Row, Value};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;

fn make_row(pairs: &[(&str, Value)]) -> Row {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Arc::new(map)
}

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

#[test]
fn test_take_limit() {
    let source = make_source("users", &["id"]);
    for i in 1..=5 {
        source.borrow_mut().add_row(
            [
                ("id".to_string(), Value::F64(i as f64)),
                ("name".to_string(), Value::Str(format!("user{}", i).into())),
            ]
            .into_iter()
            .collect(),
        );
    }

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: Some(3),
        order_by: Some(vec![OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    assert_eq!(results[0].changes.len(), 3);
}

#[test]
fn test_skip_pagination() {
    let source = make_source("users", &["id"]);
    for i in 1..=5 {
        source.borrow_mut().add_row(
            [
                ("id".to_string(), Value::F64(i as f64)),
                ("name".to_string(), Value::Str(format!("user{}", i).into())),
            ]
            .into_iter()
            .collect(),
        );
    }

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let start_row = make_row(&[("id", Value::F64(2.0))]);
    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: Some(vec![OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        start: Some(Bound {
            row: start_row,
            exclusive: true,
        }),
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    // Should skip users 1 and 2, return 3, 4, 5
    assert_eq!(results[0].changes.len(), 3);
    let first_id = results[0].changes[0]
        .row
        .as_ref()
        .unwrap()
        .get("id")
        .cloned()
        .unwrap_or(Value::Null);
    assert_eq!(first_id, Value::F64(3.0));
}

#[test]
fn test_filter_not_equal() {
    let source = make_source("users", &["id"]);
    for (id, name) in [(1.0, "Alice"), (2.0, "Bob"), (3.0, "Alice")] {
        source.borrow_mut().add_row(
            [
                ("id".to_string(), Value::F64(id)),
                ("name".to_string(), Value::Str(name.into())),
            ]
            .into_iter()
            .collect(),
        );
    }

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: Some(Condition::Simple(SimpleCondition {
            op: "!=".to_string(),
            left: ValuePosition::Column {
                name: "name".to_string(),
            },
            right: ValuePosition::Literal {
                value: Value::Str("Alice".into()),
            },
        })),
        related: vec![],
        limit: None,
        order_by: None,
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    assert_eq!(results[0].changes.len(), 1);
    let name = results[0].changes[0]
        .row
        .as_ref()
        .unwrap()
        .get("name")
        .cloned()
        .unwrap_or(Value::Null);
    assert_eq!(name, Value::Str("Bob".into()));
}

#[test]
fn test_filter_greater_than() {
    let source = make_source("users", &["id"]);
    for i in 1..=5 {
        source.borrow_mut().add_row(
            [
                ("id".to_string(), Value::F64(i as f64)),
                ("age".to_string(), Value::F64((20 + i) as f64)),
            ]
            .into_iter()
            .collect(),
        );
    }

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: Some(Condition::Simple(SimpleCondition {
            op: ">".to_string(),
            left: ValuePosition::Column {
                name: "age".to_string(),
            },
            right: ValuePosition::Literal {
                value: Value::F64(23.0),
            },
        })),
        related: vec![],
        limit: None,
        order_by: None,
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    // ages: 21, 22, 23, 24, 25 → only 24, 25 pass > 23
    assert_eq!(results[0].changes.len(), 2);
}

#[test]
fn test_filter_and() {
    let source = make_source("users", &["id"]);
    for (id, name, age) in [
        (1.0, "Alice", 25.0),
        (2.0, "Bob", 30.0),
        (3.0, "Alice", 30.0),
    ] {
        source.borrow_mut().add_row(
            [
                ("id".to_string(), Value::F64(id)),
                ("name".to_string(), Value::Str(name.into())),
                ("age".to_string(), Value::F64(age)),
            ]
            .into_iter()
            .collect(),
        );
    }

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: Some(Condition::And(vec![
            Condition::Simple(SimpleCondition {
                op: "=".to_string(),
                left: ValuePosition::Column {
                    name: "name".to_string(),
                },
                right: ValuePosition::Literal {
                    value: Value::Str("Alice".into()),
                },
            }),
            Condition::Simple(SimpleCondition {
                op: "=".to_string(),
                left: ValuePosition::Column {
                    name: "age".to_string(),
                },
                right: ValuePosition::Literal {
                    value: Value::F64(30.0),
                },
            }),
        ])),
        related: vec![],
        limit: None,
        order_by: None,
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    assert_eq!(results[0].changes.len(), 1);
    let name = results[0].changes[0]
        .row
        .as_ref()
        .unwrap()
        .get("name")
        .cloned()
        .unwrap_or(Value::Null);
    assert_eq!(name, Value::Str("Alice".into()));
}

#[test]
fn test_filter_or() {
    let source = make_source("users", &["id"]);
    for (id, name) in [(1.0, "Alice"), (2.0, "Bob"), (3.0, "Carol")] {
        source.borrow_mut().add_row(
            [
                ("id".to_string(), Value::F64(id)),
                ("name".to_string(), Value::Str(name.into())),
            ]
            .into_iter()
            .collect(),
        );
    }

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: Some(Condition::Or(vec![
            Condition::Simple(SimpleCondition {
                op: "=".to_string(),
                left: ValuePosition::Column {
                    name: "name".to_string(),
                },
                right: ValuePosition::Literal {
                    value: Value::Str("Alice".into()),
                },
            }),
            Condition::Simple(SimpleCondition {
                op: "=".to_string(),
                left: ValuePosition::Column {
                    name: "name".to_string(),
                },
                right: ValuePosition::Literal {
                    value: Value::Str("Carol".into()),
                },
            }),
        ])),
        related: vec![],
        limit: None,
        order_by: None,
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    assert_eq!(results[0].changes.len(), 2);
}

#[test]
fn test_advance_edit() {
    let source = make_source("users", &["id"]);
    source.borrow_mut().add_row(
        [
            ("id".to_string(), Value::F64(1.0)),
            ("name".to_string(), Value::Str("Alice".into())),
        ]
        .into_iter()
        .collect(),
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source.clone());

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: None,
        start: None,
    };
    engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);

    // Edit: change Alice to Bob
    let old_row = make_row(&[
        ("id", Value::F64(1.0)),
        ("name", Value::Str("Alice".into())),
    ]);
    let new_row = make_row(&[("id", Value::F64(1.0)), ("name", Value::Str("Bob".into()))]);
    let changes = engine.advance(&[(
        "users".to_string(),
        make_source_change_edit(new_row, old_row),
    )]);
    println!("advance edit produced {} changes", changes.len());
}

#[test]
fn test_multiple_queries_hydrate() {
    let source = make_source("users", &["id"]);
    for i in 1..=10 {
        source.borrow_mut().add_row(
            [
                ("id".to_string(), Value::F64(i as f64)),
                ("name".to_string(), Value::Str(format!("user{}", i).into())),
            ]
            .into_iter()
            .collect(),
        );
    }

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let queries: Vec<QuerySpec> = (0..5)
        .map(|i| QuerySpec {
            query_id: format!("q{}", i),
            ast: Ast {
                schema: None,
                table: "users".to_string(),
                alias: None,
                where_clause: None,
                related: vec![],
                limit: None,
                order_by: None,
                start: None,
            },
        })
        .collect();

    let results = engine.add_queries(&queries);
    assert_eq!(results.len(), 5);
    for r in &results {
        assert_eq!(r.changes.len(), 10);
    }
}

#[test]
fn test_values_equal_null_semantics() {
    // null ≠ null — required for join semantics
    assert!(!rust_ivm::ivm::data::values_equal(
        &Value::Null,
        &Value::Null
    ));
    assert!(rust_ivm::ivm::data::values_equal(
        &Value::F64(1.0),
        &Value::F64(1.0)
    ));
    assert!(!rust_ivm::ivm::data::values_equal(
        &Value::F64(1.0),
        &Value::F64(2.0)
    ));
    assert!(rust_ivm::ivm::data::values_equal(
        &Value::Str("hello".into()),
        &Value::Str("hello".into())
    ));
}

#[test]
fn test_compare_values() {
    use std::cmp::Ordering;
    assert_eq!(
        rust_ivm::ivm::data::compare_values(&Value::F64(1.0), &Value::F64(2.0)),
        Ordering::Less
    );
    assert_eq!(
        rust_ivm::ivm::data::compare_values(&Value::F64(2.0), &Value::F64(1.0)),
        Ordering::Greater
    );
    assert_eq!(
        rust_ivm::ivm::data::compare_values(&Value::F64(1.0), &Value::F64(1.0)),
        Ordering::Equal
    );
    assert_eq!(
        rust_ivm::ivm::data::compare_values(&Value::Str("a".into()), &Value::Str("b".into())),
        Ordering::Less
    );
    assert_eq!(
        rust_ivm::ivm::data::compare_values(&Value::Null, &Value::F64(1.0)),
        Ordering::Less
    );
    assert_eq!(
        rust_ivm::ivm::data::compare_values(&Value::F64(1.0), &Value::Null),
        Ordering::Greater
    );
    assert_eq!(
        rust_ivm::ivm::data::compare_values(&Value::Null, &Value::Null),
        Ordering::Equal
    );
}
