//! End-to-end tests for the Rust IVM engine.
//! Tests hydrate, join, filter, advance (push), and parallel behavior.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{Ast, Condition, RelatedSubquery, SimpleCondition, ValuePosition};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::source::{make_source_change_add, make_source_change_remove};

fn make_row(pairs: &[(&str, Value)]) -> rust_ivm::ivm::data::Row {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Arc::new(map)
}

fn make_source(name: &str, pk: &[&str]) -> Rc<RefCell<MemorySource>> {
    let columns: HashMap<String, rust_ivm::ivm::schema::ColumnType> = pk
        .iter()
        .map(|c| {
            (
                c.to_string(),
                rust_ivm::ivm::schema::ColumnType::Number { optional: false },
            )
        })
        .collect();
    Rc::new(RefCell::new(MemorySource::new(
        name,
        columns,
        pk.iter().map(|s| s.to_string()).collect(),
    )))
}

#[test]
fn test_hydrate_single_table() {
    // Create a source with 3 rows
    let source = make_source("users", &["id"]);
    source.borrow_mut().add_row(
        [
            ("id".to_string(), Value::F64(1.0)),
            ("name".to_string(), Value::Str("Alice".into())),
        ]
        .into_iter()
        .collect(),
    );
    source.borrow_mut().add_row(
        [
            ("id".to_string(), Value::F64(2.0)),
            ("name".to_string(), Value::Str("Bob".into())),
        ]
        .into_iter()
        .collect(),
    );
    source.borrow_mut().add_row(
        [
            ("id".to_string(), Value::F64(3.0)),
            ("name".to_string(), Value::Str("Carol".into())),
        ]
        .into_iter()
        .collect(),
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        start: None,
        order_by: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].changes.len(), 3);
    assert_eq!(results[0].changes[0].table, "users");
}

#[test]
fn test_hydrate_with_filter() {
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
        where_clause: Some(Condition::Simple(SimpleCondition {
            op: "=".to_string(),
            left: ValuePosition::Column {
                name: "name".to_string(),
            },
            right: ValuePosition::Literal {
                value: Value::Str("Bob".into()),
            },
        })),
        related: vec![],
        limit: None,
        start: None,
        order_by: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].changes.len(), 1);
    let row = results[0].changes[0].row.as_ref().unwrap();
    let name = row.get("name").cloned().unwrap_or(Value::Null);
    assert_eq!(name, Value::Str("Bob".into()));
}

#[test]
fn test_hydrate_with_join() {
    // users: [{id:1, name:"Alice"}, {id:2, name:"Bob"}]
    let users = make_source("users", &["id"]);
    users.borrow_mut().add_row(
        [
            ("id".to_string(), Value::F64(1.0)),
            ("name".to_string(), Value::Str("Alice".into())),
        ]
        .into_iter()
        .collect(),
    );
    users.borrow_mut().add_row(
        [
            ("id".to_string(), Value::F64(2.0)),
            ("name".to_string(), Value::Str("Bob".into())),
        ]
        .into_iter()
        .collect(),
    );

    // posts: [{id:10, author_id:1, title:"Hello"}, {id:11, author_id:2, title:"World"}]
    let posts = make_source("posts", &["id"]);
    posts.borrow_mut().add_row(
        [
            ("id".to_string(), Value::F64(10.0)),
            ("author_id".to_string(), Value::F64(1.0)),
            ("title".to_string(), Value::Str("Hello".into())),
        ]
        .into_iter()
        .collect(),
    );
    posts.borrow_mut().add_row(
        [
            ("id".to_string(), Value::F64(11.0)),
            ("author_id".to_string(), Value::F64(2.0)),
            ("title".to_string(), Value::Str("World".into())),
        ]
        .into_iter()
        .collect(),
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(users);
    engine.register_source(posts);

    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: None,
        related: vec![RelatedSubquery {
            subquery: Box::new(Ast {
                schema: None,
                table: "posts".to_string(),
                alias: None,
                where_clause: None,
                related: vec![],
                limit: None,
                start: None,
                order_by: None,
            }),
            relationship_name: "posts".to_string(),
            parent_key: vec!["id".to_string()],
            child_key: vec!["author_id".to_string()],
            hidden: false,
            system: None,
        }],
        limit: None,
        start: None,
        order_by: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);

    assert_eq!(results.len(), 1);
    // With recursive streamNodes: 2 users + 2 posts = 4 changes.
    assert_eq!(results[0].changes.len(), 4, "expected 2 users + 2 posts");
    let user_count = results[0]
        .changes
        .iter()
        .filter(|c| c.table == "users")
        .count();
    let post_count = results[0]
        .changes
        .iter()
        .filter(|c| c.table == "posts")
        .count();
    assert_eq!(user_count, 2, "expected 2 users");
    assert_eq!(post_count, 2, "expected 2 posts");
    for change in &results[0].changes {
        assert!(change.table == "users" || change.table == "posts");
    }
}

#[test]
fn test_advance_add() {
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

    // Build a pipeline
    let ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        start: None,
        order_by: None,
    };
    engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);

    // Advance: add a new user
    let new_row = make_row(&[("id", Value::F64(2.0)), ("name", Value::Str("Bob".into()))]);
    let changes = engine.advance(&[("users".to_string(), make_source_change_add(new_row))]);

    // Should produce changes from the push
    // The exact count depends on how many pipelines are connected
    // For now, just verify it doesn't panic
    println!("advance produced {} changes", changes.len());
}

#[test]
fn test_advance_remove() {
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
        start: None,
        order_by: None,
    };
    engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);

    // Advance: remove a user
    let row = make_row(&[
        ("id", Value::F64(1.0)),
        ("name", Value::Str("Alice".into())),
    ]);
    let changes = engine.advance(&[("users".to_string(), make_source_change_remove(row))]);

    println!("advance remove produced {} changes", changes.len());
}

#[test]
fn test_multiple_sources_independent() {
    // Verify multiple sources can coexist
    let users = make_source("users", &["id"]);
    let posts = make_source("posts", &["id"]);

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(users);
    engine.register_source(posts);

    let ast1 = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        start: None,
        order_by: None,
    };
    let ast2 = Ast {
        schema: None,
        table: "posts".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        start: None,
        order_by: None,
    };

    let results = engine.add_queries(&[
        QuerySpec {
            query_id: "q1".to_string(),
            ast: ast1,
        },
        QuerySpec {
            query_id: "q2".to_string(),
            ast: ast2,
        },
    ]);

    assert_eq!(results.len(), 2);
}
