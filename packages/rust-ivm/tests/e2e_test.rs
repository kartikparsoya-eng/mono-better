//! End-to-end correctness tests.
//!
//! These tests build full pipelines via the Engine, hydrate queries,
//! and verify the exact row data in the output — matching TS behavior.
//! They also test advance (push) and verify the incremental changes.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{
    Ast, Condition, OrderPart, RelatedSubquery, SimpleCondition, ValuePosition,
};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::data::{Row, Value};
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::{
    make_source_change_add, make_source_change_edit, make_source_change_remove,
};

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

fn add_row(source: &Rc<RefCell<MemorySource>>, pairs: &[(&str, Value)]) {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    source.borrow_mut().add_row(map);
}

fn row_val(row: &Option<Row>, key: &str) -> Value {
    row.as_ref()
        .map(|r| r.get(key).cloned().unwrap_or(Value::Null))
        .unwrap_or(Value::Null)
}

fn row_key_val(row: &Row, key: &str) -> Value {
    row.get(key).cloned().unwrap_or(Value::Null)
}

fn sorted_row_ids(changes: &[rust_ivm::streamer::RowChange]) -> Vec<String> {
    let mut ids: Vec<String> = changes
        .iter()
        .filter_map(|c| match row_val(&c.row, "id") {
            Value::F64(n) => Some(n.to_string()),
            Value::Str(s) => Some(s.to_string()),
            _ => None,
        })
        .collect();
    ids.sort();
    ids
}

fn names_from_changes(changes: &[rust_ivm::streamer::RowChange]) -> Vec<String> {
    let mut names: Vec<String> = changes
        .iter()
        .filter_map(|c| match row_val(&c.row, "name") {
            Value::Str(s) => Some(s.to_string()),
            _ => None,
        })
        .collect();
    names.sort();
    names
}

fn simple_cond(col: &str, op: &str, val: Value) -> Condition {
    Condition::Simple(SimpleCondition {
        op: op.to_string(),
        left: ValuePosition::Column {
            name: col.to_string(),
        },
        right: ValuePosition::Literal { value: val },
    })
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

// ---------------------------------------------------------------------------
// 1. Simple hydrate — all rows in PK order
// ---------------------------------------------------------------------------

#[test]
fn e2e_hydrate_all_rows() {
    let source = make_source("users", &["id"]);
    add_row(
        &source,
        &[
            ("id", Value::F64(3.0)),
            ("name", Value::Str("Carol".into())),
        ],
    );
    add_row(
        &source,
        &[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("Alice".into())),
        ],
    );
    add_row(
        &source,
        &[("id", Value::F64(2.0)), ("name", Value::Str("Bob".into()))],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast: basic_ast("users"),
    }]);
    assert_eq!(results[0].changes.len(), 3);
    assert_eq!(sorted_row_ids(&results[0].changes), vec!["1", "2", "3"]);
    assert_eq!(
        names_from_changes(&results[0].changes),
        vec!["Alice", "Bob", "Carol"]
    );
}

// ---------------------------------------------------------------------------
// 2. Hydrate with filter — only matching rows
// ---------------------------------------------------------------------------

#[test]
fn e2e_hydrate_where_eq() {
    let source = make_source("users", &["id"]);
    add_row(
        &source,
        &[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("Alice".into())),
            ("active", Value::Bool(true)),
        ],
    );
    add_row(
        &source,
        &[
            ("id", Value::F64(2.0)),
            ("name", Value::Str("Bob".into())),
            ("active", Value::Bool(false)),
        ],
    );
    add_row(
        &source,
        &[
            ("id", Value::F64(3.0)),
            ("name", Value::Str("Carol".into())),
            ("active", Value::Bool(true)),
        ],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let mut ast = basic_ast("users");
    ast.where_clause = Some(simple_cond("active", "=", Value::Bool(true)));
    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    assert_eq!(results[0].changes.len(), 2);
    assert_eq!(
        names_from_changes(&results[0].changes),
        vec!["Alice", "Carol"]
    );
}

// ---------------------------------------------------------------------------
// 3. Hydrate with limit — first N rows
// ---------------------------------------------------------------------------

#[test]
fn e2e_hydrate_limit() {
    let source = make_source("users", &["id"]);
    for i in 1..=5 {
        add_row(
            &source,
            &[
                ("id", Value::F64(i as f64)),
                ("name", Value::Str(format!("user{}", i).into())),
            ],
        );
    }

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let mut ast = basic_ast("users");
    ast.limit = Some(3);
    ast.order_by = Some(vec![OrderPart {
        column: "id".to_string(),
        direction: "asc".to_string(),
    }]);
    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    assert_eq!(results[0].changes.len(), 3);
    assert_eq!(sorted_row_ids(&results[0].changes), vec!["1", "2", "3"]);
}

// ---------------------------------------------------------------------------
// 4. Compound AND / OR filters
// ---------------------------------------------------------------------------

#[test]
fn e2e_hydrate_and_filter() {
    let source = make_source("users", &["id"]);
    add_row(
        &source,
        &[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("Alice".into())),
            ("age", Value::F64(30.0)),
        ],
    );
    add_row(
        &source,
        &[
            ("id", Value::F64(2.0)),
            ("name", Value::Str("Bob".into())),
            ("age", Value::F64(25.0)),
        ],
    );
    add_row(
        &source,
        &[
            ("id", Value::F64(3.0)),
            ("name", Value::Str("Carol".into())),
            ("age", Value::F64(30.0)),
        ],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let mut ast = basic_ast("users");
    ast.where_clause = Some(Condition::And(vec![
        simple_cond("age", "=", Value::F64(30.0)),
        simple_cond("name", "=", Value::Str("Alice".into())),
    ]));
    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    assert_eq!(results[0].changes.len(), 1);
    assert_eq!(
        row_val(&results[0].changes[0].row, "name"),
        Value::Str("Alice".into())
    );
}

#[test]
fn e2e_hydrate_or_filter() {
    let source = make_source("users", &["id"]);
    add_row(
        &source,
        &[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("Alice".into())),
        ],
    );
    add_row(
        &source,
        &[("id", Value::F64(2.0)), ("name", Value::Str("Bob".into()))],
    );
    add_row(
        &source,
        &[
            ("id", Value::F64(3.0)),
            ("name", Value::Str("Carol".into())),
        ],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let mut ast = basic_ast("users");
    ast.where_clause = Some(Condition::Or(vec![
        simple_cond("name", "=", Value::Str("Alice".into())),
        simple_cond("name", "=", Value::Str("Carol".into())),
    ]));
    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    assert_eq!(results[0].changes.len(), 2);
    assert_eq!(
        names_from_changes(&results[0].changes),
        vec!["Alice", "Carol"]
    );
}

// ---------------------------------------------------------------------------
// 5. LIKE filter
// ---------------------------------------------------------------------------

#[test]
fn e2e_hydrate_like() {
    let source = make_source("users", &["id"]);
    add_row(
        &source,
        &[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("alice".into())),
        ],
    );
    add_row(
        &source,
        &[("id", Value::F64(2.0)), ("name", Value::Str("bob".into()))],
    );
    add_row(
        &source,
        &[("id", Value::F64(3.0)), ("name", Value::Str("alex".into()))],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let mut ast = basic_ast("users");
    ast.where_clause = Some(simple_cond("name", "LIKE", Value::Str("al%".into())));
    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    assert_eq!(results[0].changes.len(), 2);
    assert_eq!(
        names_from_changes(&results[0].changes),
        vec!["alex", "alice"]
    );
}

// ---------------------------------------------------------------------------
// 6. IS NULL filter
// ---------------------------------------------------------------------------

#[test]
fn e2e_hydrate_is_null() {
    let source = make_source("users", &["id"]);
    add_row(
        &source,
        &[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("Alice".into())),
            ("bio", Value::Null),
        ],
    );
    add_row(
        &source,
        &[
            ("id", Value::F64(2.0)),
            ("name", Value::Str("Bob".into())),
            ("bio", Value::Str("Hello".into())),
        ],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let mut ast = basic_ast("users");
    ast.where_clause = Some(simple_cond("bio", "IS", Value::Null));
    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    assert_eq!(results[0].changes.len(), 1);
    assert_eq!(
        row_val(&results[0].changes[0].row, "name"),
        Value::Str("Alice".into())
    );
}

// ---------------------------------------------------------------------------
// 7. Comparison operators (>, !=)
// ---------------------------------------------------------------------------

#[test]
fn e2e_hydrate_gt() {
    let source = make_source("users", &["id"]);
    for i in 1..=5 {
        add_row(
            &source,
            &[
                ("id", Value::F64(i as f64)),
                ("age", Value::F64(i as f64 * 10.0)),
            ],
        );
    }

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let mut ast = basic_ast("users");
    ast.where_clause = Some(simple_cond("age", ">", Value::F64(30.0)));
    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    assert_eq!(results[0].changes.len(), 2);
    assert_eq!(sorted_row_ids(&results[0].changes), vec!["4", "5"]);
}

#[test]
fn e2e_hydrate_ne() {
    let source = make_source("users", &["id"]);
    add_row(
        &source,
        &[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("Alice".into())),
        ],
    );
    add_row(
        &source,
        &[("id", Value::F64(2.0)), ("name", Value::Str("Bob".into()))],
    );
    add_row(
        &source,
        &[
            ("id", Value::F64(3.0)),
            ("name", Value::Str("Carol".into())),
        ],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let mut ast = basic_ast("users");
    ast.where_clause = Some(simple_cond("name", "!=", Value::Str("Bob".into())));
    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    assert_eq!(results[0].changes.len(), 2);
    assert_eq!(
        names_from_changes(&results[0].changes),
        vec!["Alice", "Carol"]
    );
}

// ---------------------------------------------------------------------------
// 8. Advance: add → verify Add change with correct data
// ---------------------------------------------------------------------------

#[test]
fn e2e_advance_add() {
    let source = make_source("users", &["id"]);
    add_row(
        &source,
        &[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("Alice".into())),
        ],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source.clone());

    engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast: basic_ast("users"),
    }]);

    let new_row = make_row(&[("id", Value::F64(2.0)), ("name", Value::Str("Bob".into()))]);
    let changes = engine.advance(&[("users".to_string(), make_source_change_add(new_row))]);

    assert!(
        !changes.is_empty(),
        "advance should produce changes for new row"
    );
    let adds: Vec<_> = changes
        .iter()
        .filter(|c| c.change_type == rust_ivm::ivm::change::ChangeType::Add)
        .collect();
    assert!(!adds.is_empty(), "should have at least one Add change");
    assert_eq!(row_val(&adds[0].row, "id"), Value::F64(2.0));
    assert_eq!(row_val(&adds[0].row, "name"), Value::Str("Bob".into()));
}

// ---------------------------------------------------------------------------
// 9. Advance: remove → verify Remove change
// ---------------------------------------------------------------------------

#[test]
fn e2e_advance_remove() {
    let source = make_source("users", &["id"]);
    add_row(
        &source,
        &[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("Alice".into())),
        ],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source.clone());

    engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast: basic_ast("users"),
    }]);

    let row = make_row(&[
        ("id", Value::F64(1.0)),
        ("name", Value::Str("Alice".into())),
    ]);
    let changes = engine.advance(&[("users".to_string(), make_source_change_remove(row))]);

    assert!(
        !changes.is_empty(),
        "advance should produce changes for removed row"
    );
    let removes: Vec<_> = changes
        .iter()
        .filter(|c| c.change_type == rust_ivm::ivm::change::ChangeType::Remove)
        .collect();
    assert!(
        !removes.is_empty(),
        "should have at least one Remove change"
    );
    // REMOVE carries no row (TS: row: undefined) — check row_key instead.
    assert_eq!(removes[0].row, None);
    assert_eq!(row_key_val(&removes[0].row_key, "id"), Value::F64(1.0));
}

// ---------------------------------------------------------------------------
// 10. Advance: edit → verify Edit change with new data
// ---------------------------------------------------------------------------

#[test]
fn e2e_advance_edit() {
    let source = make_source("users", &["id"]);
    add_row(
        &source,
        &[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("Alice".into())),
        ],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source.clone());

    engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast: basic_ast("users"),
    }]);

    let old_row = make_row(&[
        ("id", Value::F64(1.0)),
        ("name", Value::Str("Alice".into())),
    ]);
    let new_row = make_row(&[
        ("id", Value::F64(1.0)),
        ("name", Value::Str("Alice2".into())),
    ]);
    let changes = engine.advance(&[(
        "users".to_string(),
        make_source_change_edit(new_row, old_row),
    )]);

    assert!(
        !changes.is_empty(),
        "advance should produce changes for edited row"
    );
    let edits: Vec<_> = changes
        .iter()
        .filter(|c| c.change_type == rust_ivm::ivm::change::ChangeType::Edit)
        .collect();
    assert!(!edits.is_empty(), "should have at least one Edit change");
    assert_eq!(row_val(&edits[0].row, "name"), Value::Str("Alice2".into()));
}

// ---------------------------------------------------------------------------
// 11. Multiple queries on the same source
// ---------------------------------------------------------------------------

#[test]
fn e2e_multiple_queries() {
    let source = make_source("users", &["id"]);
    for i in 1..=5 {
        add_row(
            &source,
            &[
                ("id", Value::F64(i as f64)),
                ("name", Value::Str(format!("user{}", i).into())),
            ],
        );
    }

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let mut ast1 = basic_ast("users");
    ast1.where_clause = Some(simple_cond("id", ">", Value::F64(3.0)));
    let mut ast2 = basic_ast("users");
    ast2.where_clause = Some(simple_cond("id", "<", Value::F64(3.0)));

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
    assert_eq!(results[0].changes.len(), 2); // id 4, 5
    assert_eq!(results[1].changes.len(), 2); // id 1, 2
}

// ---------------------------------------------------------------------------
// 12. Hydrate with join — verify parent rows
// ---------------------------------------------------------------------------

#[test]
fn e2e_hydrate_join() {
    let users = make_source("users", &["id"]);
    add_row(
        &users,
        &[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("Alice".into())),
        ],
    );
    add_row(
        &users,
        &[("id", Value::F64(2.0)), ("name", Value::Str("Bob".into()))],
    );

    let posts = make_source("posts", &["id"]);
    add_row(
        &posts,
        &[
            ("id", Value::F64(10.0)),
            ("author_id", Value::F64(1.0)),
            ("title", Value::Str("Hello".into())),
        ],
    );
    add_row(
        &posts,
        &[
            ("id", Value::F64(11.0)),
            ("author_id", Value::F64(2.0)),
            ("title", Value::Str("World".into())),
        ],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(users);
    engine.register_source(posts);

    let mut ast = basic_ast("users");
    ast.related = vec![RelatedSubquery {
        subquery: Box::new(basic_ast("posts")),
        relationship_name: "posts".to_string(),
        parent_key: vec!["id".to_string()],
        child_key: vec!["author_id".to_string()],
        hidden: false,
        system: None,
    }];

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    // With recursive streamNodes, parent rows + child rows are emitted.
    // 2 users + 2 posts = 4 changes.
    assert_eq!(results[0].changes.len(), 4);
    let user_names: Vec<String> = results[0]
        .changes
        .iter()
        .filter(|c| c.table == "users")
        .filter_map(|c| match row_val(&c.row, "name") {
            Value::Str(s) => Some(s.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(user_names, vec!["Alice", "Bob"]);

    // Verify child rows (posts) are emitted.
    let post_titles: Vec<String> = results[0]
        .changes
        .iter()
        .filter(|c| c.table == "posts")
        .filter_map(|c| match row_val(&c.row, "title") {
            Value::Str(s) => Some(s.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(post_titles.len(), 2);
}

// ---------------------------------------------------------------------------
// 13. Row data integrity — all columns present and correct
// ---------------------------------------------------------------------------

#[test]
fn e2e_row_data_integrity() {
    let source = make_source("users", &["id"]);
    add_row(
        &source,
        &[
            ("id", Value::F64(42.0)),
            ("name", Value::Str("Test".into())),
            ("email", Value::Str("test@example.com".into())),
            ("active", Value::Bool(true)),
            ("score", Value::F64(99.5)),
            ("bio", Value::Null),
        ],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast: basic_ast("users"),
    }]);
    assert_eq!(results[0].changes.len(), 1);
    let row = &results[0].changes[0].row;
    assert_eq!(row_val(row, "id"), Value::F64(42.0));
    assert_eq!(row_val(row, "name"), Value::Str("Test".into()));
    assert_eq!(row_val(row, "email"), Value::Str("test@example.com".into()));
    assert_eq!(row_val(row, "active"), Value::Bool(true));
    assert_eq!(row_val(row, "score"), Value::F64(99.5));
    assert_eq!(row_val(row, "bio"), Value::Null);
}

// ---------------------------------------------------------------------------
// 14. Empty source — hydrate returns zero rows
// ---------------------------------------------------------------------------

#[test]
fn e2e_empty_source() {
    let source = make_source("users", &["id"]);
    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast: basic_ast("users"),
    }]);
    assert_eq!(results[0].changes.len(), 0);
}
