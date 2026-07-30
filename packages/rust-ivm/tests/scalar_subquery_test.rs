//! Scalar-subquery resolution + live companion monitoring.
//!
//! Ports the go-ivm scalar tests (resolve_scalar_integration_test.go,
//! scalar_undefined_null_reset_test.go) against the Rust engine. Verifies:
//!   1. a scalar EXISTS on a unique-key row is pre-resolved to a literal at
//!      hydrate, and the matched subquery row ships as a companion;
//!   2. editing the resolved child field on advance RESETS (ScalarResetError),
//!      rather than emitting a companion edit;
//!   3. no matching subquery row → ALWAYS_FALSE (zero parent rows);
//!   4. the SAME subquery WITHOUT the `scalar` flag is NOT resolved — it is
//!      incrementally maintained as an EXISTS join (an edit does not reset).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{
    Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery, SimpleCondition, ValuePosition,
};
use rust_ivm::engine::{Engine, QuerySpec, ScalarResetError};
use rust_ivm::ivm::change::make_source_change_edit;
use rust_ivm::ivm::data::{Row, Value};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;
use rust_ivm::streamer::RowChange;

fn str_source(name: &str, cols: &[&str], pk: &[&str]) -> Rc<RefCell<MemorySource>> {
    let columns: HashMap<String, ColumnType> = cols
        .iter()
        .map(|c| (c.to_string(), ColumnType::String { optional: false }))
        .collect();
    Rc::new(RefCell::new(MemorySource::new(
        name,
        columns,
        pk.iter().map(|s| s.to_string()).collect(),
    )))
}

fn add_row(source: &Rc<RefCell<MemorySource>>, pairs: &[(&str, &str)]) {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), Value::Str((*v).into())))
        .collect();
    source.borrow_mut().add_row(map);
}

fn make_row(pairs: &[(&str, &str)]) -> Row {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), Value::Str((*v).into())))
        .collect();
    Arc::new(map)
}

/// issues WHERE `<scalar?>` EXISTS(users WHERE id = `user_id`) correlated on
/// ownerId = users.name. `scalar = true` triggers pre-resolution.
fn scalar_exists_ast(user_id: &str, scalar: bool) -> Ast {
    let subquery = Ast {
        schema: None,
        table: "users".to_string(),
        alias: Some("users".to_string()),
        where_clause: Some(Condition::Simple(SimpleCondition {
            op: "=".to_string(),
            left: ValuePosition::Column { name: "id".to_string() },
            right: ValuePosition::Literal { value: Value::Str(user_id.into()) },
        })),
        related: vec![],
        limit: None,
        order_by: None,
        start: None,
    };
    Ast {
        schema: None,
        table: "issues".to_string(),
        alias: None,
        where_clause: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: RelatedSubquery {
                subquery: Box::new(subquery),
                relationship_name: "users".to_string(),
                parent_key: vec!["ownerId".to_string()],
                child_key: vec!["name".to_string()],
                hidden: false,
                system: None,
            },
            op: "EXISTS".to_string(),
            flip: false,
            scalar,
        })),
        related: vec![],
        limit: None,
        order_by: None,
        start: None,
    }
}

fn setup() -> (Engine, Rc<RefCell<MemorySource>>) {
    let users = str_source("users", &["id", "name"], &["id"]);
    add_row(&users, &[("id", "u1"), ("name", "Alice")]);
    let issues = str_source("issues", &["id", "ownerId"], &["id"]);
    add_row(&issues, &[("id", "i1"), ("ownerId", "Alice")]);
    add_row(&issues, &[("id", "i2"), ("ownerId", "Bob")]);

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(users.clone());
    engine.register_source(issues);
    engine.set_unique_keys("users", vec![vec!["id".to_string()]]);
    engine.set_unique_keys("issues", vec![vec!["id".to_string()]]);
    (engine, users)
}

fn by_table(changes: &[RowChange]) -> HashMap<String, Vec<&RowChange>> {
    let mut m: HashMap<String, Vec<&RowChange>> = HashMap::new();
    for c in changes {
        m.entry(c.table.clone()).or_default().push(c);
    }
    m
}

fn str_field(row: &Option<Row>, key: &str) -> Option<String> {
    match row.as_ref()?.get(key)? {
        Value::Str(s) => Some(s.to_string()),
        _ => None,
    }
}

#[test]
fn scalar_resolves_to_literal_and_emits_companion() {
    let (mut engine, _users) = setup();
    let results = engine.add_queries(&[QuerySpec {
        query_id: "q".to_string(),
        ast: scalar_exists_ast("u1", true),
    }]);
    let changes = &results[0].changes;
    let tables = by_table(changes);

    // Resolved to `ownerId = "Alice"` → only i1 matches.
    let issues = tables.get("issues").expect("issues rows present");
    assert_eq!(issues.len(), 1, "expected exactly i1, got {:?}", issues);
    assert_eq!(str_field(&issues[0].row, "id").as_deref(), Some("i1"));

    // The matched subquery row ships as a companion.
    let users = tables.get("users").expect("companion user row present");
    assert_eq!(users.len(), 1, "expected one companion user, got {:?}", users);
    assert_eq!(str_field(&users[0].row, "id").as_deref(), Some("u1"));
    for c in changes {
        assert_eq!(c.query_id, "q", "row tagged with wrong query id");
    }
}

#[test]
fn scalar_value_change_resets_via_scalar_reset_error() {
    let (mut engine, users) = setup();
    engine.add_queries(&[QuerySpec {
        query_id: "q".to_string(),
        ast: scalar_exists_ast("u1", true),
    }]);

    // The resolver baked users.name = "Alice" as a literal. Editing it makes
    // that literal stale → the advance must raise ScalarResetError.
    let old = make_row(&[("id", "u1"), ("name", "Alice")]);
    let new = make_row(&[("id", "u1"), ("name", "Alicia")]);
    add_row(&users, &[("id", "u1"), ("name", "Alicia")]); // keep source consistent

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        engine.advance(&[("users".to_string(), make_source_change_edit(new, old))])
    }));

    let payload = result.expect_err("advance should panic with ScalarResetError");
    let err = payload
        .downcast_ref::<ScalarResetError>()
        .expect("panic payload should be a ScalarResetError");
    assert_eq!(err.table, "users");
    assert_eq!(err.resolved, "Alice");
    assert_eq!(err.new, "Alicia");
}

#[test]
fn scalar_no_match_is_always_false() {
    let (mut engine, _users) = setup();
    // Subquery pins a non-existent user → resolved undefined → ALWAYS_FALSE.
    let results = engine.add_queries(&[QuerySpec {
        query_id: "q".to_string(),
        ast: scalar_exists_ast("u_missing", true),
    }]);
    let tables = by_table(&results[0].changes);
    assert!(
        tables.get("issues").map_or(true, |r| r.is_empty()),
        "no issue should match an unresolved scalar subquery",
    );
    assert!(
        tables.get("users").map_or(true, |r| r.is_empty()),
        "no companion row when nothing matched",
    );
}

#[test]
fn non_scalar_exists_is_not_reset() {
    let (mut engine, users) = setup();
    // SAME subquery, but NOT flagged scalar → incrementally maintained as an
    // EXISTS join, never pre-resolved. Editing users must NOT raise a reset.
    engine.add_queries(&[QuerySpec {
        query_id: "q".to_string(),
        ast: scalar_exists_ast("u1", false),
    }]);

    let old = make_row(&[("id", "u1"), ("name", "Alice")]);
    let new = make_row(&[("id", "u1"), ("name", "Alicia")]);
    add_row(&users, &[("id", "u1"), ("name", "Alicia")]);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        engine.advance(&[("users".to_string(), make_source_change_edit(new, old))])
    }));
    assert!(
        result.is_ok(),
        "a non-scalar EXISTS must be incrementally maintained, not reset",
    );
}
