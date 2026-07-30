//! G15 repro: Edit that changes a filter field (leftAt: null -> timestamp)
//! should remove the row from the filtered view.
//!
//! Query: org_members WHERE orgId = 'org1' AND leftAt IS NULL
//! 1. Add row with leftAt=null -> should be in view
//! 2. Edit row: leftAt=null -> leftAt=1000 -> should be REMOVED from view

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{Ast, Condition, SimpleCondition, ValuePosition};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::change::{ChangeType, make_source_change_add, make_source_change_edit};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;
use rust_ivm::streamer::RowChange;

#[allow(dead_code)]
fn str_source(name: &str, cols: &[&str], pk: &[&str]) -> Rc<std::cell::RefCell<MemorySource>> {
    let columns: HashMap<String, ColumnType> = cols
        .iter()
        .map(|c| (c.to_string(), ColumnType::String { optional: false }))
        .collect();
    Rc::new(std::cell::RefCell::new(MemorySource::new(
        name,
        columns,
        pk.iter().map(|s| s.to_string()).collect(),
    )))
}

fn num_source(name: &str, cols: &[&str], pk: &[&str]) -> Rc<std::cell::RefCell<MemorySource>> {
    let columns: HashMap<String, ColumnType> = cols
        .iter()
        .map(|c| {
            (
                c.to_string(),
                if c == &"leftAt" || c == &"joinedAt" {
                    ColumnType::Number { optional: true }
                } else {
                    ColumnType::String { optional: false }
                },
            )
        })
        .collect();
    Rc::new(std::cell::RefCell::new(MemorySource::new(
        name,
        columns,
        pk.iter().map(|s| s.to_string()).collect(),
    )))
}

fn row(pairs: Vec<(&str, Value)>) -> Arc<FxHashMap<String, Value>> {
    Arc::new(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

fn org_members_ast() -> Ast {
    // SELECT * FROM org_members WHERE orgId = 'org1' AND leftAt IS NULL
    Ast {
        schema: None,
        table: "org_members".to_string(),
        alias: None,
        where_clause: Some(Condition::And(vec![
            Condition::Simple(SimpleCondition {
                left: ValuePosition::Column {
                    name: "orgId".to_string(),
                },
                op: "=".to_string(),
                right: ValuePosition::Literal {
                    value: Value::Str(Arc::from("org1")),
                },
            }),
            Condition::Simple(SimpleCondition {
                left: ValuePosition::Column {
                    name: "leftAt".to_string(),
                },
                op: "IS".to_string(),
                right: ValuePosition::Literal { value: Value::Null },
            }),
        ])),
        related: vec![],
        limit: None,
        order_by: Some(vec![rust_ivm::builder::ast::OrderPart {
            column: "joinedAt".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    }
}

fn member_ids(changes: &[RowChange]) -> Vec<String> {
    let mut ids: Vec<String> = changes
        .iter()
        .filter(|c| c.table == "org_members")
        .filter_map(|c| match c.row.as_ref()?.get("memberId")? {
            Value::Str(s) => Some(s.to_string()),
            _ => None,
        })
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

fn change_types(changes: &[RowChange]) -> Vec<(String, ChangeType)> {
    changes
        .iter()
        .filter(|c| c.table == "org_members")
        .map(|c| {
            let id = match c.row.as_ref().and_then(|r| r.get("memberId")) {
                Some(Value::Str(s)) => s.to_string(),
                _ => "?".to_string(),
            };
            (id, c.change_type)
        })
        .collect()
}

#[test]
fn edit_removes_row_from_filter_view() {
    let source = num_source(
        "org_members",
        &["memberId", "orgId", "leftAt", "role", "email", "joinedAt"],
        &["memberId"],
    );
    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);
    engine.add_queries(&[QuerySpec {
        query_id: "q".to_string(),
        ast: org_members_ast(),
    }]);

    // 1. Add row with leftAt=null -> should be in view (passes filter)
    let changes1 = engine.advance(&[(
        "org_members".to_string(),
        make_source_change_add(row(vec![
            ("memberId", Value::Str(Arc::from("m1"))),
            ("orgId", Value::Str(Arc::from("org1"))),
            ("leftAt", Value::Null),
            ("role", Value::Str(Arc::from("OWNER"))),
            ("email", Value::Str(Arc::from("test@example.com"))),
            ("joinedAt", Value::F64(1000.0)),
        ])),
    )]);
    println!("After Add(leftAt=null): {:?}", change_types(&changes1));
    assert_eq!(
        member_ids(&changes1),
        vec!["m1".to_string()],
        "m1 should be added to view (leftAt IS NULL passes)"
    );

    // 2. Edit: leftAt=null -> leftAt=1000 -> should be REMOVED from view
    let old_row = row(vec![
        ("memberId", Value::Str(Arc::from("m1"))),
        ("orgId", Value::Str(Arc::from("org1"))),
        ("leftAt", Value::Null),
        ("role", Value::Str(Arc::from("OWNER"))),
        ("email", Value::Str(Arc::from("test@example.com"))),
        ("joinedAt", Value::F64(1000.0)),
    ]);
    let new_row = row(vec![
        ("memberId", Value::Str(Arc::from("m1"))),
        ("orgId", Value::Str(Arc::from("org1"))),
        ("leftAt", Value::F64(1000.0)),
        ("role", Value::Str(Arc::from("OWNER"))),
        ("email", Value::Str(Arc::from("test@example.com"))),
        ("joinedAt", Value::F64(1000.0)),
    ]);
    let changes2 = engine.advance(&[(
        "org_members".to_string(),
        make_source_change_edit(new_row, old_row),
    )]);
    println!(
        "After Edit(leftAt=null -> 1000): {:?}",
        change_types(&changes2)
    );

    // The Edit should produce a Remove (old passes filter, new doesn't)
    let has_remove = changes2
        .iter()
        .any(|c| c.table == "org_members" && c.change_type == ChangeType::Remove);
    assert!(
        has_remove,
        "Edit should emit Remove when old passes filter (leftAt IS NULL) and new doesn't. Got: {:?}",
        change_types(&changes2)
    );
}

#[test]
fn edit_that_doesnt_change_filter_keeps_row() {
    // Edit that doesn't change leftAt or orgId should keep the row in view
    let source = num_source(
        "org_members",
        &["memberId", "orgId", "leftAt", "role", "email", "joinedAt"],
        &["memberId"],
    );
    let mut engine = Engine::new(HashMap::new());
    engine.register_source(source);
    engine.add_queries(&[QuerySpec {
        query_id: "q".to_string(),
        ast: org_members_ast(),
    }]);

    // Add
    engine.advance(&[(
        "org_members".to_string(),
        make_source_change_add(row(vec![
            ("memberId", Value::Str(Arc::from("m1"))),
            ("orgId", Value::Str(Arc::from("org1"))),
            ("leftAt", Value::Null),
            ("role", Value::Str(Arc::from("OWNER"))),
            ("email", Value::Str(Arc::from("test@example.com"))),
            ("joinedAt", Value::F64(1000.0)),
        ])),
    )]);

    // Edit role only (leftAt stays null)
    let old_row = row(vec![
        ("memberId", Value::Str(Arc::from("m1"))),
        ("orgId", Value::Str(Arc::from("org1"))),
        ("leftAt", Value::Null),
        ("role", Value::Str(Arc::from("OWNER"))),
        ("email", Value::Str(Arc::from("test@example.com"))),
        ("joinedAt", Value::F64(1000.0)),
    ]);
    let new_row = row(vec![
        ("memberId", Value::Str(Arc::from("m1"))),
        ("orgId", Value::Str(Arc::from("org1"))),
        ("leftAt", Value::Null),
        ("role", Value::Str(Arc::from("ADMIN"))),
        ("email", Value::Str(Arc::from("test@example.com"))),
        ("joinedAt", Value::F64(1000.0)),
    ]);
    let changes2 = engine.advance(&[(
        "org_members".to_string(),
        make_source_change_edit(new_row, old_row),
    )]);
    println!("After Edit(role change): {:?}", change_types(&changes2));

    // Should be an Edit (both old and new pass filter)
    let has_edit = changes2
        .iter()
        .any(|c| c.table == "org_members" && c.change_type == ChangeType::Edit);
    assert!(
        has_edit,
        "Edit that doesn't change filter fields should pass through as Edit"
    );
}
