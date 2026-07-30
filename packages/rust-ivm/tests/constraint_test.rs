//! Tests for constraint.ts — port of `zql/src/ivm/constraint.test.ts`.
//!
//! Tests: pullSimpleAndComponents, primaryKeyConstraintFromFilters,
//!        non-equality operators return undefined.

use rust_ivm::builder::ast::{Condition, SimpleCondition, ValuePosition};
use rust_ivm::ivm::constraint::{primary_key_constraint_from_filters, pull_simple_and_components};
use rust_ivm::ivm::data::Value;

// Helper: build a simple condition: column = literal
fn simple_eq(col: &str, val: Value) -> Condition {
    Condition::Simple(SimpleCondition {
        op: "=".to_string(),
        left: ValuePosition::Column {
            name: col.to_string(),
        },
        right: ValuePosition::Literal { value: val },
    })
}

fn simple_op(col: &str, op: &str, val: Value) -> Condition {
    Condition::Simple(SimpleCondition {
        op: op.to_string(),
        left: ValuePosition::Column {
            name: col.to_string(),
        },
        right: ValuePosition::Literal { value: val },
    })
}

// ---------------------------------------------------------------------------
// pullSimpleAndComponents
// ---------------------------------------------------------------------------

#[test]
fn test_pull_simple_from_and() {
    let condition = Condition::And(vec![
        simple_eq("id", Value::F64(1.0)),
        simple_eq("name", Value::Str("test".into())),
    ]);
    let result = pull_simple_and_components(&condition);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].op, "=");
    assert_eq!(result[1].op, "=");
}

#[test]
fn test_pull_simple_from_nested_and() {
    let condition = Condition::And(vec![
        simple_eq("id", Value::F64(1.0)),
        Condition::And(vec![
            simple_eq("name", Value::Str("test".into())),
            simple_eq("age", Value::F64(30.0)),
        ]),
    ]);
    let result = pull_simple_and_components(&condition);
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].op, "=");
    assert_eq!(result[1].op, "=");
    assert_eq!(result[2].op, "=");
}

#[test]
fn test_pull_simple_or_top_level_returns_empty() {
    let condition = Condition::Or(vec![
        simple_eq("id", Value::F64(1.0)),
        simple_eq("id", Value::F64(2.0)),
    ]);
    let result = pull_simple_and_components(&condition);
    assert!(result.is_empty());
}

#[test]
fn test_pull_simple_from_single_condition_or() {
    let condition = Condition::Or(vec![simple_eq("id", Value::F64(1.0))]);
    let result = pull_simple_and_components(&condition);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].op, "=");
}

// ---------------------------------------------------------------------------
// primaryKeyConstraintFromFilters
// ---------------------------------------------------------------------------

fn pk_result_str(condition: Option<&Condition>, primary: &[&str]) -> Option<String> {
    let primary_owned: Vec<String> = primary.iter().map(|s| s.to_string()).collect();
    primary_key_constraint_from_filters(condition, &primary_owned).map(|c| {
        let mut entries: Vec<(&String, &Value)> = c.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        format!(
            "{{{}}}",
            entries
                .iter()
                .map(|(k, v)| format!("\"{}\": {:?}", k, v))
                .collect::<Vec<_>>()
                .join(", ")
        )
    })
}

#[test]
fn test_pk_constraint_no_condition_returns_none() {
    let result = pk_result_str(None, &["id"]);
    assert_eq!(result, None);
}

#[test]
fn test_pk_constraint_or_returns_none() {
    let condition = Condition::Or(vec![
        simple_eq("id", Value::F64(1.0)),
        simple_eq("id", Value::F64(2.0)),
    ]);
    let result = pk_result_str(Some(&condition), &["id"]);
    assert_eq!(result, None);
}

#[test]
fn test_pk_constraint_simple_pk_lookup() {
    let condition = simple_eq("id", Value::F64(1.0));
    let result = pk_result_str(Some(&condition), &["id"]);
    assert_eq!(result, Some("{\"id\": F64(1.0)}".to_string()));
}

#[test]
fn test_pk_constraint_composite_pk_lookup() {
    let condition = Condition::And(vec![
        simple_eq("id", Value::F64(1.0)),
        simple_eq("tenant", Value::Str("test".into())),
    ]);
    let result = pk_result_str(Some(&condition), &["id", "tenant"]);
    assert_eq!(
        result,
        Some("{\"id\": F64(1.0), \"tenant\": Str(\"test\")}".to_string())
    );
}

#[test]
fn test_pk_constraint_partial_pk_returns_none() {
    let condition = simple_eq("id", Value::F64(1.0));
    let result = pk_result_str(Some(&condition), &["id", "tenant"]);
    assert_eq!(result, None);
}

#[test]
fn test_pk_constraint_non_pk_columns_returns_pk_only() {
    let condition = Condition::And(vec![
        simple_eq("id", Value::F64(1.0)),
        simple_eq("name", Value::Str("test".into())),
    ]);
    let result = pk_result_str(Some(&condition), &["id"]);
    assert_eq!(result, Some("{\"id\": F64(1.0)}".to_string()));
}

#[test]
fn test_pk_constraint_nested_and() {
    let condition = Condition::And(vec![
        simple_eq("id", Value::F64(1.0)),
        Condition::And(vec![simple_eq("tenant", Value::Str("test".into()))]),
    ]);
    let result = pk_result_str(Some(&condition), &["id", "tenant"]);
    assert_eq!(
        result,
        Some("{\"id\": F64(1.0), \"tenant\": Str(\"test\")}".to_string())
    );
}

#[test]
fn test_pk_constraint_nested_and_with_or() {
    let condition = Condition::And(vec![
        simple_eq("id", Value::F64(1.0)),
        Condition::And(vec![
            simple_eq("tenant", Value::Str("test".into())),
            Condition::Or(vec![
                simple_eq("status", Value::Str("active".into())),
                simple_eq("status", Value::Str("pending".into())),
            ]),
        ]),
    ]);
    let result = pk_result_str(Some(&condition), &["id", "tenant"]);
    assert_eq!(
        result,
        Some("{\"id\": F64(1.0), \"tenant\": Str(\"test\")}".to_string())
    );
}

// ---------------------------------------------------------------------------
// Non-equality operators return undefined
// ---------------------------------------------------------------------------

#[test]
fn test_non_equality_operators_return_none() {
    let operators = [
        ">",
        "<",
        ">=",
        "<=",
        "!=",
        "LIKE",
        "NOT LIKE",
        "ILIKE",
        "NOT ILIKE",
        "IN",
        "NOT IN",
        "IS",
        "IS NOT",
    ];

    for op in &operators {
        let val = if *op == "IN" || *op == "NOT IN" {
            Value::Json("[1,2,3]".into())
        } else {
            Value::F64(1.0)
        };
        let condition = simple_op("id", op, val);
        let result = pk_result_str(Some(&condition), &["id"]);
        assert_eq!(result, None, "operator {} should return None", op);
    }
}
