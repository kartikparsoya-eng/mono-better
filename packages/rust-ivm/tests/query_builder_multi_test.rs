//! Tests for query builder multiConstraints SQL generation.
//! Port of TS `query-builder.test.ts` (v1.7.0).

use rust_ivm::ivm::constraint::{Constraint, MultiConstraint};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::operator::{Basis, FetchRequest, Start};
use rust_ivm::sqlite::query_builder::build_select_query;
use std::sync::Arc;

fn str_val(s: &str) -> Value {
    Value::Str(std::sync::Arc::from(s))
}

#[test]
fn test_multi_constraints_single_column_in_list() {
    let mut mc1 = Constraint::default();
    mc1.insert("id".to_string(), str_val("i1"));
    let mut mc2 = Constraint::default();
    mc2.insert("id".to_string(), str_val("i2"));
    let mut mc3 = Constraint::default();
    mc3.insert("id".to_string(), str_val("i3"));

    let mc: MultiConstraint = vec![mc1, mc2, mc3];

    let req = FetchRequest {
        multi_constraints: vec![mc],
        ..Default::default()
    };

    let columns = vec!["id".to_string(), "name".to_string()];
    let order: Vec<(String, String)> = vec![("id".to_string(), "asc".to_string())];

    let query = build_select_query("issues", &columns, &req, None, Some(&order), false);

    assert!(query.text.contains("\"id\" IN ("));
    assert!(query.text.contains("?, ?, ?"));
    assert_eq!(query.params.len(), 3);
}

#[test]
fn test_multi_constraints_compound_key_values() {
    let mut mc1 = Constraint::default();
    mc1.insert("a".to_string(), str_val("x"));
    mc1.insert("b".to_string(), Value::F64(1.0));
    let mut mc2 = Constraint::default();
    mc2.insert("a".to_string(), str_val("y"));
    mc2.insert("b".to_string(), Value::F64(2.0));

    let mc: MultiConstraint = vec![mc1, mc2];

    let req = FetchRequest {
        multi_constraints: vec![mc],
        ..Default::default()
    };

    let columns = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let order: Vec<(String, String)> = vec![("a".to_string(), "asc".to_string())];

    let query = build_select_query("pairs", &columns, &req, None, Some(&order), false);

    assert!(
        query.text.contains("(\"a\", \"b\") IN (VALUES"),
        "Compound key should produce row-value VALUES: {}",
        query.text
    );
    assert!(query.text.contains("(?, ?)"));
    assert_eq!(query.params.len(), 4, "2 entries x 2 keys = 4 params");
}

#[test]
fn test_multi_constraints_with_constraint_and_start_and_reverse() {
    let mut mc1 = Constraint::default();
    mc1.insert("id".to_string(), str_val("i1"));
    let mut mc2 = Constraint::default();
    mc2.insert("id".to_string(), str_val("i2"));
    let mut mc3 = Constraint::default();
    mc3.insert("id".to_string(), str_val("i3"));

    let mc: MultiConstraint = vec![mc1, mc2, mc3];

    let mut constraint = Constraint::default();
    constraint.insert("org".to_string(), str_val("acme"));

    let mut start_row = rustc_hash::FxHashMap::default();
    start_row.insert("rank".to_string(), Value::F64(100.0));

    let req = FetchRequest {
        constraint: Some(constraint),
        multi_constraints: vec![mc],
        start: Some(Start {
            row: Arc::new(start_row),
            basis: Basis::After,
        }),
        reverse: true,
        ..Default::default()
    };

    let columns = vec!["id".to_string(), "org".to_string(), "rank".to_string()];
    let order: Vec<(String, String)> = vec![("rank".to_string(), "asc".to_string())];

    let query = build_select_query("issues", &columns, &req, None, Some(&order), true);

    assert!(
        query.text.contains("\"org\" = ?"),
        "Should have constraint: {}",
        query.text
    );
    assert!(
        query.text.contains("\"id\" IN ("),
        "Should have multiConstraint IN: {}",
        query.text
    );
    assert!(
        query.text.contains("ORDER BY"),
        "Should have ORDER BY: {}",
        query.text
    );
    assert!(
        query.text.contains("desc"),
        "Should have reversed order: {}",
        query.text
    );
}

#[test]
fn test_multi_constraints_multiple_independent_lists() {
    let mut mc1a = Constraint::default();
    mc1a.insert("id".to_string(), str_val("i1"));
    let mut mc1b = Constraint::default();
    mc1b.insert("id".to_string(), str_val("i2"));

    let mut mc2a = Constraint::default();
    mc2a.insert("org".to_string(), str_val("acme"));
    let mut mc2b = Constraint::default();
    mc2b.insert("org".to_string(), str_val("beta"));

    let req = FetchRequest {
        multi_constraints: vec![vec![mc1a, mc1b], vec![mc2a, mc2b]],
        ..Default::default()
    };

    let columns = vec!["id".to_string(), "org".to_string()];
    let order: Vec<(String, String)> = vec![("id".to_string(), "asc".to_string())];

    let query = build_select_query("issues", &columns, &req, None, Some(&order), false);

    assert!(
        query.text.contains("\"id\" IN (") && query.text.contains("\"org\" IN ("),
        "Should have both IN clauses: {}",
        query.text
    );
    assert!(
        query.text.contains(" AND "),
        "Clauses should be ANDed: {}",
        query.text
    );
}

#[test]
fn test_multi_constraints_empty_skipped() {
    let req = FetchRequest {
        multi_constraints: vec![],
        ..Default::default()
    };

    let columns = vec!["id".to_string()];
    let order: Vec<(String, String)> = vec![("id".to_string(), "asc".to_string())];

    let query = build_select_query("issues", &columns, &req, None, Some(&order), false);

    assert!(
        !query.text.contains("IN"),
        "No IN clause for empty multi_constraints: {}",
        query.text
    );
}

#[test]
fn test_multi_constraints_no_order() {
    let mut mc1 = Constraint::default();
    mc1.insert("id".to_string(), str_val("i1"));
    let mut mc2 = Constraint::default();
    mc2.insert("id".to_string(), str_val("i2"));

    let req = FetchRequest {
        multi_constraints: vec![vec![mc1, mc2]],
        ..Default::default()
    };

    let columns = vec!["id".to_string(), "name".to_string()];

    let query = build_select_query("issues", &columns, &req, None, None, false);

    assert!(
        query.text.contains("\"id\" IN ("),
        "Should have IN clause: {}",
        query.text
    );
    assert!(
        !query.text.contains("ORDER BY"),
        "Should not have ORDER BY: {}",
        query.text
    );
}
