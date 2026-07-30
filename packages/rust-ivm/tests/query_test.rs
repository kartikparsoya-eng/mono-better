//! Tests for the query builder DSL, expression functions, and TTL.

use std::collections::HashMap;


use rust_ivm::builder::ast::Condition;
use rust_ivm::builder::expression::{
    false_val, true_val, and, cmp_eq, negate_operator, not, or, simplify_condition,
};
use rust_ivm::builder::query::{Cardinality, ExistsOptions, Query, RelationshipSpec};
use rust_ivm::builder::ttl::{DEFAULT_TTL_MS, MAX_TTL_MS, clamp_ttl, compare_ttl, parse_ttl};
use rust_ivm::ivm::data::Value;

fn make_relationships() -> HashMap<String, HashMap<String, RelationshipSpec>> {
    let mut tables: HashMap<String, HashMap<String, RelationshipSpec>> = HashMap::new();

    let mut user_rels: HashMap<String, RelationshipSpec> = HashMap::new();
    user_rels.insert(
        "posts".to_string(),
        RelationshipSpec {
            source_field: vec!["id".to_string()],
            dest_field: vec!["author_id".to_string()],
            dest_table: "posts".to_string(),
            cardinality: Cardinality::Many,
        },
    );
    tables.insert("users".to_string(), user_rels);

    let mut post_rels: HashMap<String, RelationshipSpec> = HashMap::new();
    post_rels.insert(
        "author".to_string(),
        RelationshipSpec {
            source_field: vec!["author_id".to_string()],
            dest_field: vec!["id".to_string()],
            dest_table: "users".to_string(),
            cardinality: Cardinality::One,
        },
    );
    post_rels.insert(
        "comments".to_string(),
        RelationshipSpec {
            source_field: vec!["id".to_string()],
            dest_field: vec!["post_id".to_string()],
            dest_table: "comments".to_string(),
            cardinality: Cardinality::Many,
        },
    );
    tables.insert("posts".to_string(), post_rels);

    tables
}

// ===========================================================================
// Expression tests
// ===========================================================================

#[test]
fn test_and_combines_conditions() {
    let c1 = cmp_eq("name", Value::Str("Alice".into()));
    let c2 = cmp_eq("age", Value::F64(30.0));
    let result = and(&[c1, c2]);
    match result {
        Condition::And(conds) => assert_eq!(conds.len(), 2),
        _ => panic!("Expected AND"),
    }
}

#[test]
fn test_or_combines_conditions() {
    let c1 = cmp_eq("name", Value::Str("Alice".into()));
    let c2 = cmp_eq("name", Value::Str("Bob".into()));
    let result = or(&[c1, c2]);
    match result {
        Condition::Or(conds) => assert_eq!(conds.len(), 2),
        _ => panic!("Expected OR"),
    }
}

#[test]
fn test_not_negates_simple() {
    let c = cmp_eq("active", Value::Bool(true));
    let result = not(&c);
    match result {
        Condition::Simple(s) => assert_eq!(s.op, "!="),
        _ => panic!("Expected simple with negated op"),
    }
}

#[test]
fn test_not_negates_and_to_or() {
    let c = and(&[cmp_eq("a", Value::F64(1.0)), cmp_eq("b", Value::F64(2.0))]);
    let result = not(&c);
    match result {
        Condition::Or(conds) => assert_eq!(conds.len(), 2),
        _ => panic!("Expected OR from negated AND"),
    }
}

#[test]
fn test_negate_operator() {
    assert_eq!(negate_operator("="), "!=");
    assert_eq!(negate_operator("!="), "=");
    assert_eq!(negate_operator("<"), ">=");
    assert_eq!(negate_operator(">"), "<=");
    assert_eq!(negate_operator("IN"), "NOT IN");
    assert_eq!(negate_operator("LIKE"), "NOT LIKE");
    assert_eq!(negate_operator("IS"), "IS NOT");
    assert_eq!(negate_operator("EXISTS"), "NOT EXISTS");
}

#[test]
fn test_simplify_condition_single_and() {
    let c = and(&[cmp_eq("x", Value::F64(1.0))]);
    let result = simplify_condition(&c);
    match result {
        Condition::Simple(s) => assert_eq!(s.op, "="),
        _ => panic!("Expected simplified to single condition"),
    }
}

#[test]
fn test_true_false_conditions() {
    let t = true_val();
    let f = false_val();
    match t {
        Condition::And(conds) => assert!(conds.is_empty()),
        _ => panic!("Expected empty AND for TRUE"),
    }
    match f {
        Condition::Or(conds) => assert!(conds.is_empty()),
        _ => panic!("Expected empty OR for FALSE"),
    }
}

// ===========================================================================
// Query builder tests
// ===========================================================================

#[test]
fn test_query_basic() {
    let rels = make_relationships();
    let q = Query::new("users", rels);
    assert_eq!(q.ast().table, "users");
    assert!(q.ast().where_clause.is_none());
}

#[test]
fn test_query_where_eq() {
    let rels = make_relationships();
    let q = Query::new("users", rels).where_eq("name", Value::Str("Alice".into()));
    match &q.ast().where_clause {
        Some(Condition::Simple(s)) => {
            assert_eq!(s.op, "=");
        }
        _ => panic!("Expected simple condition"),
    }
}

#[test]
fn test_query_where_op() {
    let rels = make_relationships();
    let q = Query::new("users", rels).where_op("age", ">", Value::F64(18.0));
    match &q.ast().where_clause {
        Some(Condition::Simple(s)) => {
            assert_eq!(s.op, ">");
        }
        _ => panic!("Expected simple condition"),
    }
}

#[test]
fn test_query_multiple_where_ands() {
    let rels = make_relationships();
    let q = Query::new("users", rels)
        .where_eq("name", Value::Str("Alice".into()))
        .where_eq("age", Value::F64(30.0));
    match &q.ast().where_clause {
        Some(Condition::And(conds)) => assert_eq!(conds.len(), 2),
        Some(Condition::Simple(_)) => {} // simplified to single if one is always-true
        _ => panic!("Expected AND or Simple"),
    }
}

#[test]
fn test_query_limit() {
    let rels = make_relationships();
    let q = Query::new("users", rels).limit(10);
    assert_eq!(q.ast().limit, Some(10));
}

#[test]
fn test_query_one() {
    let rels = make_relationships();
    let q = Query::new("users", rels).one();
    assert_eq!(q.ast().limit, Some(1));
    assert!(q.format().singular);
}

#[test]
fn test_query_order_by() {
    let rels = make_relationships();
    let q = Query::new("users", rels).order_by("name", "asc");
    let order = q.ast().order_by.as_ref().unwrap();
    assert_eq!(order.len(), 1);
    assert_eq!(order[0].column, "name");
    assert_eq!(order[0].direction, "asc");
}

#[test]
fn test_query_multiple_order_by() {
    let rels = make_relationships();
    let q = Query::new("users", rels)
        .order_by("name", "asc")
        .order_by("id", "desc");
    let order = q.ast().order_by.as_ref().unwrap();
    assert_eq!(order.len(), 2);
}

#[test]
fn test_query_related() {
    let rels = make_relationships();
    let q = Query::new("users", rels).related("posts", None);
    assert_eq!(q.ast().related.len(), 1);
    assert_eq!(q.ast().related[0].relationship_name, "posts");
    assert_eq!(q.ast().related[0].parent_key, vec!["id"]);
    assert_eq!(q.ast().related[0].child_key, vec!["author_id"]);
}

#[test]
fn test_query_related_with_callback() {
    let rels = make_relationships();
    let q = Query::new("users", rels).related(
        "posts",
        Some(Box::new(|sub| {
            sub.where_op("published", "=", Value::Bool(true)).limit(5)
        })),
    );
    assert_eq!(q.ast().related.len(), 1);
    let sub = &q.ast().related[0].subquery;
    assert!(sub.where_clause.is_some());
    assert_eq!(sub.limit, Some(5));
}

#[test]
fn test_query_where_exists() {
    let rels = make_relationships();
    let q = Query::new("users", rels).where_exists(
        "posts",
        None,
        ExistsOptions {
            flip: None,
            scalar: None,
        },
    );
    match &q.ast().where_clause {
        Some(Condition::CorrelatedSubquery(csq)) => {
            assert_eq!(csq.op, "EXISTS");
            assert_eq!(csq.related.parent_key, vec!["id"]);
            assert_eq!(csq.related.child_key, vec!["author_id"]);
        }
        Some(Condition::And(conds)) => {
            // Simplified may wrap in AND
            assert!(
                conds
                    .iter()
                    .any(|c| matches!(c, Condition::CorrelatedSubquery(_)))
            );
        }
        _ => panic!("Expected correlated subquery condition"),
    }
}

// ===========================================================================
// TTL tests
// ===========================================================================

#[test]
fn test_parse_ttl_none() {
    assert_eq!(parse_ttl("none"), 0);
}

#[test]
fn test_parse_ttl_forever() {
    assert_eq!(parse_ttl("forever"), -1);
}

#[test]
fn test_parse_ttl_seconds() {
    assert_eq!(parse_ttl("5s"), 5000);
}

#[test]
fn test_parse_ttl_minutes() {
    assert_eq!(parse_ttl("5m"), 5 * 60 * 1000);
}

#[test]
fn test_parse_ttl_hours() {
    assert_eq!(parse_ttl("1h"), 60 * 60 * 1000);
}

#[test]
fn test_parse_ttl_days() {
    assert_eq!(parse_ttl("1d"), 24 * 60 * 60 * 1000);
}

#[test]
fn test_parse_ttl_default() {
    assert_eq!(parse_ttl("5m"), DEFAULT_TTL_MS as i64);
}

#[test]
fn test_clamp_ttl_within_bounds() {
    assert_eq!(clamp_ttl("5m"), DEFAULT_TTL_MS as i64);
}

#[test]
fn test_clamp_ttl_too_high() {
    assert_eq!(clamp_ttl("forever"), MAX_TTL_MS as i64);
    assert_eq!(clamp_ttl("1y"), MAX_TTL_MS as i64);
}

#[test]
fn test_compare_ttl() {
    assert!(compare_ttl("5m", "1m") > 0);
    assert!(compare_ttl("1m", "5m") < 0);
    assert_eq!(compare_ttl("5m", "5m"), 0);
    assert!(compare_ttl("forever", "5m") > 0);
}
