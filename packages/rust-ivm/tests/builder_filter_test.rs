//! Tests for builder/filter.ts — port of `zql/src/builder/filter.test.ts`.
//!
//! Tests: createPredicate with null, IS/IS NOT, basic operators, like, and/or/empty/nested.

use rustc_hash::FxHashMap;
use std::sync::Arc;

use rust_ivm::builder::ast::{Condition, SimpleCondition, ValuePosition};
use rust_ivm::builder::filter::create_predicate;
use rust_ivm::builder::like::get_like_predicate;
use rust_ivm::ivm::data::{Row, Value};

fn make_row(pairs: &[(&str, Value)]) -> Row {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Arc::new(map)
}

fn col_eq(col: &str, val: Value) -> Condition {
    Condition::Simple(SimpleCondition {
        op: "=".to_string(),
        left: ValuePosition::Column {
            name: col.to_string(),
        },
        right: ValuePosition::Literal { value: val },
    })
}

fn col_op(col: &str, op: &str, val: Value) -> Condition {
    Condition::Simple(SimpleCondition {
        op: op.to_string(),
        left: ValuePosition::Column {
            name: col.to_string(),
        },
        right: ValuePosition::Literal { value: val },
    })
}

// ---------------------------------------------------------------------------
// nulls are false in all conditions except IS / IS NOT
// ---------------------------------------------------------------------------

#[test]
fn test_null_is_false_for_basic_operators() {
    let operators = [
        "=",
        "!=",
        "<",
        "<=",
        ">",
        ">=",
        "LIKE",
        "NOT LIKE",
        "ILIKE",
        "NOT ILIKE",
    ];
    for op in &operators {
        let condition = col_op("foo", op, Value::Str("bar".into()));
        let predicate = create_predicate(&condition);
        assert!(
            !predicate(&make_row(&[("foo", Value::Null)])),
            "null should be false for operator {}",
            op
        );
    }
}

// ---------------------------------------------------------------------------
// IS / IS NOT
// ---------------------------------------------------------------------------

#[test]
fn test_is_null() {
    let condition = Condition::Simple(SimpleCondition {
        op: "IS".to_string(),
        left: ValuePosition::Column {
            name: "foo".to_string(),
        },
        right: ValuePosition::Literal { value: Value::Null },
    });
    let predicate = create_predicate(&condition);
    assert!(predicate(&make_row(&[("foo", Value::Null)])));
    assert!(!predicate(&make_row(&[("foo", Value::F64(1.0))])));
    assert!(!predicate(&make_row(&[("foo", Value::Str("null".into()))])));
    assert!(!predicate(&make_row(&[("foo", Value::Bool(true))])));
    assert!(!predicate(&make_row(&[("foo", Value::Bool(false))])));
}

#[test]
fn test_is_not_null() {
    let condition = Condition::Simple(SimpleCondition {
        op: "IS NOT".to_string(),
        left: ValuePosition::Column {
            name: "foo".to_string(),
        },
        right: ValuePosition::Literal { value: Value::Null },
    });
    let predicate = create_predicate(&condition);
    assert!(!predicate(&make_row(&[("foo", Value::Null)])));
    assert!(predicate(&make_row(&[("foo", Value::F64(1.0))])));
    assert!(predicate(&make_row(&[("foo", Value::Str("null".into()))])));
    assert!(predicate(&make_row(&[("foo", Value::Bool(true))])));
    assert!(predicate(&make_row(&[("foo", Value::Bool(false))])));
}

// ---------------------------------------------------------------------------
// basic operators
// ---------------------------------------------------------------------------

#[test]
fn test_equal_numbers() {
    let predicate = create_predicate(&col_eq("foo", Value::F64(1.0)));
    assert!(predicate(&make_row(&[("foo", Value::F64(1.0))])));
    assert!(!predicate(&make_row(&[("foo", Value::F64(2.0))])));
}

#[test]
fn test_not_equal_numbers() {
    let predicate = create_predicate(&col_op("foo", "!=", Value::F64(1.0)));
    assert!(!predicate(&make_row(&[("foo", Value::F64(1.0))])));
    assert!(predicate(&make_row(&[("foo", Value::F64(2.0))])));
}

#[test]
fn test_less_than() {
    let predicate = create_predicate(&col_op("foo", "<", Value::F64(5.0)));
    assert!(predicate(&make_row(&[("foo", Value::F64(3.0))])));
    assert!(!predicate(&make_row(&[("foo", Value::F64(5.0))])));
    assert!(!predicate(&make_row(&[("foo", Value::F64(7.0))])));
}

#[test]
fn test_less_than_or_equal() {
    let predicate = create_predicate(&col_op("foo", "<=", Value::F64(5.0)));
    assert!(predicate(&make_row(&[("foo", Value::F64(3.0))])));
    assert!(predicate(&make_row(&[("foo", Value::F64(5.0))])));
    assert!(!predicate(&make_row(&[("foo", Value::F64(7.0))])));
}

#[test]
fn test_greater_than() {
    let predicate = create_predicate(&col_op("foo", ">", Value::F64(5.0)));
    assert!(!predicate(&make_row(&[("foo", Value::F64(3.0))])));
    assert!(!predicate(&make_row(&[("foo", Value::F64(5.0))])));
    assert!(predicate(&make_row(&[("foo", Value::F64(7.0))])));
}

#[test]
fn test_greater_than_or_equal() {
    let predicate = create_predicate(&col_op("foo", ">=", Value::F64(5.0)));
    assert!(!predicate(&make_row(&[("foo", Value::F64(3.0))])));
    assert!(predicate(&make_row(&[("foo", Value::F64(5.0))])));
    assert!(predicate(&make_row(&[("foo", Value::F64(7.0))])));
}

#[test]
fn test_equal_strings() {
    let predicate = create_predicate(&col_eq("foo", Value::Str("hello".into())));
    assert!(predicate(&make_row(&[("foo", Value::Str("hello".into()))])));
    assert!(!predicate(&make_row(&[(
        "foo",
        Value::Str("world".into())
    )])));
}

#[test]
fn test_equal_bools() {
    let predicate = create_predicate(&col_eq("foo", Value::Bool(true)));
    assert!(predicate(&make_row(&[("foo", Value::Bool(true))])));
    assert!(!predicate(&make_row(&[("foo", Value::Bool(false))])));
}

#[test]
fn test_null_rhs_always_false() {
    let condition = Condition::Simple(SimpleCondition {
        op: "=".to_string(),
        left: ValuePosition::Column {
            name: "foo".to_string(),
        },
        right: ValuePosition::Literal { value: Value::Null },
    });
    let predicate = create_predicate(&condition);
    assert!(!predicate(&make_row(&[("foo", Value::Null)])));
    assert!(!predicate(&make_row(&[("foo", Value::F64(1.0))])));
}

// ---------------------------------------------------------------------------
// like
// ---------------------------------------------------------------------------

#[test]
fn test_like_predicate_via_create_predicate() {
    let condition = col_op("foo", "LIKE", Value::Str("foo".into()));
    let predicate = create_predicate(&condition);
    assert!(predicate(&make_row(&[("foo", Value::Str("foo".into()))])));
    assert!(!predicate(&make_row(&[("foo", Value::Str("bar".into()))])));
    assert!(!predicate(&make_row(&[("foo", Value::Str("Foo".into()))])));
}

#[test]
fn test_ilike_predicate_via_create_predicate() {
    let condition = col_op("foo", "ILIKE", Value::Str("foo".into()));
    let predicate = create_predicate(&condition);
    assert!(predicate(&make_row(&[("foo", Value::Str("foo".into()))])));
    assert!(predicate(&make_row(&[("foo", Value::Str("Foo".into()))])));
    assert!(predicate(&make_row(&[("foo", Value::Str("FOO".into()))])));
    assert!(!predicate(&make_row(&[("foo", Value::Str("bar".into()))])));
}

#[test]
fn test_like_with_wildcard() {
    let condition = col_op("foo", "LIKE", Value::Str("foo%".into()));
    let predicate = create_predicate(&condition);
    assert!(predicate(&make_row(&[("foo", Value::Str("foo".into()))])));
    assert!(predicate(&make_row(&[(
        "foo",
        Value::Str("foobar".into())
    )])));
    assert!(!predicate(&make_row(&[("foo", Value::Str("bar".into()))])));
}

// ---------------------------------------------------------------------------
// and / or / empty / nested
// ---------------------------------------------------------------------------

#[test]
fn test_and() {
    let predicate = create_predicate(&Condition::And(vec![
        col_eq("a", Value::F64(4.0)),
        col_eq("b", Value::Bool(false)),
    ]));
    assert!(!predicate(&make_row(&[
        ("a", Value::F64(4.0)),
        ("b", Value::Bool(true))
    ])));
    assert!(!predicate(&make_row(&[
        ("a", Value::F64(3.0)),
        ("b", Value::Bool(false))
    ])));
    assert!(!predicate(&make_row(&[
        ("a", Value::F64(3.0)),
        ("b", Value::Bool(true))
    ])));
    assert!(predicate(&make_row(&[
        ("a", Value::F64(4.0)),
        ("b", Value::Bool(false))
    ])));
}

#[test]
fn test_or() {
    let predicate = create_predicate(&Condition::Or(vec![
        col_eq("a", Value::F64(4.0)),
        col_eq("b", Value::Bool(false)),
    ]));
    assert!(predicate(&make_row(&[
        ("a", Value::F64(4.0)),
        ("b", Value::Bool(true))
    ])));
    assert!(predicate(&make_row(&[
        ("a", Value::F64(3.0)),
        ("b", Value::Bool(false))
    ])));
    assert!(!predicate(&make_row(&[
        ("a", Value::F64(3.0)),
        ("b", Value::Bool(true))
    ])));
    assert!(predicate(&make_row(&[
        ("a", Value::F64(4.0)),
        ("b", Value::Bool(false))
    ])));
}

#[test]
fn test_empty_and_is_true() {
    let predicate = create_predicate(&Condition::And(vec![]));
    assert!(predicate(&make_row(&[
        ("a", Value::F64(4.0)),
        ("b", Value::Bool(true))
    ])));
}

#[test]
fn test_empty_or_is_false() {
    let predicate = create_predicate(&Condition::Or(vec![]));
    assert!(!predicate(&make_row(&[
        ("a", Value::F64(4.0)),
        ("b", Value::Bool(true))
    ])));
}

#[test]
fn test_nested_or_with_and() {
    let predicate = create_predicate(&Condition::Or(vec![
        col_eq("a", Value::F64(4.0)),
        Condition::And(vec![
            col_eq("a", Value::F64(3.0)),
            col_eq("b", Value::Bool(false)),
        ]),
    ]));
    assert!(predicate(&make_row(&[
        ("a", Value::F64(4.0)),
        ("b", Value::Bool(true))
    ])));
    assert!(predicate(&make_row(&[
        ("a", Value::F64(4.0)),
        ("b", Value::Bool(false))
    ])));
    assert!(predicate(&make_row(&[
        ("a", Value::F64(3.0)),
        ("b", Value::Bool(false))
    ])));
    assert!(!predicate(&make_row(&[
        ("a", Value::F64(3.0)),
        ("b", Value::Bool(true))
    ])));
    assert!(!predicate(&make_row(&[
        ("a", Value::F64(5.0)),
        ("b", Value::Bool(false))
    ])));
}

// ---------------------------------------------------------------------------
// get_like_predicate direct tests (like.test.ts)
// ---------------------------------------------------------------------------

#[test]
fn test_get_like_predicate_exact_match() {
    let pred = get_like_predicate(&Value::Str("hello".into()), "");
    assert!(pred(&Value::Str("hello".into())));
    assert!(!pred(&Value::Str("world".into())));
}

#[test]
fn test_get_like_predicate_percent() {
    let pred = get_like_predicate(&Value::Str("hel%".into()), "");
    assert!(pred(&Value::Str("hello".into())));
    assert!(pred(&Value::Str("help".into())));
    assert!(pred(&Value::Str("hel".into())));
    assert!(!pred(&Value::Str("world".into())));
}

#[test]
fn test_get_like_predicate_underscore() {
    let pred = get_like_predicate(&Value::Str("h_llo".into()), "");
    assert!(pred(&Value::Str("hello".into())));
    assert!(pred(&Value::Str("hxllo".into())));
    assert!(!pred(&Value::Str("heello".into())));
}

#[test]
fn test_get_like_predicate_case_insensitive() {
    let pred = get_like_predicate(&Value::Str("hello".into()), "i");
    assert!(pred(&Value::Str("hello".into())));
    assert!(pred(&Value::Str("Hello".into())));
    assert!(pred(&Value::Str("HELLO".into())));
    assert!(!pred(&Value::Str("world".into())));
}

#[test]
fn test_get_like_predicate_escaped_percent() {
    let pred = get_like_predicate(&Value::Str("100\\%".into()), "");
    assert!(pred(&Value::Str("100%".into())));
    assert!(!pred(&Value::Str("1000".into())));
}

/// TS `getLikePredicate`'s returned predicate starts with `assertString(lhs)`
/// (like.ts:10): a non-string operand is a THROWN `invalidType` error, not a
/// `false` (the pre-2026-09-04 rust behavior this test used to pin). Nulls
/// never reach it — `createPredicate` short-circuits a null lhs to `false`
/// (filter.ts:90 / filter.rs), pinned by `test_like_null_lhs_is_false_before_the_assert`.
#[test]
#[should_panic(expected = "Invalid type: number `1`, expected string")]
fn test_get_like_predicate_non_string_asserts_like_ts() {
    let pred = get_like_predicate(&Value::Str("hello".into()), "");
    pred(&Value::F64(1.0));
}

#[test]
#[should_panic(expected = "Invalid type: boolean `true`, expected string")]
fn test_get_like_predicate_bool_asserts_like_ts() {
    let pred = get_like_predicate(&Value::Str("hello".into()), "");
    pred(&Value::Bool(true));
}

#[test]
fn test_like_null_lhs_is_false_before_the_assert() {
    let cond = Condition::Simple(SimpleCondition {
        op: "LIKE".to_string(),
        left: ValuePosition::Column {
            name: "name".to_string(),
        },
        right: ValuePosition::Literal {
            value: Value::Str("h%".into()),
        },
    });
    let pred = create_predicate(&cond);
    let mut row = FxHashMap::default();
    row.insert("name".to_string(), Value::Null);
    assert!(!pred(&Arc::new(row)));
}
