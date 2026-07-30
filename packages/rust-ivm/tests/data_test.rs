//! Tests for data.ts — port of `zql/src/ivm/data.test.ts`.
//!
//! Tests: normalizeUndefined, compareValues, valuesEqual, comparator.

use std::cmp::Ordering;

use rustc_hash::FxHashMap;
use std::sync::Arc;

use rust_ivm::ivm::data::{SortOrder, Value, compare_values, make_comparator, values_equal};

// ---------------------------------------------------------------------------
// normalizeUndefined
// ---------------------------------------------------------------------------

#[test]
fn test_normalize_undefined_null_is_null() {
    // Rust has no `undefined` — Value::Null is the single null sentinel.
    assert!(Value::Null.is_null());
}

#[test]
fn test_normalize_undefined_other_types_unchanged() {
    assert!(!Value::Bool(true).is_null());
    assert!(!Value::F64(1.0).is_null());
    assert!(!Value::Str("hello".into()).is_null());
}

// ---------------------------------------------------------------------------
// compareValues
// ---------------------------------------------------------------------------

#[test]
fn test_compare_values_null_and_null_equal() {
    assert_eq!(compare_values(&Value::Null, &Value::Null), Ordering::Equal);
}

#[test]
fn test_compare_values_null_less_than_anything() {
    assert_eq!(
        compare_values(&Value::Null, &Value::Bool(true)),
        Ordering::Less
    );
    assert_eq!(
        compare_values(&Value::Null, &Value::F64(1.0)),
        Ordering::Less
    );
    assert_eq!(
        compare_values(&Value::Null, &Value::Str("a".into())),
        Ordering::Less
    );
}

#[test]
fn test_compare_values_anything_greater_than_null() {
    assert_eq!(
        compare_values(&Value::Bool(true), &Value::Null),
        Ordering::Greater
    );
    assert_eq!(
        compare_values(&Value::F64(1.0), &Value::Null),
        Ordering::Greater
    );
    assert_eq!(
        compare_values(&Value::Str("a".into()), &Value::Null),
        Ordering::Greater
    );
}

#[test]
fn test_compare_values_bool() {
    assert_eq!(
        compare_values(&Value::Bool(true), &Value::Bool(true)),
        Ordering::Equal
    );
    assert_eq!(
        compare_values(&Value::Bool(true), &Value::Bool(false)),
        Ordering::Greater
    );
    assert_eq!(
        compare_values(&Value::Bool(false), &Value::Bool(true)),
        Ordering::Less
    );
}

#[test]
#[should_panic(expected = "Cannot compare values of different types")]
fn test_compare_values_bool_and_number_panics() {
    compare_values(&Value::Bool(true), &Value::F64(1.0));
}

#[test]
#[should_panic(expected = "Cannot compare values of different types")]
fn test_compare_values_bool_and_string_panics() {
    compare_values(&Value::Bool(true), &Value::Str("a".into()));
}

#[test]
fn test_compare_values_number() {
    assert_eq!(
        compare_values(&Value::F64(1.0), &Value::F64(2.0)),
        Ordering::Less
    );
    assert_eq!(
        compare_values(&Value::F64(2.0), &Value::F64(1.0)),
        Ordering::Greater
    );
    assert_eq!(
        compare_values(&Value::F64(1.0), &Value::F64(1.0)),
        Ordering::Equal
    );
}

#[test]
#[should_panic(expected = "Cannot compare values of different types")]
fn test_compare_values_number_and_bool_panics() {
    compare_values(&Value::F64(1.0), &Value::Bool(true));
}

#[test]
#[should_panic(expected = "Cannot compare values of different types")]
fn test_compare_values_number_and_string_panics() {
    compare_values(&Value::F64(1.0), &Value::Str("a".into()));
}

#[test]
fn test_compare_values_string_utf8() {
    assert_eq!(
        compare_values(&Value::Str("a".into()), &Value::Str("b".into())),
        Ordering::Less
    );
    assert_eq!(
        compare_values(&Value::Str("b".into()), &Value::Str("a".into())),
        Ordering::Greater
    );
    assert_eq!(
        compare_values(&Value::Str("a".into()), &Value::Str("a".into())),
        Ordering::Equal
    );
}

#[test]
#[should_panic(expected = "Cannot compare values of different types")]
fn test_compare_values_string_and_bool_panics() {
    compare_values(&Value::Str("a".into()), &Value::Bool(true));
}

#[test]
#[should_panic(expected = "Cannot compare values of different types")]
fn test_compare_values_string_and_number_panics() {
    compare_values(&Value::Str("a".into()), &Value::F64(1.0));
}

// ---------------------------------------------------------------------------
// valuesEqual
// ---------------------------------------------------------------------------

#[test]
fn test_values_equal_same_type_same_value() {
    assert!(values_equal(&Value::Bool(true), &Value::Bool(true)));
    assert!(values_equal(&Value::F64(1.0), &Value::F64(1.0)));
    assert!(values_equal(
        &Value::Str("a".into()),
        &Value::Str("a".into())
    ));
}

#[test]
fn test_values_equal_same_type_different_value() {
    assert!(!values_equal(&Value::Bool(true), &Value::Bool(false)));
    assert!(!values_equal(&Value::F64(1.0), &Value::F64(2.0)));
    assert!(!values_equal(
        &Value::Str("a".into()),
        &Value::Str("b".into())
    ));
}

#[test]
fn test_values_equal_null_never_equal() {
    // null ≠ null — required for join semantics
    assert!(!values_equal(&Value::Null, &Value::Null));
    assert!(!values_equal(&Value::Null, &Value::Bool(true)));
    assert!(!values_equal(&Value::Null, &Value::F64(1.0)));
    assert!(!values_equal(&Value::Null, &Value::Str("a".into())));
    assert!(!values_equal(&Value::Bool(true), &Value::Null));
    assert!(!values_equal(&Value::F64(1.0), &Value::Null));
    assert!(!values_equal(&Value::Str("a".into()), &Value::Null));
}

// ---------------------------------------------------------------------------
// comparator (compareRowsTest)
// ---------------------------------------------------------------------------

fn make_row(pairs: &[(&str, Value)]) -> rust_ivm::ivm::data::Row {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Arc::new(map)
}

fn sort_order(parts: &[(&str, &str)]) -> SortOrder {
    Arc::new(
        parts
            .iter()
            .map(|(c, d)| [c.to_string(), d.to_string()])
            .collect(),
    )
}

#[test]
fn test_comparator_asc() {
    let order = sort_order(&[("a", "asc")]);
    let cmp = make_comparator(order, false);
    let r1 = make_row(&[("a", Value::F64(1.0))]);
    let r2 = make_row(&[("a", Value::F64(2.0))]);
    assert_eq!(cmp(&r1, &r2), Ordering::Less);
}

#[test]
fn test_comparator_desc() {
    let order = sort_order(&[("a", "desc")]);
    let cmp = make_comparator(order, false);
    let r1 = make_row(&[("a", Value::F64(1.0))]);
    let r2 = make_row(&[("a", Value::F64(2.0))]);
    assert_eq!(cmp(&r1, &r2), Ordering::Greater);
}

#[test]
fn test_comparator_asc_reversed() {
    let order = sort_order(&[("a", "asc")]);
    let cmp = make_comparator(order, false);
    let r1 = make_row(&[("a", Value::F64(2.0))]);
    let r2 = make_row(&[("a", Value::F64(1.0))]);
    assert_eq!(cmp(&r1, &r2), Ordering::Greater);
}

#[test]
fn test_comparator_multi_column_equal() {
    let order = sort_order(&[("a", "asc"), ("b", "asc")]);
    let cmp = make_comparator(order, false);
    let r1 = make_row(&[("a", Value::F64(1.0)), ("b", Value::Str("".into()))]);
    let r2 = make_row(&[("a", Value::F64(1.0)), ("b", Value::Str("".into()))]);
    assert_eq!(cmp(&r1, &r2), Ordering::Equal);
}

#[test]
fn test_comparator_multi_column_second_differs() {
    let order = sort_order(&[("a", "asc"), ("b", "asc")]);
    let cmp = make_comparator(order, false);
    let r1 = make_row(&[("a", Value::F64(1.0)), ("b", Value::Str("".into()))]);
    let r2 = make_row(&[("a", Value::F64(1.0)), ("b", Value::Str("foo".into()))]);
    assert_eq!(cmp(&r1, &r2), Ordering::Less);
}

#[test]
fn test_comparator_multi_column_second_differs_reversed() {
    let order = sort_order(&[("a", "asc"), ("b", "asc")]);
    let cmp = make_comparator(order, false);
    let r1 = make_row(&[("a", Value::F64(1.0)), ("b", Value::Str("foo".into()))]);
    let r2 = make_row(&[("a", Value::F64(1.0)), ("b", Value::Str("bar".into()))]);
    assert_eq!(cmp(&r1, &r2), Ordering::Greater);
}

#[test]
fn test_comparator_multi_column_second_string_ordering() {
    let order = sort_order(&[("a", "asc"), ("b", "asc")]);
    let cmp = make_comparator(order, false);
    let r1 = make_row(&[("a", Value::F64(1.0)), ("b", Value::Str("foo".into()))]);
    let r2 = make_row(&[("a", Value::F64(1.0)), ("b", Value::Str("bar".into()))]);
    assert_eq!(cmp(&r1, &r2), Ordering::Greater);
}

#[test]
#[should_panic(expected = "Cannot compare values of different types")]
fn test_comparator_different_types_panics() {
    let order = sort_order(&[("a", "asc")]);
    let cmp = make_comparator(order, false);
    let r1 = make_row(&[("a", Value::F64(1.0))]);
    let r2 = make_row(&[("a", Value::Str("foo".into()))]);
    cmp(&r1, &r2);
}
