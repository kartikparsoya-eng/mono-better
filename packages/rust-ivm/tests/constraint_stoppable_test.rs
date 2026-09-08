//! Tests for `constraint_equals` (zql/src/ivm/constraint.ts `constraintEquals`)
//! and `StoppableIterator` (zql/src/ivm/stopable-iterator.ts). Both are small
//! self-contained utilities that were entirely untested (triage targets).

use std::sync::Arc;

use rust_ivm::ivm::constraint::{Constraint, constraint_equals};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::stopable_iterator::StoppableIterator;

fn str_val(s: &str) -> Value {
    Value::Str(Arc::from(s))
}

fn constraint(pairs: &[(&str, Value)]) -> Constraint {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

// Port of TS `constraintEquals`: key-order-insensitive value equality.
#[test]
fn constraint_equals_is_key_order_insensitive() {
    let a = constraint(&[("id", Value::F64(1.0)), ("name", str_val("x"))]);
    let b = constraint(&[("name", str_val("x")), ("id", Value::F64(1.0))]);
    assert!(
        constraint_equals(&a, &b),
        "same entries in any order are equal"
    );
    assert!(constraint_equals(&a, &a));
}

#[test]
fn constraint_equals_detects_length_key_and_value_diffs() {
    let base = constraint(&[("id", Value::F64(1.0))]);

    // Different length (an extra key) → not equal. A present null key vs a
    // missing key is a length difference, so they are distinct.
    let longer = constraint(&[("id", Value::F64(1.0)), ("extra", Value::Null)]);
    assert!(!constraint_equals(&base, &longer));
    assert!(!constraint_equals(&longer, &base));

    // Same length, different key name → not equal.
    let other_key = constraint(&[("other", Value::F64(1.0))]);
    assert!(!constraint_equals(&base, &other_key));

    // Same key, different value → not equal.
    let other_val = constraint(&[("id", Value::F64(2.0))]);
    assert!(!constraint_equals(&base, &other_val));
}

// StoppableIterator yields underlying items until stopped.
#[test]
fn stoppable_iterator_yields_then_stops() {
    let mut it = StoppableIterator::new(vec![1, 2, 3].into_iter());
    assert!(!it.is_stopped());
    assert_eq!(it.next(), Some(1));
    assert_eq!(it.next(), Some(2));
    it.stop();
    assert!(it.is_stopped());
}

// Port of TS: once stopped, further iteration is an error (panic in Rust).
#[test]
#[should_panic(expected = "Iterator has been stopped")]
fn stoppable_iterator_next_after_stop_panics() {
    let mut it = StoppableIterator::new(vec![1, 2, 3].into_iter());
    let _ = it.next();
    it.stop();
    let _ = it.next(); // must panic
}
