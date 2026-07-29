// Property-based laws for compare_values / values_equal (Extraction #6).
// Ports the algebraic laws exercised by fast-check in
// mono-v1.7/packages/zql/src/ivm/data.test.ts to Rust `proptest`.
//
// compare_values (data.rs:52): Null==Null, Null<everything, same-type compare,
//   cross-type panics — so strategies stay within one type family (+ Null).
// values_equal (data.rs:68): null != null (join semantics), else structural ==.

use proptest::prelude::*;
use rust_ivm::ivm::data::{compare_values, values_equal, Value};
use std::cmp::Ordering;

// Finite, non-NaN numbers (real data has no NaN; NaN would break total order).
fn num() -> impl Strategy<Value = Value> {
    (-1.0e15..1.0e15f64).prop_map(Value::F64)
}
fn num_or_null() -> impl Strategy<Value = Value> {
    prop_oneof![Just(Value::Null), num()]
}
fn strv() -> impl Strategy<Value = Value> {
    "[a-z0-9]{0,8}".prop_map(|s| Value::Str(s.into()))
}
fn str_or_null() -> impl Strategy<Value = Value> {
    prop_oneof![Just(Value::Null), strv()]
}

proptest! {
    // Law 1 — reflexivity: compare_values(a, a) == Equal.
    #[test]
    fn reflexivity_num(a in num_or_null()) {
        prop_assert_eq!(compare_values(&a, &a), Ordering::Equal);
    }
    #[test]
    fn reflexivity_str(a in str_or_null()) {
        prop_assert_eq!(compare_values(&a, &a), Ordering::Equal);
    }

    // Law 2 — antisymmetry: compare(a,b) == compare(b,a).reverse().
    #[test]
    fn antisymmetry_num(a in num_or_null(), b in num_or_null()) {
        prop_assert_eq!(compare_values(&a, &b), compare_values(&b, &a).reverse());
    }
    #[test]
    fn antisymmetry_str(a in str_or_null(), b in str_or_null()) {
        prop_assert_eq!(compare_values(&a, &b), compare_values(&b, &a).reverse());
    }

    // Law 3 — transitivity: a<b && b<c ⇒ a<c (and the > mirror).
    #[test]
    fn transitivity_num(a in num_or_null(), b in num_or_null(), c in num_or_null()) {
        if compare_values(&a, &b) == Ordering::Less && compare_values(&b, &c) == Ordering::Less {
            prop_assert_eq!(compare_values(&a, &c), Ordering::Less);
        }
        if compare_values(&a, &b) == Ordering::Greater && compare_values(&b, &c) == Ordering::Greater {
            prop_assert_eq!(compare_values(&a, &c), Ordering::Greater);
        }
    }

    // Law 4 — null ordering: null < everything non-null; null == null.
    #[test]
    fn null_orders_first(a in num()) {
        prop_assert_eq!(compare_values(&Value::Null, &a), Ordering::Less);
        prop_assert_eq!(compare_values(&a, &Value::Null), Ordering::Greater);
    }
    #[test]
    fn null_equals_null(_ in 0..1u8) {
        prop_assert_eq!(compare_values(&Value::Null, &Value::Null), Ordering::Equal);
    }

    // Law 5 — values_equal null exclusion: null is never equal to anything,
    // including null (unlike compare_values). Required for join matching.
    #[test]
    fn values_equal_null_exclusion(a in num_or_null()) {
        prop_assert!(!values_equal(&Value::Null, &a));
        prop_assert!(!values_equal(&a, &Value::Null));
    }

    // Law 6 — equal-consistency: compare==Equal ⇔ (non-null) values_equal.
    #[test]
    fn equal_consistency_num(a in num(), b in num()) {
        let cmp_eq = compare_values(&a, &b) == Ordering::Equal;
        prop_assert_eq!(cmp_eq, values_equal(&a, &b));
    }
}
