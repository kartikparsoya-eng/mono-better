//! Tests for small pure helper functions that each mirror a real TS branch
//! (triage bucket-3 promotions): `assert_ordering_includes_pk` (complete-
//! ordering.ts:31) and `is_interrupt_error` (sqlite-cost-model.ts). Both were
//! FNDA:0.

use rust_ivm::query::complete_ordering::assert_ordering_includes_pk;
use rust_ivm::sqlite::sqlite_cost_model::{INTERRUPT_ERR_PREFIX, is_interrupt_error};

fn ord(fields: &[&str]) -> Vec<(String, String)> {
    fields
        .iter()
        .map(|f| (f.to_string(), "asc".to_string()))
        .collect()
}

// Port of TS `assertOrderingIncludesPK`: passes when every PK field appears in
// the ordering (order/direction irrelevant).
#[test]
fn assert_ordering_includes_pk_passes_when_all_pk_present() {
    // Compound PK fully covered (plus an extra sort field) => no panic.
    assert_ordering_includes_pk(&ord(&["a", "b", "c"]), &["a".to_string(), "b".to_string()]);
    // Single PK present.
    assert_ordering_includes_pk(&ord(&["id"]), &["id".to_string()]);
}

// The invariant violation: a PK field missing from the ordering panics with the
// TS-faithful message naming the missing field.
#[test]
#[should_panic(expected = "Missing: b")]
fn assert_ordering_includes_pk_panics_on_missing_pk() {
    assert_ordering_includes_pk(&ord(&["a"]), &["a".to_string(), "b".to_string()]);
}

// Port of TS interrupt tagging: only strings carrying the interrupt prefix are
// interrupts; ordinary SQL errors are not.
#[test]
fn is_interrupt_error_matches_only_prefixed_strings() {
    assert!(is_interrupt_error(&format!(
        "{INTERRUPT_ERR_PREFIX}probe aborted"
    )));
    assert!(!is_interrupt_error("no such table: foo"));
    assert!(!is_interrupt_error(""));
}
