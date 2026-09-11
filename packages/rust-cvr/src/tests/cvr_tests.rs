//! Unit tests for `cvr.rs`.
//!
//! Kept out of line so the production file stays reviewable. Declared with
//! `#[path]` from `cvr.rs` under `#[cfg(test)]`, so `use super::*` sees
//! the same private items an inline `mod tests` would.

use super::*;
use std::collections::HashSet;

fn rc(pairs: &[(&str, i64)]) -> RefCounts {
    pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

#[test]
fn test_merge_both_none() {
    assert_eq!(merge_ref_counts(None, None, None), None);
}

#[test]
fn test_merge_existing_only() {
    let existing = rc(&[("a", 1), ("b", 2)]);
    let result = merge_ref_counts(Some(&existing), None, None);
    assert_eq!(result, Some(rc(&[("a", 1), ("b", 2)])));
}

#[test]
fn test_merge_received_only() {
    let received = rc(&[("a", 1), ("b", 2)]);
    let result = merge_ref_counts(None, Some(&received), None);
    assert_eq!(result, Some(rc(&[("a", 1), ("b", 2)])));
}

#[test]
fn test_merge_adds_counts() {
    let existing = rc(&[("a", 1), ("b", 2)]);
    let received = rc(&[("a", 1), ("c", 3)]);
    let result = merge_ref_counts(Some(&existing), Some(&received), None);
    assert_eq!(result, Some(rc(&[("a", 2), ("b", 2), ("c", 3)])));
}

#[test]
fn test_merge_drops_zeros() {
    let existing = rc(&[("a", 1), ("b", 2)]);
    let received = rc(&[("a", -1)]);
    let result = merge_ref_counts(Some(&existing), Some(&received), None);
    assert_eq!(result, Some(rc(&[("b", 2)])));
}

#[test]
fn test_merge_all_zero_returns_none() {
    let existing = rc(&[("a", 1)]);
    let received = rc(&[("a", -1)]);
    let result = merge_ref_counts(Some(&existing), Some(&received), None);
    assert_eq!(result, None);
}

#[test]
fn test_merge_remove_hashes() {
    let existing = rc(&[("a", 1), ("b", 2), ("c", 3)]);
    let received = rc(&[("d", 4)]);
    let mut remove = HashSet::new();
    remove.insert("a".to_string());
    remove.insert("c".to_string());
    let result = merge_ref_counts(Some(&existing), Some(&received), Some(&remove));
    // a and c removed from existing, b kept, d added
    assert_eq!(result, Some(rc(&[("b", 2), ("d", 4)])));
}

#[test]
fn test_merge_negative_in_received() {
    let existing = rc(&[("a", 2), ("b", 1)]);
    let received = rc(&[("a", -1), ("b", -1)]);
    let result = merge_ref_counts(Some(&existing), Some(&received), None);
    assert_eq!(result, Some(rc(&[("a", 1)])));
}

#[test]
fn test_merge_all_negative_returns_none() {
    let existing = rc(&[("a", 1)]);
    let received = rc(&[("a", -2)]);
    let result = merge_ref_counts(Some(&existing), Some(&received), None);
    // a = 1 + (-2) = -1, which is not > 0, so None
    assert_eq!(result, None);
}

#[test]
fn test_merge_received_only_with_negative() {
    let received = rc(&[("a", -1)]);
    let result = merge_ref_counts(None, Some(&received), None);
    // -1 is not > 0, so None
    assert_eq!(result, None);
}

#[test]
fn test_merge_received_only_retains_zero() {
    // TS `mergeRefCounts(null, received)` does `merged = received ?? {}` —
    // a raw copy that RETAINS zero entries (verified by the TS golden
    // fixture in parity_check). The result is non-null because at least one
    // count is positive.
    let received = rc(&[("a", 0), ("b", 1)]);
    let result = merge_ref_counts(None, Some(&received), None);
    assert_eq!(result, Some(rc(&[("a", 0), ("b", 1)])));
}

#[test]
fn test_merge_remove_hashes_from_existing_only() {
    let existing = rc(&[("a", 1)]);
    let received = rc(&[("a", 1)]);
    let mut remove = HashSet::new();
    remove.insert("a".to_string());
    // remove_hashes skips "a" from existing, but received "a" is still added
    let result = merge_ref_counts(Some(&existing), Some(&received), Some(&remove));
    assert_eq!(result, Some(rc(&[("a", 1)])));
}

#[test]
fn test_merge_empty_existing() {
    let existing = rc(&[]);
    let received = rc(&[("a", 1)]);
    let result = merge_ref_counts(Some(&existing), Some(&received), None);
    assert_eq!(result, Some(rc(&[("a", 1)])));
}

#[test]
fn test_merge_empty_received() {
    let existing = rc(&[("a", 1)]);
    let received = rc(&[]);
    let result = merge_ref_counts(Some(&existing), Some(&received), None);
    assert_eq!(result, Some(rc(&[("a", 1)])));
}

// Property-style tests for merge_ref_counts
#[test]
fn test_prop_merge_idempotent_received_none() {
    // merge(x, None, None) == normalize(x)
    let x = rc(&[("a", 1), ("b", 0), ("c", 3), ("d", -1)]);
    let result = merge_ref_counts(Some(&x), None, None);
    // "b" (0) dropped, "d" (-1) not > 0 so dropped
    // But wait: in the existing path, -1 is added to merged.
    // merged["d"] = 0 + (-1) = -1, which != 0 so not deleted.
    // Then at the end, values().any(|v| v > 0) is true (a=1, c=3).
    // So result includes d=-1.
    // Actually TS: "merged[hash] = (merged[hash] ?? 0) + count; if (merged[hash] === 0) delete merged[hash];"
    // So -1 stays in merged. Then the final check is ".some(v => v > 0)".
    // So d=-1 is in the result but doesn't cause None.
    assert_eq!(result, Some(rc(&[("a", 1), ("c", 3), ("d", -1)])));
}

#[test]
fn test_prop_merge_no_positive_returns_none() {
    let x = rc(&[("a", -1), ("b", -2)]);
    let result = merge_ref_counts(Some(&x), None, None);
    assert_eq!(result, None);
}

#[test]
fn test_new_query_record_client() {
    let ast = serde_json::json!({"schema": "s", "table": "t"});
    let q = new_query_record("hash1", Some(&ast), None, None);
    match q {
        QueryRecord::Client(r) => {
            assert_eq!(r.base.id, "hash1");
            assert_eq!(r.ast, ast);
            assert!(r.client_state.is_empty());
            assert!(r.patch_version.is_none());
        }
        _ => panic!("expected Client query"),
    }
}

#[test]
fn test_new_query_record_custom() {
    let args = vec![serde_json::json!(1), serde_json::json!("x")];
    let q = new_query_record("hash1", None, Some("myQuery"), Some(&args));
    match q {
        QueryRecord::Custom(r) => {
            assert_eq!(r.base.id, "hash1");
            assert_eq!(r.name, "myQuery");
            assert_eq!(r.args, args);
        }
        _ => panic!("expected Custom query"),
    }
}

#[test]
#[should_panic(expected = "Cannot provide name or args with ast")]
fn test_new_query_record_ast_and_name_panics() {
    let ast = serde_json::json!({});
    new_query_record("h", Some(&ast), Some("n"), None);
}

#[test]
fn test_assert_not_internal_client() {
    let q = new_query_record("h", Some(&serde_json::json!({})), None, None);
    assert_not_internal(&q); // should not panic
}

#[test]
#[should_panic(expected = "reserved for internal use")]
fn test_assert_not_internal_panics() {
    let q = QueryRecord::Internal(InternalQueryRecord {
        base: BaseQueryRecord {
            id: "lmids".to_string(),
            transformation_hash: None,
            transformation_version: None,
            row_set_signature: None,
        },
        ast: serde_json::json!({}),
    });
    assert_not_internal(&q);
}

// ④ Property invariant for merge_ref_counts.
use proptest::prelude::*;

proptest! {
    // The null rule (TS: `... .some(v => v > 0) ? merged : null`): a non-null
    // merge result always contains at least one positive count. Regression
    // guard for the no-existing zero-retention fix.
    #[test]
    fn prop_merge_some_has_positive(
        ex in proptest::option::of(proptest::collection::btree_map("[a-c]", -3i64..3, 0..4)),
        rv in proptest::option::of(proptest::collection::btree_map("[a-c]", -3i64..3, 0..4)),
    ) {
        if let Some(m) = merge_ref_counts(ex.as_ref(), rv.as_ref(), None) {
            prop_assert!(m.values().any(|&v| v > 0));
        }
    }

    // mergeRefCounts algebra. TS (cvr.ts `mergeRefCounts`) has TWO branches
    // with DIFFERENT semantics, so the invariants are branch-specific:
    //
    //  • existing = Some: per-hash integer ADDITION with the remove-filter on
    //    the existing side only, zeros stripped from the map. So for every
    //    hash, merged[h] (absent = 0) == filtered_existing[h] + received[h],
    //    and every retained value is nonzero.
    //  • existing = None: `merged = received ?? {}` — a raw copy that RETAINS
    //    zero entries and ignores remove_hashes. So a Some result equals
    //    `received` verbatim (this pins the documented zero-retention
    //    asymmetry — a prior real bug dropped those zeros).
    //
    // Both branches share the null rule: Some iff some count is > 0.
    #[test]
    fn prop_merge_ref_counts_algebra(
        ex in proptest::option::of(proptest::collection::btree_map("[a-e]", -3i64..4, 0..5)),
        rv in proptest::option::of(proptest::collection::btree_map("[a-e]", -3i64..4, 0..5)),
        rh in proptest::collection::hash_set("[a-e]", 0..3),
    ) {
        let rh = if rh.is_empty() { None } else { Some(rh) };
        let out = merge_ref_counts(ex.as_ref(), rv.as_ref(), rh.as_ref());

        match ex.as_ref() {
            Some(_) => {
                if let Some(m) = out.as_ref() {
                    // zeros are stripped in the existing=Some branch
                    for &v in m.values() {
                        prop_assert_ne!(v, 0);
                    }
                    // additive law over the union of hashes (remove-filter on existing)
                    let mut hashes: std::collections::BTreeSet<String> = Default::default();
                    if let Some(e) = ex.as_ref() { hashes.extend(e.keys().cloned()); }
                    if let Some(r) = rv.as_ref() { hashes.extend(r.keys().cloned()); }
                    for h in hashes {
                        let removed = rh.as_ref().is_some_and(|s| s.contains(&h));
                        let e = if removed {
                            0
                        } else {
                            ex.as_ref().and_then(|m| m.get(&h)).copied().unwrap_or(0)
                        };
                        let r = rv.as_ref().and_then(|m| m.get(&h)).copied().unwrap_or(0);
                        prop_assert_eq!(m.get(&h).copied().unwrap_or(0), e + r);
                    }
                }
            }
            None => {
                // existing=None: Some(received-verbatim, zeros kept) iff any positive.
                match rv.as_ref() {
                    Some(r) if r.values().any(|&v| v > 0) => {
                        prop_assert_eq!(out.as_ref(), Some(r));
                    }
                    _ => prop_assert!(out.is_none()),
                }
            }
        }
    }
}
