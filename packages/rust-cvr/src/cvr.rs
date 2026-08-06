//! Port of the pure functions from `cvr.ts`:
//! - `mergeRefCounts` — the critical refCounts combinator
//! - `getInactiveQueries` — finds queries that are inactive for all clients
//! - `nextEvictionTime` — computes the next eviction deadline
//! - `newQueryRecord` — creates a Client or Custom query record
//! - `getMutationResultsQuery` — creates the internal mutation-results query
//! - `assertNotInternal` — runtime check for internal query IDs

use std::collections::BTreeMap;

use serde_json::Value;

use crate::ttl::{clamp_ttl, TTL};
use crate::types::*;

/// Merge existing refCounts with received refCounts, optionally removing
/// hashes in `remove_hashes` from the existing set.
///
/// Returns `None` if no positive refs remain (i.e. the row should be deleted).
///
/// This is a pure function — byte-identical behavior to the TS implementation.
/// Key properties:
/// - `merge_ref_counts(None, None, _) == None`
/// - `merge_ref_counts(x, None, None) == normalize(x)` (drops zeros)
/// - Hashes in `remove_hashes` are skipped from `existing` (index 0) only.
/// - Zero entries are dropped inline.
pub fn merge_ref_counts(
    existing: Option<&RefCounts>,
    received: Option<&RefCounts>,
    remove_hashes: Option<&std::collections::HashSet<String>>,
) -> Option<RefCounts> {
    let mut merged: RefCounts = BTreeMap::new();

    match existing {
        None => {
            if let Some(recv) = received {
                for (hash, count) in recv {
                    let val = *count;
                    if val != 0 {
                        merged.insert(hash.clone(), val);
                    }
                }
            }
        }
        Some(existing) => {
            // Index 0: existing (with remove_hashes filter)
            for (hash, count) in existing {
                if let Some(rh) = remove_hashes {
                    if rh.contains(hash) {
                        continue;
                    }
                }
                let val = merged.get(hash).copied().unwrap_or(0) + count;
                if val == 0 {
                    merged.remove(hash);
                } else {
                    merged.insert(hash.clone(), val);
                }
            }

            // Index 1: received (no filter)
            if let Some(recv) = received {
                for (hash, count) in recv {
                    let val = merged.get(hash).copied().unwrap_or(0) + count;
                    if val == 0 {
                        merged.remove(hash);
                    } else {
                        merged.insert(hash.clone(), val);
                    }
                }
            }
        }
    }

    // Return None if no positive refs remain.
    if merged.values().any(|&v| v > 0) {
        Some(merged)
    } else {
        None
    }
}

/// Create a new query record from a desired query spec.
/// Returns a Client or Custom query record (never Internal).
pub fn new_query_record(
    id: &str,
    ast: Option<&Value>,
    name: Option<&str>,
    args: Option<&[Value]>,
) -> QueryRecord {
    if let Some(ast) = ast {
        assert!(
            name.is_none() && args.is_none(),
            "Cannot provide name or args with ast"
        );
        QueryRecord::Client(ClientQueryRecord {
            base: BaseQueryRecord {
                id: id.to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
            },
            ast: ast.clone(),
            client_state: BTreeMap::new(),
            patch_version: None,
        })
    } else {
        let name = name.expect("Must provide name and args");
        let args = args.expect("Must provide name and args");
        QueryRecord::Custom(CustomQueryRecord {
            base: BaseQueryRecord {
                id: id.to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
            },
            name: name.to_string(),
            args: args.to_vec(),
            client_state: BTreeMap::new(),
            patch_version: None,
        })
    }
}

/// Create the internal mutation-results query for a client group.
pub fn get_mutation_results_query(
    upstream_schema: &str,
    client_group_id: &str,
) -> InternalQueryRecord {
    InternalQueryRecord {
        base: BaseQueryRecord {
            id: CLIENT_MUTATION_RESULTS_QUERY_ID.to_string(),
            transformation_hash: None,
            transformation_version: None,
            row_set_signature: None,
        },
        ast: serde_json::json!({
            "schema": "",
            "table": format!("{}.mutations", upstream_schema),
            "where": {
                "type": "and",
                "conditions": [{
                    "type": "simple",
                    "left": {"type": "column", "name": "clientGroupID"},
                    "op": "=",
                    "right": {"type": "literal", "value": client_group_id}
                }]
            },
            "orderBy": [
                ["clientGroupID", "asc"],
                ["clientID", "asc"],
                ["mutationID", "asc"]
            ]
        }),
    }
}

/// Assert that a query is not internal. Panics with the same message as TS.
pub fn assert_not_internal(query: &QueryRecord) {
    if let QueryRecord::Internal(r) = query {
        panic!(
            "Query ID {} is reserved for internal use",
            r.base.id
        );
    }
}

/// A query that is inactive for all clients, with its inactivation time and TTL.
#[derive(Debug, Clone, PartialEq)]
pub struct InactiveQuery {
    pub hash: String,
    pub inactivated_at: TTLClock,
    pub ttl: i64,
}

/// Find queries that are inactive for ALL clients in the CVR.
/// A query is inactive if every client's `inactivated_at` is set.
/// Returns the one with the furthest-future expiration per query.
///
/// Port of `getInactiveQueries` from cvr.ts.
pub fn get_inactive_queries(cvr: &CVR) -> Vec<InactiveQuery> {
    let mut inactive: BTreeMap<String, InactiveQuery> = BTreeMap::new();

    for (query_id, query) in &cvr.queries {
        if query.is_internal() {
            continue;
        }
        let client_state = match query.client_state() {
            Some(cs) => cs,
            None => continue,
        };

        for state in client_state.values() {
            let inactivated_at = match state.inactivated_at {
                Some(t) => t,
                None => {
                    // Query is still active for this client — not inactive.
                    inactive.remove(query_id);
                    break;
                }
            };

            let clamped_ttl = clamp_ttl(TTL::Ms(state.ttl));
            let existing = inactive.get(query_id);

            match existing {
                Some(existing) => {
                    let existing_ttl = clamp_ttl(TTL::Ms(existing.ttl));
                    // Use the last eviction time (furthest in the future).
                    if existing_ttl + existing.inactivated_at
                        < inactivated_at + clamped_ttl
                    {
                        inactive.insert(
                            query_id.clone(),
                            InactiveQuery {
                                hash: query_id.clone(),
                                inactivated_at,
                                ttl: clamped_ttl,
                            },
                        );
                    }
                }
                None => {
                    inactive.insert(
                        query_id.clone(),
                        InactiveQuery {
                            hash: query_id.clone(),
                            inactivated_at,
                            ttl: clamped_ttl,
                        },
                    );
                }
            }
        }
    }

    // Sort by eviction time (inactivated_at + ttl), oldest first.
    let mut result: Vec<InactiveQuery> = inactive.into_values().collect();
    result.sort_by(|a, b| {
        let a_expire = a.inactivated_at + a.ttl;
        let b_expire = b.inactivated_at + b.ttl;
        a_expire.cmp(&b_expire)
    });
    result
}

/// Compute the next eviction time for the CVR.
/// Returns the earliest (inactivated_at + ttl) across all inactive queries.
pub fn next_eviction_time(cvr: &CVR) -> Option<TTLClock> {
    let mut next: Option<i64> = None;
    for q in get_inactive_queries(cvr) {
        let expire = q.inactivated_at + q.ttl;
        if next.is_none() || expire < next.unwrap() {
            next = Some(expire);
        }
    }
    next
}

#[cfg(test)]
mod tests {
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
    fn test_merge_received_only_drops_zero() {
        let received = rc(&[("a", 0), ("b", 1)]);
        let result = merge_ref_counts(None, Some(&received), None);
        assert_eq!(result, Some(rc(&[("b", 1)])));
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
}
