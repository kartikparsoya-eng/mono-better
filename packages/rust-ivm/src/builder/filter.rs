//! Filter builder — port of `zql/src/builder/filter.ts`.
//!
//! `createPredicate` — builds `Fn(&Row) -> bool` from a Condition.
//! `transformFilters` — strips correlated subquery conditions.

use std::sync::Arc;

use crate::builder::ast::{Condition, SimpleCondition, ValuePosition};
use crate::builder::like::get_like_predicate;
use crate::ivm::data::{Value, compare_values};

/// A predicate function: row → bool.
pub type Predicate = Arc<dyn Fn(&crate::ivm::data::Row) -> bool>;

/// Create a predicate from a condition (no correlated subqueries).
/// Port of TS `createPredicate` (filter.ts:36).
pub fn create_predicate(condition: &Condition) -> Predicate {
    match condition {
        Condition::Simple(simple) => create_simple_predicate(simple),
        Condition::And(conditions) => {
            let preds: Vec<Predicate> = conditions.iter().map(create_predicate).collect();
            Arc::new(move |row| preds.iter().all(|p| p(row)))
        }
        Condition::Or(conditions) => {
            let preds: Vec<Predicate> = conditions.iter().map(create_predicate).collect();
            Arc::new(move |row| preds.iter().any(|p| p(row)))
        }
        Condition::CorrelatedSubquery(_) => {
            // CSQ conditions should be stripped by transform_filters before
            // reaching here. If one slips through, return a pass-all predicate
            // (the CSQ is handled separately by apply_correlated_subquery).
            Arc::new(|_| true)
        }
    }
}

/// Create a predicate from a simple condition.
/// Port of TS `createPredicateImpl` (filter.ts:96).
pub fn create_simple_predicate(simple: &SimpleCondition) -> Predicate {
    let op = simple.op.clone();

    match (&simple.left, &simple.right) {
        (ValuePosition::Column { name }, ValuePosition::Literal { value }) => {
            let col_name = name.clone();
            let rhs = value.clone();

            // Handle IS / IS NOT (null comparison)
            if op == "IS" || op == "IS NOT" {
                let is_not = op == "IS NOT";
                return Arc::new(move |row| {
                    let lhs = row.get(&col_name).unwrap_or(&Value::Null);
                    let result = *lhs == rhs;
                    if is_not { !result } else { result }
                });
            }

            // Null rhs → always false for non-IS operators
            if rhs.is_null() {
                return Arc::new(|_| false);
            }

            let pred = create_predicate_impl(&rhs, &op);
            Arc::new(move |row| {
                let lhs = row.get(&col_name).unwrap_or(&Value::Null);
                if lhs.is_null() {
                    return false;
                }
                pred(lhs)
            })
        }
        (
            ValuePosition::Literal { value: left_val },
            ValuePosition::Literal { value: right_val },
        ) => {
            // Literal = literal — evaluate at build time
            let result = match op.as_str() {
                "=" => left_val == right_val,
                "!=" => left_val != right_val,
                "<" => compare_values(left_val, right_val) == std::cmp::Ordering::Less,
                "<=" => compare_values(left_val, right_val) != std::cmp::Ordering::Greater,
                ">" => compare_values(left_val, right_val) == std::cmp::Ordering::Greater,
                ">=" => compare_values(left_val, right_val) != std::cmp::Ordering::Less,
                _ => false,
            };
            Arc::new(move |_| result)
        }
        _ => panic!("Only column = literal and literal = literal predicates supported"),
    }
}

/// Create a predicate implementation for a non-null rhs.
/// Port of TS `createPredicateImpl` (filter.ts:96).
fn create_predicate_impl(rhs: &Value, operator: &str) -> Box<dyn Fn(&Value) -> bool> {
    match operator {
        "=" => {
            let rhs = rhs.clone();
            Box::new(move |lhs| lhs == &rhs)
        }
        "!=" => {
            let rhs = rhs.clone();
            Box::new(move |lhs| lhs != &rhs)
        }
        "<" => {
            let rhs = rhs.clone();
            Box::new(move |lhs| compare_values(lhs, &rhs) == std::cmp::Ordering::Less)
        }
        "<=" => {
            let rhs = rhs.clone();
            Box::new(move |lhs| compare_values(lhs, &rhs) != std::cmp::Ordering::Greater)
        }
        ">" => {
            let rhs = rhs.clone();
            Box::new(move |lhs| compare_values(lhs, &rhs) == std::cmp::Ordering::Greater)
        }
        ">=" => {
            let rhs = rhs.clone();
            Box::new(move |lhs| compare_values(lhs, &rhs) != std::cmp::Ordering::Less)
        }
        "LIKE" => {
            let pred = get_like_predicate(rhs, "");
            Box::new(move |lhs| pred(lhs))
        }
        "NOT LIKE" => {
            let pred = get_like_predicate(rhs, "");
            Box::new(move |lhs| !pred(lhs))
        }
        "ILIKE" => {
            let pred = get_like_predicate(rhs, "i");
            Box::new(move |lhs| pred(lhs))
        }
        "NOT ILIKE" => {
            let pred = get_like_predicate(rhs, "i");
            Box::new(move |lhs| !pred(lhs))
        }
        "IN" => {
            let set = match rhs {
                Value::Json(s) => parse_json_array(s),
                _ => vec![rhs.clone()],
            };
            Box::new(move |lhs| set.iter().any(|v| v == lhs))
        }
        "NOT IN" => {
            let set = match rhs {
                Value::Json(s) => parse_json_array(s),
                _ => vec![rhs.clone()],
            };
            Box::new(move |lhs| !set.iter().any(|v| v == lhs))
        }
        _ => panic!("Unexpected operator: {}", operator),
    }
}

fn parse_json_array(s: &str) -> Vec<Value> {
    // Parse an IN clause literal: `[v1, v2, ...]` (or a single scalar).
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(serde_json::Value::Array(arr)) => arr.iter().map(json_to_value).collect(),
        Ok(other) => vec![json_to_value(&other)],
        Err(_) => vec![Value::Str(Arc::from(s))],
    }
}

fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => Value::F64(n.as_f64().unwrap_or(0.0)),
        serde_json::Value::String(s) => Value::Str(Arc::from(s.as_str())),
        other => Value::Json(Arc::from(other.to_string().as_str())),
    }
}

/// Result of transformFilters: a condition with correlated subqueries removed.
/// Port of TS `transformFilters` (filter.ts:170).
pub struct TransformedFilters {
    pub filters: Option<Condition>,
    pub conditions_removed: bool,
}

/// Strip correlated subquery conditions from a condition tree.
/// Returns a condition that matches a superset of the original.
pub fn transform_filters(filters: Option<&Condition>) -> TransformedFilters {
    match filters {
        None => TransformedFilters {
            filters: None,
            conditions_removed: false,
        },
        Some(Condition::Simple(s)) => TransformedFilters {
            filters: Some(Condition::Simple(s.clone())),
            conditions_removed: false,
        },
        Some(Condition::CorrelatedSubquery(_)) => TransformedFilters {
            filters: None,
            conditions_removed: true,
        },
        Some(Condition::And(conditions)) => {
            let mut transformed: Vec<Condition> = Vec::new();
            let mut removed = false;
            for cond in conditions {
                let t = transform_filters(Some(cond));
                if t.filters.is_none() {
                    removed = true;
                }
                removed = removed || t.conditions_removed;
                if let Some(f) = t.filters {
                    transformed.push(f);
                }
            }
            TransformedFilters {
                filters: if transformed.is_empty() {
                    None
                } else if transformed.len() == 1 {
                    Some(transformed.into_iter().next().unwrap())
                } else {
                    Some(Condition::And(transformed))
                },
                conditions_removed: removed,
            }
        }
        Some(Condition::Or(conditions)) => {
            let mut transformed: Vec<Condition> = Vec::new();
            let mut removed = false;
            for cond in conditions {
                let t = transform_filters(Some(cond));
                // If any OR branch is empty, the whole OR must be removed
                if t.filters.is_none() {
                    return TransformedFilters {
                        filters: None,
                        conditions_removed: true,
                    };
                }
                removed = removed || t.conditions_removed;
                if let Some(f) = t.filters {
                    transformed.push(f);
                }
            }
            TransformedFilters {
                // SECURITY: an empty OR is the deny-all `FALSE` sentinel (the
                // read-authorizer's deny-by-default, and any rule that simplifies
                // to FALSE). It MUST be preserved as `Some(Or([]))` — which the
                // source enforces as `WHERE FALSE` / an all-false predicate (0
                // rows) — NOT collapsed to `None` (no filter → EVERY row served,
                // a data leak). This mirrors TS `transformFilters`, which returns
                // `simplifyCondition({or, []})` = FALSE, not undefined. This arm's
                // empty case is reachable ONLY for an empty *input* OR: any OR
                // with branches either pushes a branch or early-returns above.
                filters: if transformed.is_empty() {
                    Some(Condition::Or(Vec::new()))
                } else if transformed.len() == 1 {
                    Some(transformed.into_iter().next().unwrap())
                } else {
                    Some(Condition::Or(transformed))
                },
                conditions_removed: removed,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::query_builder::condition_to_sql;

    fn simple_eq() -> Condition {
        Condition::Simple(SimpleCondition {
            op: "=".to_string(),
            left: ValuePosition::Column {
                name: "id".to_string(),
            },
            right: ValuePosition::Literal {
                value: Value::F64(1.0),
            },
        })
    }

    /// SECURITY REGRESSION (data-leak): the deny-all empty-OR (`FALSE` sentinel —
    /// the read-authorizer's deny-by-default and any rule simplifying to FALSE)
    /// must be PRESERVED as `Some(Or([]))`, never collapsed to `None`. `None`
    /// means "no source filter" → every row served. This test pins that the
    /// sentinel survives the transform AND that the source enforces it as a
    /// zero-row filter (SQL `FALSE`, all-false in-memory predicate).
    #[test]
    fn empty_or_preserved_as_deny_all_false() {
        let empty_or = Condition::Or(Vec::new());
        let t = transform_filters(Some(&empty_or));
        match &t.filters {
            Some(Condition::Or(v)) => assert!(v.is_empty(), "must stay an empty OR (FALSE)"),
            other => panic!("empty OR must be preserved as Some(Or([])), got {other:?}"),
        }
        // SQL path: WHERE FALSE → 0 rows.
        let (sql, params) = condition_to_sql(t.filters.as_ref().unwrap());
        assert_eq!(sql, "FALSE");
        assert!(params.is_empty());
    }

    /// Guard against over-triggering: a real OR with a branch must NOT become
    /// FALSE — a single-branch OR collapses to that branch, unchanged.
    #[test]
    fn non_empty_or_not_turned_into_false() {
        let or = Condition::Or(vec![simple_eq()]);
        let t = transform_filters(Some(&or));
        assert!(
            matches!(t.filters, Some(Condition::Simple(_))),
            "a single-branch OR must collapse to the branch, not FALSE"
        );
    }

    /// Empty AND is TRUE (match-all); `None` is the behaviorally-equivalent
    /// "no filter", so leaving it None is correct (allow-all, not a leak).
    #[test]
    fn empty_and_allows_all() {
        let empty_and = Condition::And(Vec::new());
        let t = transform_filters(Some(&empty_and));
        assert!(
            t.filters.is_none(),
            "empty AND == TRUE == no filter (match-all); staying None is correct"
        );
    }
}
