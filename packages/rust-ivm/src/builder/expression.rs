//! Expression builder — port of `zql/src/query/expression.ts`.
//!
//! Provides `and`, `or`, `not`, `cmp` functions for building condition trees
//! programmatically, plus `simplifyCondition` and `flatten`.

use crate::builder::ast::{Condition, CorrelatedSubqueryCondition, SimpleCondition, ValuePosition};
use crate::ivm::data::Value;

/// Build an AND condition from multiple sub-conditions.
/// Port of TS `and` (expression.ts:146).
pub fn and(conditions: &[Condition]) -> Condition {
    let filtered: Vec<Condition> = conditions
        .iter()
        .filter(|c| !is_always_true(c))
        .cloned()
        .collect();

    if filtered.len() == 1 {
        return filtered.into_iter().next().unwrap();
    }

    if filtered.iter().any(is_always_false) {
        return FALSE();
    }

    Condition::And(filtered)
}

/// Build an OR condition from multiple sub-conditions.
/// Port of TS `or` (expression.ts:157).
pub fn or(conditions: &[Condition]) -> Condition {
    let filtered: Vec<Condition> = conditions
        .iter()
        .filter(|c| !is_always_false(c))
        .cloned()
        .collect();

    if filtered.len() == 1 {
        return filtered.into_iter().next().unwrap();
    }

    if filtered.iter().any(is_always_true) {
        return TRUE();
    }

    Condition::Or(filtered)
}

/// Negate a condition using De Morgan's laws.
/// Port of TS `not` (expression.ts:167).
pub fn not(expression: &Condition) -> Condition {
    match expression {
        Condition::And(conditions) => {
            Condition::Or(conditions.iter().map(not).collect())
        }
        Condition::Or(conditions) => {
            Condition::And(conditions.iter().map(not).collect())
        }
        Condition::CorrelatedSubquery(csq) => {
            Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
                related: csq.related.clone(),
                op: negate_operator(&csq.op),
                flip: csq.flip,
                scalar: csq.scalar,
                plan_id: None,
            })
        }
        Condition::Simple(simple) => {
            Condition::Simple(SimpleCondition {
                op: negate_operator(&simple.op),
                left: simple.left.clone(),
                right: simple.right.clone(),
            })
        }
    }
}

/// Build a simple comparison condition.
/// Port of TS `cmp` (expression.ts:213).
/// 2-arg form: cmp(field, value) → field = value
/// 3-arg form: cmp(field, op, value)
pub fn cmp(field: &str, op_or_value: &str, value: Option<&Value>) -> Condition {
    // When called with 3 args: cmp(field, op, value)
    let actual_value = value.cloned().unwrap_or(Value::Null);
    Condition::Simple(SimpleCondition {
        op: op_or_value.to_string(),
        left: ValuePosition::Column { name: field.to_string() },
        right: ValuePosition::Literal { value: actual_value },
    })
}

/// Build a comparison with `=` operator (2-arg shorthand).
pub fn cmp_eq(field: &str, value: Value) -> Condition {
    Condition::Simple(SimpleCondition {
        op: "=".to_string(),
        left: ValuePosition::Column { name: field.to_string() },
        right: ValuePosition::Literal { value },
    })
}

/// Simplify a condition tree: flatten nested AND/OR, collapse single-element
/// branches, detect always-true/false.
/// Port of TS `simplifyCondition` (expression.ts:266).
pub fn simplify_condition(c: &Condition) -> Condition {
    match c {
        Condition::Simple(_) | Condition::CorrelatedSubquery(_) => c.clone(),
        Condition::And(conditions) => {
            let simplified: Vec<Condition> = conditions.iter().map(simplify_condition).collect();
            if simplified.len() == 1 {
                return simplified.into_iter().next().unwrap();
            }
            let flattened = flatten(conditions.len(), &simplified);
            if flattened.iter().any(is_always_false) {
                return FALSE();
            }
            Condition::And(flattened)
        }
        Condition::Or(conditions) => {
            let simplified: Vec<Condition> = conditions.iter().map(simplify_condition).collect();
            if simplified.len() == 1 {
                return simplified.into_iter().next().unwrap();
            }
            let flattened = flatten(conditions.len(), &simplified);
            if flattened.iter().any(is_always_true) {
                return TRUE();
            }
            Condition::Or(flattened)
        }
    }
}

/// Flatten nested conditions of the same type.
/// Port of TS `flatten` (expression.ts:290).
fn flatten(_len: usize, conditions: &[Condition]) -> Vec<Condition> {
    let mut flattened: Vec<Condition> = Vec::new();
    for c in conditions {
        match c {
            Condition::And(inner) if conditions.iter().all(|x| matches!(x, Condition::And(_))) => {
                for ic in inner {
                    flattened.push(ic.clone());
                }
            }
            Condition::Or(inner) if conditions.iter().all(|x| matches!(x, Condition::Or(_))) => {
                for ic in inner {
                    flattened.push(ic.clone());
                }
            }
            _ => {
                flattened.push(c.clone());
            }
        }
    }
    flattened
}

/// Negate a simple operator (= → !=, < → >=, etc.).
/// Port of TS `negateOperator` (expression.ts:310).
pub fn negate_operator(op: &str) -> String {
    match op {
        "=" => "!=".to_string(),
        "!=" => "=".to_string(),
        "<" => ">=".to_string(),
        ">" => "<=".to_string(),
        ">=" => "<".to_string(),
        "<=" => ">".to_string(),
        "IN" => "NOT IN".to_string(),
        "NOT IN" => "IN".to_string(),
        "LIKE" => "NOT LIKE".to_string(),
        "NOT LIKE" => "LIKE".to_string(),
        "ILIKE" => "NOT ILIKE".to_string(),
        "NOT ILIKE" => "ILIKE".to_string(),
        "IS" => "IS NOT".to_string(),
        "IS NOT" => "IS".to_string(),
        "EXISTS" => "NOT EXISTS".to_string(),
        "NOT EXISTS" => "EXISTS".to_string(),
        _ => panic!("Unknown operator to negate: {}", op),
    }
}

/// Always-true condition: AND with no conditions.
pub fn TRUE() -> Condition {
    Condition::And(Vec::new())
}

/// Always-false condition: OR with no conditions.
pub fn FALSE() -> Condition {
    Condition::Or(Vec::new())
}

fn is_always_true(c: &Condition) -> bool {
    matches!(c, Condition::And(conditions) if conditions.is_empty())
}

fn is_always_false(c: &Condition) -> bool {
    matches!(c, Condition::Or(conditions) if conditions.is_empty())
}
