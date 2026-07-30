//! Constraint types — port of `zql/src/ivm/constraint.ts`.

use crate::builder::ast::{Condition, SimpleCondition, ValuePosition};
use crate::ivm::data::{Value, values_equal};

/// A constraint: column-name → value. Maps to TS `Constraint` (constraint.ts:5).
pub type Constraint = rustc_hash::FxHashMap<String, Value>;

/// A multi-row IN clause: a non-empty list of constraints all sharing the
/// same column shape. Sources treat it as `(col_a, col_b, ...) IN VALUES (...)`.
/// Maps to TS `MultiConstraint` (operator.ts:56).
pub type MultiConstraint = Vec<crate::ivm::constraint::Constraint>;

/// Check if a constraint matches a row — port of TS `constraintMatchesRow`.
pub fn constraint_matches_row(constraint: &Constraint, row: &crate::ivm::data::Row) -> bool {
    for (key, value) in constraint {
        let row_val = row.get(key).cloned().unwrap_or(Value::Null);
        if !values_equal(&row_val, value) {
            return false;
        }
    }
    true
}

/// Check if two constraints are compatible — port of TS `constraintsAreCompatible`.
/// Compatible if: no keys in common, OR shared keys have equal values.
pub fn constraints_are_compatible(left: &Constraint, right: &Constraint) -> bool {
    for (key, value) in left {
        if let Some(right_val) = right.get(key)
            && !values_equal(value, right_val)
        {
            return false;
        }
    }
    true
}

/// Check if a constraint matches a primary key — port of TS `constraintMatchesPrimaryKey`.
pub fn constraint_matches_primary_key(constraint: &Constraint, primary: &[String]) -> bool {
    let mut constraint_keys: Vec<&String> = constraint.keys().collect();
    if constraint_keys.len() != primary.len() {
        return false;
    }
    constraint_keys.sort();
    let mut sorted_primary: Vec<String> = primary.to_vec();
    sorted_primary.sort();
    for (ck, pk) in constraint_keys.iter().zip(sorted_primary.iter()) {
        if ck.as_str() != pk.as_str() {
            return false;
        }
    }
    true
}

/// Check if a row matches all multi-constraints.
/// Within one MultiConstraint, entries are OR'd (IN semantics).
/// Across the list, entries are AND'd.
/// Port of TS `RowMatchesMultiConstraints` (Go operator.go:49).
pub fn row_matches_multi_constraints(
    multis: &[MultiConstraint],
    row: &crate::ivm::data::Row,
) -> bool {
    for mc in multis {
        if mc.is_empty() {
            continue;
        }
        let any = mc.iter().any(|c| constraint_matches_row(c, row));
        if !any {
            return false;
        }
    }
    true
}

/// Check if a key matches a primary key — port of TS `keyMatchesPrimaryKey`.
pub fn key_matches_primary_key(key: impl IntoIterator<Item = String>, primary: &[String]) -> bool {
    let mut constraint_keys: Vec<String> = key.into_iter().collect();
    if constraint_keys.len() != primary.len() {
        return false;
    }
    constraint_keys.sort();
    let mut sorted_primary: Vec<String> = primary.to_vec();
    sorted_primary.sort();
    for (ck, pk) in constraint_keys.iter().zip(sorted_primary.iter()) {
        if ck != pk {
            return false;
        }
    }
    true
}

/// Pull top-level AND components out of a condition tree.
/// The resulting array of simple conditions matches a superset of values
/// that the original condition would match.
/// Port of TS `pullSimpleAndComponents` (constraint.ts:60).
pub fn pull_simple_and_components(condition: &Condition) -> Vec<SimpleCondition> {
    match condition {
        Condition::And(conditions) => conditions
            .iter()
            .flat_map(pull_simple_and_components)
            .collect(),
        Condition::Simple(simple) => vec![simple.clone()],
        Condition::Or(conditions) if conditions.len() == 1 => {
            pull_simple_and_components(&conditions[0])
        }
        _ => Vec::new(),
    }
}

/// Extract a column reference and value from a simple condition.
/// Port of TS `extractColumn` (constraint.ts:126).
fn extract_column(condition: &SimpleCondition) -> Option<(String, Value)> {
    match &condition.left {
        ValuePosition::Column { name } => match &condition.right {
            ValuePosition::Literal { value } => Some((name.clone(), value.clone())),
            _ => None,
        },
        _ => None,
    }
}

/// Check if filters constitute a primary key lookup.
/// If so, returns the constraint that would be used to look up the primary key.
/// Port of TS `primaryKeyConstraintFromFilters` (constraint.ts:94).
pub fn primary_key_constraint_from_filters(
    condition: Option<&Condition>,
    primary: &[String],
) -> Option<Constraint> {
    let condition = condition?;
    let conditions = pull_simple_and_components(condition);
    if conditions.is_empty() {
        return None;
    }

    let mut ret: Constraint = Constraint::default();
    for sub in &conditions {
        if sub.op == "="
            && let Some((name, value)) = extract_column(sub)
            && primary.contains(&name)
        {
            ret.insert(name, value);
        }
    }

    if ret.len() != primary.len() {
        return None;
    }

    Some(ret)
}

/// Check if two constraints are deeply equal.
/// Port of TS `constraintEquals` (constraint.ts:165).
pub fn constraint_equals(a: &Constraint, b: &Constraint) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for (key, val) in a {
        match b.get(key) {
            None => return false,
            Some(bval) => {
                if !values_equal(val, bval) {
                    return false;
                }
            }
        }
    }
    true
}
