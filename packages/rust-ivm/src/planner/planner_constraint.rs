//! Planner constraint — port of `planner-constraint.ts`.
//!
//! A constraint represents a column that will be constrained at runtime
//! (e.g., `issue.assignee_id`). We know the column name but not the value.

use std::collections::BTreeMap;

/// A planner constraint: column name → (value known at runtime, not now).
/// Uses `BTreeMap` for deterministic iteration order (tests rely on it).
pub type PlannerConstraint = BTreeMap<String, Option<crate::ivm::data::Value>>;

/// Merge two constraints (last-wins on key collision).
pub fn merge_constraints(
    a: Option<&PlannerConstraint>,
    b: Option<&PlannerConstraint>,
) -> Option<PlannerConstraint> {
    match (a, b) {
        (None, None) => None,
        (Some(x), None) => Some(x.clone()),
        (None, Some(y)) => Some(y.clone()),
        (Some(x), Some(y)) => {
            let mut merged = x.clone();
            for (k, v) in y {
                merged.insert(k.clone(), v.clone());
            }
            Some(merged)
        }
    }
}
