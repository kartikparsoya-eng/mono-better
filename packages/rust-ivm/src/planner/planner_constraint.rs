//! Planner constraint — port of `planner-constraint.ts`.
//!
//! A constraint represents a column that will be constrained at runtime
//! (e.g., `issue.assignee_id`). We know the column name but not the value.

/// A planner constraint: column name → (value known at runtime, not now).
///
/// TS `PlannerConstraint` is a plain `Record`, and the planner RELIES on its
/// insertion-order key iteration: `translateConstraintsForFlippedJoin`
/// (planner-join.ts:34-44) pairs `parentKeys[i] ↔ childKeys[i]` positionally,
/// where the positions are the correlation-array order `extractConstraint`
/// inserted (planner-builder.ts:297). A `BTreeMap` here re-sorted the keys
/// alphabetically, so multi-column correlations whose two sides sort
/// differently paired the WRONG columns (NEW-2) — hence `IndexMap`, the
/// insertion-ordered twin of a JS object.
pub type PlannerConstraint = indexmap::IndexMap<String, Option<crate::ivm::data::Value>>;

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
