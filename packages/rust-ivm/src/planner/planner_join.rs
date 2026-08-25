//! Planner join — port of `planner-join.ts`.

use crate::planner::planner_constraint::{PlannerConstraint, merge_constraints};
use crate::planner::planner_node::{
    CostEstimate, JoinOrConnection, JoinType, PlannerNode, PlannerNodeWeak,
};

fn translate_constraints_for_flipped_join(
    incoming: Option<&PlannerConstraint>,
    parent_constraint: &PlannerConstraint,
    child_constraint: &PlannerConstraint,
) -> Option<PlannerConstraint> {
    let incoming = incoming?;
    let parent_keys: Vec<&String> = parent_constraint.keys().collect();
    let child_keys: Vec<&String> = child_constraint.keys().collect();
    let mut translated = PlannerConstraint::new();
    for (key, value) in incoming {
        if let Some(index) = parent_keys.iter().position(|k| *k == key)
            && let Some(child_key) = child_keys.get(index)
        {
            translated.insert(child_key.to_string(), value.clone());
        }
    }
    if translated.is_empty() {
        None
    } else {
        Some(translated)
    }
}

// Port of TS planner-join.ts:2 — the planner imports the RUNTIME's
// `getMultiConstraintChunkSize` from ivm/flipped-join.ts (256). A local shadow
// here previously returned 500, underestimating flipped-join chunk startup
// cost vs TS and skewing plan choice (NEW-1).
use crate::ivm::flipped_join::get_multi_constraint_chunk_size;

pub struct PlannerJoin {
    parent: PlannerNode,
    child: PlannerNode,
    parent_constraint: PlannerConstraint,
    child_constraint: PlannerConstraint,
    flippable: bool,
    pub plan_id: usize,
    /// Upward back-edge to the consumer — WEAK so the graph stays acyclic
    /// (see `PlannerNodeWeak`); TS holds this strong and lets GC break the
    /// cycle.
    output: Option<PlannerNodeWeak>,
    join_type: JoinType,
    initial_type: JoinType,
}

impl PlannerJoin {
    pub fn new(
        parent: PlannerNode,
        child: PlannerNode,
        parent_constraint: PlannerConstraint,
        child_constraint: PlannerConstraint,
        flippable: bool,
        plan_id: usize,
        initial_type: JoinType,
    ) -> Self {
        crate::live_count::inc(&crate::live_count::PLANNER_NODE);
        PlannerJoin {
            parent,
            child,
            parent_constraint,
            child_constraint,
            flippable,
            plan_id,
            output: None,
            join_type: initial_type,
            initial_type,
        }
    }

    pub fn set_output(&mut self, node: PlannerNode) {
        self.output = Some(node.downgrade());
    }

    pub fn closest_join_or_source(&self) -> JoinOrConnection {
        JoinOrConnection::Join
    }

    pub fn flip(&mut self) {
        assert!(self.join_type == JoinType::Semi);
        assert!(self.flippable);
        self.join_type = JoinType::Flipped;
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type
    }
    pub fn is_flippable(&self) -> bool {
        self.flippable
    }

    pub fn propagate_unlimit(&mut self) {
        self.child.propagate_unlimit_from_flipped_join();
    }

    pub fn propagate_unlimit_from_flipped_join(&mut self) {
        self.parent.propagate_unlimit_from_flipped_join();
    }

    pub fn propagate_constraints(
        &mut self,
        branch_pattern: &[usize],
        constraint: Option<&PlannerConstraint>,
        _from: Option<&PlannerNode>,
    ) {
        match self.join_type {
            JoinType::Semi => {
                self.child.propagate_constraints(
                    branch_pattern,
                    Some(&self.child_constraint),
                    None,
                );
                self.parent
                    .propagate_constraints(branch_pattern, constraint, None);
            }
            JoinType::Flipped => {
                let translated = translate_constraints_for_flipped_join(
                    constraint,
                    &self.parent_constraint,
                    &self.child_constraint,
                );
                self.child
                    .propagate_constraints(branch_pattern, translated.as_ref(), None);
                let merged = merge_constraints(constraint, Some(&self.parent_constraint));
                self.parent
                    .propagate_constraints(branch_pattern, merged.as_ref(), None);
            }
        }
    }

    pub fn estimate_cost(
        &self,
        downstream_child_selectivity: f64,
        branch_pattern: &[usize],
    ) -> CostEstimate {
        let child = self.child.estimate_cost(1.0, branch_pattern);
        let child_keys: Vec<String> = self.child_constraint.keys().cloned().collect();
        let fanout_factor = (child.fanout)(&child_keys);
        let scaled_child_selectivity = 1.0 - (1.0 - child.selectivity).powf(fanout_factor.fanout);

        let parent_dcs = match self.join_type {
            JoinType::Flipped => 1.0 * downstream_child_selectivity,
            JoinType::Semi => scaled_child_selectivity * downstream_child_selectivity,
        };
        let parent = self.parent.estimate_cost(parent_dcs, branch_pattern);

        match self.join_type {
            JoinType::Semi => {
                let scan_est = match parent.limit {
                    None => parent.returned_rows,
                    Some(lim) => {
                        if downstream_child_selectivity == 0.0 {
                            0.0
                        } else {
                            parent.returned_rows.min(lim / downstream_child_selectivity)
                        }
                    }
                };
                CostEstimate {
                    startup_cost: parent.startup_cost,
                    scan_est,
                    cost: parent.cost
                        + parent.scan_est * (child.startup_cost + child.cost + child.scan_est),
                    returned_rows: parent.returned_rows * child.selectivity,
                    selectivity: child.selectivity * parent.selectivity,
                    limit: parent.limit,
                    fanout: parent.fanout,
                }
            }
            JoinType::Flipped => {
                let scan_est = match parent.limit {
                    None => parent.returned_rows * child.returned_rows,
                    Some(lim) => {
                        if downstream_child_selectivity == 0.0 {
                            0.0
                        } else {
                            (parent.returned_rows * child.returned_rows)
                                .min(lim / downstream_child_selectivity)
                        }
                    }
                };
                let chunks = (child.scan_est / get_multi_constraint_chunk_size() as f64).ceil();
                CostEstimate {
                    startup_cost: child.startup_cost,
                    scan_est,
                    cost: child.cost
                        + chunks * parent.startup_cost
                        + child.scan_est * (parent.cost + parent.scan_est),
                    returned_rows: parent.returned_rows * child.returned_rows,
                    selectivity: parent.selectivity * child.selectivity,
                    limit: parent.limit,
                    fanout: parent.fanout,
                }
            }
        }
    }

    pub fn get_output(&self) -> Option<PlannerNode> {
        self.output.as_ref().and_then(|w| w.upgrade())
    }

    pub fn reset(&mut self) {
        self.join_type = self.initial_type;
    }

    pub fn get_name(&self) -> String {
        format!("{} ⋈ {}", self.parent.name(), self.child.name())
    }
}

impl Drop for PlannerJoin {
    fn drop(&mut self) {
        crate::live_count::dec(&crate::live_count::PLANNER_NODE);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// NEW-1: the planner must use the RUNTIME's chunk size (TS
    /// planner-join.ts:2 imports `getMultiConstraintChunkSize` from
    /// ivm/flipped-join.ts, value 256). A local shadow returning 500
    /// underestimated flipped-join chunk startup cost and skewed plan choice.
    /// Pre-fix the first assert failed (500 != 256) — proven by temp-revert.
    #[test]
    fn chunk_size_is_the_runtime_value() {
        assert_eq!(
            get_multi_constraint_chunk_size(),
            256,
            "TS MULTI_CONSTRAINT_CHUNK_SIZE (flipped-join.ts:55)"
        );
        // The planner's symbol must BE the ivm one: the test override must be
        // visible through it.
        let restore = crate::ivm::flipped_join::set_multi_constraint_chunk_size_for_test(7);
        assert_eq!(
            get_multi_constraint_chunk_size(),
            7,
            "planner must read the runtime chunk size, not a local copy"
        );
        restore();
    }

    /// NEW-2: `translateConstraintsForFlippedJoin` pairs
    /// `parentKeys[i] ↔ childKeys[i]` POSITIONALLY in Record insertion order
    /// (TS planner-join.ts:34-44; the order `extractConstraint` inserted =
    /// the correlation-array order). With a BTreeMap the keys re-sorted
    /// alphabetically, so a multi-column correlation whose sides sort
    /// differently paired the wrong columns: parent ['b','a'] ↔ child
    /// ['x','y'] must map b→x, but sorted ['a','b'] mapped b→y. Proven by
    /// temp-revert (PlannerConstraint as BTreeMap fails the b→x assert).
    #[test]
    fn translate_constraints_pairs_by_insertion_order() {
        let mut parent = PlannerConstraint::new();
        parent.insert("b".to_string(), None);
        parent.insert("a".to_string(), None);
        let mut child = PlannerConstraint::new();
        child.insert("x".to_string(), None);
        child.insert("y".to_string(), None);

        let mut incoming = PlannerConstraint::new();
        incoming.insert("b".to_string(), None);
        let translated = translate_constraints_for_flipped_join(Some(&incoming), &parent, &child)
            .expect("translated");
        assert!(
            translated.contains_key("x") && !translated.contains_key("y"),
            "b is parentKeys[0] (insertion order) and must map to childKeys[0] = x; got {:?}",
            translated.keys().collect::<Vec<_>>()
        );

        let mut incoming_a = PlannerConstraint::new();
        incoming_a.insert("a".to_string(), None);
        let translated_a =
            translate_constraints_for_flipped_join(Some(&incoming_a), &parent, &child)
                .expect("translated");
        assert!(
            translated_a.contains_key("y"),
            "a is parentKeys[1] and must map to childKeys[1] = y"
        );
    }
}
