//! Planner fan-out — port of `planner-fan-out.ts`.

use crate::planner::constraint::PlannerConstraint;
use crate::planner::node::{CostEstimate, FanOutType, JoinOrConnection, PlannerNode};

pub struct PlannerFanOut {
    node_type: FanOutType,
    outputs: Vec<PlannerNode>,
    input: PlannerNode,
}

impl PlannerFanOut {
    pub fn new(input: PlannerNode) -> Self {
        PlannerFanOut {
            node_type: FanOutType::FO,
            outputs: Vec::new(),
            input,
        }
    }

    pub fn node_type(&self) -> FanOutType {
        self.node_type
    }

    pub fn add_output(&mut self, node: PlannerNode) {
        self.outputs.push(node);
    }

    pub fn outputs(&self) -> &[PlannerNode] {
        &self.outputs
    }

    /// Drop the upward `outputs` back-edges to break the graph's Rc cycle at
    /// teardown (see `impl Drop for PlannerGraph`).
    pub fn clear_outputs(&mut self) {
        self.outputs.clear();
    }

    pub fn closest_join_or_source(&self) -> JoinOrConnection {
        self.input.closest_join_or_source()
    }

    pub fn propagate_constraints(
        &self,
        branch_pattern: &[usize],
        constraint: Option<&PlannerConstraint>,
        _from: Option<&PlannerNode>,
    ) {
        self.input
            .propagate_constraints(branch_pattern, constraint, None);
    }

    pub fn estimate_cost(
        &self,
        downstream_child_selectivity: f64,
        branch_pattern: &[usize],
    ) -> CostEstimate {
        self.input
            .estimate_cost(downstream_child_selectivity, branch_pattern)
    }

    pub fn convert_to_ufo(&mut self) {
        self.node_type = FanOutType::UFO;
    }

    pub fn reset(&mut self) {
        self.node_type = FanOutType::FO;
    }

    pub fn propagate_unlimit_from_flipped_join(&self) {
        self.input.propagate_unlimit_from_flipped_join();
    }
}
