//! Planner fan-out — port of `planner-fan-out.ts`.

use crate::planner::constraint::PlannerConstraint;
use crate::planner::node::{
    CostEstimate, FanOutType, JoinOrConnection, PlannerNode, PlannerNodeWeak,
};

pub struct PlannerFanOut {
    node_type: FanOutType,
    /// Upward back-edges — WEAK so the graph stays acyclic (see
    /// `PlannerNodeWeak`); TS holds these strong and lets GC break the cycle.
    outputs: Vec<PlannerNodeWeak>,
    input: PlannerNode,
}

impl PlannerFanOut {
    pub fn new(input: PlannerNode) -> Self {
        crate::live_count::inc(&crate::live_count::PLANNER_NODE);
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
        self.outputs.push(node.downgrade());
    }

    /// Upgraded outputs. Only read during planning (FO→FI BFS), while the
    /// graph holds every node strong — dead entries (impossible there) are
    /// skipped.
    pub fn outputs(&self) -> Vec<PlannerNode> {
        self.outputs.iter().filter_map(|w| w.upgrade()).collect()
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

impl Drop for PlannerFanOut {
    fn drop(&mut self) {
        crate::live_count::dec(&crate::live_count::PLANNER_NODE);
    }
}
