//! Planner terminus — port of `planner-terminus.ts`.

use crate::planner::node::{CostEstimate, JoinOrConnection, PlannerNode};

pub struct PlannerTerminus {
    input: PlannerNode,
}

impl PlannerTerminus {
    pub fn new(input: PlannerNode) -> Self {
        crate::live_count::inc(&crate::live_count::PLANNER_NODE);
        PlannerTerminus { input }
    }

    pub fn closest_join_or_source(&self) -> JoinOrConnection {
        self.input.closest_join_or_source()
    }

    pub fn propagate_constraints(&self) {
        self.input.propagate_constraints(&[], None, None);
    }

    pub fn estimate_cost(&self) -> CostEstimate {
        self.input.estimate_cost(1.0, &[])
    }

    pub fn propagate_unlimit_from_flipped_join(&self) {
        // No-op
    }
}

impl Drop for PlannerTerminus {
    fn drop(&mut self) {
        crate::live_count::dec(&crate::live_count::PLANNER_NODE);
    }
}
