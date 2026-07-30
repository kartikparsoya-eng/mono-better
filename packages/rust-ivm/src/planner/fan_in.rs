//! Planner fan-in — port of `planner-fan-in.ts`.

use crate::planner::constraint::PlannerConstraint;
use crate::planner::node::{CostEstimate, FanInType, JoinOrConnection, PlannerNode};

pub struct PlannerFanIn {
    node_type: FanInType,
    output: Option<PlannerNode>,
    inputs: Vec<PlannerNode>,
}

impl PlannerFanIn {
    pub fn new(inputs: Vec<PlannerNode>) -> Self {
        PlannerFanIn {
            node_type: FanInType::FI,
            output: None,
            inputs,
        }
    }

    pub fn node_type(&self) -> FanInType {
        self.node_type
    }

    pub fn closest_join_or_source(&self) -> JoinOrConnection {
        JoinOrConnection::Join
    }

    pub fn set_output(&mut self, node: PlannerNode) {
        self.output = Some(node);
    }

    pub fn reset(&mut self) {
        self.node_type = FanInType::FI;
    }

    pub fn convert_to_ufi(&mut self) {
        self.node_type = FanInType::UFI;
    }

    pub fn propagate_unlimit_from_flipped_join(&self) {
        for input in &self.inputs {
            input.propagate_unlimit_from_flipped_join();
        }
    }

    pub fn estimate_cost(
        &self,
        downstream_child_selectivity: f64,
        branch_pattern: &[usize],
    ) -> CostEstimate {
        let mut total = CostEstimate::default();

        match self.node_type {
            FanInType::FI => {
                let mut updated = vec![0];
                updated.extend_from_slice(branch_pattern);
                let mut max_rows = 0.0_f64;
                let mut max_running = 0.0_f64;
                let mut max_startup = 0.0_f64;
                let mut max_scan = 0.0_f64;
                let mut no_match_prob = 1.0_f64;

                for input in &self.inputs {
                    let cost = input.estimate_cost(downstream_child_selectivity, &updated);
                    total.fanout = cost.fanout.clone();
                    max_rows = max_rows.max(cost.returned_rows);
                    max_running = max_running.max(cost.cost);
                    max_startup = max_startup.max(cost.startup_cost);
                    max_scan = max_scan.max(cost.scan_est);
                    no_match_prob *= 1.0 - cost.selectivity;
                    total.limit = cost.limit;
                }
                total.returned_rows = max_rows;
                total.cost = max_running;
                total.selectivity = 1.0 - no_match_prob;
                total.startup_cost = max_startup;
                total.scan_est = max_scan;
            }
            FanInType::UFI => {
                let mut no_match_prob = 1.0_f64;
                for (i, input) in self.inputs.iter().enumerate() {
                    let mut updated = vec![i];
                    updated.extend_from_slice(branch_pattern);
                    let cost = input.estimate_cost(downstream_child_selectivity, &updated);
                    total.fanout = cost.fanout.clone();
                    total.returned_rows += cost.returned_rows;
                    total.cost += cost.cost;
                    total.scan_est += cost.scan_est;
                    total.startup_cost += cost.startup_cost;
                    no_match_prob *= 1.0 - cost.selectivity;
                    total.limit = cost.limit;
                }
                total.selectivity = 1.0 - no_match_prob;
            }
        }
        total
    }

    pub fn propagate_constraints(
        &mut self,
        branch_pattern: &[usize],
        constraint: Option<&PlannerConstraint>,
        _from: Option<&PlannerNode>,
    ) {
        match self.node_type {
            FanInType::FI => {
                let mut updated = vec![0];
                updated.extend_from_slice(branch_pattern);
                for input in &self.inputs {
                    input.propagate_constraints(&updated, constraint, None);
                }
            }
            FanInType::UFI => {
                for (i, input) in self.inputs.iter().enumerate() {
                    let mut updated = vec![i];
                    updated.extend_from_slice(branch_pattern);
                    input.propagate_constraints(&updated, constraint, None);
                }
            }
        }
    }
}
