//! Planner connection — port of `planner-connection.ts`.

use std::collections::HashMap;
use std::rc::Rc;

use crate::builder::ast::Condition;
use crate::planner::constraint::{PlannerConstraint, merge_constraints};
use crate::planner::node::{CostEstimate, FanoutCostModel, JoinOrConnection};

/// Cost model output for a connection.
#[derive(Clone)]
pub struct CostModelCost {
    pub startup_cost: f64,
    pub rows: f64,
    pub fanout: FanoutCostModel,
}

pub type ConnectionCostModel = Rc<
    dyn Fn(
        &str,
        &[(String, String)],
        Option<&Condition>,
        Option<&PlannerConstraint>,
    ) -> CostModelCost,
>;

pub struct PlannerConnection {
    // Immutable
    sort: Vec<(String, String)>,
    filters: Option<Condition>,
    model: ConnectionCostModel,
    pub table: String,
    pub name: String,
    base_constraints: Option<PlannerConstraint>,
    base_limit: Option<usize>,
    pub selectivity: f64,
    /// Upward back-edge — WEAK so the graph stays acyclic (see
    /// `PlannerNodeWeak`); TS holds this strong and lets GC break the cycle.
    output: Option<crate::planner::node::PlannerNodeWeak>,

    // Mutable
    pub limit: Option<usize>,
    constraints: HashMap<String, Option<PlannerConstraint>>,
    cached_costs: HashMap<String, CostEstimate>,
    is_root: bool,
}

impl PlannerConnection {
    pub fn new(
        table: &str,
        model: ConnectionCostModel,
        sort: Vec<(String, String)>,
        filters: Option<Condition>,
        is_root: bool,
        base_constraints: Option<PlannerConstraint>,
        limit: Option<usize>,
    ) -> Self {
        let selectivity = if let (Some(_), Some(f)) = (limit, &filters) {
            let with_filters = model(table, &sort, Some(f), None);
            let without_filters = model(table, &sort, None, None);
            if without_filters.rows > 0.0 {
                with_filters.rows / without_filters.rows
            } else {
                1.0
            }
        } else {
            1.0
        };

        crate::live_count::inc(&crate::live_count::PLANNER_NODE);
        PlannerConnection {
            sort,
            filters,
            model,
            table: table.to_string(),
            name: table.to_string(),
            base_constraints,
            base_limit: limit,
            selectivity,
            output: None,
            limit,
            constraints: HashMap::new(),
            cached_costs: HashMap::new(),
            is_root,
        }
    }

    pub fn set_output(&mut self, node: crate::planner::node::PlannerNode) {
        self.output = Some(node.downgrade());
    }

    pub fn closest_join_or_source(&self) -> JoinOrConnection {
        JoinOrConnection::Connection
    }

    pub fn propagate_constraints(
        &mut self,
        branch_pattern: &[usize],
        c: Option<&PlannerConstraint>,
        _from: Option<&crate::planner::node::PlannerNode>,
    ) {
        let key = branch_pattern
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        self.constraints.insert(key, c.cloned());
        self.cached_costs.clear();
    }

    pub fn estimate_cost(
        &self,
        downstream_child_selectivity: f64,
        branch_pattern: &[usize],
    ) -> CostEstimate {
        let key = branch_pattern
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");

        if let Some(cached) = self.cached_costs.get(&key) {
            return cached.clone();
        }

        let constraint = self.constraints.get(&key).cloned().flatten();
        let merged = merge_constraints(self.base_constraints.as_ref(), constraint.as_ref());

        let result = (self.model)(
            &self.table,
            &self.sort,
            self.filters.as_ref(),
            merged.as_ref(),
        );

        let scan_est = match self.limit {
            None => result.rows,
            Some(lim) => result
                .rows
                .min(lim as f64 / downstream_child_selectivity.max(1e-10)),
        };

        CostEstimate {
            startup_cost: result.startup_cost,
            scan_est,
            cost: 0.0,
            returned_rows: result.rows,
            selectivity: self.selectivity,
            limit: self.limit.map(|l| l as f64),
            fanout: result.fanout,
        }
    }

    pub fn unlimit(&mut self) {
        if self.is_root {
            return;
        }
        self.limit = None;
    }

    pub fn propagate_unlimit_from_flipped_join(&mut self) {
        self.unlimit();
    }

    pub fn reset(&mut self) {
        self.constraints.clear();
        self.limit = self.base_limit;
        self.cached_costs.clear();
    }

    pub fn capture_constraints(&self) -> HashMap<String, Option<PlannerConstraint>> {
        self.constraints.clone()
    }

    pub fn restore_constraints(&mut self, constraints: HashMap<String, Option<PlannerConstraint>>) {
        self.constraints = constraints;
        self.cached_costs.clear();
    }
}

impl Drop for PlannerConnection {
    fn drop(&mut self) {
        crate::live_count::dec(&crate::live_count::PLANNER_NODE);
    }
}
