//! Planner connection — port of `planner-connection.ts`.

use std::collections::HashMap;
use std::rc::Rc;

use crate::builder::ast::Condition;
use crate::planner::planner_constraint::{PlannerConstraint, merge_constraints};
use crate::planner::planner_node::{CostEstimate, FanoutCostModel, JoinOrConnection};

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
    output: Option<crate::planner::planner_node::PlannerNodeWeak>,

    // Mutable
    pub limit: Option<usize>,
    constraints: HashMap<String, Option<PlannerConstraint>>,
    /// Port of TS `#cachedConstraintCosts` (planner-connection.ts:196-226).
    /// `RefCell` because TS WRITES the cache inside the (conceptually const)
    /// `estimateCost`. NB the key is the branch pattern ONLY — TS deliberately
    /// does NOT key on `downstreamChildSelectivity`, so a repeat visit with
    /// the same pattern but a different dcs returns the FIRST visit's
    /// `scanEst`. That staleness is part of the cost model (NEW-3: never
    /// populating the cache made Rust recompute per-dcs and diverge from the
    /// TS totals the flip choice ranks).
    cached_costs: std::cell::RefCell<HashMap<String, CostEstimate>>,
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
            cached_costs: std::cell::RefCell::new(HashMap::new()),
            is_root,
        }
    }

    pub fn set_output(&mut self, node: crate::planner::planner_node::PlannerNode) {
        self.output = Some(node.downgrade());
    }

    pub fn closest_join_or_source(&self) -> JoinOrConnection {
        JoinOrConnection::Connection
    }

    pub fn propagate_constraints(
        &mut self,
        branch_pattern: &[usize],
        c: Option<&PlannerConstraint>,
        _from: Option<&crate::planner::planner_node::PlannerNode>,
    ) {
        let key = branch_pattern
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        self.constraints.insert(key, c.cloned());
        self.cached_costs.borrow_mut().clear();
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

        if let Some(cached) = self.cached_costs.borrow().get(&key) {
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

        let cost = CostEstimate {
            startup_cost: result.startup_cost,
            scan_est,
            cost: 0.0,
            returned_rows: result.rows,
            selectivity: self.selectivity,
            limit: self.limit.map(|l| l as f64),
            fanout: result.fanout,
        };
        // TS: `this.#cachedConstraintCosts.set(key, cost)` (planner-connection.ts:226).
        self.cached_costs.borrow_mut().insert(key, cost.clone());
        cost
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
        self.cached_costs.borrow_mut().clear();
    }

    pub fn capture_constraints(&self) -> HashMap<String, Option<PlannerConstraint>> {
        self.constraints.clone()
    }

    pub fn restore_constraints(&mut self, constraints: HashMap<String, Option<PlannerConstraint>>) {
        self.constraints = constraints;
        self.cached_costs.borrow_mut().clear();
    }
}

impl Drop for PlannerConnection {
    fn drop(&mut self) {
        crate::live_count::dec(&crate::live_count::PLANNER_NODE);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::planner_node::{Confidence, FanoutEst};
    use std::cell::Cell;

    /// NEW-3: TS `estimateCost` CACHES the computed cost keyed by the branch
    /// pattern ONLY (planner-connection.ts:196-226) — deliberately NOT by
    /// `downstreamChildSelectivity`, so a repeat visit with the same pattern
    /// returns the FIRST visit's (dcs-dependent) `scanEst`. The Rust cache was
    /// read but never written, so it recomputed per-dcs and its cost totals
    /// diverged from the TS values the flip choice ranks. Proven by
    /// temp-revert (removing the insert fails both asserts).
    #[test]
    fn estimate_cost_caches_by_branch_pattern_like_ts() {
        let calls = Rc::new(Cell::new(0u32));
        let calls_in_model = calls.clone();
        let model: ConnectionCostModel = Rc::new(move |_t, _s, _f, _c| {
            calls_in_model.set(calls_in_model.get() + 1);
            CostModelCost {
                startup_cost: 1.0,
                rows: 100.0,
                fanout: Rc::new(|_cols| FanoutEst {
                    fanout: 1.0,
                    confidence: Confidence::None,
                }),
            }
        });
        let conn = PlannerConnection::new("t", model, Vec::new(), None, false, None, Some(10));

        let first = conn.estimate_cost(1.0, &[0]);
        assert_eq!(first.scan_est, 10.0, "min(rows=100, limit 10 / dcs 1.0)");
        assert_eq!(calls.get(), 1);

        // Same pattern, DIFFERENT dcs: must hit the cache and return the
        // FIRST dcs's scanEst (uncached would be min(100, 10/0.5) = 20).
        let second = conn.estimate_cost(0.5, &[0]);
        assert_eq!(
            calls.get(),
            1,
            "same branch pattern must not re-run the cost model"
        );
        assert_eq!(
            second.scan_est, 10.0,
            "cache key excludes dcs (TS staleness is the spec)"
        );

        // A different pattern recomputes.
        let _third = conn.estimate_cost(1.0, &[1]);
        assert_eq!(calls.get(), 2);
    }
}
