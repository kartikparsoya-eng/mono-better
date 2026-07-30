//! Planner source — port of `planner-source.ts`.

use crate::builder::ast::Condition;
use crate::planner::connection::{ConnectionCostModel, PlannerConnection};
use crate::planner::constraint::PlannerConstraint;

pub struct PlannerSource {
    pub name: String,
    model: ConnectionCostModel,
}

impl PlannerSource {
    pub fn new(name: &str, model: ConnectionCostModel) -> Self {
        PlannerSource {
            name: name.to_string(),
            model,
        }
    }

    pub fn connect(
        &self,
        sort: Vec<(String, String)>,
        filters: Option<Condition>,
        is_root: bool,
        base_constraints: Option<PlannerConstraint>,
        limit: Option<usize>,
    ) -> PlannerConnection {
        PlannerConnection::new(
            &self.name,
            self.model.clone(),
            sort,
            filters,
            is_root,
            base_constraints,
            limit,
        )
    }
}
