//! Planner module — port of `zql/src/planner/`.
//!
//! The query planner builds a cost-based plan graph from an AST and decides
//! which EXISTS joins to flip (child-outer instead of parent-outer). The
//! result is an AST with `flip` annotations set on correlated subquery
//! conditions.
//!
//! Port of TS `planQuery` (planner-builder.ts) + the plan graph
//! (planner-graph.ts, planner-join.ts, planner-connection.ts, etc.). Filenames
//! mirror TS's `planner-*.ts` 1:1 (`planner_builder.rs` ⟵ `planner-builder.ts`,
//! …). `runtime.rs` is Rust-only: the actor's snapshot-backed cost-model
//! runtime entry (`plan_ast_flips`), with no single TS origin file.

pub mod planner_builder;
pub mod planner_connection;
pub mod planner_constraint;
pub mod planner_fan_in;
pub mod planner_fan_out;
pub mod planner_graph;
pub mod planner_join;
pub mod planner_node;
pub mod planner_source;
pub mod planner_terminus;
pub mod runtime;

pub use planner_builder::{Plans, apply_plans_to_ast, build_plan_graph, plan_query};
pub use planner_connection::{ConnectionCostModel, CostModelCost};
pub use planner_constraint::{PlannerConstraint, merge_constraints};
pub use planner_node::{Confidence, FanoutCostModel, FanoutEst};
pub use runtime::{
    PlanCountCache, create_snapshot_cost_model, create_snapshot_cost_model_cached, flip_order,
    plan_ast_flips,
};
