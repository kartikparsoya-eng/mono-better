//! Planner module — port of `zql/src/planner/`.
//!
//! The query planner builds a cost-based plan graph from an AST and decides
//! which EXISTS joins to flip (child-outer instead of parent-outer). The
//! result is an AST with `flip` annotations set on correlated subquery
//! conditions.
//!
//! Port of TS `planQuery` (planner-builder.ts) + the plan graph
//! (planner-graph.ts, planner-join.ts, planner-connection.ts, etc.).

pub mod builder;
pub mod connection;
pub mod constraint;
pub mod fan_in;
pub mod fan_out;
pub mod graph;
pub mod join;
pub mod node;
pub mod runtime;
pub mod source;
pub mod terminus;

pub use builder::{Plans, apply_plans_to_ast, build_plan_graph, plan_query};
pub use connection::{ConnectionCostModel, CostModelCost};
pub use constraint::{PlannerConstraint, merge_constraints};
pub use node::{Confidence, FanoutCostModel, FanoutEst};
pub use runtime::{create_snapshot_cost_model, flip_order, plan_ast_flips};
