//! Planner module — port of `zql/src/planner/`.
//!
//! The query planner builds a cost-based plan graph from an AST and decides
//! which EXISTS joins to flip (child-outer instead of parent-outer). The
//! result is an AST with `flip` annotations set on correlated subquery
//! conditions.
//!
//! Port of TS `planQuery` (planner-builder.ts) + the plan graph
//! (planner-graph.ts, planner-join.ts, planner-connection.ts, etc.).

pub mod constraint;
pub mod node;
pub mod source;
pub mod connection;
pub mod join;
pub mod fan_out;
pub mod fan_in;
pub mod terminus;
pub mod graph;
pub mod builder;

pub use builder::{build_plan_graph, plan_query, apply_plans_to_ast, Plans};
pub use connection::{ConnectionCostModel, CostModelCost};
pub use node::{FanoutCostModel, FanoutEst, Confidence};
pub use constraint::{PlannerConstraint, merge_constraints};
