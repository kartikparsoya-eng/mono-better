//! AST types — port of `zero-protocol/src/ast.ts`.
//! Simplified for the initial port — just enough to build pipelines.

use crate::ivm::data::{Row, Value};

/// A query AST node — simplified port of TS `AST`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct Ast {
    pub schema: Option<String>,
    pub table: String,
    pub alias: Option<String>,
    pub where_clause: Option<Condition>,
    pub related: Vec<RelatedSubquery>,
    pub limit: Option<usize>,
    pub order_by: Option<Vec<OrderPart>>,
    pub start: Option<Bound>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct OrderPart {
    pub column: String,
    pub direction: String,
}

/// A start/pagination bound — port of TS `Bound` (ast.ts:228).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Bound {
    pub row: Row,
    pub exclusive: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Condition {
    Simple(SimpleCondition),
    And(Vec<Condition>),
    Or(Vec<Condition>),
    CorrelatedSubquery(CorrelatedSubqueryCondition),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SimpleCondition {
    pub op: String,
    pub left: ValuePosition,
    pub right: ValuePosition,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum ValuePosition {
    Column { name: String },
    Literal { value: Value },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CorrelatedSubqueryCondition {
    pub related: RelatedSubquery,
    pub op: String, // "EXISTS" | "NOT EXISTS"
    /// Tri-state flip annotation (matches TS `condition.flip`):
    /// - `None` (absent): planner decides whether to flip
    /// - `Some(true)`: force flipped (planner may not change)
    /// - `Some(false)`: force not-flipped (planner may not change)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flip: Option<bool>,
    /// TS `condition.scalar` — set by the permission system's
    /// `whereExists(rel, q, {scalar: true})`. Only scalar-flagged subqueries
    /// are pre-resolved to literals by `resolve_simple_scalar_subqueries`.
    #[serde(default)]
    pub scalar: bool,
    /// Planner-assigned ID (TS `planIdSymbol`). Set during `build_plan_graph`,
    /// read by `apply_to_condition` to determine which conditions to flip.
    /// Not serialized — internal to planning.
    #[serde(skip)]
    pub plan_id: Option<usize>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RelatedSubquery {
    pub subquery: Box<Ast>,
    pub relationship_name: String,
    pub parent_key: Vec<String>,
    pub child_key: Vec<String>,
    pub hidden: bool,
    pub system: Option<crate::ivm::schema::System>,
}
