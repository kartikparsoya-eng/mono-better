//! AST types — port of `zero-protocol/src/ast.ts`.
//! Simplified for the initial port — just enough to build pipelines.

use crate::ivm::data::{Row, Value};

/// A query AST node — simplified port of TS `AST`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
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

impl Default for Ast {
    fn default() -> Self {
        Ast {
            schema: None,
            table: String::new(),
            alias: None,
            where_clause: None,
            related: Vec::new(),
            limit: None,
            order_by: None,
            start: None,
        }
    }
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
    pub flip: bool,
    /// TS `condition.scalar` — set by the permission system's
    /// `whereExists(rel, q, {scalar: true})`. Only scalar-flagged subqueries
    /// are pre-resolved to literals by `resolve_simple_scalar_subqueries`.
    #[serde(default)]
    pub scalar: bool,
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
