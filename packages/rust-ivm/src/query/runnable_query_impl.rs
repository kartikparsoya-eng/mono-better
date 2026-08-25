//! Runnable query — port of `zql/src/query/runnable-query-impl.ts` and `static-query.ts`.
//!
//! A runnable query extends QueryImpl with the ability to materialize and run.
//! Static query is a query created in the permissions system context.

use std::collections::HashMap;

use crate::query::query_impl::{Query, RelationshipSpec};

/// Create a new runnable query.
/// Port of TS `newRunnableQuery` (runnable-query-impl.ts:23).
pub fn new_runnable_query(
    table: &str,
    relationships: HashMap<String, HashMap<String, RelationshipSpec>>,
) -> Query {
    Query::new(table, relationships)
}

/// Create a new static query (permissions system context).
/// Port of TS `newStaticQuery` (static-query.ts:11).
pub fn new_static_query(
    table: &str,
    relationships: HashMap<String, HashMap<String, RelationshipSpec>>,
) -> Query {
    // Static queries use the permissions system.
    // This is set via the system field in the builder.
    Query::new(table, relationships)
}

/// Create a new expression builder for a table.
/// Port of TS `newExpressionBuilder` (static-query.ts:22).
pub fn new_expression_builder(
    table: &str,
    relationships: HashMap<String, HashMap<String, RelationshipSpec>>,
) -> Query {
    new_static_query(table, relationships)
}
