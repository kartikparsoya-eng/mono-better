//! Schema query — port of `zql/src/query/schema-query.ts`.
//!
//! Maps table names to Query builders for each table in a schema.

use std::collections::HashMap;

use crate::builder::query::{Query, RelationshipSpec};

/// A schema query: maps table names to their relationship specs.
/// Port of TS `SchemaQuery<S>` (schema-query.ts:13).
pub type SchemaQuery = HashMap<String, HashMap<String, RelationshipSpec>>;

/// Create a query builder for a specific table in the schema.
/// Port of TS `createBuilder` (create-builder.ts:16).
pub fn create_builder(schema: &SchemaQuery, table: &str) -> Query {
    Query::new(table, schema.clone())
}

/// Create query builders for all tables in a schema.
pub fn create_builders(schema: &SchemaQuery) -> HashMap<String, Query> {
    let mut builders = HashMap::new();
    for table in schema.keys() {
        builders.insert(table.clone(), Query::new(table, schema.clone()));
    }
    builders
}
