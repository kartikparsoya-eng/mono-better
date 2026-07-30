//! Query builder — port of `zql/src/query/query-impl.ts`.
//!
//! A fluent builder that constructs an AST from chained method calls.
//! `Query::new(schema, table)` → `.where()`, `.related()`, `.limit()`,
//! `.order_by()`, `.start()`, `.one()`.

use std::collections::HashMap;

use crate::builder::ast::{
    Ast, Bound, Condition, CorrelatedSubqueryCondition, OrderPart, RelatedSubquery,
    SimpleCondition, ValuePosition,
};
use crate::builder::expression::{cmp_eq, simplify_condition};
use crate::ivm::data::{Row, Value};
use crate::ivm::schema::System;
use crate::ivm::view::{Format, default_format};

/// A query builder that accumulates AST state through fluent method calls.
/// Port of TS `QueryImpl` (query-impl.ts:92).
#[derive(Clone)]
pub struct Query {
    table: String,
    ast: Ast,
    format: Format,
    system: System,
    /// Schema relationships: table_name → (relationship_name → relationship spec)
    relationships: HashMap<String, HashMap<String, RelationshipSpec>>,
}

/// A relationship specification: source field, dest field, dest table, cardinality.
#[derive(Clone, Debug)]
pub struct RelationshipSpec {
    pub source_field: Vec<String>,
    pub dest_field: Vec<String>,
    pub dest_table: String,
    pub cardinality: Cardinality,
}

/// Relationship cardinality.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cardinality {
    One,
    Many,
}

/// Options for EXISTS subqueries.
#[derive(Clone, Debug, Default)]
pub struct ExistsOptions {
    pub flip: Option<bool>,
    pub scalar: Option<bool>,
}

impl Query {
    /// Create a new query on the given table.
    pub fn new(
        table: &str,
        relationships: HashMap<String, HashMap<String, RelationshipSpec>>,
    ) -> Self {
        Query {
            table: table.to_string(),
            ast: Ast {
                schema: None,
                table: table.to_string(),
                alias: None,
                where_clause: None,
                related: Vec::new(),
                limit: None,
                order_by: None,
                start: None,
            },
            format: default_format(),
            system: System::Client,
            relationships,
        }
    }

    /// Get the AST.
    pub fn ast(&self) -> &Ast {
        &self.ast
    }

    /// Get the format.
    pub fn format(&self) -> &Format {
        &self.format
    }

    /// Set limit to 1 and format to singular.
    pub fn one(mut self) -> Self {
        self.ast.limit = Some(1);
        self.format.singular = true;
        self
    }

    /// Add a WHERE condition.
    /// 2-arg form: where(field, value) → field = value
    /// 3-arg form: where(field, op, value)
    pub fn where_eq(mut self, field: &str, value: Value) -> Self {
        let cond = cmp_eq(field, value);
        let combined = match &self.ast.where_clause {
            Some(existing) => crate::builder::expression::and(&[existing.clone(), cond]),
            None => cond,
        };
        self.ast.where_clause = Some(simplify_condition(&combined));
        self
    }

    /// Add a WHERE condition with explicit operator.
    pub fn where_op(mut self, field: &str, op: &str, value: Value) -> Self {
        let cond = Condition::Simple(SimpleCondition {
            op: op.to_string(),
            left: ValuePosition::Column {
                name: field.to_string(),
            },
            right: ValuePosition::Literal { value },
        });
        let combined = match &self.ast.where_clause {
            Some(existing) => crate::builder::expression::and(&[existing.clone(), cond]),
            None => cond,
        };
        self.ast.where_clause = Some(simplify_condition(&combined));
        self
    }

    /// Add a WHERE condition from an expression.
    pub fn where_cond(mut self, cond: Condition) -> Self {
        let combined = match &self.ast.where_clause {
            Some(existing) => crate::builder::expression::and(&[existing.clone(), cond]),
            None => cond,
        };
        self.ast.where_clause = Some(simplify_condition(&combined));
        self
    }

    /// Add a related subquery (one-hop relationship).
    pub fn related(mut self, relationship: &str, cb: Option<Box<dyn Fn(Query) -> Query>>) -> Self {
        let rel = self
            .relationships
            .get(&self.table)
            .and_then(|rels| rels.get(relationship))
            .expect("Invalid relationship");

        let dest_schema = &rel.dest_table;
        let dest_field = &rel.dest_field;
        let source_field = &rel.source_field;

        let sub_query = Query {
            table: dest_schema.clone(),
            ast: Ast {
                schema: None,
                table: dest_schema.clone(),
                alias: Some(relationship.to_string()),
                where_clause: None,
                related: Vec::new(),
                limit: None,
                order_by: None,
                start: None,
            },
            format: default_format(),
            system: self.system,
            relationships: self.relationships.clone(),
        };

        let sub_query = match cb {
            Some(cb) => cb(sub_query),
            None => sub_query,
        };

        let related = RelatedSubquery {
            subquery: Box::new(sub_query.ast.clone()),
            relationship_name: relationship.to_string(),
            parent_key: source_field.clone(),
            child_key: dest_field.clone(),
            hidden: false,
            system: Some(self.system),
        };

        self.ast.related.push(related);
        self.format
            .relationships
            .insert(relationship.to_string(), sub_query.format);
        self
    }

    /// Add WHERE EXISTS for a relationship.
    pub fn where_exists(
        mut self,
        relationship: &str,
        cb: Option<Box<dyn Fn(Query) -> Query>>,
        options: ExistsOptions,
    ) -> Self {
        let rel = self
            .relationships
            .get(&self.table)
            .and_then(|rels| rels.get(relationship))
            .expect("Invalid relationship");

        let dest_schema = &rel.dest_table;
        let dest_field = &rel.dest_field;
        let source_field = &rel.source_field;

        let sub_query = Query {
            table: dest_schema.clone(),
            ast: Ast {
                schema: None,
                table: dest_schema.clone(),
                alias: Some(format!("_subq_{}", relationship)),
                where_clause: None,
                related: Vec::new(),
                limit: None,
                order_by: None,
                start: None,
            },
            format: default_format(),
            system: self.system,
            relationships: self.relationships.clone(),
        };

        let sub_query = match cb {
            Some(cb) => cb(sub_query),
            None => sub_query,
        };

        let csq = CorrelatedSubqueryCondition {
            related: RelatedSubquery {
                subquery: Box::new(sub_query.ast.clone()),
                relationship_name: relationship.to_string(),
                parent_key: source_field.clone(),
                child_key: dest_field.clone(),
                hidden: false,
                system: Some(self.system),
            },
            op: "EXISTS".to_string(),
            flip: options.flip,
            scalar: options.scalar.unwrap_or(false),
            plan_id: None,
        };

        let cond = Condition::CorrelatedSubquery(csq);
        let combined = match &self.ast.where_clause {
            Some(existing) => crate::builder::expression::and(&[existing.clone(), cond]),
            None => cond,
        };
        self.ast.where_clause = Some(simplify_condition(&combined));
        self
    }

    /// Set limit.
    pub fn limit(mut self, limit: usize) -> Self {
        assert!(limit > 0, "Limit must be positive");
        self.ast.limit = Some(limit);
        self
    }

    /// Add ORDER BY clause.
    pub fn order_by(mut self, field: &str, direction: &str) -> Self {
        let order = self.ast.order_by.clone().unwrap_or_default();
        let mut order = order;
        order.push(OrderPart {
            column: field.to_string(),
            direction: direction.to_string(),
        });
        self.ast.order_by = Some(order);
        self
    }

    /// Set start position (pagination).
    pub fn start(mut self, row: Row, inclusive: bool) -> Self {
        self.ast.start = Some(Bound {
            row,
            exclusive: !inclusive,
        });
        self
    }
}
