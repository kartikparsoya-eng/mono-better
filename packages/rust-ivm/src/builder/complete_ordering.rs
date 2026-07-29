//! Complete ordering — port of `zql/src/query/complete-ordering.ts`.
//!
//! Adds primary key columns to orderBy if they're missing.
//! Recursively processes related subqueries.

use crate::builder::ast::{Ast, Condition};

/// Add primary key columns to orderBy if missing.
/// Port of TS `completeOrdering` (complete-ordering.ts:6).
pub fn complete_ordering(ast: &Ast, get_primary_key: &dyn Fn(&str) -> Vec<String>) -> Ast {
    let primary_key = get_primary_key(&ast.table);
    let mut result = ast.clone();

    // Recursively complete related subqueries
    if !result.related.is_empty() {
        let mut related = Vec::new();
        for r in &result.related {
            let mut sr = r.clone();
            sr.subquery = Box::new(complete_ordering(&r.subquery, get_primary_key));
            related.push(sr);
        }
        result.related = related;
    }

    // Complete where clause conditions
    if let Some(where_clause) = &result.where_clause {
        result.where_clause = Some(complete_ordering_in_condition(
            where_clause,
            get_primary_key,
        ));
    }

    result.order_by = Some(add_primary_keys(&primary_key, &result.order_by));
    result
}

/// Assert that ordering includes all PK fields.
/// Port of TS `assertOrderingIncludesPK` (complete-ordering.ts:31).
pub fn assert_ordering_includes_pk(ordering: &[(String, String)], pk: &[String]) {
    let ordering_fields: Vec<&str> = ordering.iter().map(|o| o.0.as_str()).collect();
    let missing: Vec<&str> = pk
        .iter()
        .filter(|pk_field| !ordering_fields.contains(&pk_field.as_str()))
        .map(|s| s.as_str())
        .collect();

    assert!(
        missing.is_empty(),
        "Ordering must include all primary key fields. Missing: {}",
        missing.join(", ")
    );
}

fn complete_ordering_in_condition(
    condition: &Condition,
    get_primary_key: &dyn Fn(&str) -> Vec<String>,
) -> Condition {
    match condition {
        Condition::Simple(_) => condition.clone(),
        Condition::CorrelatedSubquery(csq) => {
            let mut csq = csq.clone();
            csq.related.subquery = Box::new(complete_ordering(
                &csq.related.subquery,
                get_primary_key,
            ));
            Condition::CorrelatedSubquery(csq)
        }
        Condition::And(conds) => {
            Condition::And(conds.iter().map(|c| complete_ordering_in_condition(c, get_primary_key)).collect())
        }
        Condition::Or(conds) => {
            Condition::Or(conds.iter().map(|c| complete_ordering_in_condition(c, get_primary_key)).collect())
        }
    }
}

fn add_primary_keys(
    primary_key: &[String],
    order_by: &Option<Vec<crate::builder::ast::OrderPart>>,
) -> Vec<crate::builder::ast::OrderPart> {
    let mut result = match order_by {
        Some(o) => o.clone(),
        None => Vec::new(),
    };

    let existing: std::collections::HashSet<String> = result.iter().map(|o| o.column.clone()).collect();

    for pk_col in primary_key {
        if !existing.contains(pk_col) {
            result.push(crate::builder::ast::OrderPart {
                column: pk_col.clone(),
                direction: "asc".to_string(),
            });
        }
    }

    result
}
