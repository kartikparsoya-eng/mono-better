//! Resolve scalar subqueries — port of `zqlite/src/resolve-scalar-subqueries.ts`.
//!
//! Resolves "simple" scalar subqueries by calling a provided executor
//! and replacing them with literal conditions. A scalar subquery is simple
//! when all columns of at least one unique index on the subquery table are
//! equality-constrained by literal values in the subquery's WHERE clause.

use std::collections::HashMap;

use crate::builder::ast::{Ast, Condition, CorrelatedSubqueryCondition, SimpleCondition, ValuePosition};
use crate::ivm::data::Value;

/// Table spec with unique keys for scalar subquery resolution.
pub struct TableSpecWithUniqueKeys {
    pub unique_keys: Vec<Vec<String>>,
}

/// A companion subquery: the original scalar subquery AST and its resolved value.
#[derive(Clone, Debug)]
pub struct CompanionSubquery {
    pub ast: Ast,
    pub child_field: String,
    /// The value the executor pulled from `child_field` on the matched row.
    /// `None` with `matched == false` means "no row found" (→ ALWAYS_FALSE).
    /// `None` with `matched == true` means "row found but field was NULL"
    /// (same predicate effect, but distinct for companion-monitoring: a
    /// REMOVE→undefined push must not equal a resolved NULL, only a resolved
    /// no-match). Mirrors go-ivm's `Matched` flag.
    pub resolved_value: Option<Value>,
    pub matched: bool,
}

/// Result of resolving scalar subqueries.
#[derive(Clone, Debug)]
pub struct ResolveResult {
    pub ast: Ast,
    pub companions: Vec<CompanionSubquery>,
}

/// Executor callback: executes a scalar subquery and returns the value of
/// `child_field` from the (at most one) matching row.
///   - `(_, false)`  — no row matched
///   - `(None, true)` — row matched but `child_field` was NULL
///   - `(Some(v), true)` — row matched, `child_field` had value `v`
pub type ScalarExecutor<'a> = Box<dyn Fn(&Ast, &str) -> (Option<Value>, bool) + 'a>;

/// Resolve simple scalar subqueries by calling the provided executor
/// and replacing them with literal conditions.
/// Port of TS `resolveSimpleScalarSubqueries` (resolve-scalar-subqueries.ts:55).
pub fn resolve_simple_scalar_subqueries(
    ast: &Ast,
    table_specs: &HashMap<String, TableSpecWithUniqueKeys>,
    execute: &ScalarExecutor<'_>,
) -> ResolveResult {
    let mut companions = Vec::new();
    let resolved = resolve_ast_recursive(ast, table_specs, execute, &mut companions);
    ResolveResult {
        ast: resolved,
        companions,
    }
}

fn resolve_ast_recursive(
    ast: &Ast,
    table_specs: &HashMap<String, TableSpecWithUniqueKeys>,
    execute: &ScalarExecutor<'_>,
    companions: &mut Vec<CompanionSubquery>,
) -> Ast {
    let where_clause = ast
        .where_clause
        .as_ref()
        .map(|w| resolve_condition(w, table_specs, execute, companions));

    let related = ast
        .related
        .iter()
        .map(|r| {
            let subquery = resolve_ast_recursive(&r.subquery, table_specs, execute, companions);
            crate::builder::ast::RelatedSubquery {
                subquery: Box::new(subquery),
                relationship_name: r.relationship_name.clone(),
                parent_key: r.parent_key.clone(),
                child_key: r.child_key.clone(),
                hidden: r.hidden,
                system: r.system,
            }
        })
        .collect::<Vec<_>>();

    Ast {
        schema: ast.schema.clone(),
        table: ast.table.clone(),
        alias: ast.alias.clone(),
        where_clause,
        related,
        limit: ast.limit,
        order_by: ast.order_by.clone(),
        start: ast.start.clone(),
    }
}

fn resolve_condition(
    condition: &Condition,
    table_specs: &HashMap<String, TableSpecWithUniqueKeys>,
    execute: &ScalarExecutor<'_>,
    companions: &mut Vec<CompanionSubquery>,
) -> Condition {
    match condition {
        Condition::CorrelatedSubquery(csq) => {
            // Only subqueries explicitly flagged `scalar` (TS's
            // `whereExists(rel, q, {scalar: true})`) are pre-resolved to
            // literals. An ordinary single-field EXISTS is NOT scalar — it
            // stays an incrementally-maintained Join/Exists. This mirrors TS
            // `resolveCondition` gating on `condition.scalar`.
            if csq.scalar {
                return resolve_scalar_subquery(csq, table_specs, execute, companions);
            }
            // Non-scalar correlated subquery: recurse into its subquery
            let resolved_subquery =
                resolve_ast_recursive(&csq.related.subquery, table_specs, execute, companions);
            Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
                related: crate::builder::ast::RelatedSubquery {
                    subquery: Box::new(resolved_subquery),
                    relationship_name: csq.related.relationship_name.clone(),
                    parent_key: csq.related.parent_key.clone(),
                    child_key: csq.related.child_key.clone(),
                    hidden: csq.related.hidden,
                    system: csq.related.system,
                },
                op: csq.op.clone(),
                flip: csq.flip,
                scalar: csq.scalar,
                plan_id: None,
            })
        }
        Condition::And(conditions) => {
            let resolved: Vec<Condition> = conditions
                .iter()
                .map(|c| resolve_condition(c, table_specs, execute, companions))
                .collect();
            Condition::And(resolved)
        }
        Condition::Or(conditions) => {
            let resolved: Vec<Condition> = conditions
                .iter()
                .map(|c| resolve_condition(c, table_specs, execute, companions))
                .collect();
            Condition::Or(resolved)
        }
        Condition::Simple(_) => condition.clone(),
    }
}

fn resolve_scalar_subquery(
    condition: &CorrelatedSubqueryCondition,
    table_specs: &HashMap<String, TableSpecWithUniqueKeys>,
    execute: &ScalarExecutor<'_>,
    companions: &mut Vec<CompanionSubquery>,
) -> Condition {
    let parent_field = &condition.related.parent_key[0];
    let child_field = &condition.related.child_key[0];

    // Recursively resolve any scalar subqueries nested in the subquery's own WHERE.
    let subquery =
        resolve_ast_recursive(&condition.related.subquery, table_specs, execute, companions);

    if !is_simple_subquery(&subquery, table_specs) {
        // Return with the (possibly partially-resolved) subquery.
        return Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: crate::builder::ast::RelatedSubquery {
                subquery: Box::new(subquery),
                relationship_name: condition.related.relationship_name.clone(),
                parent_key: condition.related.parent_key.clone(),
                child_key: condition.related.child_key.clone(),
                hidden: condition.related.hidden,
                system: condition.related.system,
            },
            op: condition.op.clone(),
            flip: condition.flip,
            scalar: condition.scalar,
            plan_id: None,
        });
    }

    let (value, matched) = execute(&subquery, child_field);

    // Record the companion subquery AST so a live companion pipeline can be
    // built to monitor whether the resolved value changes on advance.
    companions.push(CompanionSubquery {
        ast: subquery.clone(),
        child_field: child_field.clone(),
        resolved_value: value.clone(),
        matched,
    });

    match &value {
        _ if !matched || value.is_none() => {
            // No row matched, OR row matched but the field was NULL — both
            // `x = NULL` and `x != NULL` are false in SQL → ALWAYS_FALSE.
            Condition::Simple(SimpleCondition {
                op: "=".to_string(),
                left: ValuePosition::Literal { value: Value::F64(1.0) },
                right: ValuePosition::Literal { value: Value::F64(0.0) },
            })
        }
        Some(v) => {
            let op = if condition.op == "EXISTS" { "=" } else { "IS NOT" };
            Condition::Simple(SimpleCondition {
                op: op.to_string(),
                left: ValuePosition::Column { name: parent_field.clone() },
                right: ValuePosition::Literal { value: v.clone() },
            })
        }
        None => unreachable!("None handled by the !matched || is_none guard above"),
    }
}

/// Check if the subquery is guaranteed to return at most one deterministic row.
/// True when all columns of at least one unique index are equality-constrained
/// by literal values in the WHERE clause (using only AND conjunctions).
/// Port of TS `isSimpleSubquery` (resolve-scalar-subqueries.ts:189).
pub fn is_simple_subquery(
    subquery: &Ast,
    table_specs: &HashMap<String, TableSpecWithUniqueKeys>,
) -> bool {
    let spec = match table_specs.get(&subquery.table) {
        Some(s) => s,
        None => return false,
    };

    let where_clause = match &subquery.where_clause {
        Some(w) => w,
        None => return false,
    };

    let constraints = extract_literal_equality_constraints(where_clause);
    if constraints.is_empty() {
        return false;
    }

    spec.unique_keys
        .iter()
        .any(|key| key.iter().all(|col| constraints.contains_key(col)))
}

/// Extract column=literal equality constraints from a condition tree,
/// only following AND conjunctions (not OR).
/// Port of TS `extractLiteralEqualityConstraints` (resolve-scalar-subqueries.ts:214).
pub fn extract_literal_equality_constraints(condition: &Condition) -> HashMap<String, Value> {
    let mut constraints = HashMap::new();
    collect_constraints(condition, &mut constraints);
    constraints
}

fn collect_constraints(condition: &Condition, constraints: &mut HashMap<String, Value>) {
    match condition {
        Condition::Simple(simple) => {
            if simple.op == "=" {
                if let (
                    ValuePosition::Column { name },
                    ValuePosition::Literal { value },
                ) = (&simple.left, &simple.right)
                {
                    constraints.insert(name.clone(), value.clone());
                }
            }
        }
        Condition::And(conditions) => {
            for c in conditions {
                collect_constraints(c, constraints);
            }
        }
        // OR, correlatedSubquery (non-scalar) — don't contribute constraints
        _ => {}
    }
}
