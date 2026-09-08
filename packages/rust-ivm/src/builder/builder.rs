//! Pipeline builder — port of `zql/src/builder/builder.ts`.
//!
//! Compiles a query AST into an operator tree. Handles:
//! - Source connection with sort + filter
//! - Skip (pagination)
//! - EXISTS / NOT EXISTS correlated subqueries (via Cap + Exists)
//! - WHERE clause application (applyWhere, applyFilterWithFlips)
//! - Related subqueries (Joins)
//! - Limit (Take for ordered, Cap for EXISTS)

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::builder::ast::{Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery};
use crate::builder::filter::{create_predicate, create_simple_predicate, transform_filters};
use crate::ivm::cap::Cap;
use crate::ivm::data::SortOrder;
use crate::ivm::exists::Exists;
use crate::ivm::fan_in::FanIn;
use crate::ivm::fan_out::FanOut;
use crate::ivm::filter::Filter;
use crate::ivm::filter_operators::{FilterInputHandle, build_filter_pipeline};
use crate::ivm::flipped_join::{FlippedJoin, FlippedJoinArgs};
use crate::ivm::join::{Join, JoinArgs};
use crate::ivm::operator::{Input, InputBase, Shared, Storage};
use crate::ivm::schema::System;
use crate::ivm::skip::Skip;
use crate::ivm::source::Source;
use crate::ivm::take::Take;

/// EXISTS subquery limit — small cap since we only need to know if any row exists.
const EXISTS_LIMIT: usize = 3;
/// Permissions EXISTS limit — even smaller for auth checks.
const PERMISSIONS_EXISTS_LIMIT: usize = 1;

/// Interface required of caller to buildPipeline. Connects to constructed
/// pipeline to delegate environment to provide sources and storage.
pub trait BuilderDelegate {
    /// Called once for each source needed by the AST.
    fn get_source(&self, table_name: &str) -> Option<Shared<dyn Source>>;

    /// Whether NOT EXISTS is allowed. Server-only feature.
    fn enable_not_exists(&self) -> bool {
        false
    }

    /// Create a new storage instance for an operator that requires it.
    /// Port of TS `createStorage()`.
    fn create_storage(&mut self) -> Shared<dyn Storage> {
        Rc::new(RefCell::new(
            crate::ivm::memory_storage::MemoryStorage::new(),
        ))
    }

    /// The debug delegate to thread into every source connection. Port of TS
    /// `BuilderDelegate.debug?` (builder.ts:57), passed to `source.connect(...,
    /// delegate.debug)` (builder.ts:316). Default `None` — prod does no
    /// vended-row tracking. Returns a clone of the shared handle per connect.
    fn debug(&self) -> Option<crate::builder::debug_delegate::SharedDebug> {
        None
    }
}

/// Build a pipeline from an AST.
/// Port of TS `buildPipeline` (builder.ts:131).
pub fn build_pipeline(ast: &Ast, delegate: &mut dyn BuilderDelegate) -> Shared<dyn Input> {
    build_pipeline_internal(ast, delegate, "", None, false)
}

/// Internal recursive pipeline builder.
/// Port of TS `buildPipelineInternal` (builder.ts:230).
fn build_pipeline_internal(
    ast: &Ast,
    delegate: &mut dyn BuilderDelegate,
    name: &str,
    partition_key: Option<Vec<String>>,
    is_non_flipped_exists_child: bool,
) -> Shared<dyn Input> {
    let source = match delegate.get_source(&ast.table) {
        Some(s) => s,
        None => {
            return Rc::new(RefCell::new(crate::ivm::memory_source::EmptyInput::new()));
        }
    };

    // Validate NOT EXISTS if not enabled
    if !delegate.enable_not_exists()
        && let Some(ref where_clause) = ast.where_clause
    {
        assert_no_not_exists(where_clause);
    }

    // Uniquify correlated subquery aliases: each CSQ gets alias + "_" + counter.
    // Port of TS uniquifyCorrelatedSubqueryConditionAliases (builder.ts:763).
    let ast = uniquify_correlated_subquery_condition_aliases(ast.clone());

    // Gather correlated subquery conditions from the WHERE clause
    let csq_conditions = gather_correlated_subquery_query_conditions(ast.where_clause.as_ref());

    // Collect split edit keys from CSQ correlations
    let mut split_edit_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
    if let Some(ref pk) = partition_key {
        for key in pk {
            split_edit_keys.insert(key.clone());
        }
    }
    for csq in &csq_conditions {
        for key in &csq.related.parent_key {
            split_edit_keys.insert(key.clone());
        }
    }
    for csq in &ast.related {
        for key in &csq.parent_key {
            split_edit_keys.insert(key.clone());
        }
    }

    // EXISTS subqueries must not have start or related
    if is_non_flipped_exists_child {
        assert!(ast.start.is_none(), "EXISTS subqueries must not have start");
        assert!(
            ast.related.is_empty(),
            "EXISTS subqueries must not have related"
        );
    }

    // The Cap optimization needs the source connect to be unordered, but
    // applyFilterWithFlips builds a UnionFanIn over the source whenever
    // ast.where contains a flipped subquery, and UnionFanIn requires a
    // sort on its inputs. Fall back to ordered + Take path in that case.
    let use_cap = is_non_flipped_exists_child
        && !(ast
            .where_clause
            .as_ref()
            .map(condition_includes_flipped_subquery_at_any_level)
            .unwrap_or(false));

    // Build sort order
    let sort: Option<SortOrder> = if use_cap {
        None
    } else {
        ast.order_by.as_ref().map(|order| {
            Arc::new(
                order
                    .iter()
                    .map(|p| [p.column.clone(), p.direction.clone()])
                    .collect::<Vec<_>>(),
            )
        })
    };

    // Transform filters: strip correlated subquery conditions for source-level filtering
    let transformed = transform_filters(ast.where_clause.as_ref());
    let filter_predicate = transformed.filters.as_ref().map(|c| create_predicate(c));
    let filter_condition = transformed.filters.clone();

    let split_keys = if split_edit_keys.is_empty() {
        None
    } else {
        Some(split_edit_keys.into_iter().collect::<Vec<_>>())
    };

    let conn = source.borrow_mut().connect(
        sort,
        filter_condition,
        filter_predicate,
        split_keys,
        // Port of TS `source.connect(..., delegate.debug)` (builder.ts:316):
        // thread the (optional) debug delegate so the source records vended rows.
        delegate.debug(),
    );
    let mut current: Shared<dyn Input> = conn;

    // Apply Skip (start/pagination) if specified
    if let Some(ref bound) = ast.start {
        let skip = Skip::new(current.clone(), bound.clone());
        current = skip;
    }

    // Apply non-flipped EXISTS correlated subqueries
    for csq_condition in &csq_conditions {
        if csq_condition.flip != Some(true) {
            current =
                apply_correlated_subquery(csq_condition, delegate, current.clone(), name, true);
        }
    }

    // Apply WHERE clause if not fully applied at source or if there are flipped conditions
    let needs_where = ast.where_clause.is_some()
        && (transformed.conditions_removed
            || condition_includes_flipped_subquery_at_any_level(
                ast.where_clause.as_ref().unwrap(),
            ));

    let _ = std::env::var("IVM_TRACE");
    if needs_where {
        current = apply_where(
            current.clone(),
            ast.where_clause.as_ref().unwrap(),
            delegate,
            name,
        );
    }

    // Apply limit
    let _ = std::env::var("IVM_TRACE");
    if let Some(limit) = ast.limit {
        if use_cap {
            let cap = Cap::new(
                current.clone(),
                Rc::new(RefCell::new(crate::ivm::cap::CapStorage::new())),
                limit,
                partition_key.clone(),
            );
            current = cap;
        } else {
            let take = Take::new(
                current.clone(),
                Rc::new(RefCell::new(crate::ivm::take::TakeStorage::new())),
                limit,
                partition_key.clone(),
            );
            current = take;
        }
    }

    // Apply related subqueries (non-condition joins)
    // Dedupe by relationship_name — last one wins. Preserve the original
    // order of distinct aliases, matching TS `byAlias.values()` insertion
    // order (builder.ts:385-393). Reversing the list puts the deepest join
    // closest to the source and inverts the push-change order for distinct
    // relationships.
    let mut last_def: std::collections::HashMap<String, RelatedSubquery> =
        std::collections::HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for csq in &ast.related {
        if !last_def.contains_key(&csq.relationship_name) {
            order.push(csq.relationship_name.clone());
        }
        last_def.insert(csq.relationship_name.clone(), csq.clone());
    }
    for rel_name in order {
        let csq = last_def.get(&rel_name).expect("relationship in order map");
        current = apply_correlated_subquery_join(csq, delegate, current.clone(), name, false);
    }

    current
}

/// Apply a WHERE clause to the pipeline.
/// Port of TS `applyWhere` (builder.ts:399).
/// Port of TS `applyWhere` (builder.ts:399): the non-flipped WHERE clause is
/// built as a filter sub-graph (FilterStart -> filter chain -> FilterEnd);
/// flipped subqueries take the Union fan path.
fn apply_where(
    input: Shared<dyn Input>,
    condition: &Condition,
    delegate: &mut dyn BuilderDelegate,
    name: &str,
) -> Shared<dyn Input> {
    if !condition_includes_flipped_subquery_at_any_level(condition) {
        return build_filter_pipeline(input, |filter_input| {
            apply_filter(filter_input, condition, delegate, name)
        });
    }
    apply_filter_with_flips(input, condition, delegate, name)
}

/// Port of TS `applyFilter` (builder.ts:523).
fn apply_filter(
    input: FilterInputHandle,
    condition: &Condition,
    delegate: &mut dyn BuilderDelegate,
    name: &str,
) -> FilterInputHandle {
    match condition {
        Condition::And(conditions) => apply_and(input, conditions, delegate, name),
        Condition::Or(conditions) => apply_or(input, conditions, delegate, name),
        Condition::CorrelatedSubquery(csq) => {
            apply_correlated_subquery_condition(input, csq, delegate, name)
        }
        Condition::Simple(simple) => apply_simple_condition(input, delegate, simple),
    }
}

/// Port of TS `applyAnd` (builder.ts:541).
fn apply_and(
    mut input: FilterInputHandle,
    conditions: &[Condition],
    delegate: &mut dyn BuilderDelegate,
    name: &str,
) -> FilterInputHandle {
    for sub_condition in conditions {
        input = apply_filter(input, sub_condition, delegate, name);
    }
    input
}

/// Port of TS `applyOr` (builder.ts:556): no subquery conditions -> a single
/// predicate Filter; otherwise FanOut -> per-subquery branches (+ one Filter
/// branch for the plain conditions) -> FanIn.
fn apply_or(
    input: FilterInputHandle,
    conditions: &[Condition],
    delegate: &mut dyn BuilderDelegate,
    name: &str,
) -> FilterInputHandle {
    let (subquery_conditions, other_conditions) = group_subquery_conditions(conditions);
    if subquery_conditions.is_empty() {
        let or_cond = Condition::Or(other_conditions.iter().cloned().cloned().collect());
        let predicate = create_predicate(&or_cond);
        let filter: FilterInputHandle = Filter::new(input, predicate);
        return filter;
    }

    let fan_out = FanOut::new(input);
    let mut branches: Vec<FilterInputHandle> = subquery_conditions
        .iter()
        .map(|sub_condition| {
            let fo: FilterInputHandle = fan_out.clone();
            apply_filter(fo, sub_condition, delegate, name)
        })
        .collect();
    if !other_conditions.is_empty() {
        let or_cond = Condition::Or(other_conditions.iter().cloned().cloned().collect());
        let fo: FilterInputHandle = fan_out.clone();
        let filter: FilterInputHandle = Filter::new(fo, create_predicate(&or_cond));
        branches.push(filter);
    }
    let schema = fan_out.borrow().get_schema();
    let ret = FanIn::new(schema, branches);
    fan_out.borrow_mut().set_fan_in(ret.clone());
    ret
}

/// Port of TS `groupSubqueryConditions` (builder.ts:597).
fn group_subquery_conditions(conditions: &[Condition]) -> (Vec<&Condition>, Vec<&Condition>) {
    let mut partitioned: (Vec<&Condition>, Vec<&Condition>) = (Vec::new(), Vec::new());
    for sub_condition in conditions {
        if is_not_and_does_not_contain_subquery(sub_condition) {
            partitioned.1.push(sub_condition);
        } else {
            partitioned.0.push(sub_condition);
        }
    }
    partitioned
}

/// Check if a condition does NOT contain any subquery.
/// Port of TS `isNotAndDoesNotContainSubquery` (builder.ts:613).
fn is_not_and_does_not_contain_subquery(condition: &Condition) -> bool {
    match condition {
        Condition::CorrelatedSubquery(_) => false,
        Condition::Simple(_) => true,
        Condition::And(conditions) | Condition::Or(conditions) => {
            conditions.iter().all(is_not_and_does_not_contain_subquery)
        }
    }
}

/// Port of TS `applySimpleCondition` (builder.ts:625).
fn apply_simple_condition(
    input: FilterInputHandle,
    _delegate: &mut dyn BuilderDelegate,
    simple: &crate::builder::ast::SimpleCondition,
) -> FilterInputHandle {
    let predicate = create_simple_predicate(simple);
    let filter: FilterInputHandle = Filter::new(input, predicate);
    filter
}

/// Apply a correlated subquery condition (EXISTS/NOT EXISTS) as a filter.
/// Port of TS `applyCorrelatedSubqueryCondition` (builder.ts:689).
/// (The Join for the subquery was already applied in the main pipeline.)
fn apply_correlated_subquery_condition(
    input: FilterInputHandle,
    csq: &CorrelatedSubqueryCondition,
    _delegate: &mut dyn BuilderDelegate,
    _name: &str,
) -> FilterInputHandle {
    let sq = &csq.related;
    let op = csq.op.as_str();

    assert!(
        op == "EXISTS" || op == "NOT EXISTS",
        "Expected EXISTS or NOT EXISTS operator"
    );

    if sq.subquery.limit == Some(0) {
        let filter: FilterInputHandle = if op == "EXISTS" {
            Filter::new(input, Arc::new(|_| false))
        } else {
            Filter::new(input, Arc::new(|_| true))
        };
        return filter;
    }

    let not = op == "NOT EXISTS";
    let exists: FilterInputHandle = Exists::new(
        input,
        sq.relationship_name.clone(),
        sq.parent_key.clone(),
        not,
    );
    exists
}

fn apply_filter_with_flips(
    input: Shared<dyn Input>,
    condition: &Condition,
    delegate: &mut dyn BuilderDelegate,
    name: &str,
) -> Shared<dyn Input> {
    let mut end = input.clone();

    match condition {
        Condition::And(conditions) => {
            let cond_refs: Vec<&Condition> = conditions.iter().collect();
            let (with_flipped, without_flipped): (Vec<&Condition>, Vec<&Condition>) =
                partition_branches(&cond_refs, |c| {
                    condition_includes_flipped_subquery_at_any_level(c)
                });

            // TS wraps the non-flipped conjuncts in ONE filter pipeline
            // (builder.ts:429-441).
            if !without_flipped.is_empty() {
                let conds: Vec<Condition> = without_flipped.iter().cloned().cloned().collect();
                end = build_filter_pipeline(end, |filter_input| {
                    apply_and(filter_input, &conds, delegate, name)
                });
            }

            for cond in &with_flipped {
                end = apply_filter_with_flips(end.clone(), cond, delegate, name);
            }
        }
        Condition::Or(conditions) => {
            let cond_refs: Vec<&Condition> = conditions.iter().collect();
            let (with_flipped, without_flipped): (Vec<&Condition>, Vec<&Condition>) =
                partition_branches(&cond_refs, |c| {
                    condition_includes_flipped_subquery_at_any_level(c)
                });

            // UnionFanOut fans the stream into branches
            let ufo = crate::ivm::union_fan_out::UnionFanOut::new(end.clone());
            end = ufo.clone();

            let mut branches: Vec<Shared<dyn Input>> = Vec::new();

            // TS wraps the non-flipped disjuncts branch in a filter pipeline
            // (builder.ts:461-474).
            if !without_flipped.is_empty() {
                let conds: Vec<Condition> = without_flipped.iter().cloned().cloned().collect();
                let branch = build_filter_pipeline(end.clone(), |filter_input| {
                    apply_or(filter_input, &conds, delegate, name)
                });
                branches.push(branch);
            }

            for cond in &with_flipped {
                let branch = apply_filter_with_flips(end.clone(), cond, delegate, name);
                branches.push(branch);
            }

            let schema = end.borrow().get_schema();
            let ufi = crate::ivm::union_fan_in::UnionFanIn::new(schema);
            // Port of TS `new UnionFanIn(fanOut, branches)`: register each
            // branch as a fan-in input (drives `fetch` + push dedup, and runs
            // the schema/relationship validation), wire the branch's output to
            // the fan-in, and give the fan-out its fan-in back-reference (so
            // push batches open/close via fanOutStarted/DonePushing).
            for branch in &branches {
                ufi.borrow_mut().add_input(branch.clone());
                branch
                    .borrow()
                    .set_output(crate::ivm::union_fan_in::UnionFanIn::output_adapter(
                        ufi.clone(),
                    ));
            }
            ufo.borrow().set_fan_in(ufi.clone());
            end = ufi;
        }
        Condition::CorrelatedSubquery(csq) => {
            // Flipped EXISTS: build a FlippedJoin
            let child = build_pipeline_internal(
                &csq.related.subquery,
                delegate,
                &format!("{}.{}", name, csq.related.relationship_name),
                Some(csq.related.child_key.clone()),
                false,
            );

            let flipped_join = FlippedJoin::new(FlippedJoinArgs {
                parent: end.clone(),
                child,
                parent_key: csq.related.parent_key.clone(),
                child_key: csq.related.child_key.clone(),
                relationship_name: csq.related.relationship_name.clone(),
                hidden: csq.related.hidden,
                system: csq.related.system.unwrap_or(System::Client),
            });

            end = flipped_join;
        }
        Condition::Simple(_) => {
            panic!("Simple conditions cannot have flips");
        }
    }

    end
}

/// Apply a correlated subquery condition (EXISTS / NOT EXISTS).
/// Port of TS `applyCorrelatedSubqueryCondition` (builder.ts:676).
fn apply_correlated_subquery(
    csq_condition: &CorrelatedSubqueryCondition,
    delegate: &mut dyn BuilderDelegate,
    end: Shared<dyn Input>,
    name: &str,
    from_condition: bool,
) -> Shared<dyn Input> {
    let sq = &csq_condition.related;
    let op = csq_condition.op.as_str();
    let _flip = csq_condition.flip.unwrap_or(false);

    assert!(
        op == "EXISTS" || op == "NOT EXISTS",
        "Expected EXISTS or NOT EXISTS operator"
    );

    // TS (builder.ts:658-662): the join is omitted for a `limit(0)` CONDITION
    // subquery — the always-false/true Filter is applied at the FILTER level
    // (apply_correlated_subquery_condition). A related `limit(0)` subquery
    // still builds its join (an empty `related` array).
    if sq.subquery.limit == Some(0) && from_condition {
        return end;
    }

    // Build the EXISTS subquery pipeline with a limit
    let mut subquery = (*sq.subquery).clone();
    let exists_limit = if sq.system == Some(System::Permissions) {
        PERMISSIONS_EXISTS_LIMIT
    } else {
        EXISTS_LIMIT
    };
    subquery.limit = Some(exists_limit);

    let child = build_pipeline_internal(
        &subquery,
        delegate,
        &format!("{}.{}", name, sq.relationship_name),
        Some(sq.child_key.clone()),
        true, // is_non_flipped_exists_child
    );

    // Only create a Join here — the Exists filter is created separately
    // by apply_where → apply_filter → apply_csq_condition.
    // This matches TS: applyCorrelatedSubQuery creates Join (attaches
    // relationship), applyCorrelatedSubqueryCondition creates Exists
    // (checks relationship size).
    let join = Join::new(JoinArgs {
        parent: end.clone(),
        child: child.clone(),
        parent_key: sq.parent_key.clone(),
        child_key: sq.child_key.clone(),
        relationship_name: sq.relationship_name.clone(),
        hidden: sq.hidden,
        system: sq.system.unwrap_or(System::Client),
    });
    let _ = std::env::var("IVM_TRACE");
    join
}

/// Apply a related subquery as a Join (not from a WHERE condition).
/// Port of TS `applyCorrelatedSubQuery` (builder.ts:593).
fn apply_correlated_subquery_join(
    sq: &RelatedSubquery,
    delegate: &mut dyn BuilderDelegate,
    end: Shared<dyn Input>,
    name: &str,
    _from_condition: bool,
) -> Shared<dyn Input> {
    let child = build_pipeline_internal(
        &sq.subquery,
        delegate,
        &format!("{}.{}", name, sq.relationship_name),
        Some(sq.child_key.clone()),
        false,
    );

    let join = Join::new(JoinArgs {
        parent: end.clone(),
        child: child.clone(),
        parent_key: sq.parent_key.clone(),
        child_key: sq.child_key.clone(),
        relationship_name: sq.relationship_name.clone(),
        hidden: sq.hidden,
        system: sq.system.unwrap_or(System::Client),
    });
    let _ = std::env::var("IVM_TRACE");
    join
}

/// Gather all correlated subquery conditions from a condition tree.
/// Port of TS `gatherCorrelatedSubqueryQueryConditions` (builder.ts:700).
fn gather_correlated_subquery_query_conditions(
    condition: Option<&Condition>,
) -> Vec<CorrelatedSubqueryCondition> {
    let mut csqs = Vec::new();
    if let Some(cond) = condition {
        gather_csq_conditions(cond, &mut csqs);
    }
    csqs
}

fn gather_csq_conditions(condition: &Condition, csqs: &mut Vec<CorrelatedSubqueryCondition>) {
    match condition {
        Condition::CorrelatedSubquery(csq) => {
            csqs.push(csq.clone());
        }
        Condition::And(conditions) | Condition::Or(conditions) => {
            for c in conditions {
                gather_csq_conditions(c, csqs);
            }
        }
        Condition::Simple(_) => {}
    }
}

/// Check if a condition tree contains any flipped subquery at any level.
/// Port of TS `conditionIncludesFlippedSubqueryAtAnyLevel` (builder.ts:819).
pub fn condition_includes_flipped_subquery_at_any_level(cond: &Condition) -> bool {
    match cond {
        Condition::CorrelatedSubquery(csq) => csq.flip.unwrap_or(false),
        Condition::And(conditions) | Condition::Or(conditions) => conditions
            .iter()
            .any(condition_includes_flipped_subquery_at_any_level),
        Condition::Simple(_) => false,
    }
}

/// Partition conditions into two groups based on a predicate.
/// Port of TS `partitionBranches` (builder.ts:833).
pub fn partition_branches<'a, F>(
    conditions: &'a [&Condition],
    predicate: F,
) -> (Vec<&'a Condition>, Vec<&'a Condition>)
where
    F: Fn(&Condition) -> bool,
{
    let mut matched = Vec::new();
    let mut not_matched = Vec::new();
    for c in conditions {
        if predicate(c) {
            matched.push(*c);
        } else {
            not_matched.push(*c);
        }
    }
    (matched, not_matched)
}

/// Assert that a condition tree does not contain NOT EXISTS.
/// Port of TS `assertNoNotExists` (builder.ts:215).
pub fn assert_no_not_exists(condition: &Condition) {
    match condition {
        Condition::Simple(_) => {}
        Condition::CorrelatedSubquery(csq) => {
            if csq.op == "NOT EXISTS" {
                panic!(
                    "not(exists()) is not supported on the client - see https://bugs.rocicorp.dev/issue/3438"
                );
            }
        }
        Condition::And(conditions) | Condition::Or(conditions) => {
            for c in conditions {
                assert_no_not_exists(c);
            }
        }
    }
}

/// Complete ordering: append PK columns to orderBy if missing.
/// Port of TS `completeOrdering` — delegates to `complete_ordering` module.
pub fn complete_ordering_ast(ast: &Ast, get_primary_key: &dyn Fn(&str) -> Vec<String>) -> Ast {
    crate::query::complete_ordering::complete_ordering(ast, get_primary_key)
}

/// Uniquify correlated subquery condition aliases by appending "_<counter>".
/// Port of TS `uniquifyCorrelatedSubqueryConditionAliases` (builder.ts:763).
fn uniquify_correlated_subquery_condition_aliases(mut ast: Ast) -> Ast {
    // Port of TS: only uniquify AND/OR conditions (not single CSQ).
    // TS: if (where.type !== 'and' && where.type !== 'or') return ast;
    let mut count = 0u32;
    if let Some(ref mut where_clause) = ast.where_clause {
        match where_clause {
            Condition::And(_) | Condition::Or(_) => {
                uniquify_condition(where_clause, &mut count);
            }
            _ => {} // Single CSQ — no uniquify
        }
    }
    ast
}

fn uniquify_condition(condition: &mut Condition, count: &mut u32) {
    match condition {
        Condition::Simple(_) => {}
        Condition::CorrelatedSubquery(csq) => {
            if let Some(ref mut alias) = csq.related.subquery.alias {
                let new_name = format!("{}_{}", alias, count);
                *alias = new_name.clone();
                csq.related.relationship_name = new_name;
                *count += 1;
            }
        }
        Condition::And(conditions) => {
            for c in conditions.iter_mut() {
                uniquify_condition(c, count);
            }
        }
        Condition::Or(conditions) => {
            for c in conditions.iter_mut() {
                uniquify_condition(c, count);
            }
        }
    }
}
