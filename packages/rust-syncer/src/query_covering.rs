//! Port of `view-syncer/query-covering.ts` (zero/v1.9.0 #6182).
//!
//! Detects when every row a "covered" query can produce is also produced by a
//! "covering" query that is already running against the same root table. This
//! is **shadow-logging only**: the view-syncer uses it to log aggregate
//! coverage stats during hydration and it has NO effect on what is served to
//! clients. Accordingly the analysis is intentionally conservative — any case
//! it does not understand returns `false` rather than guessing.
//!
//! The TS operates on `normalizeAST(ast)` (`Required<AST>`); here we reuse
//! [`crate::permissions::normalize_ast`], which produces the identical
//! canonical JSON (sorted/flattened conditions, sorted+recursively-normalized
//! related, undefined fields omitted). All the implication logic below then
//! runs over `serde_json::Value`, mirroring `jsonEqual` == `deepEqual` with
//! `Value` structural equality.

use crate::permissions::normalize_ast;
use serde_json::{Value, json};
use std::collections::HashMap;

/// A query currently running in the pipeline, as seen by the covering index.
/// Mirrors TS `RunningQuery`.
#[derive(Clone, Debug)]
pub struct RunningQuery {
    pub transformed_ast: Value,
    pub transformation_hash: String,
    pub query_name: Option<String>,
}

/// The running query found to cover a hydrating query. Mirrors TS
/// `CoveringQuery`.
#[derive(Clone, Debug, PartialEq)]
pub struct CoveringQuery {
    pub query_id: String,
    pub transformation_hash: String,
    pub query_name: Option<String>,
}

struct IndexedRunningQuery {
    query_id: String,
    normalized_ast: Value,
    transformation_hash: String,
    query_name: Option<String>,
}

/// A hydrating query that a running query already covers, with both sides'
/// identifiers for the shadow-mode summary log. Port of TS
/// `QueryCoverageShadowHit`.
#[derive(Clone, Debug)]
pub struct QueryCoverageShadowHit {
    pub covered_query_hash: String,
    pub covered_transformation_hash: String,
    pub covered_query_name: Option<String>,
    pub covering_query_hash: String,
    pub covering_transformation_hash: String,
    pub covering_query_name: Option<String>,
}

/// Emit the per-hydration aggregate coverage summary. Port of TS
/// `#logQueryCoverageShadowSummary` — a single structured `info` log (no
/// metrics), skipped when nothing was hydrated. `hydration_path` is `"add"` or
/// `"hydrate-unchanged"`.
#[allow(clippy::too_many_arguments)]
pub fn log_shadow_summary(
    app_id: &str,
    shard_num: u32,
    client_group_id: &str,
    hydration_path: &str,
    total_hydrated_queries: usize,
    covered_hydrated_queries: usize,
    first: Option<&QueryCoverageShadowHit>,
) {
    if total_hydrated_queries == 0 {
        return;
    }
    tracing::info!(
        appID = app_id,
        shardNum = shard_num,
        clientGroupID = client_group_id,
        queryCoverageMode = "shadow",
        hydrationPath = hydration_path,
        totalHydratedQueries = total_hydrated_queries,
        coveredHydratedQueries = covered_hydrated_queries,
        uncoveredHydratedQueries = total_hydrated_queries - covered_hydrated_queries,
        firstCoveredQueryHash = first.map(|f| f.covered_query_hash.as_str()),
        firstCoveredTransformationHash = first.map(|f| f.covered_transformation_hash.as_str()),
        firstCoveringQueryHash = first.map(|f| f.covering_query_hash.as_str()),
        firstCoveringTransformationHash = first.map(|f| f.covering_transformation_hash.as_str()),
        firstCoveredQueryName = first.and_then(|f| f.covered_query_name.as_deref()),
        firstCoveringQueryName = first.and_then(|f| f.covering_query_name.as_deref()),
        "query coverage shadow summary"
    );
}

/// Returns true when every row that can be produced by `covered` is also
/// produced by `covering`. Port of `isQueryCoveredBy`.
pub fn is_query_covered_by(covered: &Value, covering: &Value) -> bool {
    ast_covered_by(&normalize_ast(covered), &normalize_ast(covering))
}

/// One-shot convenience over an ordered set of running queries. Port of the
/// free `findCoveringQuery` function. `running_queries` is `(query_id, query)`
/// in the order the pipeline reports them, so the "first covering query" tie
/// break matches TS Map-insertion order.
#[allow(dead_code)]
pub fn find_covering_query(
    covered_query_id: &str,
    covered_ast: &Value,
    running_queries: &[(String, RunningQuery)],
) -> Option<CoveringQuery> {
    let mut index = QueryCoveringIndex::new();
    for (qid, q) in running_queries {
        index.add(qid, q);
    }
    index.find_covering_query(covered_query_id, covered_ast)
}

/// Index of running queries bucketed by root table, for repeated covering
/// lookups within a hydration batch. Port of the TS class of the same name.
pub struct QueryCoveringIndex {
    /// root-key → ordered list of indexed queries (insertion order preserved so
    /// `find_covering_query` returns the first match, matching TS Map order).
    by_root: HashMap<String, Vec<IndexedRunningQuery>>,
    query_id_to_root: HashMap<String, String>,
}

impl Default for QueryCoveringIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryCoveringIndex {
    pub fn new() -> Self {
        Self {
            by_root: HashMap::new(),
            query_id_to_root: HashMap::new(),
        }
    }

    pub fn add(&mut self, query_id: &str, query: &RunningQuery) {
        self.remove(query_id);

        let normalized_ast = normalize_ast(&query.transformed_ast);
        let root = root_key(&normalized_ast);
        self.by_root
            .entry(root.clone())
            .or_default()
            .push(IndexedRunningQuery {
                query_id: query_id.to_string(),
                normalized_ast,
                transformation_hash: query.transformation_hash.clone(),
                query_name: query.query_name.clone(),
            });
        self.query_id_to_root.insert(query_id.to_string(), root);
    }

    pub fn remove(&mut self, query_id: &str) {
        let Some(root) = self.query_id_to_root.remove(query_id) else {
            return;
        };
        if let Some(queries) = self.by_root.get_mut(&root) {
            queries.retain(|q| q.query_id != query_id);
            if queries.is_empty() {
                self.by_root.remove(&root);
            }
        }
    }

    pub fn find_covering_query(
        &self,
        covered_query_id: &str,
        covered_ast: &Value,
    ) -> Option<CoveringQuery> {
        let normalized_covered = normalize_ast(covered_ast);
        let queries = self.by_root.get(&root_key(&normalized_covered))?;
        for q in queries {
            if q.query_id == covered_query_id {
                continue;
            }
            if ast_covered_by(&normalized_covered, &q.normalized_ast) {
                return Some(CoveringQuery {
                    query_id: q.query_id.clone(),
                    transformation_hash: q.transformation_hash.clone(),
                    query_name: q.query_name.clone(),
                });
            }
        }
        None
    }
}

fn root_key(ast: &Value) -> String {
    json!([
        ast.get("schema").cloned().unwrap_or(Value::Null),
        ast.get("table").cloned().unwrap_or(Value::Null),
        ast.get("alias").cloned().unwrap_or(Value::Null),
    ])
    .to_string()
}

fn ast_covered_by(covered: &Value, covering: &Value) -> bool {
    field_eq(covered, covering, "schema")
        && field_eq(covered, covering, "table")
        && field_eq(covered, covering, "alias")
        && condition_implies(
            present(covered.get("where")),
            present(covering.get("where")),
        )
        && related_covered_by(
            covered.get("related").and_then(Value::as_array),
            covering.get("related").and_then(Value::as_array),
        )
        && bounds_covered_by(covered, covering)
}

fn bounds_covered_by(covered: &Value, covering: &Value) -> bool {
    let covering_limit = present(covering.get("limit"));
    if covering_limit.is_none() {
        if present(covering.get("start")).is_none() {
            return true;
        }
        return json_eq(covered.get("start"), covering.get("start"))
            && json_eq(covered.get("orderBy"), covering.get("orderBy"));
    }

    let covered_limit = present(covered.get("limit"));
    match (covered_limit.and_then(num), covering_limit.and_then(num)) {
        (Some(cov), Some(covering_lim)) if covering_lim >= cov => {}
        _ => return false,
    }

    // A limited broader query does not necessarily contain a limited narrower
    // query. The ordered input to the limit must be equivalent.
    condition_equivalent(
        present(covered.get("where")),
        present(covering.get("where")),
    ) && json_eq(covered.get("start"), covering.get("start"))
        && json_eq(covered.get("orderBy"), covering.get("orderBy"))
}

fn related_covered_by(covered: Option<&Vec<Value>>, covering: Option<&Vec<Value>>) -> bool {
    let covered = match covered {
        None => return true,
        Some(c) if c.is_empty() => return true,
        Some(c) => c,
    };
    let Some(covering) = covering else {
        return false;
    };
    covered.iter().all(|covered_related| {
        covering.iter().any(|covering_related| {
            same_related_edge(covered_related, covering_related)
                && ast_covered_by(subquery(covered_related), subquery(covering_related))
        })
    })
}

fn condition_equivalent(a: Option<&Value>, b: Option<&Value>) -> bool {
    condition_implies(a, b) && condition_implies(b, a)
}

fn condition_implies(covered: Option<&Value>, covering: Option<&Value>) -> bool {
    let Some(covering) = covering else {
        return true;
    };
    let Some(covered) = covered else {
        return false;
    };
    if covered == covering {
        return true;
    }

    if ctype(covered) == "or" {
        return conditions(covered)
            .iter()
            .all(|c| condition_implies(Some(c), Some(covering)));
    }
    if ctype(covering) == "or" {
        return conditions(covering)
            .iter()
            .any(|c| condition_implies(Some(covered), Some(c)));
    }
    if ctype(covering) == "and" {
        return conditions(covering)
            .iter()
            .all(|c| condition_implies(Some(covered), Some(c)));
    }
    if ctype(covered) == "and" {
        return conditions(covered)
            .iter()
            .any(|c| condition_implies(Some(c), Some(covering)));
    }
    if ctype(covered) == "simple" && ctype(covering) == "simple" {
        return simple_condition_implies(covered, covering);
    }
    if ctype(covered) == "correlatedSubquery" && ctype(covering) == "correlatedSubquery" {
        return correlated_condition_implies(covered, covering);
    }
    false
}

fn correlated_condition_implies(covered: &Value, covering: &Value) -> bool {
    if covered.get("op") != covering.get("op")
        || covered.get("scalar") != covering.get("scalar")
        || !same_related_edge(related_of(covered), related_of(covering))
    {
        return false;
    }

    if covered.get("op").and_then(Value::as_str) == Some("EXISTS") {
        return ast_covered_by(
            subquery(related_of(covered)),
            subquery(related_of(covering)),
        );
    }

    // NOT EXISTS: coverage reverses — the covering (broader NOT EXISTS) must
    // require the *narrower* subquery to be empty.
    ast_covered_by(
        subquery(related_of(covering)),
        subquery(related_of(covered)),
    )
}

fn same_related_edge(a: &Value, b: &Value) -> bool {
    a.get("correlation") == b.get("correlation")
        && a.get("hidden") == b.get("hidden")
        && a.get("system") == b.get("system")
        && subquery(a).get("alias") == subquery(b).get("alias")
}

fn simple_condition_implies(covered: &Value, covering: &Value) -> bool {
    let (Some(covered_parts), Some(covering_parts)) = (
        column_literal_parts(covered),
        column_literal_parts(covering),
    ) else {
        return false;
    };
    if covered_parts.column != covering_parts.column {
        return false;
    }

    let (covered_op, covered_value) = (covered_parts.op, covered_parts.value);
    let (covering_op, covering_value) = (covering_parts.op, covering_parts.value);

    if is_equality_op(covered_op) && is_non_null_scalar_literal(covered_value) {
        return equality_implies(covered_value, covering_op, covering_value);
    }

    if covered_op == "IN"
        && covering_op == "IN"
        && covered_value.is_array()
        && covering_value.is_array()
    {
        let covering_arr = covering_value.as_array().unwrap();
        return covered_value
            .as_array()
            .unwrap()
            .iter()
            .all(|v| literal_array_includes(covering_arr, v));
    }

    if is_numeric_order_op(covered_op) && is_numeric_order_op(covering_op) {
        return order_condition_implies(covered_op, covered_value, covering_op, covering_value);
    }

    false
}

fn equality_implies(value: &Value, covering_op: &str, covering_value: &Value) -> bool {
    match covering_op {
        "=" | "IS" => value == covering_value,
        "!=" | "IS NOT" => value != covering_value,
        "IN" => covering_value
            .as_array()
            .is_some_and(|a| literal_array_includes(a, value)),
        "<" => cmp_num(value, covering_value, |a, b| a < b),
        "<=" => cmp_num(value, covering_value, |a, b| a <= b),
        ">" => cmp_num(value, covering_value, |a, b| a > b),
        ">=" => cmp_num(value, covering_value, |a, b| a >= b),
        "NOT IN" | "LIKE" | "NOT LIKE" | "ILIKE" | "NOT ILIKE" => false,
        _ => false,
    }
}

fn order_condition_implies(
    covered_op: &str,
    covered_value: &Value,
    covering_op: &str,
    covering_value: &Value,
) -> bool {
    let (Some(cov), Some(covering)) = (num(covered_value), num(covering_value)) else {
        return false;
    };

    // Simplified from the TS's per-op disjunction: for `>`/`<` both covering-op
    // branches share the same threshold comparison, so they collapse to a single
    // `>=`/`<=` test; the semantics are identical to `orderConditionImplies`.
    match covered_op {
        ">" => (covering_op == ">" || covering_op == ">=") && cov >= covering,
        ">=" => (covering_op == ">" && cov > covering) || (covering_op == ">=" && cov >= covering),
        "<" => (covering_op == "<" || covering_op == "<=") && cov <= covering,
        "<=" => (covering_op == "<" && cov < covering) || (covering_op == "<=" && cov <= covering),
        _ => false,
    }
}

struct ColumnLiteralParts<'a> {
    column: &'a str,
    op: &'a str,
    value: &'a Value,
}

fn column_literal_parts(condition: &Value) -> Option<ColumnLiteralParts<'_>> {
    let left = condition.get("left")?;
    let right = condition.get("right")?;
    if left.get("type").and_then(Value::as_str) != Some("column")
        || right.get("type").and_then(Value::as_str) != Some("literal")
    {
        return None;
    }
    Some(ColumnLiteralParts {
        column: left.get("name").and_then(Value::as_str)?,
        op: condition.get("op").and_then(Value::as_str)?,
        value: right.get("value").unwrap_or(&Value::Null),
    })
}

fn is_equality_op(op: &str) -> bool {
    op == "=" || op == "IS"
}

fn is_numeric_order_op(op: &str) -> bool {
    op == "<" || op == ">" || op == "<=" || op == ">="
}

fn is_non_null_scalar_literal(value: &Value) -> bool {
    !value.is_null() && !value.is_array()
}

fn literal_array_includes(values: &[Value], value: &Value) -> bool {
    values.iter().any(|v| v == value)
}

// ─── small helpers ───────────────────────────────────────────────────────────

/// Maps `Some(Null)` to `None` so an omitted-and-null field read identically,
/// matching TS `undefined` semantics for optional AST fields.
fn present(v: Option<&Value>) -> Option<&Value> {
    match v {
        Some(Value::Null) => None,
        other => other,
    }
}

fn field_eq(a: &Value, b: &Value, key: &str) -> bool {
    present(a.get(key)) == present(b.get(key))
}

fn json_eq(a: Option<&Value>, b: Option<&Value>) -> bool {
    present(a) == present(b)
}

fn ctype(v: &Value) -> &str {
    v.get("type").and_then(Value::as_str).unwrap_or("")
}

fn conditions(v: &Value) -> &[Value] {
    v.get("conditions")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

/// The `related` entry of a `correlatedSubquery` condition.
fn related_of(cond: &Value) -> &Value {
    cond.get("related").unwrap_or(&Value::Null)
}

/// The `subquery` of a related edge (a `CorrelatedSubquery`).
fn subquery(related: &Value) -> &Value {
    related.get("subquery").unwrap_or(&Value::Null)
}

fn num(v: &Value) -> Option<f64> {
    // Matches TS `typeof x === 'number'`: booleans are not numbers.
    if v.is_boolean() {
        return None;
    }
    v.as_f64()
}

fn cmp_num(a: &Value, b: &Value, f: impl Fn(f64, f64) -> bool) -> bool {
    match (num(a), num(b)) {
        (Some(a), Some(b)) => f(a, b),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn all_issues() -> Value {
        json!({"table": "issues", "orderBy": [["id", "asc"]]})
    }
    fn all_comments() -> Value {
        json!({"table": "comments", "orderBy": [["id", "asc"]]})
    }
    fn where_ast(condition: Value) -> Value {
        let mut ast = all_issues();
        ast["where"] = condition;
        ast
    }
    fn eq(column: &str, value: Value) -> Value {
        json!({"type": "simple", "left": {"type": "column", "name": column}, "op": "=", "right": {"type": "literal", "value": value}})
    }
    fn gt(column: &str, value: Value) -> Value {
        json!({"type": "simple", "left": {"type": "column", "name": column}, "op": ">", "right": {"type": "literal", "value": value}})
    }
    fn and(conditions: Vec<Value>) -> Value {
        json!({"type": "and", "conditions": conditions})
    }
    fn or(conditions: Vec<Value>) -> Value {
        json!({"type": "or", "conditions": conditions})
    }
    fn comments_related(mut subquery: Value) -> Value {
        subquery["alias"] = json!("comments");
        json!({"system": "client", "correlation": {"parentField": ["id"], "childField": ["issueID"]}, "subquery": subquery})
    }

    #[test]
    fn same_query_covers_itself() {
        assert!(is_query_covered_by(
            &where_ast(eq("id", json!("123"))),
            &where_ast(eq("id", json!("123")))
        ));
    }

    #[test]
    fn unfiltered_covers_filtered_same_table() {
        assert!(is_query_covered_by(
            &where_ast(eq("id", json!("123"))),
            &all_issues()
        ));
    }

    #[test]
    fn conjunction_covered_by_subset() {
        let covered = where_ast(and(vec![
            eq("status", json!("open")),
            eq("owner", json!("alice")),
        ]));
        let covering = where_ast(eq("status", json!("open")));
        assert!(is_query_covered_by(&covered, &covering));
        assert!(!is_query_covered_by(&covering, &covered));
    }

    #[test]
    fn equality_and_range_implication() {
        assert!(is_query_covered_by(
            &where_ast(eq("id", json!("1"))),
            &where_ast(
                json!({"type": "simple", "left": {"type": "column", "name": "id"}, "op": "IN", "right": {"type": "literal", "value": ["1", "2"]}})
            ),
        ));
        assert!(is_query_covered_by(
            &where_ast(gt("priority", json!(5))),
            &where_ast(gt("priority", json!(3))),
        ));
    }

    #[test]
    fn or_coverage_is_conservative() {
        let bug = eq("type", json!("bug"));
        let feature = eq("type", json!("feature"));
        assert!(is_query_covered_by(
            &where_ast(bug.clone()),
            &where_ast(or(vec![bug.clone(), feature]))
        ));
        assert!(!is_query_covered_by(
            &where_ast(or(vec![bug.clone(), eq("type", json!("feature"))])),
            &where_ast(bug)
        ));
    }

    #[test]
    fn unlimited_covers_limited_and_paged() {
        let mut covered = where_ast(eq("status", json!("open")));
        covered["limit"] = json!(10);
        covered["start"] = json!({"row": {"id": "abc"}, "exclusive": true});
        assert!(is_query_covered_by(&covered, &all_issues()));
    }

    #[test]
    fn limited_covering_needs_equivalent_input_and_large_limit() {
        let mut covered = where_ast(eq("status", json!("open")));
        covered["limit"] = json!(10);
        let mut same_input_larger = where_ast(eq("status", json!("open")));
        same_input_larger["limit"] = json!(20);
        let mut broader_same_limit = all_issues();
        broader_same_limit["limit"] = json!(10);
        assert!(is_query_covered_by(&covered, &same_input_larger));
        assert!(!is_query_covered_by(&covered, &broader_same_limit));
    }

    #[test]
    fn related_coverage_is_recursive() {
        let mut comments_with_text = all_comments();
        comments_with_text["where"] = eq("text", json!("hello"));
        let mut covered = where_ast(eq("status", json!("open")));
        covered["related"] = json!([comments_related(comments_with_text)]);
        let mut covering = all_issues();
        covering["related"] = json!([comments_related(all_comments())]);
        assert!(is_query_covered_by(&covered, &covering));
        assert!(!is_query_covered_by(&covered, &all_issues()));
    }

    #[test]
    fn not_exists_reverses_subquery_implication() {
        let no_comments = where_ast(
            json!({"type": "correlatedSubquery", "op": "NOT EXISTS", "related": comments_related(all_comments())}),
        );
        let mut hello = all_comments();
        hello["where"] = eq("text", json!("hello"));
        let no_hello_comments = where_ast(
            json!({"type": "correlatedSubquery", "op": "NOT EXISTS", "related": comments_related(hello)}),
        );
        assert!(is_query_covered_by(&no_comments, &no_hello_comments));
        assert!(!is_query_covered_by(&no_hello_comments, &no_comments));
    }

    #[test]
    fn correlated_subquery_flip_does_not_affect_semantics() {
        let unflipped = where_ast(
            json!({"type": "correlatedSubquery", "op": "EXISTS", "related": comments_related(all_comments())}),
        );
        let flipped = where_ast(
            json!({"type": "correlatedSubquery", "op": "EXISTS", "flip": true, "related": comments_related(all_comments())}),
        );
        assert!(is_query_covered_by(&unflipped, &flipped));
        assert!(is_query_covered_by(&flipped, &unflipped));
    }

    #[test]
    fn find_covering_query_returns_first_active() {
        let running = vec![
            (
                "query-1".to_string(),
                RunningQuery {
                    transformed_ast: all_comments(),
                    transformation_hash: "hash-1".to_string(),
                    query_name: None,
                },
            ),
            (
                "query-2".to_string(),
                RunningQuery {
                    transformed_ast: all_issues(),
                    transformation_hash: "hash-2".to_string(),
                    query_name: Some("allIssues".to_string()),
                },
            ),
        ];
        assert_eq!(
            find_covering_query("query-3", &where_ast(eq("id", json!("123"))), &running),
            Some(CoveringQuery {
                query_id: "query-2".to_string(),
                transformation_hash: "hash-2".to_string(),
                query_name: Some("allIssues".to_string()),
            })
        );
    }

    #[test]
    fn index_only_considers_matching_root() {
        let mut index = QueryCoveringIndex::new();
        index.add(
            "query-1",
            &RunningQuery {
                transformed_ast: all_comments(),
                transformation_hash: "hash-1".to_string(),
                query_name: None,
            },
        );
        assert_eq!(
            index.find_covering_query("query-2", &where_ast(eq("id", json!("123")))),
            None
        );
    }

    #[test]
    fn index_can_be_updated_during_batch() {
        let mut index = QueryCoveringIndex::new();
        assert_eq!(
            index.find_covering_query("query-2", &where_ast(eq("id", json!("123")))),
            None
        );
        index.add(
            "query-1",
            &RunningQuery {
                transformed_ast: all_issues(),
                transformation_hash: "hash-1".to_string(),
                query_name: None,
            },
        );
        assert_eq!(
            index.find_covering_query("query-2", &where_ast(eq("id", json!("123")))),
            Some(CoveringQuery {
                query_id: "query-1".to_string(),
                transformation_hash: "hash-1".to_string(),
                query_name: None,
            })
        );
    }

    #[test]
    fn index_replaces_query_when_root_changes() {
        let mut index = QueryCoveringIndex::new();
        index.add(
            "query-1",
            &RunningQuery {
                transformed_ast: all_issues(),
                transformation_hash: "issues-hash".to_string(),
                query_name: None,
            },
        );
        index.add(
            "query-1",
            &RunningQuery {
                transformed_ast: all_comments(),
                transformation_hash: "comments-hash".to_string(),
                query_name: None,
            },
        );
        assert_eq!(
            index.find_covering_query("query-2", &where_ast(eq("id", json!("123")))),
            None
        );
        assert_eq!(
            index.find_covering_query("query-2", &all_comments()),
            Some(CoveringQuery {
                query_id: "query-1".to_string(),
                transformation_hash: "comments-hash".to_string(),
                query_name: None,
            })
        );
    }
}
