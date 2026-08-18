//! Query builder — port of `zqlite/src/query-builder.ts`.
//!
//! Compiles a FetchRequest into a SQL string + bound parameters.
//! Supports: constraints, multiConstraints (batched IN), filters,
//! ORDER BY (with reverse), start (pagination).

use std::collections::HashMap;

use crate::builder::ast::{Condition, SimpleCondition, ValuePosition};
use crate::ivm::constraint::MultiConstraint;
use crate::ivm::data::Value;
use crate::ivm::operator::{Basis, FetchRequest, Start};
use crate::ivm::schema::ColumnType;

/// A compiled SQL query — text + parameter values.
#[derive(Clone, Debug)]
pub struct SqlQuery {
    pub text: String,
    pub params: Vec<SqlParam>,
}

/// A SQL parameter value — maps to rusqlite ToSql.
#[derive(Clone, Debug)]
pub enum SqlParam {
    Null,
    Int(i64),
    F64(f64),
    Text(String),
    Bool(bool),
}

impl From<&Value> for SqlParam {
    fn from(v: &Value) -> Self {
        match v {
            Value::Null => SqlParam::Null,
            Value::Bool(b) => SqlParam::Bool(*b),
            Value::F64(n) => {
                if n.fract() == 0.0
                    && n.is_finite()
                    && *n >= i64::MIN as f64
                    && *n <= i64::MAX as f64
                {
                    SqlParam::Int(*n as i64)
                } else {
                    SqlParam::F64(*n)
                }
            }
            Value::Str(s) => SqlParam::Text(s.to_string()),
            Value::Json(s) => SqlParam::Text(s.to_string()),
        }
    }
}

/// Build a SELECT query from a FetchRequest.
/// Port of TS `buildSelectQuery` (query-builder.ts:23).
pub fn build_select_query(
    table_name: &str,
    columns: &[String],
    column_types: &HashMap<String, ColumnType>,
    req: &FetchRequest,
    filters: Option<&Condition>,
    order: Option<&[(String, String)]>,
    reverse: bool,
) -> SqlQuery {
    let mut sql = String::new();
    let mut params: Vec<SqlParam> = Vec::new();

    // SELECT col1, col2, ... FROM table (or SELECT * if no columns specified)
    sql.push_str("SELECT ");
    if columns.is_empty() {
        sql.push('*');
    } else {
        for (i, col) in columns.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&quote_ident(col));
        }
    }
    sql.push_str(" FROM ");
    sql.push_str(&quote_ident(table_name));

    // Build WHERE clauses
    let mut where_clauses: Vec<String> = Vec::new();

    // Constraint (equality)
    if let Some(constraint) = &req.constraint {
        for (key, value) in constraint {
            where_clauses.push(format!("{} = ?", quote_ident(key)));
            params.push(SqlParam::from(value));
        }
    }

    // Multi-constraints (batched IN)
    for mc in &req.multi_constraints {
        if mc.is_empty() {
            continue;
        }
        let (mc_sql, mc_params) = multi_constraint_to_sql(mc);
        where_clauses.push(mc_sql);
        params.extend(mc_params);
    }

    // Start (pagination)
    if let Some(start) = &req.start
        && let Some(order) = order
    {
        let (start_sql, start_params) =
            gather_start_constraints(start, reverse, order, column_types);
        where_clauses.push(start_sql);
        params.extend(start_params);
    }

    // Filters (WHERE clause from the AST, with CSQ conditions stripped)
    if let Some(filters) = filters {
        let (filter_sql, filter_params) = condition_to_sql(filters);
        if !filter_sql.is_empty() {
            where_clauses.push(filter_sql);
            params.extend(filter_params);
        }
    }

    if !where_clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clauses.join(" AND "));
    }

    // ORDER BY
    if let Some(order) = order
        && !order.is_empty()
    {
        sql.push_str(" ORDER BY ");
        for (i, ord) in order.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            let dir = if reverse {
                if ord.1 == "asc" { "desc" } else { "asc" }
            } else {
                ord.1.as_str()
            };
            sql.push_str(&format!("{} {}", quote_ident(&ord.0), dir));
        }
    }

    // NOTE: Do NOT push LIMIT to SQL. The Cap/Take limit applies AFTER
    // all filters (including EXISTS subqueries). Pushing LIMIT to SQL
    // would limit the base table fetch BEFORE EXISTS filtering, causing
    // the Exists to see 0 children when the first N rows don't pass.
    // The Cap/Take operators already break early in Rust after N rows.

    SqlQuery { text: sql, params }
}

/// Build a batched IN clause from a MultiConstraint.
/// Single-column: `col IN (?, ?, ?)`
/// Compound: `(a, b) IN (VALUES (?, ?), (?, ?))`
/// Port of TS `multiConstraintToSQL` (query-builder.ts:98).
fn multi_constraint_to_sql(mc: &MultiConstraint) -> (String, Vec<SqlParam>) {
    assert!(!mc.is_empty(), "multiConstraint must be non-empty");

    let keys: Vec<&String> = mc[0].keys().collect();
    assert!(
        !keys.is_empty(),
        "multiConstraint entries must have at least one key"
    );

    let mut params: Vec<SqlParam> = Vec::new();
    let mut sql = String::new();

    if keys.len() == 1 {
        let key = keys[0];
        sql.push_str(&quote_ident(key));
        sql.push_str(" IN (");
        for (i, constraint) in mc.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push('?');
            if let Some(v) = constraint.get(key.as_str()) {
                params.push(SqlParam::from(v));
            } else {
                params.push(SqlParam::Null);
            }
        }
        sql.push(')');
    } else {
        sql.push('(');
        for (i, key) in keys.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&quote_ident(key));
        }
        sql.push_str(") IN (VALUES ");
        for (i, constraint) in mc.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push('(');
            for (j, key) in keys.iter().enumerate() {
                if j > 0 {
                    sql.push_str(", ");
                }
                sql.push('?');
                if let Some(v) = constraint.get(key.as_str()) {
                    params.push(SqlParam::from(v));
                } else {
                    params.push(SqlParam::Null);
                }
            }
            sql.push(')');
        }
        sql.push(')');
    }

    (sql, params)
}

/// Build start constraints for pagination.
/// Port of TS `gatherStartConstraints` (query-builder.ts:332).
///
/// For `after` (a=1, b=2, c=3) with [a asc, b desc, c asc]:
/// `WHERE a > 1 OR (a = 1 AND b < 2) OR (a = 1 AND b = 2 AND c > 3)`
#[allow(clippy::needless_range_loop)]
fn gather_start_constraints(
    start: &Start,
    reverse: bool,
    order: &[(String, String)],
    column_types: &HashMap<String, ColumnType>,
) -> (String, Vec<SqlParam>) {
    let mut params: Vec<SqlParam> = Vec::new();
    let mut constraints: Vec<String> = Vec::new();
    let from = &start.row;

    for i in 0..order.len() {
        let mut group: Vec<String> = Vec::new();
        let (i_field, i_dir) = &order[i];

        for j in 0..=i {
            if j == i {
                let val = from.get(i_field).unwrap_or(&Value::Null);
                let operator = if i_dir == "asc" {
                    if reverse { "<" } else { ">" }
                } else if reverse {
                    ">"
                } else {
                    "<"
                };
                let optional = column_is_optional(column_types, i_field);
                group.push(nullable_aware_range_comparison(
                    i_field,
                    val,
                    column_types.get(i_field),
                    operator,
                    optional,
                    &mut params,
                ));
            } else {
                let (j_field, _) = &order[j];
                let val = from.get(j_field).unwrap_or(&Value::Null);
                let optional = column_is_optional(column_types, j_field);
                group.push(nullable_aware_equality(
                    j_field,
                    val,
                    column_types.get(j_field),
                    optional,
                    &mut params,
                ));
            }
        }
        constraints.push(format!("({})", group.join(" AND ")));
    }

    // For 'at' basis, add exact match
    if start.basis == Basis::At {
        let mut group: Vec<String> = Vec::new();
        for (field, _) in order {
            let val = from.get(field).unwrap_or(&Value::Null);
            let optional = column_is_optional(column_types, field);
            group.push(nullable_aware_equality(
                field,
                val,
                column_types.get(field),
                optional,
                &mut params,
            ));
        }
        constraints.push(format!("({})", group.join(" AND ")));
    }

    (format!("({})", constraints.join(" OR ")), params)
}

fn column_is_optional(column_types: &HashMap<String, ColumnType>, field: &str) -> bool {
    match column_types.get(field) {
        Some(ColumnType::Boolean { optional })
        | Some(ColumnType::Number { optional })
        | Some(ColumnType::String { optional })
        | Some(ColumnType::Json { optional }) => *optional,
        None => false,
    }
}

fn nullable_aware_equality(
    field: &str,
    value: &Value,
    column_type: Option<&ColumnType>,
    optional: bool,
    params: &mut Vec<SqlParam>,
) -> String {
    // VALUE-aware NULL guard (2026-08-12, take-bound divergence fix): a NULL
    // bound value with `=` yields always-false SQL (`col = NULL`), so a Take
    // bound/At fetch silently returns EMPTY and the operator's persisted
    // bound diverges from the source — the prod take.rs:545/:702 panic class
    // (and, worse, silent wrong LIMIT rows; see take_bound_fuzz_test.rs).
    // This fires only when the declared optionality is wrong for the data
    // (spec drift) — TS has the same declared-optionality keying and the same
    // latent hole, but the correctness cost here is a wedged pipeline, so we
    // choose robustness: when the VALUE is NULL, use `IS` regardless of the
    // declared type. For non-NULL values behavior is unchanged (index-
    // friendly `=` on non-optional columns, per the MULTI-INDEX OR note).
    params.push(to_sqlite_column_value(value, column_type));
    format!(
        "{} {} ?",
        quote_ident(field),
        if optional || value.is_null() {
            "IS"
        } else {
            "="
        }
    )
}

fn nullable_aware_range_comparison(
    field: &str,
    value: &Value,
    column_type: Option<&ColumnType>,
    operator: &str,
    optional: bool,
    params: &mut Vec<SqlParam>,
) -> String {
    let param = || to_sqlite_column_value(value, column_type);
    // VALUE-aware NULL guard — see nullable_aware_equality. `col > NULL` /
    // `col < NULL` are always-false; when the bound value is NULL the
    // NULL-ordered branches below are the only correct SQL, regardless of
    // the declared optionality.
    if !optional && !value.is_null() {
        params.push(param());
        return format!("{} {} ?", quote_ident(field), operator);
    }

    if operator == ">" {
        params.push(param());
        params.push(param());
        format!("(? IS NULL OR {} > ?)", quote_ident(field))
    } else {
        params.push(param());
        format!(
            "({} IS NULL OR {} < ?)",
            quote_ident(field),
            quote_ident(field)
        )
    }
}

/// Port of zqlite `toSQLiteType(value, column.type)` for values whose declared
/// column type is known. JSON is stored as JSON text even when the parsed JS
/// value is a scalar, so cursors must bind `"x"`, `true`, and `1` as text.
fn to_sqlite_column_value(value: &Value, column_type: Option<&ColumnType>) -> SqlParam {
    if matches!(column_type, Some(ColumnType::Json { .. })) {
        return SqlParam::Text(
            serde_json::to_string(value)
                .expect("IVM values must be serializable for a JSON SQLite column"),
        );
    }
    SqlParam::from(value)
}

/// Convert a Condition (with CSQ stripped) to a SQL WHERE clause.
/// Port of TS `filtersToSQL` (query-builder.ts:169).
/// `pub(crate)`: also used by the planner cost model to build its probe SQL
/// (sqlite_cost_model.rs) so probe and execution SQL stay one implementation.
pub(crate) fn condition_to_sql(cond: &Condition) -> (String, Vec<SqlParam>) {
    match cond {
        Condition::Simple(s) => simple_condition_to_sql(s),
        Condition::And(conds) => {
            if conds.is_empty() {
                return ("TRUE".to_string(), vec![]);
            }
            let parts: Vec<(String, Vec<SqlParam>)> = conds.iter().map(condition_to_sql).collect();
            let sql = format!(
                "({})",
                parts
                    .iter()
                    .map(|(s, _)| s.as_str())
                    .collect::<Vec<_>>()
                    .join(" AND ")
            );
            let params = parts.into_iter().flat_map(|(_, p)| p).collect();
            (sql, params)
        }
        Condition::Or(conds) => {
            if conds.is_empty() {
                return ("FALSE".to_string(), vec![]);
            }
            let parts: Vec<(String, Vec<SqlParam>)> = conds.iter().map(condition_to_sql).collect();
            let sql = format!(
                "({})",
                parts
                    .iter()
                    .map(|(s, _)| s.as_str())
                    .collect::<Vec<_>>()
                    .join(" OR ")
            );
            let params = parts.into_iter().flat_map(|(_, p)| p).collect();
            (sql, params)
        }
        Condition::CorrelatedSubquery(_) => {
            // Should have been stripped by transform_filters.
            ("TRUE".to_string(), vec![])
        }
    }
}

/// Convert a SimpleCondition to SQL.
/// Port of TS `simpleConditionToSQL` (query-builder.ts:194).
fn simple_condition_to_sql(s: &SimpleCondition) -> (String, Vec<SqlParam>) {
    let op = s.op.as_str();

    // Left side: a column quotes to an identifier (no param); a literal binds a
    // `?` parameter. Both are threaded through so a literal LEFT keeps its
    // parameter — e.g. the `1 = 0` ALWAYS_FALSE produced by scalar-subquery
    // resolution (or `1 = 1`). Without this the `? op ?` string would receive
    // only the right param and SQLite would bind the left `?` to NULL,
    // silently breaking the condition (and poisoning any enclosing OR).
    let (left_sql, left_params) = value_position_to_sql_param(&s.left);

    // IN / NOT IN: inline JSON array via json_each
    if op == "IN" || op == "NOT IN" {
        if let ValuePosition::Literal {
            value: Value::Json(json),
        } = &s.right
        {
            let mut params = left_params;
            params.push(SqlParam::Text(json.to_string()));
            return (
                format!("{} {} (SELECT value FROM json_each(?))", left_sql, op),
                params,
            );
        }
        // Fallback: convert any value to JSON and use json_each with parameter
        if let ValuePosition::Literal { value } = &s.right {
            let json_str = match value {
                Value::Json(s) => s.to_string(),
                Value::Str(s) => format!("[\"{}\"]", s),
                Value::F64(n) => format!("[{}]", n),
                Value::Bool(b) => format!("[{}]", b),
                Value::Null => "[]".to_string(),
            };
            let mut params = left_params;
            params.push(SqlParam::Text(json_str));
            return (
                format!("{} {} (SELECT value FROM json_each(?))", left_sql, op),
                params,
            );
        }
    }

    // LIKE / NOT LIKE / ILIKE / NOT ILIKE
    if op == "ILIKE" || op == "NOT ILIKE" {
        let negated = op == "NOT ILIKE";
        let like_op = if negated { "NOT LIKE" } else { "LIKE" };
        let (right_sql, right_params) = value_position_to_sql_param(&s.right);
        let mut params = left_params;
        params.extend(right_params);
        return (
            format!(
                "lower({}) {} lower({}) ESCAPE '\\'",
                left_sql, like_op, right_sql
            ),
            params,
        );
    }
    if op == "LIKE" || op == "NOT LIKE" {
        let (right_sql, right_params) = value_position_to_sql_param(&s.right);
        let mut params = left_params;
        params.extend(right_params);
        return (
            format!("{} {} {} ESCAPE '\\'", left_sql, op, right_sql),
            params,
        );
    }

    // IS / IS NOT (null comparison)
    if op == "IS" || op == "IS NOT" {
        // Generate IS NULL / IS NOT NULL explicitly for null values
        if let ValuePosition::Literal { value: Value::Null } = &s.right {
            let is_not = op == "IS NOT";
            return (
                format!("{} IS{} NULL", left_sql, if is_not { " NOT" } else { "" }),
                left_params,
            );
        }
        let (right_sql, right_params) = value_position_to_sql_param(&s.right);
        let mut params = left_params;
        params.extend(right_params);
        return (format!("{} {} {}", left_sql, op, right_sql), params);
    }

    // Standard comparison: <col|literal> op value
    let (right_sql, right_params) = value_position_to_sql_param(&s.right);
    let mut params = left_params;
    params.extend(right_params);
    (format!("{} {} {}", left_sql, op, right_sql), params)
}

/// Convert a ValuePosition to SQL with params (returns placeholder + params).
fn value_position_to_sql_param(vp: &ValuePosition) -> (String, Vec<SqlParam>) {
    match vp {
        ValuePosition::Column { name } => (quote_ident(name), vec![]),
        ValuePosition::Literal { value } => ("?".to_string(), vec![SqlParam::from(value)]),
    }
}

/// Quote an identifier for SQLite (double quotes, escape internal quotes).
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Convert a Value to its SQLite storage type.
/// Port of TS `toSQLiteType` (query-builder.ts:280).
pub fn to_sqlite_value(v: &Value) -> SqlParam {
    SqlParam::from(v)
}

/// Implement rusqlite::ToSql for SqlParam.
impl rusqlite::ToSql for SqlParam {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            SqlParam::Null => rusqlite::types::Null.to_sql(),
            SqlParam::Int(n) => n.to_sql(),
            SqlParam::F64(n) => n.to_sql(),
            SqlParam::Text(s) => s.to_sql(),
            SqlParam::Bool(b) => Ok(rusqlite::types::ToSqlOutput::Owned(
                rusqlite::types::Value::Integer(if *b { 1 } else { 0 }),
            )),
        }
    }
}

#[cfg(test)]
mod literal_left_tests {
    use super::*;
    use crate::builder::ast::{SimpleCondition, ValuePosition};
    use crate::ivm::data::Value;
    use crate::ivm::operator::{Basis, Start};
    use crate::ivm::schema::ColumnType;
    use rustc_hash::FxHashMap;
    use std::sync::Arc;

    fn lit(v: Value) -> ValuePosition {
        ValuePosition::Literal { value: v }
    }
    fn col(n: &str) -> ValuePosition {
        ValuePosition::Column {
            name: n.to_string(),
        }
    }
    fn placeholders(sql: &str) -> usize {
        sql.matches('?').count()
    }

    // The invariant the literal-left bug violated: every `?` placeholder must
    // have a bound parameter. A literal on the LEFT (e.g. `1 = 0` ALWAYS_FALSE
    // from scalar-subquery resolution, or `1 = 1`) previously emitted a `?`
    // with no param, so SQLite bound the left `?` to NULL.
    #[test]
    fn literal_left_binds_both_params() {
        for op in ["=", "!=", "<", "<=", ">", ">="] {
            let cond = SimpleCondition {
                op: op.to_string(),
                left: lit(Value::F64(1.0)),
                right: lit(Value::F64(0.0)),
            };
            let (sql, params) = simple_condition_to_sql(&cond);
            assert_eq!(
                placeholders(&sql),
                params.len(),
                "op {op}: {sql} / {params:?}"
            );
            assert_eq!(params.len(), 2, "op {op}: expected 2 params");
        }
    }

    #[test]
    fn column_left_unchanged() {
        let cond = SimpleCondition {
            op: "=".to_string(),
            left: col("c0"),
            right: lit(Value::F64(5.0)),
        };
        let (sql, params) = simple_condition_to_sql(&cond);
        assert_eq!(placeholders(&sql), params.len());
        assert_eq!(params.len(), 1);
        assert!(sql.contains("\"c0\""));
    }

    #[test]
    fn literal_left_like_and_in_balance() {
        // LIKE: `? LIKE ? ESCAPE ...` — 2 placeholders, 2 params.
        let like = SimpleCondition {
            op: "LIKE".to_string(),
            left: lit(Value::Str("a".into())),
            right: lit(Value::Str("a%".into())),
        };
        let (sql, params) = simple_condition_to_sql(&like);
        assert_eq!(placeholders(&sql), params.len(), "LIKE: {sql} / {params:?}");

        // IN with a scalar literal right: `? IN (SELECT value FROM json_each(?))`.
        let in_cond = SimpleCondition {
            op: "IN".to_string(),
            left: lit(Value::Str("x".into())),
            right: lit(Value::Str("y".into())),
        };
        let (sql, params) = simple_condition_to_sql(&in_cond);
        assert_eq!(placeholders(&sql), params.len(), "IN: {sql} / {params:?}");
    }

    #[test]
    fn start_constraints_match_typescript_nullable_rules() {
        let mut row = FxHashMap::default();
        row.insert("optional".to_string(), Value::F64(0.0));
        row.insert("required".to_string(), Value::F64(0.0));
        let start = Start {
            row: Arc::new(row),
            basis: Basis::After,
        };
        let columns = HashMap::from([
            (
                "optional".to_string(),
                ColumnType::Number { optional: true },
            ),
            (
                "required".to_string(),
                ColumnType::Number { optional: false },
            ),
        ]);

        let (optional_sql, optional_params) = gather_start_constraints(
            &start,
            false,
            &[("optional".to_string(), "desc".to_string())],
            &columns,
        );
        assert_eq!(
            optional_sql,
            "(((\"optional\" IS NULL OR \"optional\" < ?)))"
        );
        assert_eq!(optional_params.len(), 1);

        let (required_sql, required_params) = gather_start_constraints(
            &start,
            false,
            &[("required".to_string(), "desc".to_string())],
            &columns,
        );
        assert_eq!(required_sql, "((\"required\" < ?))");
        assert_eq!(required_params.len(), 1);
    }

    #[test]
    fn null_start_constraints_match_typescript_nullable_rules() {
        let mut row = FxHashMap::default();
        row.insert("optional".to_string(), Value::Null);
        row.insert("required".to_string(), Value::Null);
        let start = Start {
            row: Arc::new(row),
            basis: Basis::At,
        };
        let columns = HashMap::from([
            (
                "optional".to_string(),
                ColumnType::Number { optional: true },
            ),
            (
                "required".to_string(),
                ColumnType::Number { optional: false },
            ),
        ]);

        let (optional_sql, optional_params) = gather_start_constraints(
            &start,
            false,
            &[("optional".to_string(), "asc".to_string())],
            &columns,
        );
        assert_eq!(
            optional_sql,
            "(((? IS NULL OR \"optional\" > ?)) OR (\"optional\" IS ?))"
        );
        assert_eq!(optional_params.len(), 3);

        // VALUE-aware NULL guard (take-bound divergence fix): a NULL start
        // value now takes the NULL-aware branches even on a declared
        // non-optional column. The old TS-shaped output
        // `(("required" > ?) OR ("required" = ?))` with NULL params is
        // always-false SQL — an At/bound fetch through it returns EMPTY,
        // which is exactly the take.rs:545/:702 divergence class
        // (see tests/take_bound_fuzz_test.rs).
        let (required_sql, required_params) = gather_start_constraints(
            &start,
            false,
            &[("required".to_string(), "asc".to_string())],
            &columns,
        );
        assert_eq!(
            required_sql,
            "(((? IS NULL OR \"required\" > ?)) OR (\"required\" IS ?))"
        );
        assert_eq!(required_params.len(), 3);
    }

    #[test]
    fn json_start_values_are_stringified_like_typescript() {
        let json = ColumnType::Json { optional: false };
        let cases = [
            (Value::Null, "null"),
            (Value::Bool(true), "true"),
            (Value::F64(1.0), "1"),
            (Value::Str("x".into()), "\"x\""),
            (Value::Json("{\"x\":1}".into()), "{\"x\":1}"),
        ];

        for (value, expected) in cases {
            match to_sqlite_column_value(&value, Some(&json)) {
                SqlParam::Text(actual) => assert_eq!(actual, expected),
                actual => panic!("expected JSON text parameter, got {actual:?}"),
            }
        }
    }
}
