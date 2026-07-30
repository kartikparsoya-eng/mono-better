//! Query builder — port of `zqlite/src/query-builder.ts`.
//!
//! Compiles a FetchRequest into a SQL string + bound parameters.
//! Supports: constraints, multiConstraints (batched IN), filters,
//! ORDER BY (with reverse), start (pagination).

use crate::builder::ast::{Condition, SimpleCondition, ValuePosition};
use crate::ivm::constraint::MultiConstraint;
use crate::ivm::data::Value;
use crate::ivm::operator::{Basis, FetchRequest, Start};

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
        let (start_sql, start_params) = gather_start_constraints(start, reverse, order);
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
) -> (String, Vec<SqlParam>) {
    let mut params: Vec<SqlParam> = Vec::new();
    let mut constraints: Vec<String> = Vec::new();
    let from = &start.row;

    for i in 0..order.len() {
        let mut group: Vec<String> = Vec::new();
        let (i_field, i_dir) = &order[i];

        for j in 0..=i {
            if j == i {
                // Range comparison
                let val = from.get(i_field).cloned().unwrap_or(Value::Null);
                if val.is_null() {
                    // The IVM comparator treats Null as less than any non-null
                    // value. For a NULL boundary:
                    //   - operator '>' means "after NULL": every non-null value
                    //     qualifies → use IS NOT NULL.
                    //   - operator '<' means "before NULL": nothing qualifies.
                    let operator = if i_dir == "asc" {
                        if reverse { "<" } else { ">" }
                    } else if reverse {
                        ">"
                    } else {
                        "<"
                    };
                    if operator == ">" {
                        group.push(format!("{} IS NOT NULL", quote_ident(i_field)));
                    } else {
                        group.push("FALSE".to_string());
                    }
                } else {
                    let operator = if i_dir == "asc" {
                        if reverse { "<" } else { ">" }
                    } else if reverse {
                        ">"
                    } else {
                        "<"
                    };
                    // The IVM comparator treats Null as less than any non-null
                    // value (TS compareValues). When the range operator is '<',
                    // rows with NULL in this column must be included because
                    // they sort before the start value. SQLite's `NULL < x`
                    // evaluates to UNKNOWN, so add an explicit IS NULL clause.
                    if operator == "<" {
                        group.push(format!(
                            "({} {} ? OR {} IS NULL)",
                            quote_ident(i_field),
                            operator,
                            quote_ident(i_field)
                        ));
                    } else {
                        group.push(format!("{} {} ?", quote_ident(i_field), operator));
                    }
                    params.push(SqlParam::from(&val));
                }
            } else {
                // Equality on previous columns
                let (j_field, _) = &order[j];
                let val = from.get(j_field).cloned().unwrap_or(Value::Null);
                if val.is_null() {
                    group.push(format!("{} IS NULL", quote_ident(j_field)));
                } else {
                    group.push(format!("{} = ?", quote_ident(j_field)));
                    params.push(SqlParam::from(&val));
                }
            }
        }
        constraints.push(format!("({})", group.join(" AND ")));
    }

    // For 'at' basis, add exact match
    if start.basis == Basis::At {
        let mut group: Vec<String> = Vec::new();
        for (field, _) in order {
            let val = from.get(field).cloned().unwrap_or(Value::Null);
            if val.is_null() {
                group.push(format!("{} IS NULL", quote_ident(field)));
            } else {
                group.push(format!("{} = ?", quote_ident(field)));
                params.push(SqlParam::from(&val));
            }
        }
        constraints.push(format!("({})", group.join(" AND ")));
    }

    (format!("({})", constraints.join(" OR ")), params)
}

/// Convert a Condition (with CSQ stripped) to a SQL WHERE clause.
/// Port of TS `filtersToSQL` (query-builder.ts:169).
fn condition_to_sql(cond: &Condition) -> (String, Vec<SqlParam>) {
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
}
