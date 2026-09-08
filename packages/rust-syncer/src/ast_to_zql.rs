//! Port of `packages/ast-to-zql/src/ast-to-zql.ts` — `astToZQL`.
//!
//! Renders a query AST back to the equivalent ZQL query-builder source text,
//! used by `analyzeQuery`'s `afterPermissions` field to show the permission-
//! transformed query. Operates on the TS AST wire shape (serde_json `Value`,
//! internally-tagged conditions), 1:1 with the TS functions.
//!
//! No `ast-to-zql` crate twin exists; per AGENTS rule 3 this is folded into the
//! consuming crate (`run_ast`), keeping the TS function names.
//!
//! `formatOutput` (format.ts) is NOT ported: it runs the `oxfmt` JS formatter,
//! whose own fallback on error is to return the unformatted string. The rust
//! `afterPermissions` uses the raw `ast_to_zql` output — equivalent to that
//! fallback path (cosmetic line-wrapping only; the emitted ZQL is identical).

use std::collections::BTreeSet;

use serde_json::Value;

/// Reserved correlated-subquery alias prefix — port of TS `SUBQ_PREFIX` (ast.ts:17).
const SUBQ_PREFIX: &str = "zsubq_";

fn as_str(v: Option<&Value>) -> &str {
    v.and_then(Value::as_str).unwrap_or("")
}

/// TS `has additional subquery properties` — `where || related.length>0 ||
/// orderBy || limit`, with JS truthiness (empty `orderBy` is truthy; `limit: 0`
/// is falsy).
fn has_sub_query_props(sub: &Value) -> bool {
    sub.get("where").is_some_and(|v| !v.is_null())
        || sub
            .get("related")
            .and_then(Value::as_array)
            .is_some_and(|a| !a.is_empty())
        || sub.get("orderBy").is_some_and(|v| !v.is_null())
        || sub
            .get("limit")
            .and_then(Value::as_f64)
            .is_some_and(|n| n != 0.0)
}

/// Port of `astToZQL` (ast-to-zql.ts:29). Renders the AST's where / related /
/// orderBy / limit / start clauses to ZQL builder-call text.
pub fn ast_to_zql(ast: &Value) -> String {
    let mut code = String::new();

    // Where conditions.
    if let Some(where_c) = ast.get("where").filter(|v| !v.is_null()) {
        code += &transform_condition(where_c, ".where", &mut BTreeSet::new());
    }

    // Related subqueries.
    if let Some(related) = ast
        .get("related")
        .and_then(Value::as_array)
        .filter(|a| !a.is_empty())
    {
        for r in related {
            if r.get("hidden").and_then(Value::as_bool) == Some(true) {
                // nestedRelated = related.subquery.related?.[0]
                if let Some(nested) = r
                    .get("subquery")
                    .and_then(|s| s.get("related"))
                    .and_then(Value::as_array)
                    .and_then(|a| a.first())
                {
                    code += &transform_related(nested);
                }
            } else {
                code += &transform_related(r);
            }
        }
    }

    // orderBy.
    if let Some(order_by) = ast
        .get("orderBy")
        .and_then(Value::as_array)
        .filter(|a| !a.is_empty())
    {
        code += &transform_order(order_by);
    }

    // limit (`ast.limit !== undefined`).
    if let Some(limit) = ast.get("limit").filter(|v| !v.is_null()) {
        code += &format!(".limit({limit})");
    }

    // start.
    if let Some(start) = ast.get("start").filter(|v| !v.is_null()) {
        let row = start.get("row").cloned().unwrap_or(Value::Null);
        let exclusive = start
            .get("exclusive")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let row_json = serde_json::to_string(&row).unwrap_or_else(|_| "null".to_string());
        code += &format!(
            ".start({row_json}{})",
            if exclusive {
                ""
            } else {
                ", { inclusive: true }"
            }
        );
    }

    code
}

fn transform_condition(condition: &Value, prefix: &str, args: &mut BTreeSet<String>) -> String {
    match condition.get("type").and_then(Value::as_str) {
        Some("simple") => transform_simple_condition(condition, prefix),
        Some("and") | Some("or") => transform_logical_condition(condition, prefix, args),
        Some("correlatedSubquery") => transform_exists_condition(condition, prefix, args),
        // TS `unreachable(condition)` — port faithfully as an empty rendering.
        _ => String::new(),
    }
}

fn transform_simple_condition(condition: &Value, prefix: &str) -> String {
    let left = transform_value_position(condition.get("left").unwrap_or(&Value::Null));
    let right = transform_value_position(condition.get("right").unwrap_or(&Value::Null));
    let op = as_str(condition.get("op"));
    // Shorthand for equality.
    if op == "=" {
        format!("{prefix}({left}, {right})")
    } else {
        format!("{prefix}({left}, '{op}', {right})")
    }
}

fn transform_logical_condition(
    condition: &Value,
    prefix: &str,
    args: &mut BTreeSet<String>,
) -> String {
    let type_ = as_str(condition.get("type"));
    let empty = Vec::new();
    let conditions = condition
        .get("conditions")
        .and_then(Value::as_array)
        .unwrap_or(&empty);

    // Single condition — no logical operator needed.
    if conditions.len() == 1 {
        return transform_condition(&conditions[0], prefix, args);
    }

    // Top-level AND → multiple chained `.where` calls (or an `and(...)` in a cmp).
    if type_ == "and" {
        let parts: Vec<String> = conditions
            .iter()
            .map(|c| transform_condition(c, prefix, args))
            .collect();
        if prefix == ".where" {
            return parts.join("");
        }
        args.insert("and".to_string());
        return format!("and({})", parts.join(", "));
    }

    // OR (or nested) — a fresh args set for the callback form (TS reassigns
    // `args = new Set()`), collect conditions with the `cmp` prefix.
    let mut local_args: BTreeSet<String> = BTreeSet::new();
    let conditions_code = conditions
        .iter()
        .map(|c| transform_condition(c, "cmp", &mut local_args))
        .collect::<Vec<_>>()
        .join(", ");
    local_args.insert("cmp".to_string());
    local_args.insert(type_.to_string());
    let args_code = local_args.iter().cloned().collect::<Vec<_>>().join(", ");
    format!(".where(({{{args_code}}}) => {type_}({conditions_code}))")
}

fn transform_exists_condition(
    condition: &Value,
    prefix: &str,
    args: &mut BTreeSet<String>,
) -> String {
    let related = condition.get("related").unwrap_or(&Value::Null);
    let op = as_str(condition.get("op"));
    let relationship = extract_relationship_name(related);
    let next_subquery = get_next_exists_subquery(related);
    let has_props = has_sub_query_props(next_subquery);

    // Options string for flip / scalar (`!== undefined`).
    let mut option_parts: Vec<String> = Vec::new();
    if let Some(flip) = condition.get("flip").filter(|v| !v.is_null()) {
        option_parts.push(format!("flip: {flip}"));
    }
    if let Some(scalar) = condition.get("scalar").filter(|v| !v.is_null()) {
        option_parts.push(format!("scalar: {scalar}"));
    }
    let options_str = if option_parts.is_empty() {
        String::new()
    } else {
        format!(", {{{}}}", option_parts.join(", "))
    };

    if op == "EXISTS" {
        if !has_props {
            if prefix == ".where" {
                return format!(".whereExists('{relationship}'{options_str})");
            }
            args.insert("exists".to_string());
            return format!("exists('{relationship}'{options_str})");
        }
        if prefix == ".where" {
            return format!(
                ".whereExists('{relationship}', q => q{}{options_str})",
                ast_to_zql(next_subquery)
            );
        }
        args.insert("exists".to_string());
        return format!(
            "exists('{relationship}', q => q{}{options_str})",
            ast_to_zql(next_subquery)
        );
    }

    // op === 'NOT EXISTS'
    if has_props {
        if prefix == ".where" {
            return format!(
                ".where(({{exists, not}}) => not(exists('{relationship}', q => q{}{options_str})))",
                ast_to_zql(next_subquery)
            );
        }
        args.insert("not".to_string());
        args.insert("exists".to_string());
        return format!(
            "not(exists('{relationship}', q => q{}{options_str}))",
            ast_to_zql(next_subquery)
        );
    }

    if prefix == ".where" {
        return format!(".where(({{exists, not}}) => not(exists('{relationship}'{options_str})))");
    }
    args.insert("not".to_string());
    args.insert("exists".to_string());
    // Faithful to TS (ast-to-zql.ts:213) — note the extra trailing paren.
    format!("not(exists('{relationship}'{options_str})))")
}

/// If `exists` is applied against a junction edge, both hops share the alias and
/// both are exists conditions — descend to the terminal subquery. Port of
/// `getNextExistsSubquery` (ast-to-zql.ts:217).
fn get_next_exists_subquery(related: &Value) -> &Value {
    let sq_where = related.get("subquery").and_then(|s| s.get("where"));
    if sq_where.and_then(|w| w.get("type")).and_then(Value::as_str) == Some("correlatedSubquery") {
        let inner_alias = sq_where
            .and_then(|w| w.get("related"))
            .and_then(|r| r.get("subquery"))
            .and_then(|s| s.get("alias"))
            .and_then(Value::as_str);
        if inner_alias.is_some_and(|a| a.contains("zsubq_zhidden_"))
            && let Some(inner_related) = sq_where.and_then(|w| w.get("related"))
        {
            return get_next_exists_subquery(inner_related);
        }
    }
    related.get("subquery").unwrap_or(&Value::Null)
}

fn extract_relationship_name(related: &Value) -> String {
    let alias = as_str(related.get("subquery").and_then(|s| s.get("alias")));
    match alias.strip_prefix(SUBQ_PREFIX) {
        Some(stripped) => stripped.to_string(),
        None => alias.to_string(),
    }
}

fn transform_related(related: &Value) -> String {
    let subquery = related.get("subquery").unwrap_or(&Value::Null);
    let alias = subquery.get("alias").and_then(Value::as_str);
    let Some(relationship) = alias.filter(|a| !a.is_empty()) else {
        return String::new();
    };
    let mut code = format!(".related('{relationship}'");
    if has_sub_query_props(subquery) {
        code += &format!(", q => q{}", ast_to_zql(subquery));
    }
    code += ")";
    code
}

fn transform_order(order_by: &[Value]) -> String {
    let mut code = String::new();
    for entry in order_by {
        if let Some(pair) = entry.as_array() {
            let field = as_str(pair.first());
            let direction = as_str(pair.get(1));
            code += &format!(".orderBy('{field}', '{direction}')");
        }
    }
    code
}

fn transform_value_position(value: &Value) -> String {
    match value.get("type").and_then(Value::as_str) {
        Some("literal") => transform_literal(value),
        Some("column") => format!("'{}'", as_str(value.get("name"))),
        Some("static") => transform_parameter(value),
        _ => String::new(),
    }
}

fn transform_literal(literal: &Value) -> String {
    match literal.get("value").unwrap_or(&Value::Null) {
        Value::Null => "null".to_string(),
        arr @ Value::Array(_) => serde_json::to_string(arr).unwrap_or_default(),
        Value::String(s) => format!("'{}'", s.replace('\'', "\\'")),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        // Objects can't be literal values in practice; JSON-render rather than panic.
        obj => serde_json::to_string(obj).unwrap_or_default(),
    }
}

fn transform_parameter(param: &Value) -> String {
    let field_str = match param.get("field") {
        Some(Value::Array(arr)) => {
            let parts: Vec<String> = arr
                .iter()
                .map(|f| format!("'{}'", f.as_str().unwrap_or_default()))
                .collect();
            format!("[{}]", parts.join(", "))
        }
        Some(Value::String(s)) => format!("'{s}'"),
        _ => "''".to_string(),
    };
    format!("authParam({field_str})")
}

#[cfg(test)]
mod tests {
    use super::ast_to_zql;
    use serde_json::json;

    /// Golden ZQL renderings from the real TS `astToZQL` (ast-to-zql.ts) via tsx.
    /// Pins byte-exact parity across simple/logical/exists/related/order/limit/
    /// start/static-param forms. NON-VACUOUS: any divergence in the emitted
    /// string (operator shorthand, callback arg-set ordering, quoting/escaping,
    /// relationship-alias stripping) fails the exact-equality assertion.
    #[test]
    fn ast_to_zql_matches_ts_golden_vectors() {
        let cases: &[(serde_json::Value, &str)] = &[
            (
                json!({"table":"issue","where":{"type":"simple","op":"=","left":{"type":"column","name":"open"},"right":{"type":"literal","value":true}}}),
                ".where('open', true)",
            ),
            (
                json!({"table":"issue","where":{"type":"simple","op":">","left":{"type":"column","name":"id"},"right":{"type":"literal","value":5}}}),
                ".where('id', '>', 5)",
            ),
            (
                json!({"table":"issue","where":{"type":"simple","op":"=","left":{"type":"column","name":"title"},"right":{"type":"literal","value":"a'b"}}}),
                ".where('title', 'a\\'b')",
            ),
            (
                json!({"table":"issue","where":{"type":"or","conditions":[
                    {"type":"simple","op":"=","left":{"type":"column","name":"a"},"right":{"type":"literal","value":1}},
                    {"type":"simple","op":"=","left":{"type":"column","name":"b"},"right":{"type":"literal","value":2}}]}}),
                ".where(({cmp, or}) => or(cmp('a', 1), cmp('b', 2)))",
            ),
            (
                json!({"table":"issue","where":{"type":"and","conditions":[
                    {"type":"simple","op":"=","left":{"type":"column","name":"a"},"right":{"type":"literal","value":1}},
                    {"type":"simple","op":"=","left":{"type":"column","name":"b"},"right":{"type":"literal","value":2}}]}}),
                ".where('a', 1).where('b', 2)",
            ),
            (
                json!({"table":"issue","orderBy":[["id","asc"],["title","desc"]],"limit":10}),
                ".orderBy('id', 'asc').orderBy('title', 'desc').limit(10)",
            ),
            (
                json!({"table":"issue","where":{"type":"simple","op":"=","left":{"type":"column","name":"owner"},"right":{"type":"static","anchor":"authData","field":"sub"}}}),
                ".where('owner', authParam('sub'))",
            ),
            (
                json!({"table":"issue","where":{"type":"simple","op":"=","left":{"type":"column","name":"o"},"right":{"type":"static","anchor":"authData","field":["a","b"]}}}),
                ".where('o', authParam(['a', 'b']))",
            ),
            (
                json!({"table":"issue","related":[{"subquery":{"table":"comment","alias":"comments","orderBy":[["id","asc"]]}}]}),
                ".related('comments', q => q.orderBy('id', 'asc'))",
            ),
            (
                json!({"table":"issue","where":{"type":"correlatedSubquery","op":"EXISTS","related":{"subquery":{"table":"comment","alias":"zsubq_comments"}}}}),
                ".whereExists('comments')",
            ),
            (
                json!({"table":"issue","start":{"row":{"id":5},"exclusive":true}}),
                ".start({\"id\":5})",
            ),
        ];
        for (ast, expected) in cases {
            assert_eq!(&ast_to_zql(ast), expected, "astToZQL mismatch for {ast}");
        }
    }
}
