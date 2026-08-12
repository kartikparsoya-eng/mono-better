//! Read-permission query transformation (faithful port).
//!
//! Ports Zero's read-authorization pipeline to Rust so the syncer returns only
//! rows a user is allowed to read. Faithful to the TS sources:
//!   - `zero-cache/src/auth/read-authorizer.ts`  — transformQuery / transformAndHashQuery
//!   - `zql/src/builder/builder.ts`               — bindStaticParameters
//!   - `zql/src/query/expression.ts`              — simplifyCondition / flatten
//!   - `zero-protocol/src/ast.ts`                 — normalizeAST / cmpCondition
//!   - `zero-protocol/src/query-hash.ts`          — hashOfAST
//!   - `zero-cache/src/auth/load-permissions.ts`  — loadPermissions
//!
//! All transforms operate on the TS AST wire shape (`serde_json::Value`, with
//! serde_json's `preserve_order` feature) so `JSON.stringify` byte-parity holds
//! for the transformation hash. Conditions use the TS internally-tagged shape
//! (`{"type":"simple"|"and"|"or"|"correlatedSubquery", ...}`), which is the same
//! shape the client sends and the compiled permissions store.
//!
//! Security-critical behavior preserved exactly:
//!   - Deny-by-default: a table with no `select` rules yields an empty OR
//!     (`{or,conditions:[]}`) — the engine's filter treats empty OR as false
//!     (0 rows). See rust-ivm `builder::filter::create_predicate` (`.any()`).
//!   - Oracle prevention: read rules are applied recursively into EXISTS
//!     subqueries and related subqueries.

use rusqlite::Connection;
use serde_json::{Map, Value, json};

// ─── Entry point ─────────────────────────────────────────────────────────────

/// A deny-all compiled-permissions config: no table has any `select` rule, so
/// the read-authorizer's deny-by-default kicks in and every client query
/// returns zero rows. Used as the fail-CLOSED fallback when permissions cannot
/// be loaded.
pub fn deny_all_permissions() -> Value {
    json!({"tables": {}})
}

/// Resolve the outcome of loading read-permissions into the compiled config the
/// engine should enforce, applying fail-CLOSED semantics on error:
///
/// - `Ok(Some(perms))` → enforce those permissions.
/// - `Ok(None)` → no permissions deployed. Pass client queries through
///   untransformed — matches TS `load-permissions.ts`, which merely warns and
///   serves without authorization when nothing is deployed.
/// - `Err(_)` → a permissions doc exists but could not be opened / parsed /
///   validated. Do NOT fall through to `None` (that would execute client
///   queries with no authorization — a fail-OPEN security hole). Enforce
///   `deny_all_permissions()` so no unauthorized row is ever served. (TS throws
///   on an unparseable permissions doc; deny-all is the equivalent fail-closed
///   posture that keeps the rest of the CG serving.)
pub fn resolve_permissions(loaded: Result<Option<Value>, String>) -> Option<Value> {
    match loaded {
        Ok(perms) => perms,
        Err(_) => Some(deny_all_permissions()),
    }
}

/// Transform a query AST for read-permissions and compute its transformation
/// hash. Port of `transformAndHashQuery`. `internal` queries (e.g. the
/// mutation-results / lmid internal queries) skip transformation.
///
/// `auth_data` is the decoded JWT claims object (`{}` when unauthenticated).
pub fn transform_and_hash(
    query: &Value,
    permissions: &Value,
    auth_data: &Value,
    internal: bool,
) -> (Value, String) {
    let transformed = if internal {
        query.clone()
    } else {
        transform_query(query, permissions, auth_data)
    };
    let hash = hash_of_ast(&transformed);
    (transformed, hash)
}

/// Port of `transformQuery`: apply read rules, then bind static auth params.
pub fn transform_query(query: &Value, permissions: &Value, auth_data: &Value) -> Value {
    let with_perms = transform_query_internal(query, permissions);
    bind_static_parameters(&with_perms, auth_data)
}

// ─── transformQueryInternal / addRulesToWhere / transformCondition ───────────

fn transform_query_internal(query: &Value, permissions: &Value) -> Value {
    let table = query.get("table").and_then(Value::as_str).unwrap_or("");

    // rowSelectRules = permissions.tables[table].row.select — an array of
    // ["allow", Condition] tuples. Map to the condition (index 1).
    let rule_conditions: Vec<Value> = permissions
        .get("tables")
        .and_then(|t| t.get(table))
        .and_then(|t| t.get("row"))
        .and_then(|r| r.get("select"))
        .and_then(Value::as_array)
        .map(|rules| {
            rules
                .iter()
                .filter_map(|rule| rule.get(1).cloned())
                .collect::<Vec<_>>()
        })
        .filter(|c| !c.is_empty())
        // Deny-by-default: no rules => a single empty-OR (always false).
        .unwrap_or_else(|| vec![json!({"type": "or", "conditions": []})]);

    let transformed_where = query
        .get("where")
        .filter(|w| !w.is_null())
        .map(|w| transform_condition(w, permissions));
    let updated_where = add_rules_to_where(transformed_where, rule_conditions);

    // {...query, where: simplify(updatedWhere), related: query.related?.map(...)}
    let mut out = query.as_object().cloned().unwrap_or_default();
    out.insert("where".to_string(), simplify_condition(updated_where));
    if let Some(related) = query.get("related").and_then(Value::as_array) {
        let new_related: Vec<Value> = related
            .iter()
            .map(|sq| {
                let mut e = sq.as_object().cloned().unwrap_or_default();
                if let Some(subq) = sq.get("subquery") {
                    e.insert(
                        "subquery".to_string(),
                        transform_query_internal(subq, permissions),
                    );
                }
                Value::Object(e)
            })
            .collect();
        out.insert("related".to_string(), Value::Array(new_related));
    }
    Value::Object(out)
}

fn add_rules_to_where(where_opt: Option<Value>, rule_conditions: Vec<Value>) -> Value {
    let mut conditions: Vec<Value> = Vec::new();
    if let Some(w) = where_opt {
        conditions.push(w);
    }
    conditions.push(json!({"type": "or", "conditions": rule_conditions}));
    json!({"type": "and", "conditions": conditions})
}

/// Port of `transformCondition` — apply read rules into EXISTS/related subqueries
/// so users can't infer the existence of rows they can't read.
fn transform_condition(cond: &Value, permissions: &Value) -> Value {
    match cond.get("type").and_then(Value::as_str) {
        Some("simple") => cond.clone(),
        Some("and") | Some("or") => {
            let mut out = cond.as_object().cloned().unwrap_or_default();
            if let Some(conds) = cond.get("conditions").and_then(Value::as_array) {
                let mapped: Vec<Value> = conds
                    .iter()
                    .map(|c| transform_condition(c, permissions))
                    .collect();
                out.insert("conditions".to_string(), Value::Array(mapped));
            }
            Value::Object(out)
        }
        Some("correlatedSubquery") => {
            let mut out = cond.as_object().cloned().unwrap_or_default();
            if let Some(related) = cond.get("related").and_then(Value::as_object) {
                let mut new_related = related.clone();
                if let Some(subq) = related.get("subquery") {
                    new_related.insert(
                        "subquery".to_string(),
                        transform_query_internal(subq, permissions),
                    );
                }
                out.insert("related".to_string(), Value::Object(new_related));
            }
            Value::Object(out)
        }
        _ => cond.clone(),
    }
}

// ─── bindStaticParameters ────────────────────────────────────────────────────

/// Port of `bindStaticParameters`: resolve `{type:'static', anchor, field}`
/// value positions to literals using the auth data (`anchor == "authData"`).
pub fn bind_static_parameters(ast: &Value, auth_data: &Value) -> Value {
    let static_params = json!({ "authData": auth_data });
    bind_visit(ast, &static_params)
}

fn bind_visit(ast: &Value, static_params: &Value) -> Value {
    let mut out = ast.as_object().cloned().unwrap_or_default();
    if let Some(where_c) = ast.get("where").filter(|w| !w.is_null()) {
        out.insert("where".to_string(), bind_condition(where_c, static_params));
    }
    if let Some(related) = ast.get("related").and_then(Value::as_array) {
        let mapped: Vec<Value> = related
            .iter()
            .map(|sq| {
                let mut e = sq.as_object().cloned().unwrap_or_default();
                if let Some(subq) = sq.get("subquery") {
                    e.insert("subquery".to_string(), bind_visit(subq, static_params));
                }
                Value::Object(e)
            })
            .collect();
        out.insert("related".to_string(), Value::Array(mapped));
    }
    Value::Object(out)
}

fn bind_condition(cond: &Value, static_params: &Value) -> Value {
    match cond.get("type").and_then(Value::as_str) {
        Some("simple") => {
            let mut out = cond.as_object().cloned().unwrap_or_default();
            if let Some(left) = cond.get("left") {
                out.insert("left".to_string(), bind_value(left, static_params));
            }
            if let Some(right) = cond.get("right") {
                out.insert("right".to_string(), bind_value(right, static_params));
            }
            Value::Object(out)
        }
        Some("correlatedSubquery") => {
            let mut out = cond.as_object().cloned().unwrap_or_default();
            if let Some(related) = cond.get("related").and_then(Value::as_object) {
                let mut new_related = related.clone();
                if let Some(subq) = related.get("subquery") {
                    new_related.insert("subquery".to_string(), bind_visit(subq, static_params));
                }
                out.insert("related".to_string(), Value::Object(new_related));
            }
            Value::Object(out)
        }
        _ => {
            // and / or
            let mut out = cond.as_object().cloned().unwrap_or_default();
            if let Some(conds) = cond.get("conditions").and_then(Value::as_array) {
                let mapped: Vec<Value> = conds
                    .iter()
                    .map(|c| bind_condition(c, static_params))
                    .collect();
                out.insert("conditions".to_string(), Value::Array(mapped));
            }
            Value::Object(out)
        }
    }
}

fn bind_value(value: &Value, static_params: &Value) -> Value {
    if value.get("type").and_then(Value::as_str) == Some("static") {
        let anchor_name = value.get("anchor").and_then(Value::as_str).unwrap_or("");
        let anchor = static_params.get(anchor_name);
        let resolved = resolve_field(anchor, value.get("field"));
        return json!({"type": "literal", "value": resolved});
    }
    value.clone()
}

/// Port of `resolveField`: `field` is a string or an array path; returns the
/// resolved value or `null`.
fn resolve_field(anchor: Option<&Value>, field: Option<&Value>) -> Value {
    let Some(anchor) = anchor else {
        return Value::Null;
    };
    match field {
        Some(Value::Array(path)) => {
            let mut acc = anchor;
            for f in path {
                let Some(key) = f.as_str() else {
                    return Value::Null;
                };
                match acc.get(key) {
                    Some(v) => acc = v,
                    None => return Value::Null,
                }
            }
            if acc.is_null() {
                Value::Null
            } else {
                acc.clone()
            }
        }
        Some(Value::String(key)) => anchor.get(key).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

// ─── simplifyCondition ───────────────────────────────────────────────────────

fn is_always_false(c: &Value) -> bool {
    c.get("type").and_then(Value::as_str) == Some("or")
        && c.get("conditions")
            .and_then(Value::as_array)
            .map(|a| a.is_empty())
            == Some(true)
}

fn is_always_true(c: &Value) -> bool {
    c.get("type").and_then(Value::as_str) == Some("and")
        && c.get("conditions")
            .and_then(Value::as_array)
            .map(|a| a.is_empty())
            == Some(true)
}

fn flatten(kind: &str, conditions: Vec<Value>) -> Vec<Value> {
    let mut out = Vec::new();
    for c in conditions {
        if c.get("type").and_then(Value::as_str) == Some(kind) {
            if let Some(inner) = c.get("conditions").and_then(Value::as_array) {
                out.extend(inner.iter().cloned());
                continue;
            }
        }
        out.push(c);
    }
    out
}

/// Port of `simplifyCondition`.
pub fn simplify_condition(c: Value) -> Value {
    let kind = c.get("type").and_then(Value::as_str).unwrap_or("");
    if kind == "simple" || kind == "correlatedSubquery" {
        return c;
    }
    let conditions = c
        .get("conditions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if conditions.len() == 1 {
        return simplify_condition(conditions.into_iter().next().unwrap());
    }
    let simplified: Vec<Value> = conditions.into_iter().map(simplify_condition).collect();
    let flat = flatten(kind, simplified);
    if kind == "and" && flat.iter().any(is_always_false) {
        return json!({"type": "or", "conditions": []}); // FALSE
    }
    if kind == "or" && flat.iter().any(is_always_true) {
        return json!({"type": "and", "conditions": []}); // TRUE
    }
    json!({"type": kind, "conditions": flat})
}

// ─── normalizeAST + hashOfAST ────────────────────────────────────────────────

/// Port of `hashOfAST`: `h64(JSON.stringify(normalizeAST(ast))).toString(36)`.
pub fn hash_of_ast(ast: &Value) -> String {
    let normalized = normalize_ast(ast);
    let json = serde_json::to_string(&normalized).unwrap_or_default();
    base36(rust_cvr::hash::h64(&json))
}

/// Port of `normalizeAST` (transformAST with the NORMALIZE_TRANSFORM: identity
/// names, sorted related, flattened + sorted conditions). Produces a canonical
/// object with a fixed key order matching TS `Required<AST>` for stringify
/// parity (undefined fields omitted).
pub fn normalize_ast(ast: &Value) -> Value {
    // where = ast.where ? flattened(ast.where) : undefined, then transformWhere.
    let where_norm = ast
        .get("where")
        .filter(|w| !w.is_null())
        .and_then(|w| flattened(w))
        .map(|w| normalize_where(&w));

    let mut out = Map::new();
    insert_if_present(&mut out, "schema", ast.get("schema"));
    out.insert(
        "table".to_string(),
        ast.get("table").cloned().unwrap_or(Value::Null),
    );
    insert_if_present(&mut out, "alias", ast.get("alias"));
    if let Some(w) = where_norm {
        out.insert("where".to_string(), w);
    }
    if let Some(related) = ast.get("related").and_then(Value::as_array) {
        let mut entries: Vec<Value> = related.iter().map(normalize_related_entry).collect();
        entries.sort_by(cmp_related);
        out.insert("related".to_string(), Value::Array(entries));
    }
    insert_if_present(&mut out, "start", ast.get("start"));
    insert_if_present(&mut out, "limit", ast.get("limit"));
    insert_if_present(&mut out, "orderBy", ast.get("orderBy"));
    Value::Object(out)
}

fn insert_if_present(out: &mut Map<String, Value>, key: &str, v: Option<&Value>) {
    if let Some(v) = v {
        if !v.is_null() {
            out.insert(key.to_string(), v.clone());
        }
    }
}

/// A normalized related entry: `{correlation:{parentField,childField}, hidden,
/// subquery, system}` (fixed order; undefined omitted).
fn normalize_related_entry(r: &Value) -> Value {
    let mut out = Map::new();
    if let Some(corr) = r.get("correlation") {
        let mut c = Map::new();
        insert_if_present(&mut c, "parentField", corr.get("parentField"));
        insert_if_present(&mut c, "childField", corr.get("childField"));
        out.insert("correlation".to_string(), Value::Object(c));
    }
    insert_if_present(&mut out, "hidden", r.get("hidden"));
    if let Some(subq) = r.get("subquery") {
        out.insert("subquery".to_string(), normalize_ast(subq));
    }
    insert_if_present(&mut out, "system", r.get("system"));
    Value::Object(out)
}

/// Port of `transformWhere` under normalization (recurse + sort and/or).
fn normalize_where(cond: &Value) -> Value {
    match cond.get("type").and_then(Value::as_str) {
        Some("simple") => cond.clone(),
        Some("correlatedSubquery") => {
            let mut out = cond.as_object().cloned().unwrap_or_default();
            if let Some(related) = cond.get("related") {
                out.insert("related".to_string(), normalize_related_entry(related));
            }
            Value::Object(out)
        }
        _ => {
            // and / or: {type, conditions: sort(map(normalize_where))}
            let kind = cond.get("type").and_then(Value::as_str).unwrap_or("");
            let mut conds: Vec<Value> = cond
                .get("conditions")
                .and_then(Value::as_array)
                .map(|a| a.iter().map(normalize_where).collect())
                .unwrap_or_default();
            conds.sort_by(cmp_condition);
            json!({"type": kind, "conditions": conds})
        }
    }
}

/// Port of `flattened` (ast.ts): flatten nested same-type, drop empties, unwrap
/// singletons. Returns `None` for an empty conjunction.
fn flattened(cond: &Value) -> Option<Value> {
    let kind = cond.get("type").and_then(Value::as_str).unwrap_or("");
    if kind == "simple" || kind == "correlatedSubquery" {
        return Some(cond.clone());
    }
    let mut conditions: Vec<Value> = Vec::new();
    if let Some(conds) = cond.get("conditions").and_then(Value::as_array) {
        for c in conds {
            if c.get("type").and_then(Value::as_str) == Some(kind) {
                if let Some(inner) = c.get("conditions").and_then(Value::as_array) {
                    for ic in inner {
                        if let Some(f) = flattened(ic) {
                            conditions.push(f);
                        }
                    }
                    continue;
                }
            }
            if let Some(f) = flattened(c) {
                conditions.push(f);
            }
        }
    }
    match conditions.len() {
        0 => None,
        1 => Some(conditions.into_iter().next().unwrap()),
        _ => Some(json!({"type": kind, "conditions": conditions})),
    }
}

// ─── Condition comparators (cmpCondition) ────────────────────────────────────

fn ctype(c: &Value) -> &str {
    c.get("type").and_then(Value::as_str).unwrap_or("")
}

fn cmp_condition(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    let (at, bt) = (ctype(a), ctype(b));
    if at == "simple" {
        if bt != "simple" {
            return Less; // SimpleConditions first
        }
        return compare_value_position(a.get("left"), b.get("left"))
            .then_with(|| compare_utf8_maybe_null(a.get("op"), b.get("op")))
            .then_with(|| compare_value_position(a.get("right"), b.get("right")));
    }
    if bt == "simple" {
        return Greater;
    }
    if at == "correlatedSubquery" {
        if bt != "correlatedSubquery" {
            return Less; // subquery before conj/disj
        }
        return cmp_related(
            a.get("related").unwrap_or(&Value::Null),
            b.get("related").unwrap_or(&Value::Null),
        )
        .then_with(|| compare_utf8_maybe_null(a.get("op"), b.get("op")))
        .then_with(|| cmp_optional_bool(a.get("flip"), b.get("flip")))
        .then_with(|| cmp_optional_bool(a.get("scalar"), b.get("scalar")));
    }
    if bt == "correlatedSubquery" {
        // Faithful to TS (ast.ts:509): returns -1 here.
        return Less;
    }
    // both and/or
    let type_cmp = compare_utf8_maybe_null(a.get("type"), b.get("type"));
    if type_cmp != Equal {
        return type_cmp;
    }
    let empty = Vec::new();
    let ac = a
        .get("conditions")
        .and_then(Value::as_array)
        .unwrap_or(&empty);
    let bc = b
        .get("conditions")
        .and_then(Value::as_array)
        .unwrap_or(&empty);
    for (l, r) in ac.iter().zip(bc.iter()) {
        let c = cmp_condition(l, r);
        if c != Equal {
            return c;
        }
    }
    ac.len().cmp(&bc.len()) // prefixes first
}

fn compare_value_position(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    let (a, b) = (a.unwrap_or(&Value::Null), b.unwrap_or(&Value::Null));
    let at = a.get("type").and_then(Value::as_str).unwrap_or("");
    let bt = b.get("type").and_then(Value::as_str).unwrap_or("");
    if at != bt {
        return at.cmp(bt);
    }
    match at {
        "literal" => js_string(a.get("value")).cmp(&js_string(b.get("value"))),
        "column" => {
            let an = a.get("name").and_then(Value::as_str).unwrap_or("");
            let bn = b.get("name").and_then(Value::as_str).unwrap_or("");
            an.cmp(bn)
        }
        // "static" should be resolved before normalization; treat as equal
        // rather than panic (bindStaticParameters runs first).
        _ => std::cmp::Ordering::Equal,
    }
}

fn cmp_related(a: &Value, b: &Value) -> std::cmp::Ordering {
    let aa = a
        .get("subquery")
        .and_then(|s| s.get("alias"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let bb = b
        .get("subquery")
        .and_then(|s| s.get("alias"))
        .and_then(Value::as_str)
        .unwrap_or("");
    aa.cmp(bb)
}

fn compare_utf8_maybe_null(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    let a = a.and_then(Value::as_str);
    let b = b.and_then(Value::as_str);
    match (a, b) {
        (Some(a), Some(b)) => a.cmp(b),
        (Some(_), None) => Greater,
        (None, Some(_)) => Less,
        (None, None) => Equal,
    }
}

fn cmp_optional_bool(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    // undefined(0) < false(1) < true(2)
    let to_num = |v: Option<&Value>| match v.and_then(Value::as_bool) {
        None => 0,
        Some(false) => 1,
        Some(true) => 2,
    };
    to_num(a).cmp(&to_num(b))
}

/// Mirror of JS `String(value)` for the values used in literal comparison.
fn js_string(v: Option<&Value>) -> String {
    match v {
        None | Some(Value::Null) => "null".to_string(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        Some(other) => other.to_string(),
    }
}

/// JS `BigInt.toString(36)` for an unsigned 64-bit value.
fn base36(mut n: u64) -> String {
    if n == 0 {
        return "0".to_string();
    }
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut buf = Vec::new();
    while n > 0 {
        buf.push(DIGITS[(n % 36) as usize]);
        n /= 36;
    }
    buf.reverse();
    String::from_utf8(buf).unwrap()
}

// ─── loadPermissions ─────────────────────────────────────────────────────────

/// Loaded permissions: the compiled `PermissionsConfig` JSON and its hash, or
/// `None` when no permissions have been deployed.
pub struct LoadedPermissions {
    pub permissions: Option<Value>,
    pub hash: Option<String>,
}

/// Port of `loadPermissions`: read the `{app}.permissions` row from the replica.
pub fn load_permissions(conn: &Connection, app_id: &str) -> Result<LoadedPermissions, String> {
    let sql = format!("SELECT permissions, hash FROM \"{app_id}.permissions\"");
    let row = conn.query_row(&sql, [], |row| {
        let permissions: Option<String> = row.get(0)?;
        let hash: Option<String> = row.get(1)?;
        Ok((permissions, hash))
    });
    match row {
        Ok((Some(permissions_json), hash)) => {
            let permissions = serde_json::from_str::<Value>(&permissions_json)
                .map_err(|e| format!("could not parse upstream permissions: {e}"))?;
            Ok(LoadedPermissions {
                permissions: Some(permissions),
                hash,
            })
        }
        // No permissions deployed (NULL row).
        Ok((None, _)) => Ok(LoadedPermissions {
            permissions: None,
            hash: None,
        }),
        // Table doesn't exist yet, etc. — treat as "not deployed".
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(LoadedPermissions {
            permissions: None,
            hash: None,
        }),
        Err(e) => Err(format!("load permissions: {e}")),
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn perms_anyone_can(table: &str) -> Value {
        // ANYONE_CAN == a single always-true rule ({and, conditions:[]}).
        json!({
            "tables": { table: { "row": { "select": [["allow", {"type":"and","conditions":[]}]] } } }
        })
    }

    #[test]
    fn deny_by_default_when_no_rules() {
        let query = json!({"table": "secret"});
        let out = transform_query(&query, &json!({"tables": {}}), &json!({}));
        // where must be the always-false empty OR.
        assert_eq!(out["where"], json!({"type":"or","conditions":[]}));
    }

    #[test]
    fn anyone_can_yields_always_true_where() {
        let query = json!({"table": "issue"});
        let out = transform_query(&query, &perms_anyone_can("issue"), &json!({}));
        // and[ or[ and[] ] ] simplifies: inner or has an always-true → TRUE.
        assert_eq!(out["where"], json!({"type":"and","conditions":[]}));
    }

    #[test]
    fn merges_user_where_with_rules() {
        let query = json!({
            "table": "issue",
            "where": {"type":"simple","op":"=",
                "left":{"type":"column","name":"open"},
                "right":{"type":"literal","value":true}}
        });
        // A rule referencing ownerId == authData.sub.
        let perms = json!({"tables":{"issue":{"row":{"select":[["allow",
            {"type":"simple","op":"=",
             "left":{"type":"column","name":"ownerId"},
             "right":{"type":"static","anchor":"authData","field":"sub"}}]]}}}});
        let out = transform_query(&query, &perms, &json!({"sub": "u1"}));
        let w = &out["where"];
        assert_eq!(w["type"], "and");
        let conds = w["conditions"].as_array().unwrap();
        // user condition + the rule (static bound to literal "u1").
        assert_eq!(conds.len(), 2);
        let joined = serde_json::to_string(w).unwrap();
        assert!(joined.contains("\"open\""));
        assert!(joined.contains("ownerId"));
        // Static param was bound to the literal user id.
        assert!(joined.contains("\"u1\""));
        assert!(!joined.contains("static"));
    }

    #[test]
    fn recurses_into_related_subqueries() {
        let query = json!({
            "table": "issue",
            "related": [{
                "correlation": {"parentField":["id"], "childField":["issueId"]},
                "subquery": {"table":"comment", "alias":"comments"}
            }]
        });
        // issue has ANYONE_CAN; comment has NO rules → subquery denied.
        let perms = perms_anyone_can("issue");
        let out = transform_query(&query, &perms, &json!({}));
        let sub_where = &out["related"][0]["subquery"]["where"];
        assert_eq!(*sub_where, json!({"type":"or","conditions":[]}));
    }

    #[test]
    fn hash_is_deterministic_and_order_independent() {
        // Two ASTs whose AND conditions differ only in order must hash equal
        // (normalizeAST sorts them).
        let a = json!({"table":"t","where":{"type":"and","conditions":[
            {"type":"simple","op":"=","left":{"type":"column","name":"a"},"right":{"type":"literal","value":1}},
            {"type":"simple","op":"=","left":{"type":"column","name":"b"},"right":{"type":"literal","value":2}}
        ]}});
        let b = json!({"table":"t","where":{"type":"and","conditions":[
            {"type":"simple","op":"=","left":{"type":"column","name":"b"},"right":{"type":"literal","value":2}},
            {"type":"simple","op":"=","left":{"type":"column","name":"a"},"right":{"type":"literal","value":1}}
        ]}});
        assert_eq!(hash_of_ast(&a), hash_of_ast(&b));
        // And distinct queries hash differently.
        let c = json!({"table":"other"});
        assert_ne!(hash_of_ast(&a), hash_of_ast(&c));
    }

    #[test]
    fn hash_of_ast_matches_ts_golden_vectors() {
        // Golden values computed from the TS `hashOfAST` (zero-protocol) via
        // `tsx`. Proves byte-exact normalizeAST + JSON.stringify + h64 + base36
        // parity with the reference implementation.
        let ast1 = json!({
            "table": "issue",
            "where": {"type":"and","conditions":[
                {"type":"simple","op":"=","left":{"type":"column","name":"b"},"right":{"type":"literal","value":2}},
                {"type":"simple","op":"=","left":{"type":"column","name":"a"},"right":{"type":"literal","value":1}}
            ]},
            "orderBy": [["id","asc"]]
        });
        assert_eq!(hash_of_ast(&ast1), "12hnuwu8c3cdu");

        let ast2 = json!({
            "table": "issue",
            "related": [{
                "correlation": {"parentField":["id"],"childField":["issueId"]},
                "subquery": {"table":"comment","alias":"comments"}
            }]
        });
        assert_eq!(hash_of_ast(&ast2), "2xc2t07zjznlf");
    }

    #[test]
    fn base36_matches_js() {
        assert_eq!(base36(0), "0");
        assert_eq!(base36(35), "z");
        assert_eq!(base36(36), "10");
        // sanity: 2^63
        assert_eq!(base36(9223372036854775808), "1y2p0ij32e8e8");
    }

    #[test]
    fn resolve_permissions_fails_closed_on_load_error() {
        // Present → enforced as-is.
        let perms = json!({"tables":{"issue":{"row":{"select":[]}}}});
        assert_eq!(resolve_permissions(Ok(Some(perms.clone()))), Some(perms));
        // Absent → pass-through (None), matching TS (warn + serve).
        assert_eq!(resolve_permissions(Ok(None)), None);
        // Load/parse error → deny-all, NOT None. A load failure must never fall
        // through to unauthorized pass-through (the fail-open hole).
        let denied = resolve_permissions(Err("corrupt permissions".to_string()));
        assert_eq!(denied, Some(deny_all_permissions()));
        assert_ne!(
            denied, None,
            "a permissions load failure must fail closed, not pass through"
        );
    }

    #[test]
    fn deny_all_permissions_denies_every_client_query() {
        // Under the deny-all config, a client query against ANY table is rewritten
        // with an always-false (empty-OR) where, so the read-authorizer returns
        // zero rows — no unauthorized data escapes.
        let out = transform_query(&json!({"table":"issue"}), &deny_all_permissions(), &json!({}));
        assert_eq!(out["where"], json!({"type":"or","conditions":[]}));
    }
}
