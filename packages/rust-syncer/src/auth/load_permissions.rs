//! Loading + hot-reloading the compiled read-permissions doc.
//!
//! Port of `zero-cache/src/auth/load-permissions.ts` (`loadPermissions`,
//! `reloadPermissionsIfChanged`). The structural validation performed by the
//! `validate_*` family is the rust twin of the TS valita
//! `permissionsConfigSchema` parse inside `loadPermissions`. (`getSchema`, the
//! third export of the TS file, has its established twin in the replica
//! table-spec computation — not duplicated here.)
//!
//! `deny_all_permissions` / `resolve_permissions` are rust-only fail-CLOSED
//! helpers around the load outcome (TS throws on an unparseable doc; rust
//! keeps the CG serving under deny-all instead) — see their doc-comments.

use rusqlite::Connection;
use serde_json::{Map, Value, json};

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
/// - `Ok(None)` → no permissions deployed. Returns `None`, matching TS
///   `loadPermissions` returning `{permissions: null}`. The CONSUMER
///   (sync_engine, mirroring view-syncer.ts:1549) then transforms client
///   queries with `permissions ?? {tables: {}}` — an empty config that
///   deny-by-defaults every table. `None` does NOT mean passthrough.
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

// ─── loadPermissions ─────────────────────────────────────────────────────────

/// Loaded permissions: the compiled `PermissionsConfig` JSON and its hash, or
/// `None` when no permissions have been deployed.
#[derive(Debug)]
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
            validate_permissions_config(&permissions)?;
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

fn validate_permissions_config(value: &Value) -> Result<(), String> {
    let root = value
        .as_object()
        .ok_or_else(|| "permissions config must be an object".to_string())?;
    let Some(tables) = root.get("tables") else {
        return Ok(());
    };
    let tables = tables
        .as_object()
        .ok_or_else(|| "permissions.tables must be an object".to_string())?;
    for (table, config) in tables {
        let config = config
            .as_object()
            .ok_or_else(|| format!("permissions table {table} must be an object"))?;
        if let Some(row) = config.get("row") {
            validate_permission_asset(row, &format!("tables.{table}.row"))?;
        }
        if let Some(cells) = config.get("cell") {
            let cells = cells
                .as_object()
                .ok_or_else(|| format!("tables.{table}.cell must be an object"))?;
            for (column, asset) in cells {
                validate_permission_asset(asset, &format!("tables.{table}.cell.{column}"))?;
            }
        }
    }
    Ok(())
}

fn validate_permission_asset(value: &Value, path: &str) -> Result<(), String> {
    let asset = value
        .as_object()
        .ok_or_else(|| format!("{path} must be an object"))?;
    for operation in ["select", "insert", "delete"] {
        if let Some(policy) = asset.get(operation) {
            validate_policy(policy, &format!("{path}.{operation}"))?;
        }
    }
    if let Some(update) = asset.get("update") {
        let update = update
            .as_object()
            .ok_or_else(|| format!("{path}.update must be an object"))?;
        for phase in ["preMutation", "postMutation"] {
            if let Some(policy) = update.get(phase) {
                validate_policy(policy, &format!("{path}.update.{phase}"))?;
            }
        }
    }
    Ok(())
}

fn validate_policy(value: &Value, path: &str) -> Result<(), String> {
    let rules = value
        .as_array()
        .ok_or_else(|| format!("{path} must be an array"))?;
    for (index, rule) in rules.iter().enumerate() {
        let rule = rule
            .as_array()
            .ok_or_else(|| format!("{path}[{index}] must be a tuple"))?;
        if rule.len() != 2 || rule[0].as_str() != Some("allow") {
            return Err(format!("{path}[{index}] must be [\"allow\", condition]"));
        }
        validate_permission_condition(&rule[1], &format!("{path}[{index}][1]"))?;
    }
    Ok(())
}

fn validate_permission_condition(value: &Value, path: &str) -> Result<(), String> {
    let condition = value
        .as_object()
        .ok_or_else(|| format!("{path} must be a condition object"))?;
    match condition.get("type").and_then(Value::as_str) {
        Some("simple") => {
            const OPS: &[&str] = &[
                "=",
                "!=",
                "IS",
                "IS NOT",
                "<",
                ">",
                "<=",
                ">=",
                "LIKE",
                "NOT LIKE",
                "ILIKE",
                "NOT ILIKE",
                "IN",
                "NOT IN",
            ];
            if !condition
                .get("op")
                .and_then(Value::as_str)
                .is_some_and(|op| OPS.contains(&op))
            {
                return Err(format!("{path}.op is not a supported operator"));
            }
            validate_condition_value(condition.get("left"), true, &format!("{path}.left"))?;
            validate_condition_value(condition.get("right"), false, &format!("{path}.right"))?;
        }
        Some("and") | Some("or") => {
            let children = condition
                .get("conditions")
                .and_then(Value::as_array)
                .ok_or_else(|| format!("{path}.conditions must be an array"))?;
            for (index, child) in children.iter().enumerate() {
                validate_permission_condition(child, &format!("{path}.conditions[{index}]"))?;
            }
        }
        Some("correlatedSubquery") => {
            let related = condition
                .get("related")
                .and_then(Value::as_object)
                .ok_or_else(|| format!("{path}.related must be an object"))?;
            if !matches!(
                condition.get("op").and_then(Value::as_str),
                Some("EXISTS" | "NOT EXISTS")
            ) {
                return Err(format!("{path}.op must be EXISTS or NOT EXISTS"));
            }
            for flag in ["flip", "scalar"] {
                if condition.get(flag).is_some_and(|value| !value.is_boolean()) {
                    return Err(format!("{path}.{flag} must be a boolean"));
                }
            }
            validate_related_subquery(related, &format!("{path}.related"))?;
        }
        _ => return Err(format!("{path} has an unknown condition type")),
    }
    Ok(())
}

fn validate_condition_value(
    value: Option<&Value>,
    allow_column: bool,
    path: &str,
) -> Result<(), String> {
    let value = value
        .and_then(Value::as_object)
        .ok_or_else(|| format!("{path} must be a value reference"))?;
    match value.get("type").and_then(Value::as_str) {
        Some("column") if allow_column => {
            if value.get("name").and_then(Value::as_str).is_none() {
                return Err(format!("{path}.name must be a string"));
            }
        }
        Some("literal") => {
            let literal = value
                .get("value")
                .ok_or_else(|| format!("{path}.value is required"))?;
            let valid = literal.is_null()
                || literal.is_string()
                || literal.is_number()
                || literal.is_boolean()
                || literal.as_array().is_some_and(|items| {
                    items
                        .iter()
                        .all(|item| item.is_string() || item.is_number() || item.is_boolean())
                });
            if !valid {
                return Err(format!("{path}.value is not a protocol literal"));
            }
        }
        Some("static") => {
            if !matches!(
                value.get("anchor").and_then(Value::as_str),
                Some("authData" | "preMutationRow")
            ) {
                return Err(format!("{path}.anchor is invalid"));
            }
            let field = value.get("field");
            if !field.is_some_and(|field| {
                field.is_string()
                    || field
                        .as_array()
                        .is_some_and(|parts| parts.iter().all(Value::is_string))
            }) {
                return Err(format!("{path}.field must be a string or string array"));
            }
        }
        _ => return Err(format!("{path} has an invalid value-reference type")),
    }
    Ok(())
}

fn validate_related_subquery(related: &Map<String, Value>, path: &str) -> Result<(), String> {
    let correlation = related
        .get("correlation")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("{path}.correlation must be an object"))?;
    for field in ["parentField", "childField"] {
        let valid = correlation
            .get(field)
            .and_then(Value::as_array)
            .is_some_and(|parts| !parts.is_empty() && parts.iter().all(Value::is_string));
        if !valid {
            return Err(format!(
                "{path}.correlation.{field} must be a non-empty string array"
            ));
        }
    }
    if related
        .get("hidden")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err(format!("{path}.hidden must be a boolean"));
    }
    if related
        .get("system")
        .is_some_and(|value| !matches!(value.as_str(), Some("permissions" | "client" | "test")))
    {
        return Err(format!("{path}.system is invalid"));
    }
    let subquery = related
        .get("subquery")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("{path}.subquery must be an AST object"))?;
    if subquery.get("table").and_then(Value::as_str).is_none() {
        return Err(format!("{path}.subquery.table must be a string"));
    }
    if let Some(condition) = subquery.get("where") {
        validate_permission_condition(condition, &format!("{path}.subquery.where"))?;
    }
    if let Some(nested) = subquery.get("related") {
        let nested = nested
            .as_array()
            .ok_or_else(|| format!("{path}.subquery.related must be an array"))?;
        for (index, child) in nested.iter().enumerate() {
            let child = child
                .as_object()
                .ok_or_else(|| format!("{path}.subquery.related[{index}] must be an object"))?;
            validate_related_subquery(child, &format!("{path}.subquery.related[{index}]"))?;
        }
    }
    Ok(())
}

/// Outcome of a hot-reload check against the deployed permissions doc.
pub enum PermissionsReload {
    /// The deployed permissions hash is unchanged (or unreadable — see below);
    /// keep the currently-loaded permissions.
    Unchanged,
    /// The deployed permissions hash differs from the currently-loaded one.
    /// `permissions` is the newly-resolved config (fail-CLOSED via
    /// [`resolve_permissions`] if the reload errored) and `hash` is the new
    /// deployed hash to remember for the next check.
    Changed {
        permissions: Option<Value>,
        hash: Option<String>,
    },
}

/// Port of TS `reloadPermissionsIfChanged`: cheaply read just the deployed
/// permissions `hash` and, only if it differs from `current_hash`, reload the
/// full doc.
///
/// Faithful differences from the TS version, both deliberate:
///  - The full reload is routed through [`resolve_permissions`] so a doc that
///    exists but fails to parse yields deny-all (fail-CLOSED), matching the
///    posture established at CG creation, rather than throwing.
///  - If the cheap `hash` read itself errors (e.g. a transient replica read
///    failure), we return [`PermissionsReload::Unchanged`] rather than
///    clobbering a working permission set — a persistent problem still surfaces
///    via the pipeline reset path. (TS lets the read error bubble to a reset.)
pub fn reload_permissions_if_changed(
    conn: &Connection,
    app_id: &str,
    current_hash: Option<&str>,
) -> PermissionsReload {
    let sql = format!("SELECT hash FROM \"{app_id}.permissions\"");
    let new_hash: Option<String> = match conn.query_row(&sql, [], |row| row.get(0)) {
        Ok(h) => h,
        // No row / no table yet == nothing deployed.
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => {
            tracing::warn!("permissions hash read failed ({e}); keeping current permissions");
            return PermissionsReload::Unchanged;
        }
    };
    if new_hash.as_deref() == current_hash {
        return PermissionsReload::Unchanged;
    }
    // Hash moved — reload the full doc (fail-CLOSED on parse/read error).
    let loaded = load_permissions(conn, app_id).map(|l| l.permissions);
    let permissions = resolve_permissions(loaded);
    PermissionsReload::Changed {
        permissions,
        hash: new_hash,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn load_permissions_rejects_structurally_invalid_json() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"CREATE TABLE "zero.permissions" (permissions TEXT, hash TEXT);
               INSERT INTO "zero.permissions" VALUES
                 ('{"tables":{"issue":{"row":{"select":"allow-all"}}}}', 'bad');"#,
        )
        .unwrap();
        let err = load_permissions(&conn, "zero").unwrap_err();
        assert!(err.contains("select must be an array"), "{err}");

        conn.execute(
            r#"UPDATE "zero.permissions" SET permissions = ?1"#,
            [r#"{"tables":{"issue":{"row":{"select":[["allow",{"type":"simple","op":"DROP","left":{"type":"column","name":"id"},"right":{"type":"literal","value":1}}]]}}}}"#],
        )
        .unwrap();
        let err = load_permissions(&conn, "zero").unwrap_err();
        assert!(err.contains("not a supported operator"), "{err}");
    }

    /// Build an in-memory replica with a `{app}.permissions(permissions, hash)`
    /// row, matching the shape `load_permissions` reads.
    fn perms_replica(app_id: &str, permissions: Option<&str>, hash: Option<&str>) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "CREATE TABLE \"{app_id}.permissions\" (permissions TEXT, hash TEXT);"
        ))
        .unwrap();
        conn.execute(
            &format!("INSERT INTO \"{app_id}.permissions\" (permissions, hash) VALUES (?1, ?2)"),
            rusqlite::params![permissions, hash],
        )
        .unwrap();
        conn
    }

    #[test]
    fn reload_permissions_unchanged_when_hash_matches() {
        let doc = r#"{"tables":{}}"#;
        let conn = perms_replica("zero", Some(doc), Some("h1"));
        // Same hash as currently loaded → no reload.
        assert!(matches!(
            reload_permissions_if_changed(&conn, "zero", Some("h1")),
            PermissionsReload::Unchanged
        ));
    }

    #[test]
    fn reload_permissions_reloads_when_hash_changes() {
        let doc = r#"{"tables":{"issue":{"row":{"select":[]}}}}"#;
        let conn = perms_replica("zero", Some(doc), Some("h2"));
        // A redeploy changed the hash h1 → h2: reload the full doc.
        match reload_permissions_if_changed(&conn, "zero", Some("h1")) {
            PermissionsReload::Changed { permissions, hash } => {
                assert_eq!(hash.as_deref(), Some("h2"));
                assert_eq!(
                    permissions,
                    Some(json!({"tables":{"issue":{"row":{"select":[]}}}}))
                );
            }
            PermissionsReload::Unchanged => panic!("expected a reload on hash change"),
        }
    }

    #[test]
    fn reload_permissions_detects_first_deploy_from_none() {
        // Nothing loaded yet (current_hash None); a doc is now deployed.
        let conn = perms_replica("zero", Some(r#"{"tables":{}}"#), Some("h1"));
        assert!(matches!(
            reload_permissions_if_changed(&conn, "zero", None),
            PermissionsReload::Changed { hash: Some(h), .. } if h == "h1"
        ));
    }

    #[test]
    fn reload_permissions_fails_closed_on_unparseable_redeploy() {
        // The hash moved (a redeploy happened) but the doc is corrupt: the
        // reload must resolve to deny-all, never silently pass through.
        let conn = perms_replica("zero", Some("{ not json"), Some("h2"));
        match reload_permissions_if_changed(&conn, "zero", Some("h1")) {
            PermissionsReload::Changed { permissions, hash } => {
                assert_eq!(hash.as_deref(), Some("h2"));
                assert_eq!(permissions, Some(deny_all_permissions()));
                assert_ne!(permissions, None, "corrupt redeploy must fail closed");
            }
            PermissionsReload::Unchanged => panic!("a hash change must trigger a reload"),
        }
    }
}
