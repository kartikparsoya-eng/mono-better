//! Loading + hot-reloading the compiled read-permissions doc.
//!
//! Port of `zero-cache/src/auth/load-permissions.ts` (`loadPermissions`,
//! `reloadPermissionsIfChanged`), plus the `permissionsConfigSchema` twins of
//! `zero-schema/src/compiled-permissions.ts` that `loadPermissions` parses
//! with — folded into this consumer because the crate has no zero-schema
//! twin (rule 3). (`getSchema`, the third export of the TS file, has its
//! established twin in the replica table-spec computation — not duplicated
//! here.)
//!
//! `deny_all_permissions` / `resolve_permissions` are rust-only fail-CLOSED
//! helpers around the load outcome (TS throws on an unparseable doc; rust
//! keeps the CG serving under deny-all instead) — see their doc-comments.

use std::collections::HashMap;

use rusqlite::Connection;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::protocol::ast::strict::Condition;
use crate::protocol::optional_no_null;
use crate::ws_server::elide;

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
            // load-permissions.ts:46-59: `JSON.parse` then
            // `v.parse(obj, permissionsConfigSchema)` under one catch. The
            // accepted doc stays JSON, as TS keeps the parsed object; the
            // thrown Error's `cause` is appended to the message here.
            let permissions = serde_json::from_str::<Value>(&permissions_json)
                .map_err(|e| e.to_string())
                .and_then(|doc| {
                    match crate::protocol::valita::deserialize_at::<PermissionsConfig>(
                        &doc,
                        &[],
                        &doc,
                    ) {
                        Ok(_) => Ok(doc),
                        Err(issue) => Err(crate::protocol::valita::get_message(&issue, &doc)),
                    }
                })
                .map_err(|cause| {
                    format!(
                        "Could not parse upstream permissions: '{}'.\n\
                         This may happen if Permissions with a new internal format are \
                         deployed before the supporting server has been fully rolled out.\n\
                         cause: {cause}",
                        elide(&permissions_json, 100)
                    )
                })?;
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

// ─── permissionsConfigSchema (zero-schema/src/compiled-permissions.ts) ───────
// Parsed by `v.parse(obj, permissionsConfigSchema)` (load-permissions.ts:50)
// in valita's default STRICT mode: unknown keys are rejected at every level
// and `.optional()` is absent-or-value, never `null`. The `conditionSchema`
// member is the strict `ast.ts` twin.

/// `v.literal('allow')` (compiled-permissions.ts:4).
#[derive(Debug, Clone, Copy, Deserialize)]
pub enum Allow {
    #[serde(rename = "allow")]
    Allow,
}

/// `ruleSchema` (compiled-permissions.ts:4): `['allow', condition]`.
pub type Rule = (Allow, Condition);

/// `policySchema` (compiled-permissions.ts:6).
pub type Policy = Vec<Rule>;

/// The `update` object of `assetSchema` (compiled-permissions.ts:12-17).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdatePermissions {
    #[serde(default, deserialize_with = "optional_no_null")]
    pub pre_mutation: Option<Policy>,
    #[serde(default, deserialize_with = "optional_no_null")]
    pub post_mutation: Option<Policy>,
}

/// `assetSchema` (compiled-permissions.ts:9-19).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssetPermissions {
    #[serde(default, deserialize_with = "optional_no_null")]
    pub select: Option<Policy>,
    #[serde(default, deserialize_with = "optional_no_null")]
    pub insert: Option<Policy>,
    #[serde(default, deserialize_with = "optional_no_null")]
    pub update: Option<UpdatePermissions>,
    #[serde(default, deserialize_with = "optional_no_null")]
    pub delete: Option<Policy>,
}

/// The per-table object of `tablePermissionsSchema`
/// (compiled-permissions.ts:23-28): `{row?, cell?}`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableAssets {
    #[serde(default, deserialize_with = "optional_no_null")]
    pub row: Option<AssetPermissions>,
    #[serde(default, deserialize_with = "optional_no_null")]
    pub cell: Option<HashMap<String, AssetPermissions>>,
}

/// `tablePermissionsSchema` (compiled-permissions.ts:23-28).
pub type TablePermissions = HashMap<String, TableAssets>;

/// `permissionsConfigSchema` (compiled-permissions.ts:30-32).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionsConfig {
    #[serde(default, deserialize_with = "optional_no_null")]
    pub tables: Option<TablePermissions>,
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
        assert!(
            err.starts_with(
                "Could not parse upstream permissions: '{\"tables\":{\"issue\":{\"row\":{\"select\":\"allow-all\"}}}}'.\n\
                 This may happen if Permissions with a new internal format are \
                 deployed before the supporting server has been fully rolled out.\ncause: "
            ),
            "{err}"
        );

        conn.execute(
            r#"UPDATE "zero.permissions" SET permissions = ?1"#,
            [r#"{"tables":{"issue":{"row":{"select":[["allow",{"type":"simple","op":"DROP","left":{"type":"column","name":"id"},"right":{"type":"literal","value":1}}]]}}}}"#],
        )
        .unwrap();
        let err = load_permissions(&conn, "zero").unwrap_err();
        assert!(
            err.contains("cause: Expected literal value \"=\"")
                && err.contains("at tables.issue.row.select.0.1.op Got \"DROP\""),
            "{err}"
        );
    }

    /// `v.parse(obj, permissionsConfigSchema)` runs in valita's default
    /// strict mode (load-permissions.ts:50): an unknown key at any level —
    /// the config root, a table entry, an asset, a rule's condition — fails
    /// the parse. The hand-written checks this replaced accepted all four.
    #[test]
    fn load_permissions_rejects_unknown_keys_like_strict_valita() {
        for doc in [
            r#"{"tables":{},"extra":1}"#,
            r#"{"tables":{"issue":{"row":{},"extra":1}}}"#,
            r#"{"tables":{"issue":{"row":{"select":[],"extra":1}}}}"#,
            r#"{"tables":{"issue":{"row":{"select":[["allow",{"type":"simple","op":"=","left":{"type":"column","name":"id"},"right":{"type":"literal","value":1},"extra":1}]]}}}}"#,
        ] {
            let conn = perms_replica("zero", Some(doc), Some("h"));
            let err = load_permissions(&conn, "zero").unwrap_err();
            assert!(
                err.starts_with("Could not parse upstream permissions: '")
                    && err.contains("cause: Unexpected property extra"),
                "{doc}: {err}"
            );
        }
        // The elision TS applies to the quoted doc (`elide(…, 100)`).
        let long = format!(r#"{{"tables":{{"{}":{{}}}},"extra":1}}"#, "t".repeat(120));
        let conn = perms_replica("zero", Some(&long), Some("h"));
        let err = load_permissions(&conn, "zero").unwrap_err();
        assert!(err.contains(&format!("'{}'.", elide(&long, 100))), "{err}");
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
