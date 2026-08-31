//! Inspector protocol op dispatch — port of
//! `services/view-syncer/inspect-handler.ts` (`handleInspect`).
//!
//! The caller (the per-CG dispatch in `router.rs`, twin of the TS
//! `viewSyncer.inspect` lock body) resolves the client's socket and TTL clock
//! and passes them in, mirroring how TS hands `handleInspect` the resolved
//! `client` / `cvr` / `cvrStore`. Every op — present, unported, and unknown —
//! answers a frame; a silent drop would hang the client's inspector RPC
//! forever (inspect-handler.ts:171-178 catch block).

use rust_cvr::ttl_clock::TTLClock;

use crate::config::zero_config::is_admin_password_valid;

use super::view_syncer::ViewSyncerService;

/// Port of `handleInspect` (inspect-handler.ts). The auth gate lives here:
/// every op except `authenticate` requires a previously-authenticated client
/// group; unauthenticated requests get an `authenticated:false` challenge.
/// `inspector_authenticated` is the CG's auth flag (TS
/// `InspectorDelegate.isAuthenticated(clientGroupID)`), mutated on
/// `authenticate`.
#[allow(clippy::too_many_arguments)]
pub async fn handle_inspect(
    cg_id: &str,
    body: &serde_json::Value,
    ws_id: &str,
    sync_engine: &ViewSyncerService,
    inspector_authenticated: &mut bool,
    admin_password: Option<&str>,
    server_version: &str,
    ttl_clock: TTLClock,
) {
    let op = body.get("op").and_then(|v| v.as_str()).unwrap_or("");
    let id = body.get("id").cloned().unwrap_or(serde_json::Value::Null);

    // Auth gate — only `authenticate` is allowed before authenticating.
    if op != "authenticate" && !*inspector_authenticated {
        sync_engine.send_inspect_response(
            ws_id,
            serde_json::json!({"op": "authenticated", "id": id, "value": false}),
        );
        return;
    }

    // Each arm yields `Ok((responseOp, value))` or `Err(message)`; the
    // response frame (success vs `op:"error"`) is assembled once below so
    // every path — present and future — flows through the error shape.
    let result: Result<(&str, serde_json::Value), String> = match op {
        "authenticate" => {
            let password = body.get("value").and_then(|v| v.as_str()).unwrap_or("");
            let dev_mode = std::env::var("NODE_ENV").as_deref() == Ok("development");
            let ok = is_admin_password_valid(password, admin_password, dev_mode);
            *inspector_authenticated = ok;
            Ok(("authenticated", serde_json::json!(ok)))
        }
        "version" => Ok(("version", serde_json::json!(server_version))),
        "queries" => {
            let filter_client = body
                .get("clientID")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let value =
                inspect_queries_value(cg_id, sync_engine, filter_client.as_deref(), ttl_clock)
                    .await;
            Ok(("queries", value))
        }
        "metrics" => {
            // The wire value is a RECORD with two REQUIRED TDigest fields
            // (`serverMetricsSchema`, inspect-down.ts:7-10) — an array or
            // `{}` fails the client's valita parse and rejects the RPC.
            // Rust tracks no server TDigests yet, so send empty digests:
            // `[1000]` is `new TDigest().toJSON()` (default compression,
            // no centroids), which zero-client parses as a valid digest.
            Ok((
                "metrics",
                serde_json::json!({
                    "query-materialization-server": [1000],
                    "query-update-server": [1000],
                }),
            ))
        }
        // `analyze-query` — port of the TS inspect-handler `analyze-query` case
        // (inspect-handler.ts:115) → `analyzeQuery`. Runs a throwaway read-only
        // analysis engine over the replica and returns an `AnalyzeQueryResult`.
        "analyze-query" => analyze_query_op(cg_id, sync_engine, body).await,
        other => {
            tracing::warn!("CG {cg_id}: unknown inspect op {other:?}");
            Err(format!("unknown inspect op: {other}"))
        }
    };
    let frame = match result {
        Ok((resp_op, value)) => {
            serde_json::json!({"op": resp_op, "id": id, "value": value})
        }
        Err(message) => serde_json::json!({"op": "error", "id": id, "value": message}),
    };
    sync_engine.send_inspect_response(ws_id, frame);
}

/// Port of the TS inspect-handler `analyze-query` case (inspect-handler.ts:115).
/// Extracts the AST from the body, then runs [`analyze_query`] over the replica
/// on a blocking thread (the IVM analysis engine is `!Send`) and returns the
/// serialized `AnalyzeQueryResult`.
///
/// DEFERRED vs TS (labeled): named-query transform (`body.name`/`body.args` →
/// `transformCustomQuery`) and the read-permission transform (see
/// `services::run_ast`). Only the legacy `body.ast` path is ported.
async fn analyze_query_op(
    cg_id: &str,
    sync_engine: &ViewSyncerService,
    body: &serde_json::Value,
) -> Result<(&'static str, serde_json::Value), String> {
    if body.get("name").is_some() && body.get("args").is_some() {
        return Err(
            "analyze-query for named queries is not yet supported by the rust syncer \
             (legacy AST only)"
                .to_string(),
        );
    }
    // TS: `let ast = body.ast ?? body.value`.
    let ast = match body.get("ast").or_else(|| body.get("value")) {
        Some(v) if !v.is_null() => v.clone(),
        _ => {
            return Err(
                "AST is required for analyze-query operation. Either provide an AST \
                 directly or ensure custom query transformation is configured."
                    .to_string(),
            );
        }
    };
    let ast_json =
        serde_json::to_string(&ast).map_err(|e| format!("analyze-query: serialize AST: {e}"))?;

    let replica_path = match sync_engine.replica_path() {
        Some(p) => p.to_string(),
        None => {
            return Err(
                "analyze-query requires a SQLite replica (not available for in-memory CGs)"
                    .to_string(),
            );
        }
    };
    let app_id = sync_engine.app_id().to_string();

    // TS `body.options` defaults: syncedRows=true, vendedRows=false.
    let opts = body.get("options");
    let synced_rows = opts
        .and_then(|o| o.get("syncedRows"))
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let vended_rows = opts
        .and_then(|o| o.get("vendedRows"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // The analysis engine is `!Send` (Rc/RefCell IVM), so build + run it on a
    // blocking thread; only the `Send` result crosses back.
    // A4 wires `auth` (the connection's JWT claims); until then analyze binds
    // permission static-params against NULL.
    let result = tokio::task::spawn_blocking(move || {
        // TS inspect-handler.ts:135-147 — for a legacy query, load the deployed
        // permissions from the replica so analyze applies the same read-rules the
        // client sees. (Named queries skip this — legacyQuery=false; see A3.)
        let permissions = load_legacy_analyze_permissions(&replica_path, &app_id)?;
        crate::services::analyze::analyze_query(
            &replica_path,
            &app_id,
            &ast_json,
            synced_rows,
            vended_rows,
            permissions,
            None,
        )
    })
    .await
    .map_err(|e| {
        tracing::warn!("CG {cg_id}: analyze-query task join error: {e}");
        format!("analyze-query task failed: {e}")
    })??;

    let value = serde_json::to_value(&result)
        .map_err(|e| format!("analyze-query: serialize result: {e}"))?;
    Ok(("analyze-query", value))
}

/// Load the deployed permissions for a legacy analyze-query. Port of the
/// `legacyQuery` block in the TS `analyze-query` case (inspect-handler.ts:135-147):
/// open the replica, `loadPermissions(app.id)`, use the config when deployed and
/// log an info line otherwise. A load/parse error propagates to the client as an
/// `op:error` frame (the TS `try/catch` in `handleInspect`). Runs on the
/// blocking thread (SQLite I/O).
fn load_legacy_analyze_permissions(
    replica_path: &str,
    app_id: &str,
) -> Result<Option<serde_json::Value>, String> {
    let conn = crate::db::lite_tables::open_replica_read_only(replica_path)?;
    let loaded = crate::load_permissions(&conn, app_id)?;
    if loaded.permissions.is_none() {
        tracing::info!(
            "No permissions loaded; analyze-query will run without applying permissions."
        );
    }
    Ok(loaded.permissions)
}

/// Build the `queries` inspector value by delegating to the CVR store's
/// `inspect_queries` (SQL port of TS `CVRStore.inspectQueries`), then adding
/// `metrics: null` to each row. The InspectorDelegate materialization metrics
/// and the custom-query transformed-AST fallback are server-side machinery not
/// ported to the Rust syncer (the TS inspect-handler.ts enrichment layer).
async fn inspect_queries_value(
    cg_id: &str,
    sync_engine: &ViewSyncerService,
    filter_client: Option<&str>,
    ttl_clock: TTLClock,
) -> serde_json::Value {
    let rows = match sync_engine.inspect_queries(ttl_clock, filter_client).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!("CG {cg_id}: inspect_queries failed: {e}");
            return serde_json::json!([]);
        }
    };
    let out: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let mut v = serde_json::to_value(row).unwrap_or(serde_json::Value::Null);
            if let serde_json::Value::Object(map) = &mut v {
                map.insert("metrics".to_string(), serde_json::Value::Null);
            }
            v
        })
        .collect();
    serde_json::Value::Array(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a replica file with the `_zero.*` metadata, a 3-row `users` table,
    /// and a deployed `{app}.permissions` row holding `permissions_json`.
    fn build_replica_with_permissions(path: &str, app_id: &str, permissions_json: &str) {
        for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
        let conn = rusqlite::Connection::open(path).unwrap();
        let _ = conn.pragma_update(None, "journal_mode", "wal2");
        conn.execute_batch(
            r#"
            CREATE TABLE "_zero.replicationConfig" (
                lock TEXT PRIMARY KEY DEFAULT 'singleton',
                replicaVersion TEXT NOT NULL, publications TEXT NOT NULL);
            CREATE TABLE "_zero.replicationState" (
                lock TEXT PRIMARY KEY DEFAULT 'singleton', stateVersion TEXT NOT NULL);
            INSERT INTO "_zero.replicationConfig" VALUES ('singleton', 'v1', '[]');
            INSERT INTO "_zero.replicationState" VALUES ('singleton', 'v1');
            CREATE TABLE "users" ("id" "text|NOT_NULL", "name" "text|NOT_NULL", "_0_version" "text");
            CREATE UNIQUE INDEX "u_id" ON "users" ("id");
            INSERT INTO "users" VALUES ('u1','Alice','01'),('u2','Bob','01'),('u3','Carol','01');
            "#,
        )
        .unwrap();
        // The deployed permissions row lives in `{app}.permissions` (loadPermissions).
        conn.execute_batch(&format!(
            "CREATE TABLE \"{app_id}.permissions\" (\"lock\" INTEGER PRIMARY KEY, \
             \"permissions\" TEXT, \"hash\" TEXT);"
        ))
        .unwrap();
        conn.execute(
            &format!(
                "INSERT INTO \"{app_id}.permissions\" (lock, permissions, hash) VALUES (1, ?1, 'h')"
            ),
            [permissions_json],
        )
        .unwrap();
    }

    /// A2: the analyze-query handler loads the DEPLOYED permissions from the
    /// replica (TS inspect-handler.ts:135-147) and applies them. NON-VACUOUS:
    /// with a deployed permissions doc that grants `users` no select rule, the
    /// loader returns exactly that doc and analyze filters every row to 0.
    /// Reverting the loader to return `None` (the pre-A2 state) makes the exact
    /// equality fail AND makes analyze return 3 rows.
    #[test]
    fn legacy_analyze_loads_and_applies_deployed_permissions() {
        let path = std::env::temp_dir()
            .join(format!("rs_inspect_a2_{}.db", std::process::id()))
            .to_string_lossy()
            .to_string();
        // A valid permissions doc that deploys NO select rule for `users`
        // (deny-by-default): distinct from "no permissions deployed" (None).
        let deployed = r#"{"tables":{}}"#;
        build_replica_with_permissions(&path, "app", deployed);

        // The exact deployed doc is read back (proves the load path runs).
        let loaded = load_legacy_analyze_permissions(&path, "app").unwrap();
        assert_eq!(
            loaded,
            Some(serde_json::json!({"tables": {}})),
            "loader must return the deployed permissions doc verbatim"
        );

        // Applying those permissions denies every `users` row.
        let ast = serde_json::json!({"table": "users", "orderBy": [["id", "asc"]]}).to_string();
        let result =
            crate::services::analyze::analyze_query(&path, "app", &ast, true, false, loaded, None)
                .unwrap();
        assert_eq!(
            result.synced_row_count, 0,
            "deployed deny-by-default permissions filter every row; got {}",
            result.synced_row_count
        );

        for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }
}
