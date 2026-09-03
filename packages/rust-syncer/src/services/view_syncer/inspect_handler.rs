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
use crate::custom_queries::transform_query::{
    CustomQueryContext, CustomQuerySpec, CustomTransformed, transform,
};

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
    // The requesting connection's decoded JWT claims (TS `ctx.auth`), consumed
    // only by the `analyze-query` op for read-permission static-param binding.
    analyze_auth: Option<serde_json::Value>,
    // The requesting connection's custom-query transform context (TS `ctx`),
    // consumed only by the `analyze-query` named-query path.
    analyze_custom_ctx: Option<CustomQueryContext>,
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
            let value = inspect_queries_value(
                cg_id,
                sync_engine,
                ws_id,
                filter_client.as_deref(),
                ttl_clock,
            )
            .await;
            Ok(("queries", value))
        }
        "metrics" => {
            // Port of the TS `metrics` case (inspect-handler.ts:80):
            // `inspectorDelegate.getMetricsJSON()` — the two global aggregate
            // TDigests for this client group. The wire value is a RECORD with
            // both `serverMetricsSchema` fields REQUIRED (inspect-down.ts:7);
            // an empty digest serializes as `[1000]` (`new TDigest().toJSON()`).
            Ok((
                "metrics",
                sync_engine
                    .inspector_delegate()
                    .borrow_mut()
                    .get_metrics_json(),
            ))
        }
        // `analyze-query` — port of the TS inspect-handler `analyze-query` case
        // (inspect-handler.ts:115) → `analyzeQuery`. Runs a throwaway read-only
        // analysis engine over the replica and returns an `AnalyzeQueryResult`.
        "analyze-query" => {
            analyze_query_op(cg_id, sync_engine, body, analyze_auth, analyze_custom_ctx).await
        }
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
/// Resolves the AST — either the legacy `body.ast`/`body.value` or, for a named
/// query (`body.name`/`body.args`), transformed against the user's query API
/// server (`transformCustomQuery`) — then runs [`analyze_query`] over the replica
/// on a blocking thread (the IVM analysis engine is `!Send`) and returns the
/// serialized `AnalyzeQueryResult`. Only legacy queries load read-permissions;
/// a named query is already transformed by the API server (`legacyQuery=false`).
async fn analyze_query_op(
    cg_id: &str,
    sync_engine: &ViewSyncerService,
    body: &serde_json::Value,
    analyze_auth: Option<serde_json::Value>,
    analyze_custom_ctx: Option<CustomQueryContext>,
) -> Result<(&'static str, serde_json::Value), String> {
    // Resolve the AST (legacy `body.ast`/`body.value` or a named-query transform)
    // and whether it's a legacy (permission-loading) query.
    let (ast, legacy_query) =
        resolve_analyze_ast(body, analyze_custom_ctx, sync_engine.shard()).await?;
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
    // TS `body.options?.joinPlans` (inspect-handler.ts:158; default false).
    let join_plans = opts
        .and_then(|o| o.get("joinPlans"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // The analysis engine is `!Send` (Rc/RefCell IVM), so build + run it on a
    // blocking thread; only the `Send` result crosses back. The connection's
    // decoded JWT claims (TS `ctx.auth`) bind the permission static-params.
    let result = tokio::task::spawn_blocking(move || {
        // TS inspect-handler.ts:135-147 — ONLY a legacy query loads the deployed
        // permissions (a named query is already permission-transformed by the API
        // server, so `legacyQuery=false` skips the load).
        let permissions = if legacy_query {
            load_legacy_analyze_permissions(&replica_path, &app_id)?
        } else {
            None
        };
        crate::services::analyze::analyze_query(
            &replica_path,
            &app_id,
            &ast_json,
            synced_rows,
            vended_rows,
            permissions,
            analyze_auth,
            join_plans,
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

/// Resolve the AST to analyze from an `analyze-query` body, returning
/// `(ast, legacy_query)`. Port of inspect-handler.ts:116-133:
/// `let ast = body.ast ?? body.value; let legacyQuery = true;` then, for a named
/// query (`body.name && body.args`), transform it against the user's query API
/// server (`transformCustomQuery` → `hashOfNameAndArgs` id) and set
/// `legacyQuery=false`; finally throw when no AST is available. A named query
/// with no configured transform context errors like the TS `assert`.
async fn resolve_analyze_ast(
    body: &serde_json::Value,
    analyze_custom_ctx: Option<CustomQueryContext>,
    shard: &rust_cvr::shards::ShardID,
) -> Result<(serde_json::Value, bool), String> {
    // TS: `let ast = body.ast ?? body.value; let legacyQuery = true;`.
    let mut ast: Option<serde_json::Value> = body
        .get("ast")
        .or_else(|| body.get("value"))
        .filter(|v| !v.is_null())
        .cloned();
    let mut legacy_query = true;

    // TS: `if (body.name && body.args)` — a named query. Get its AST from the API
    // server by transforming it (inspect-handler.ts:119-127); legacyQuery=false.
    if let (Some(name), Some(args)) = (
        body.get("name")
            .and_then(|v| v.as_str())
            .filter(|n| !n.is_empty()),
        body.get("args").and_then(|v| v.as_array()),
    ) {
        let ctx = analyze_custom_ctx.ok_or_else(|| {
            "Custom query transformation requested but no CustomQueryTransformer is configured"
                .to_string()
        })?;
        // TS `transformCustomQuery` uses `hashOfNameAndArgs(name, args)` as the id.
        let spec = CustomQuerySpec {
            id: crate::hash_of_name_and_args(name, args),
            name: name.to_string(),
            args: args.clone(),
        };
        let transformed = transform(&ctx, shard, std::slice::from_ref(&spec))
            .await
            .map_err(|e| format!("Error transforming custom query {name}: {e}"))?;
        ast = Some(match transformed.result.into_iter().next() {
            Some(CustomTransformed::Ok(tq)) => tq.ast,
            Some(CustomTransformed::Errored { error, .. }) => {
                return Err(format!("Error transforming custom query {name}: {error}"));
            }
            None => return Err("No transformation result returned".to_string()),
        });
        legacy_query = false;
    }

    // TS: `if (ast === undefined) throw ...`.
    let ast = ast.ok_or_else(|| {
        "AST is required for analyze-query operation. Either provide an AST \
         directly or ensure custom query transformation is configured."
            .to_string()
    })?;
    Ok((ast, legacy_query))
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
/// `inspect_queries` (SQL port of TS `CVRStore.inspectQueries`), then enriching
/// each row exactly as TS does (inspect-handler.ts:63-70):
/// - `ast`: `row.ast ?? inspectorDelegate.getASTForQuery(row.queryID) ?? null`
///   — the server-generated AST fallback for custom queries whose stored `ast`
///   is null.
/// - `metrics`: `metricsForProtocol(inspectorDelegate.getMetricsJSONForQuery(
///   row.queryID), ctx.protocolVersion)` — the per-query server metrics in the
///   wire shape the connection's protocol version expects.
async fn inspect_queries_value(
    cg_id: &str,
    sync_engine: &ViewSyncerService,
    ws_id: &str,
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
    let protocol_version = sync_engine.protocol_version_for_ws(ws_id);
    let delegate = sync_engine.inspector_delegate();
    let out: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let mut v = serde_json::to_value(row).unwrap_or(serde_json::Value::Null);
            let per_query = delegate
                .borrow_mut()
                .get_metrics_json_for_query(&row.query_id);
            let metrics = metrics_for_protocol(per_query, protocol_version)
                .unwrap_or(serde_json::Value::Null);
            if let serde_json::Value::Object(map) = &mut v {
                // AST fallback: only when the stored `ast` is null/absent.
                let needs_ast = map.get("ast").is_none_or(serde_json::Value::is_null);
                if needs_ast {
                    let ast = delegate
                        .borrow()
                        .get_ast_for_query(&row.query_id)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    map.insert("ast".to_string(), ast);
                }
                map.insert("metrics".to_string(), metrics);
            }
            v
        })
        .collect();
    serde_json::Value::Array(out)
}

/// Port of `metricsForProtocol` (inspect-handler.ts:193). Protocol `>= 51`: the
/// new format passes through unchanged. Protocol `< 51`: wrap the scalar
/// `query-hydration-server-ms` into a one-point TDigest under the old field name
/// `query-materialization-server`, alongside `query-update-server`, so 1.5
/// clients can parse the response.
pub fn metrics_for_protocol(
    metrics: Option<serde_json::Value>,
    protocol_version: u32,
) -> Option<serde_json::Value> {
    use crate::tdigest::TDigest;
    match metrics {
        // TS: `if (protocolVersion >= 51 || metrics === null) return metrics;`.
        None => None,
        Some(m) if protocol_version >= 51 => Some(m),
        Some(m) => {
            let mut hydrate_digest = TDigest::default();
            // TS: `if (hydrateMs !== undefined) hydrateDigest.add(hydrateMs);`.
            if let Some(ms) = m
                .get("query-hydration-server-ms")
                .and_then(serde_json::Value::as_f64)
            {
                hydrate_digest.add(ms, 1.0);
            }
            let update = m
                .get("query-update-server")
                .cloned()
                .unwrap_or_else(|| TDigest::default().to_json_value());
            Some(serde_json::json!({
                "query-materialization-server": hydrate_digest.to_json_value(),
                "query-update-server": update,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Port-parity for `metricsForProtocol` (inspect-handler.ts:193): new protocol
    /// passes through, `null` passes through, old protocol wraps the scalar
    /// hydration ms into a one-point legacy digest.
    #[test]
    fn metrics_for_protocol_new_passes_through_old_wraps_hydration() {
        let per_query = serde_json::json!({
            "query-hydration-server-ms": 9.0,
            "query-update-server": [1000, 2.0, 1],
        });
        // Protocol >= 51: unchanged.
        assert_eq!(
            metrics_for_protocol(Some(per_query.clone()), 51),
            Some(per_query.clone())
        );
        // null passes through regardless.
        assert_eq!(metrics_for_protocol(None, 40), None);
        // Protocol < 51: hydration ms wrapped under the legacy key.
        assert_eq!(
            metrics_for_protocol(Some(per_query), 50),
            Some(serde_json::json!({
                "query-materialization-server": [1000, 9, 1],
                "query-update-server": [1000, 2.0, 1],
            }))
        );
        // Old protocol with NO hydration ms → empty legacy digest.
        assert_eq!(
            metrics_for_protocol(Some(serde_json::json!({"query-update-server": [1000]})), 50),
            Some(serde_json::json!({
                "query-materialization-server": [1000],
                "query-update-server": [1000],
            }))
        );
    }

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
        let result = crate::services::analyze::analyze_query(
            &path, "app", &ast, true, false, loaded, None, false,
        )
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

    fn test_ctx() -> CustomQueryContext {
        CustomQueryContext {
            url: "http://api.example/query".to_string(),
            allowed_urls: vec![],
            api_key: None,
            client_headers: vec![],
            request_headers: vec![],
            cookie: None,
            origin: None,
            auth: None,
            user_id: None,
            client_id: String::new(),
            ws_id: String::new(),
            revision: 0,
        }
    }

    fn test_shard() -> rust_cvr::shards::ShardID {
        rust_cvr::shards::ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        }
    }

    /// A3: a named analyze-query (`body.name`/`body.args`) is transformed against
    /// the user's query API server and analyzed as a NON-legacy query (so the
    /// deployed read-permissions are NOT re-applied — the API server already did).
    /// The transform cache is seeded so no network call is made. NON-VACUOUS:
    /// reverting the named path (treating it as legacy / not transforming) either
    /// returns `legacy_query=true` or fails to find the AST.
    #[tokio::test]
    async fn named_query_transforms_via_cache_and_is_not_legacy() {
        use crate::custom_queries::transform_query::{
            TransformedQuery, seed_transform_cache_for_test,
        };

        let ctx = test_ctx();
        let transformed_ast = serde_json::json!({"table": "users", "orderBy": [["id", "asc"]]});
        let id = crate::hash_of_name_and_args("myQuery", &[serde_json::json!(7)]);
        seed_transform_cache_for_test(
            &ctx,
            &id,
            &TransformedQuery {
                id: id.clone(),
                ast: transformed_ast.clone(),
                hash: "h".to_string(),
            },
        );

        let body = serde_json::json!({"op": "analyze-query", "name": "myQuery", "args": [7]});
        let (ast, legacy) = resolve_analyze_ast(&body, Some(ctx), &test_shard())
            .await
            .expect("named query resolves via the seeded transform cache");
        assert_eq!(ast, transformed_ast, "the transformed AST is returned");
        assert!(
            !legacy,
            "a named query is not a legacy query (skips loadPermissions)"
        );
    }

    /// A3: a named query with no configured transform context errors like the TS
    /// `assert(this.#customQueryTransformer, ...)`. NON-VACUOUS: the error text
    /// pins the branch; removing the `ok_or_else` guard changes the failure.
    #[tokio::test]
    async fn named_query_without_transform_context_errors() {
        let body = serde_json::json!({"op": "analyze-query", "name": "q", "args": []});
        let err = resolve_analyze_ast(&body, None, &test_shard())
            .await
            .unwrap_err();
        assert!(
            err.contains("no CustomQueryTransformer is configured"),
            "expected the missing-transformer error; got {err:?}"
        );
    }

    /// A legacy analyze-query (`body.ast`) resolves to that AST as a legacy query.
    #[tokio::test]
    async fn legacy_body_ast_is_legacy_query() {
        let ast = serde_json::json!({"table": "users"});
        let body = serde_json::json!({"op": "analyze-query", "ast": ast});
        let (out, legacy) = resolve_analyze_ast(&body, None, &test_shard())
            .await
            .expect("legacy ast resolves");
        assert_eq!(out, ast);
        assert!(
            legacy,
            "a body.ast query is a legacy (permission-loading) query"
        );
    }

    /// A bodiless analyze-query (no ast, no named query) errors "AST is required".
    #[tokio::test]
    async fn bodiless_analyze_query_requires_ast() {
        let body = serde_json::json!({"op": "analyze-query"});
        let err = resolve_analyze_ast(&body, None, &test_shard())
            .await
            .unwrap_err();
        assert!(err.contains("AST is required"), "got {err:?}");
    }
}
