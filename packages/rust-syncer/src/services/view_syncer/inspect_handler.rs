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

use super::view_syncer::ViewSyncerService;

/// Port of TS `isAdminPasswordValid` (config/zero-config.ts). In DEVELOPMENT
/// mode (`NODE_ENV=development`) with no admin password configured and none
/// provided, access is allowed (open inspector). Otherwise a configured admin
/// password must be non-empty and match. rust previously omitted the dev-mode
/// branch (`admin_password.is_some_and(...)` alone), so a dev sandbox with no
/// `ZERO_ADMIN_PASSWORD` LOCKED the inspector where TS OPENED it — caught by the
/// G49/E inspect-auth differential (2026-08-28: rust authenticated:false, TS
/// answered `queries` as an authenticated CG).
pub fn is_admin_password_valid(
    password: &str,
    admin_password: Option<&str>,
    dev_mode: bool,
) -> bool {
    if password.is_empty() && admin_password.is_none() && dev_mode {
        return true;
    }
    admin_password.is_some_and(|p| !p.is_empty() && p == password)
}

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
        // `analyzeQuery` (query plan / vended-rows analysis) is not ported.
        // Route through the error op: an `analyze-query` success frame with
        // an `{error}` payload would fail `analyzeQueryResultSchema` on the
        // client and hang the RPC.
        "analyze-query" => Err("analyze-query is not supported by the rust syncer yet".to_string()),
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
    use super::is_admin_password_valid;

    /// Port fidelity for TS `isAdminPasswordValid` (config/zero-config.ts). The
    /// dev-mode-no-password branch is the one rust omitted (G49/E finding).
    /// Non-vacuous: dropping that branch (the pre-fix `admin_password.is_some_and`
    /// alone) makes the first assertion fail — dev sandbox would lock the inspector.
    #[test]
    fn is_admin_password_valid_matches_ts() {
        // dev mode + no admin password + no password provided → OPEN (the fix).
        assert!(is_admin_password_valid("", None, true));
        // production (not dev) + no admin password → LOCKED.
        assert!(!is_admin_password_valid("", None, false));
        // admin password configured: must match exactly.
        assert!(is_admin_password_valid("secret", Some("secret"), true));
        assert!(!is_admin_password_valid("wrong", Some("secret"), true));
        // empty configured password never authenticates.
        assert!(!is_admin_password_valid("", Some(""), true));
        // dev mode does NOT bypass a configured password.
        assert!(!is_admin_password_valid("", Some("secret"), true));
    }
}
