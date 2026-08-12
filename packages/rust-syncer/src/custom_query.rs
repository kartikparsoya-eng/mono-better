//! Custom (named) query transformation — port of `CustomQueryTransformer`
//! (`custom-queries/transform-query.ts`) + the request construction in
//! `custom/fetch.ts`.
//!
//! Named queries arrive from the client as `{name, args}` (no AST). Before they
//! can be hydrated, the syncer POSTs them to the user's query API server
//! (`userQueryURL`), which returns a concrete AST per query. The response is
//! cached for 5s per (url, auth, query id) — matching TS, which notes the
//! ViewSyncer would otherwise call the API server 3-4× with identical queries.
//!
//! Whole-request failures (`TransformFailed` / HTTP error) surface as `Err` so
//! the caller can fail the connection while leaving existing pipelines intact
//! (TS throws a `ProtocolErrorWithLevel`). Per-query errors are returned inline
//! as `Errored` so the caller can forward them to the client via
//! `transformError` without dropping the healthy queries.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex as StdMutex};
use std::time::{Duration, Instant};

use rust_cvr::types::ShardID;
use serde_json::Value;

use crate::permissions::hash_of_ast;

/// TS `CustomQueryTransformer` cache TTL — 5s (chosen to be shorter than a
/// typical short-lived auth token, so a re-auth re-transforms promptly).
const CACHE_TTL: Duration = Duration::from_secs(5);

/// Cached per-query transform results, keyed by `url|auth|id`. Mirrors the TS
/// per-connection `TimedCache`, but process-wide (the key encodes the request
/// identity that matters: URL, auth, and query id).
static TRANSFORM_CACHE: LazyLock<StdMutex<HashMap<String, (Instant, TransformedQuery)>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

/// The per-connection context needed to reach the user's query API server.
#[derive(Clone)]
pub struct CustomQueryContext {
    /// The `userQueryURL` from the client's `initConnection`.
    pub url: String,
    /// Custom headers forwarded to the API server (already filtered by the
    /// caller against the allow-list): may include `Cookie`, `Origin`,
    /// `X-Api-Key`, etc.
    pub headers: Vec<(String, String)>,
    /// The connection's raw JWT, sent as `Authorization: Bearer <auth>`.
    pub auth: Option<String>,
}

/// One named query to transform.
pub struct CustomQuerySpec {
    pub id: String,
    pub name: String,
    pub args: Vec<Value>,
}

/// A successfully transformed query (its concrete AST + `hashOfAST`).
#[derive(Clone)]
pub struct TransformedQuery {
    pub id: String,
    pub ast: Value,
    pub hash: String,
}

/// The per-query outcome of a transform.
pub enum CustomTransformed {
    /// A concrete AST was returned.
    Ok(TransformedQuery),
    /// The API server reported a per-query error (`{error, id, name, ...}`);
    /// forwarded to the client as a `transformError` without failing others.
    Errored { id: String, error: Value },
}

/// Transform a batch of named queries against the user's query API server.
/// Cached results are reused; only cache-missing queries hit the network.
/// Returns `Err(TransformFailed body)` on a whole-request failure.
pub fn transform_custom_queries(
    handle: &tokio::runtime::Handle,
    ctx: &CustomQueryContext,
    shard: &ShardID,
    specs: &[CustomQuerySpec],
) -> Result<Vec<CustomTransformed>, Value> {
    let mut results: Vec<CustomTransformed> = Vec::new();
    let mut to_fetch: Vec<&CustomQuerySpec> = Vec::new();

    // Split into cached vs. uncached (TS `transform()` cache split).
    for spec in specs {
        if let Some(cached) = cache_get(&ctx.url, ctx.auth.as_deref(), &spec.id) {
            results.push(CustomTransformed::Ok(cached));
        } else {
            to_fetch.push(spec);
        }
    }
    if to_fetch.is_empty() {
        return Ok(results);
    }

    let body = serde_json::json!([
        "transform",
        to_fetch
            .iter()
            .map(|s| serde_json::json!({"id": s.id, "name": s.name, "args": s.args}))
            .collect::<Vec<_>>()
    ]);

    let response = handle.block_on(post_transform(ctx, shard, &body))?;

    // A `QueryResponse` carries `queries: [...]`; a `TransformFailed` (has
    // `kind:"TransformFailed"` or no `queries`) fails the whole request.
    let queries = response
        .get("queries")
        .and_then(|q| q.as_array())
        .cloned()
        .ok_or_else(|| response.clone())?;

    for q in queries {
        let id = q
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if q.get("error").is_some() {
            results.push(CustomTransformed::Errored { id, error: q });
            continue;
        }
        let Some(ast) = q.get("ast").cloned() else {
            // Malformed entry — treat as a per-query error so it doesn't take
            // the whole batch down.
            results.push(CustomTransformed::Errored {
                id,
                error: serde_json::json!({"error": "parse", "id": q.get("id"), "message": "missing ast"}),
            });
            continue;
        };
        let hash = hash_of_ast(&ast);
        let transformed = TransformedQuery {
            id: id.clone(),
            ast,
            hash,
        };
        cache_set(&ctx.url, ctx.auth.as_deref(), &id, &transformed);
        results.push(CustomTransformed::Ok(transformed));
    }

    Ok(results)
}

/// POST `["transform", [...]]` to the API server. Port of `fetchFromAPIServer`
/// (single attempt — the retry/backoff loop is a follow-up). Appends the
/// `schema` + `appID` query params and sets the auth/header set.
async fn post_transform(
    ctx: &CustomQueryContext,
    shard: &ShardID,
    body: &Value,
) -> Result<Value, Value> {
    let transform_failed = |reason: &str, msg: String| -> Value {
        serde_json::json!({
            "kind": "TransformFailed",
            "origin": "zero-cache",
            "reason": reason,
            "message": msg,
            "queryIDs": [],
        })
    };

    // Append `?schema={app}_{shard}&appID={app}` (TS `fetchFromAPIServer`).
    let mut url = reqwest::Url::parse(&ctx.url)
        .map_err(|e| transform_failed("Internal", format!("invalid userQueryURL: {e}")))?;
    url.query_pairs_mut()
        .append_pair("schema", &format!("{}_{}", shard.app_id, shard.shard_num))
        .append_pair("appID", &shard.app_id);

    let client = reqwest::Client::new();
    let mut req = client.post(url).header("Content-Type", "application/json");
    for (k, v) in &ctx.headers {
        req = req.header(k, v);
    }
    if let Some(auth) = &ctx.auth {
        req = req.header("Authorization", format!("Bearer {auth}"));
    }

    let resp =
        req.json(body).send().await.map_err(|e| {
            transform_failed("HTTP", format!("query transform request failed: {e}"))
        })?;
    let status = resp.status();
    if !status.is_success() {
        let preview = resp.text().await.unwrap_or_default();
        return Err(transform_failed(
            "HTTP",
            format!("query transform returned {status}: {preview}"),
        ));
    }
    resp.json::<Value>()
        .await
        .map_err(|e| transform_failed("Internal", format!("invalid transform response: {e}")))
}

fn cache_key(url: &str, auth: Option<&str>, id: &str) -> String {
    format!("{url}|{}|{id}", auth.unwrap_or(""))
}

fn cache_get(url: &str, auth: Option<&str>, id: &str) -> Option<TransformedQuery> {
    let cache = TRANSFORM_CACHE.lock().ok()?;
    let (at, q) = cache.get(&cache_key(url, auth, id))?;
    if at.elapsed() >= CACHE_TTL {
        return None;
    }
    Some(q.clone())
}

fn cache_set(url: &str, auth: Option<&str>, id: &str, q: &TransformedQuery) {
    if let Ok(mut cache) = TRANSFORM_CACHE.lock() {
        cache.insert(cache_key(url, auth, id), (Instant::now(), q.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard() -> ShardID {
        ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        }
    }

    #[test]
    fn empty_specs_short_circuit_without_network() {
        // No specs → returns an empty result without touching the network.
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = CustomQueryContext {
            url: "http://127.0.0.1:1/never".to_string(),
            headers: vec![],
            auth: None,
        };
        let out = transform_custom_queries(rt.handle(), &ctx, &shard(), &[]).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn cached_queries_skip_the_network() {
        // Seed the cache so a query resolves without a request. The bogus URL
        // proves no network call happens (it would error otherwise).
        let rt = tokio::runtime::Runtime::new().unwrap();
        let url = "http://127.0.0.1:1/cached-test";
        let tq = TransformedQuery {
            id: "q1".to_string(),
            ast: serde_json::json!({"table": "issue"}),
            hash: "hash1".to_string(),
        };
        cache_set(url, Some("tok"), "q1", &tq);

        let ctx = CustomQueryContext {
            url: url.to_string(),
            headers: vec![],
            auth: Some("tok".to_string()),
        };
        let specs = vec![CustomQuerySpec {
            id: "q1".to_string(),
            name: "myQuery".to_string(),
            args: vec![],
        }];
        let out = transform_custom_queries(rt.handle(), &ctx, &shard(), &specs).unwrap();
        assert_eq!(out.len(), 1);
        match &out[0] {
            CustomTransformed::Ok(t) => {
                assert_eq!(t.hash, "hash1");
                assert_eq!(t.ast, serde_json::json!({"table": "issue"}));
            }
            CustomTransformed::Errored { .. } => panic!("expected cached Ok result"),
        }
    }

    #[test]
    fn cache_key_distinguishes_url_auth_and_id() {
        assert_ne!(
            cache_key("u", Some("a"), "1"),
            cache_key("u", Some("b"), "1")
        );
        assert_ne!(
            cache_key("u", Some("a"), "1"),
            cache_key("v", Some("a"), "1")
        );
        assert_ne!(
            cache_key("u", Some("a"), "1"),
            cache_key("u", Some("a"), "2")
        );
        assert_eq!(cache_key("u", None, "1"), cache_key("u", None, "1"));
    }
}
