//! Custom (named) query transformation — port of `CustomQueryTransformer`
//! (`custom-queries/transform-query.ts`) + the request construction in
//! `custom/fetch.ts`.
//!
//! Named queries arrive from the client as `{name, args}` (no AST). Before they
//! can be hydrated, the syncer POSTs them to the user's query API server
//! (`userQueryURL`), which returns a concrete AST per query. The response is
//! cached for 5s per (url, auth, forwarded-headers, query id) — matching TS,
//! whose `getCacheKey` includes url + token + cookie + origin + userID +
//! customHeaders (the ViewSyncer would otherwise call the API server 3-4× with
//! identical queries).
//!
//! Whole-request failures (`TransformFailed` / HTTP error) surface as `Err` so
//! the caller can fail the connection while leaving existing pipelines intact
//! (TS throws a `ProtocolErrorWithLevel`). Per-query errors are returned inline
//! as `Errored` so the caller can forward them to the client via
//! `transformError` without dropping the healthy queries.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex as StdMutex};
use std::time::{Duration, Instant};

use rust_cvr::shards::ShardID;
use serde_json::Value;

use crate::auth::read_authorizer::hash_of_ast;
use crate::custom::fetch::{get_backoff_delay_ms, url_match};

/// TS `CustomQueryTransformer` cache TTL — 5s (chosen to be shorter than a
/// typical short-lived auth token, so a re-auth re-transforms promptly).
const CACHE_TTL: Duration = Duration::from_secs(5);

/// Cached per-query transform results, keyed by `url|auth|headers-digest|id`.
/// Mirrors the TS per-connection `TimedCache`, but process-wide — so the key
/// MUST encode the full request identity that scopes authorization (URL, token,
/// AND the forwarded cookie/origin/custom headers). Omitting the headers would
/// let one connection read another's authorization-scoped transform.
static TRANSFORM_CACHE: LazyLock<StdMutex<HashMap<String, (Instant, TransformedQuery)>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

/// The per-connection context needed to reach the user's query API server.
///
/// Header sources are kept SEPARATE so the outgoing request can compose them
/// with TS `fetchFromAPIServer`'s exact overwrite precedence (fetch.ts):
/// `X-Api-Key` → client custom headers → forwarded request headers →
/// `Authorization` → `Cookie` → `Origin` — later sources REPLACE earlier
/// same-name headers rather than appending duplicate header lines.
#[derive(Clone, Default)]
pub struct CustomQueryContext {
    /// The `userQueryURL` from the client's `initConnection` (or the server's
    /// configured default).
    pub url: String,
    /// The configured `ZERO_QUERY_URL` allow-list (URL patterns). Checked at
    /// request time, exactly like TS `fetchFromAPIServer`'s `urlMatch` — a
    /// disallowed override surfaces as a per-request `TransformFailed`, not a
    /// connection close.
    pub allowed_urls: Vec<String>,
    /// Configured API key (`X-Api-Key`) — lowest precedence.
    pub api_key: Option<String>,
    /// Allowlisted client custom headers (`userQueryHeaders`).
    pub client_headers: Vec<(String, String)>,
    /// Allowlisted forwarded incoming request headers (#6144); override client
    /// headers on collision (TS `Object.assign(customHeaders, requestHeaders)`).
    pub request_headers: Vec<(String, String)>,
    /// Config-gated forwarded `Cookie` (overrides everything below Origin).
    pub cookie: Option<String>,
    /// The WS upgrade `Origin`, forwarded unconditionally (highest precedence).
    pub origin: Option<String>,
    /// The connection's raw JWT, sent as `Authorization: Bearer <auth>`.
    pub auth: Option<String>,
    /// The group's pinned userID — part of the transform cache key (TS
    /// `getCacheKey` includes userID).
    pub user_id: Option<String>,
}

/// Insert-or-replace (case-insensitive) — the composition primitive matching
/// TS record-key overwrite semantics for outgoing headers.
fn set_header(headers: &mut Vec<(String, String)>, name: &str, value: String) {
    if let Some(entry) = headers
        .iter_mut()
        .find(|(existing, _)| existing.eq_ignore_ascii_case(name))
    {
        entry.1 = value;
    } else {
        headers.push((name.to_string(), value));
    }
}

impl CustomQueryContext {
    /// The composed outgoing header set in TS `fetchFromAPIServer` order.
    /// `Content-Type` is set by the request builder.
    pub fn composed_headers(&self) -> Vec<(String, String)> {
        let mut headers: Vec<(String, String)> = Vec::new();
        if let Some(api_key) = self.api_key.as_ref().filter(|v| !v.is_empty()) {
            set_header(&mut headers, "X-Api-Key", api_key.clone());
        }
        for (k, v) in &self.client_headers {
            set_header(&mut headers, k, v.clone());
        }
        for (k, v) in &self.request_headers {
            set_header(&mut headers, k, v.clone());
        }
        if let Some(auth) = self.auth.as_ref().filter(|v| !v.is_empty()) {
            set_header(&mut headers, "Authorization", format!("Bearer {auth}"));
        }
        if let Some(cookie) = &self.cookie {
            set_header(&mut headers, "Cookie", cookie.clone());
        }
        if let Some(origin) = &self.origin {
            set_header(&mut headers, "Origin", origin.clone());
        }
        headers
    }
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
pub async fn transform(
    ctx: &CustomQueryContext,
    shard: &ShardID,
    queries: &[CustomQuerySpec],
) -> Result<Vec<CustomTransformed>, Value> {
    let mut results: Vec<CustomTransformed> = Vec::new();
    let mut to_fetch: Vec<&CustomQuerySpec> = Vec::new();

    // Split into cached vs. uncached (TS `transform()` cache split).
    for spec in queries {
        if let Some(cached) = cache_get(ctx, &spec.id) {
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

    // The IDs of the queries in THIS batch. On a whole-request failure the
    // `TransformFailed` body must carry them (TS `transform-query.ts` overrides
    // fetch.ts's empty `[]` with `request.map(({id})=>id)`) so the client can
    // attribute the failure to specific queries and mark/retry them.
    let query_ids: Vec<Value> = to_fetch
        .iter()
        .map(|s| Value::String(s.id.clone()))
        .collect();

    let response = request_transform(ctx, shard, &body, &query_ids).await?;

    // A `QueryResponse` carries `queries: [...]`; a legacy `["transformed", [...]]`
    // tuple is a client-fallback response. Anything else (e.g. a `TransformFailed`
    // body) fails the whole request.
    let queries = extract_transform_queries(&response).ok_or_else(|| response.clone())?;

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
        cache_set(ctx, &id, &transformed);
        results.push(CustomTransformed::Ok(transformed));
    }

    Ok(results)
}

/// Extract the per-query results from a transform response. Port of the response
/// handling in TS `transform-query.ts`: a `QueryResponse` carries `queries: [...]`;
/// a legacy API server returns a `["transformed", [...queries]]` tuple (treated as
/// a client-fallback response). Returns `None` for anything else (e.g. a
/// `TransformFailed` body) so the caller fails the whole request.
fn extract_transform_queries(response: &Value) -> Option<Vec<Value>> {
    if let Some(arr) = response.get("queries").and_then(|q| q.as_array()) {
        return Some(arr.clone());
    }
    if response
        .as_array()
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        == Some("transformed")
    {
        return Some(
            response
                .as_array()
                .and_then(|a| a.get(1))
                .and_then(|q| q.as_array())
                .cloned()
                .unwrap_or_default(),
        );
    }
    None
}

/// Force the empty `/query` validation request used by auth maintenance. Port of
/// TS `CustomQueryTransformer.validate` (`transform-query.ts`).
///
/// Kept separate from `transform` because that path short-circuits
/// locally on an empty batch (`to_fetch.is_empty()`) and never hits the API
/// server — but validation MUST make the request so a token revoked/deauthorized
/// at the app layer (still cryptographically valid) is surfaced by the server.
/// Success is intentionally opaque (`Ok(())`); callers only care pass/fail.
pub async fn validate(ctx: &CustomQueryContext, shard: &ShardID) -> Result<(), Value> {
    let body = serde_json::json!(["transform", []]);
    request_transform(ctx, shard, &body, &[]).await.map(|_| ())
}

/// Whether an error body denotes an authorization failure. Port of TS
/// `isAuthErrorBody` (`auth/auth.ts`):
///  - `{error: "http", status: 401|403}`
///  - `{kind: "AuthInvalidated" | "Unauthorized"}`
///  - `{kind: "TransformFailed" | "PushFailed", reason: "http", status: 401|403}`
///
/// Used by the auth-maintenance revocation probe to decide invalidate (auth
/// error → close) vs defer (transient/API-down → keep + retry).
pub fn is_auth_error_body(body: &Value) -> bool {
    let is_auth_status = |body: &Value| {
        matches!(
            body.get("status").and_then(Value::as_u64),
            Some(401) | Some(403)
        )
    };

    if body.get("error").and_then(Value::as_str) == Some("http") {
        return is_auth_status(body);
    }
    match body.get("kind").and_then(Value::as_str) {
        Some("AuthInvalidated") | Some("Unauthorized") => true,
        Some("TransformFailed") | Some("PushFailed") => {
            // TS `ErrorReason.HTTP` is the lowercase `"http"`.
            body.get("reason").and_then(Value::as_str) == Some("http") && is_auth_status(body)
        }
        _ => false,
    }
}

/// TS `fetchFromAPIServer` retry parameters (#6315): up to 4 attempts, 5xx and
/// network errors retry with `min(1000, 100 * 2^(attempt-1) + jitter(0..100))`
/// ms of backoff; 4xx and malformed responses fail immediately.
const FETCH_MAX_ATTEMPTS: u32 = 4;

/// Query params reserved for zero-cache (TS `reservedParams`): the configured
/// URL may not already carry them.
const RESERVED_PARAMS: [&str; 2] = ["schema", "appID"];

/// POST `["transform", [...]]` to the API server. Port of `fetchFromAPIServer`:
/// URL allow-check (`urlMatch`), reserved-param guard, composed headers with
/// TS overwrite precedence, and the 4-attempt retry loop with backoff+jitter
/// on 5xx / network errors. Appends the `schema` + `appID` query params.
async fn request_transform(
    ctx: &CustomQueryContext,
    shard: &ShardID,
    body: &Value,
    query_ids: &[Value],
) -> Result<Value, Value> {
    let transform_failed = |reason: &str, msg: String| -> Value {
        serde_json::json!({
            "kind": "TransformFailed",
            "origin": "zero-cache",
            "reason": reason,
            "message": msg,
            // Real batch IDs (not `[]`) so the client can attribute the failure
            // to the specific queries. Port of TS `transform-query.ts` catch.
            "queryIDs": query_ids,
        })
    };

    // URL allow-check at request time (TS fetch.ts). An override the config
    // does not allow fails THIS transform; the connection survives.
    if !ctx
        .allowed_urls
        .iter()
        .any(|pattern| url_match(pattern, &ctx.url))
    {
        crate::custom::metrics::record_api_request("url_not_allowed");
        return Err(transform_failed(
            "internal",
            format!(
                "URL \"{}\" is not allowed by the ZERO_QUERY_URL configuration",
                ctx.url
            ),
        ));
    }

    let mut url = reqwest::Url::parse(&ctx.url)
        .map_err(|e| transform_failed("internal", format!("invalid userQueryURL: {e}")))?;
    // Reserved-param guard (TS `reservedParams`): the configured URL may not
    // already carry the params zero-cache appends.
    for reserved in RESERVED_PARAMS {
        if url.query_pairs().any(|(k, _)| k == reserved) {
            crate::custom::metrics::record_api_request("config_error");
            return Err(transform_failed(
                "internal",
                format!("The query URL cannot contain the reserved query param \"{reserved}\""),
            ));
        }
    }
    // Append `?schema={app}_{shard}&appID={app}` (TS `fetchFromAPIServer`).
    url.query_pairs_mut()
        .append_pair("schema", &format!("{}_{}", shard.app_id, shard.shard_num))
        .append_pair("appID", &shard.app_id);

    let headers = ctx.composed_headers();
    // One process-wide client: reqwest pools + keep-alives connections per
    // host, so repeated transforms reuse the TCP connection to the API server
    // instead of paying DNS + connect + slow-start on every request (TS's
    // `fetch` shares Node's global agent the same way).
    //
    // The timeout is NOT optional: `transform` is awaited
    // inline on the CG event loop, so a query-API server that accepts the
    // connection and never responds would otherwise freeze that client group
    // FOREVER (its message channel just queues; reconnecting clients land on
    // the same stuck CG). reqwest has no default timeout. A timeout maps to
    // the existing `fetch_error` retry branch. Node's undici enforces a 300s
    // headers timeout on the TS side; 30s is tighter because the caller
    // retries and a healthy transform is ~15ms.
    static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest client build cannot fail with static config")
    });
    let client = &*HTTP_CLIENT;
    crate::custom::metrics::record_api_in_flight(1);
    let result = post_transform_attempts(client, url, &headers, body, &transform_failed).await;
    crate::custom::metrics::record_api_in_flight(-1);
    result
}

async fn post_transform_attempts(
    client: &reqwest::Client,
    url: reqwest::Url,
    headers: &[(String, String)],
    body: &Value,
    transform_failed: &dyn Fn(&str, String) -> Value,
) -> Result<Value, Value> {
    let request_started = Instant::now();
    let mut attempt = 1u32;
    let outcome = loop {
        let mut req = client
            .post(url.clone())
            .header("Content-Type", "application/json");
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let attempt_started = Instant::now();
        let send_result = req.json(body).send().await;
        let attempt_ms = attempt_started.elapsed().as_secs_f64() * 1000.0;
        match send_result {
            Err(e) => {
                // Network errors can be transient (TS retries `fetch failed`).
                let will_retry = attempt < FETCH_MAX_ATTEMPTS;
                crate::custom::metrics::record_api_attempt(
                    "fetch_error",
                    will_retry,
                    attempt_ms,
                    attempt,
                    None,
                );
                if will_retry {
                    tokio::time::sleep(Duration::from_millis(get_backoff_delay_ms(attempt))).await;
                    attempt += 1;
                    continue;
                }
                break Err((
                    "fetch_error",
                    // A network failure (no HTTP response) is the ZeroCache
                    // non-`http` variant — no `status`, so `reason: 'internal'`
                    // per TS `transformFailedBodySchema` (not an auth failure).
                    transform_failed("internal", format!("query transform request failed: {e}")),
                ));
            }
            Ok(resp) => {
                let status = resp.status();
                if !status.is_success() {
                    // 5xx can be transient (TS retries them); 4xx fails now.
                    let will_retry = status.is_server_error() && attempt < FETCH_MAX_ATTEMPTS;
                    crate::custom::metrics::record_api_attempt(
                        "http_error",
                        will_retry,
                        attempt_ms,
                        attempt,
                        Some(status.as_u16()),
                    );
                    if will_retry {
                        tokio::time::sleep(Duration::from_millis(get_backoff_delay_ms(attempt)))
                            .await;
                        attempt += 1;
                        continue;
                    }
                    let preview = resp.text().await.unwrap_or_default();
                    // Port of the ZeroCache `reason: 'http'` TransformFailed
                    // variant (`error.ts` transformFailedBodySchema): carry the
                    // HTTP `status` (+ `bodyPreview`) so a 401/403 is recognizable
                    // as a server-side auth failure — see `is_auth_error_body`,
                    // used by the auth-maintenance revocation probe.
                    let mut failure = transform_failed(
                        "http",
                        format!("query transform returned {status}: {preview}"),
                    );
                    if let Some(obj) = failure.as_object_mut() {
                        obj.insert("status".into(), serde_json::json!(status.as_u16()));
                        obj.insert("bodyPreview".into(), serde_json::json!(preview));
                    }
                    break Err(("http_error", failure));
                }
                match resp.json::<Value>().await {
                    Ok(v) => {
                        crate::custom::metrics::record_api_attempt(
                            "success",
                            false,
                            attempt_ms,
                            attempt,
                            Some(status.as_u16()),
                        );
                        break Ok(v);
                    }
                    Err(e) => {
                        crate::custom::metrics::record_api_attempt(
                            "parse_error",
                            false,
                            attempt_ms,
                            attempt,
                            Some(status.as_u16()),
                        );
                        break Err((
                            "parse_error",
                            transform_failed(
                                "internal",
                                format!("invalid transform response: {e}"),
                            ),
                        ));
                    }
                }
            }
        }
    };
    let request_ms = request_started.elapsed().as_secs_f64() * 1000.0;
    match outcome {
        Ok(v) => {
            crate::custom::metrics::record_api_request("success");
            crate::custom::metrics::record_api_request_duration(request_ms);
            Ok(v)
        }
        Err((result, body)) => {
            crate::custom::metrics::record_api_request(result);
            crate::custom::metrics::record_api_request_duration(request_ms);
            Err(body)
        }
    }
}

/// A stable digest of the COMPOSED outgoing headers (api-key, client custom,
/// forwarded request headers, cookie, origin). Folded into the transform cache
/// key so two connections that share a URL + token but differ in forwarded
/// credentials can NOT read each other's cached (authorization-scoped)
/// transform. Port of TS `getCacheKey`, which includes cookie, origin, userID,
/// and customHeaders alongside url+token+id.
fn normalized_headers(headers: &[(String, String)]) -> String {
    let mut pairs: Vec<&(String, String)> = headers.iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let canonical: String = pairs.iter().map(|(k, v)| format!("{k}={v}\n")).collect();
    format!("{:016x}", rust_cvr::hash::h64(&canonical))
}

fn get_cache_key(ctx: &CustomQueryContext, id: &str) -> String {
    format!(
        "{}|{}|{}|{}|{id}",
        ctx.url,
        ctx.auth.as_deref().unwrap_or(""),
        ctx.user_id.as_deref().unwrap_or(""),
        normalized_headers(&ctx.composed_headers())
    )
}

fn cache_get(ctx: &CustomQueryContext, id: &str) -> Option<TransformedQuery> {
    let mut cache = TRANSFORM_CACHE.lock().ok()?;
    let key = get_cache_key(ctx, id);
    match cache.get(&key) {
        Some((at, q)) if at.elapsed() < CACHE_TTL => Some(q.clone()),
        // Expired → evict on read so a key that is never re-requested doesn't
        // linger forever (TS `TimedCache` reclaims via a periodic sweep).
        Some(_) => {
            cache.remove(&key);
            None
        }
        None => None,
    }
}

fn cache_set(ctx: &CustomQueryContext, id: &str, q: &TransformedQuery) {
    if let Ok(mut cache) = TRANSFORM_CACHE.lock() {
        // Sweep expired entries before inserting. Without this the process-wide
        // cache grows unbounded as rotating short-lived JWTs mint fresh keys that
        // are never re-read (a leak TS's `TimedCache` interval cleanup avoids).
        cache.retain(|_, (at, _)| at.elapsed() < CACHE_TTL);
        cache.insert(get_cache_key(ctx, id), (Instant::now(), q.clone()));
    }
}

/// Test-only: seed the process-wide transform cache so a custom query resolves
/// without a network call. Used by parity integration tests
/// (`tests/stage_e_test.rs`) to drive the full custom-query hydrate path
/// (transform → executed → hydrate → poke) offline. `#[doc(hidden)]` — not part
/// of the real API.
#[doc(hidden)]
pub fn seed_transform_cache_for_test(ctx: &CustomQueryContext, id: &str, q: &TransformedQuery) {
    cache_set(ctx, id, q);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_transform_queries_handles_modern_and_legacy() {
        // Modern `QueryResponse` with `queries: [...]`.
        let modern = serde_json::json!({"queries": [{"id": "a", "ast": {}}]});
        assert_eq!(extract_transform_queries(&modern).unwrap().len(), 1);
        // Legacy `["transformed", [...]]` tuple → client-fallback (F-TQ-7). Fails
        // on the pre-fix code, which only read `queries` → this arm went to Err.
        let legacy =
            serde_json::json!(["transformed", [{"id": "a", "ast": {}}, {"id": "b", "ast": {}}]]);
        assert_eq!(extract_transform_queries(&legacy).unwrap().len(), 2);
        // A `TransformFailed` body (or any other shape) → None (whole-batch fail).
        let failed = serde_json::json!({"kind": "TransformFailed", "message": "boom"});
        assert!(extract_transform_queries(&failed).is_none());
    }

    #[tokio::test]
    async fn transform_failure_carries_batch_query_ids() {
        // A whole-request failure must carry the real batch IDs (F-TQ-1), not `[]`.
        // The URL-not-allowed path fails synchronously (no network) with the
        // `transform_failed` body. Pre-fix this hardcoded `queryIDs: []` → the
        // assertion below fails on the old code.
        let ctx = CustomQueryContext {
            // Unique URL so the process-wide TRANSFORM_CACHE never has a hit here.
            url: "https://f-tq-1.example/query".to_string(),
            allowed_urls: vec!["https://allowed.example/*".to_string()],
            ..CustomQueryContext::default()
        };
        let shard = ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        };
        let specs = vec![
            CustomQuerySpec {
                id: "q1".to_string(),
                name: "n1".to_string(),
                args: vec![],
            },
            CustomQuerySpec {
                id: "q2".to_string(),
                name: "n2".to_string(),
                args: vec![],
            },
        ];
        let err = transform(&ctx, &shard, &specs)
            .await
            .err()
            .expect("transform should fail for a disallowed URL");
        let ids: Vec<&str> = err["queryIDs"]
            .as_array()
            .expect("queryIDs array")
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["q1", "q2"]);
    }

    #[test]
    fn is_auth_error_body_matches_ts_is_auth_error_body() {
        // {error:"http", status:401|403} → auth
        assert!(is_auth_error_body(
            &serde_json::json!({"error": "http", "status": 401})
        ));
        assert!(is_auth_error_body(
            &serde_json::json!({"error": "http", "status": 403})
        ));
        assert!(!is_auth_error_body(
            &serde_json::json!({"error": "http", "status": 500})
        ));
        // {kind: AuthInvalidated|Unauthorized} → auth
        assert!(is_auth_error_body(
            &serde_json::json!({"kind": "Unauthorized"})
        ));
        assert!(is_auth_error_body(
            &serde_json::json!({"kind": "AuthInvalidated"})
        ));
        // TransformFailed is auth ONLY when reason=="http" AND status in {401,403}.
        assert!(is_auth_error_body(&serde_json::json!({
            "kind": "TransformFailed", "reason": "http", "status": 401
        })));
        assert!(!is_auth_error_body(&serde_json::json!({
            "kind": "TransformFailed", "reason": "http", "status": 500
        })));
        // A transient/API-down TransformFailed must NOT count as auth (→ defer,
        // don't close the connection).
        assert!(!is_auth_error_body(&serde_json::json!({
            "kind": "TransformFailed", "reason": "internal", "message": "boom"
        })));
        assert!(!is_auth_error_body(&serde_json::json!({
            "kind": "TransformFailed", "reason": "http", "status": 503
        })));
    }

    /// One-shot HTTP stub: accepts a single connection, consumes the request
    /// (headers + Content-Length body), answers with `status` + `body`, closes.
    fn spawn_http_stub(status: &'static str, body: &'static str) -> String {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            // Read until the end of headers, then the Content-Length body.
            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];
            let (mut header_end, mut content_len) = (None, 0usize);
            loop {
                let n = stream.read(&mut chunk).unwrap_or(0);
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                if header_end.is_none()
                    && let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n")
                {
                    header_end = Some(pos + 4);
                    let headers = String::from_utf8_lossy(&buf[..pos]).to_lowercase();
                    content_len = headers
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .and_then(|v| v.trim().parse().ok())
                        .unwrap_or(0);
                }
                if let Some(end) = header_end
                    && buf.len() >= end + content_len
                {
                    break;
                }
            }
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        });
        format!("http://{addr}/query")
    }

    /// Port of TS `CustomQueryTransformer.validate` (custom-queries/
    /// transform-query.ts): the auth-maintenance probe POSTs an EMPTY
    /// `["transform", []]` batch. A 200 response validates (`Ok(())`); a 401
    /// rejection surfaces the exact ZeroCache TransformFailed/http body —
    /// `{kind: TransformFailed, origin: zero-cache, reason: http, status: 401,
    /// queryIDs: []}` — which `is_auth_error_body` classifies as an auth error
    /// (revoked token → close), per the fetch.ts 4xx no-retry branch.
    #[tokio::test]
    async fn validate_custom_queries_ok_on_200_and_auth_error_body_on_401() {
        let shard = ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        };

        // Happy path: 200 with an empty QueryResponse → opaque Ok(()).
        let ok_url = spawn_http_stub("200 OK", r#"{"queries":[]}"#);
        let ctx = CustomQueryContext {
            url: ok_url.clone(),
            allowed_urls: vec![ok_url],
            ..CustomQueryContext::default()
        };
        assert!(validate(&ctx, &shard).await.is_ok());

        // Auth-revoked path: 401 fails immediately (4xx is never retried) with
        // the reason-http body carrying the status.
        let unauth_url = spawn_http_stub("401 Unauthorized", r#"{"message":"revoked"}"#);
        let ctx = CustomQueryContext {
            url: unauth_url.clone(),
            allowed_urls: vec![unauth_url],
            ..CustomQueryContext::default()
        };
        let err = validate(&ctx, &shard)
            .await
            .expect_err("401 must fail validation");
        assert_eq!(err["kind"], "TransformFailed");
        assert_eq!(err["origin"], "zero-cache");
        assert_eq!(err["reason"], "http");
        assert_eq!(err["status"], 401);
        assert_eq!(err["queryIDs"], serde_json::json!([]));
        assert!(
            is_auth_error_body(&err),
            "the 401 body must classify as an auth error: {err}"
        );
    }

    fn shard() -> ShardID {
        ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        }
    }

    fn ctx_at(url: &str) -> CustomQueryContext {
        CustomQueryContext {
            url: url.to_string(),
            allowed_urls: vec![url.to_string()],
            ..CustomQueryContext::default()
        }
    }

    #[test]
    fn empty_specs_short_circuit_without_network() {
        // No specs → returns an empty result without touching the network.
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = ctx_at("http://127.0.0.1:1/never");
        let out = rt.block_on(transform(&ctx, &shard(), &[])).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn cached_queries_skip_the_network() {
        // Seed the cache so a query resolves without a request. The bogus URL
        // proves no network call happens (it would error otherwise).
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut ctx = ctx_at("http://127.0.0.1:1/cached-test");
        ctx.auth = Some("tok".to_string());
        let tq = TransformedQuery {
            id: "q1".to_string(),
            ast: serde_json::json!({"table": "issue"}),
            hash: "hash1".to_string(),
        };
        cache_set(&ctx, "q1", &tq);

        let specs = vec![CustomQuerySpec {
            id: "q1".to_string(),
            name: "myQuery".to_string(),
            args: vec![],
        }];
        let out = rt.block_on(transform(&ctx, &shard(), &specs)).unwrap();
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
    fn cache_key_distinguishes_url_auth_user_headers_and_id() {
        let base = ctx_at("u");
        let with_auth = |auth: &str| {
            let mut c = base.clone();
            c.auth = Some(auth.to_string());
            c
        };
        assert_ne!(
            get_cache_key(&with_auth("a"), "1"),
            get_cache_key(&with_auth("b"), "1")
        );
        assert_ne!(
            get_cache_key(&with_auth("a"), "1"),
            get_cache_key(&ctx_at("v"), "1")
        );
        assert_ne!(
            get_cache_key(&with_auth("a"), "1"),
            get_cache_key(&with_auth("a"), "2")
        );
        assert_eq!(get_cache_key(&base, "1"), get_cache_key(&base, "1"));

        // The pinned userID partitions the cache (TS getCacheKey has userID).
        let mut ua = base.clone();
        ua.user_id = Some("alice".to_string());
        let mut ub = base.clone();
        ub.user_id = Some("bob".to_string());
        assert_ne!(get_cache_key(&ua, "1"), get_cache_key(&ub, "1"));

        // Forwarded credentials (cookie/origin/custom headers) must partition
        // the cache: same url+auth+id but a different cookie → different key.
        let mut ca = base.clone();
        ca.cookie = Some("session=A".to_string());
        let mut cb = base.clone();
        cb.cookie = Some("session=B".to_string());
        assert_ne!(get_cache_key(&ca, "1"), get_cache_key(&cb, "1"));
    }

    /// TS `fetchFromAPIServer` header precedence (fetch.ts): api-key → client
    /// custom → forwarded request headers → Authorization → Cookie → Origin,
    /// each REPLACING (not appending) a same-name earlier entry.
    #[test]
    fn composed_headers_apply_ts_overwrite_precedence() {
        let ctx = CustomQueryContext {
            url: "u".to_string(),
            allowed_urls: vec![],
            api_key: Some("config-key".to_string()),
            client_headers: vec![
                ("x-api-key".to_string(), "client-key".to_string()),
                ("x-tenant".to_string(), "client-tenant".to_string()),
                ("authorization".to_string(), "client-auth".to_string()),
            ],
            request_headers: vec![("x-tenant".to_string(), "forwarded-tenant".to_string())],
            cookie: Some("session=cfg".to_string()),
            origin: Some("https://app".to_string()),
            auth: Some("jwt".to_string()),
            user_id: None,
        };
        let headers = ctx.composed_headers();
        let get = |name: &str| {
            headers
                .iter()
                .filter(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone())
                .collect::<Vec<_>>()
        };
        // Client X-Api-Key REPLACES the configured key (single header line).
        assert_eq!(get("x-api-key"), vec!["client-key"]);
        // Forwarded request header overrides the client header.
        assert_eq!(get("x-tenant"), vec!["forwarded-tenant"]);
        // The Bearer token overrides a client-smuggled authorization.
        assert_eq!(get("authorization"), vec!["Bearer jwt"]);
        assert_eq!(get("cookie"), vec!["session=cfg"]);
        assert_eq!(get("origin"), vec!["https://app"]);
    }

    /// URLPattern-subset matching (TS `urlMatch`): literals, `*`, `:name` path
    /// params; candidate query/hash ignored.
    #[test]
    fn url_pattern_matching() {
        assert!(url_match(
            "https://api.example.com/query",
            "https://api.example.com/query"
        ));
        assert!(url_match(
            "https://api.example.com/query",
            "https://api.example.com/query?tenant=1"
        ));
        assert!(url_match(
            "https://api.example.com/*",
            "https://api.example.com/v2/query"
        ));
        assert!(url_match(
            "https://*.example.com/query",
            "https://tenant-a.example.com/query"
        ));
        assert!(url_match(
            "https://api.example.com/:tenant/query",
            "https://api.example.com/acme/query"
        ));
        // A :param is a single path segment.
        assert!(!url_match(
            "https://api.example.com/:tenant/query",
            "https://api.example.com/a/b/query"
        ));
        assert!(!url_match(
            "https://api.example.com/query",
            "https://evil.example.com/query"
        ));
        // A port stays literal (':8080' is not a param — digits can't start a
        // param name).
        assert!(url_match(
            "http://localhost:8080/query",
            "http://localhost:8080/query"
        ));
        assert!(!url_match(
            "http://localhost:8080/query",
            "http://localhost:9090/query"
        ));
    }
}
