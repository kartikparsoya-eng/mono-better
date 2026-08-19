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

use rust_cvr::types::ShardID;
use serde_json::Value;

use crate::permissions::hash_of_ast;

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

/// URLPattern-style match of `url` against `pattern` (TS `urlMatch` /
/// `compileUrlPattern`). Supported subset: literal URLs, `*` (any characters)
/// and `:name` path params (one non-`/` segment). The candidate's query string
/// and fragment are ignored, matching URLPattern's implicit `search`/`hash`
/// wildcards. Unsupported URLPattern syntax simply fails to match (the
/// conservative direction).
pub fn url_pattern_matches(pattern: &str, url: &str) -> bool {
    let candidate = url.split(['?', '#']).next().unwrap_or(url);
    let pattern = pattern.split(['?', '#']).next().unwrap_or(pattern);
    glob_match(pattern.as_bytes(), candidate.as_bytes())
}

fn glob_match(p: &[u8], t: &[u8]) -> bool {
    match p.first() {
        None => t.is_empty(),
        Some(b'*') => (0..=t.len()).any(|i| glob_match(&p[1..], &t[i..])),
        // `:name` — a named path segment (starts with a letter/underscore, so a
        // port `:8080` or the `://` in a scheme stays literal). Matches one or
        // more non-`/` characters.
        Some(b':') if p.len() > 1 && (p[1].is_ascii_alphabetic() || p[1] == b'_') => {
            let mut name_end = 1;
            while name_end < p.len() && (p[name_end].is_ascii_alphanumeric() || p[name_end] == b'_')
            {
                name_end += 1;
            }
            let rest = &p[name_end..];
            if t.is_empty() || t[0] == b'/' {
                return false;
            }
            let mut consumed = 1;
            loop {
                if glob_match(rest, &t[consumed..]) {
                    return true;
                }
                if consumed == t.len() || t[consumed] == b'/' {
                    return false;
                }
                consumed += 1;
            }
        }
        Some(&c) => t.first() == Some(&c) && glob_match(&p[1..], &t[1..]),
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
pub async fn transform_custom_queries(
    ctx: &CustomQueryContext,
    shard: &ShardID,
    specs: &[CustomQuerySpec],
) -> Result<Vec<CustomTransformed>, Value> {
    let mut results: Vec<CustomTransformed> = Vec::new();
    let mut to_fetch: Vec<&CustomQuerySpec> = Vec::new();

    // Split into cached vs. uncached (TS `transform()` cache split).
    for spec in specs {
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

    let response = post_transform(ctx, shard, &body).await?;

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
        cache_set(ctx, &id, &transformed);
        results.push(CustomTransformed::Ok(transformed));
    }

    Ok(results)
}

/// TS `fetchFromAPIServer` retry parameters (#6315): up to 4 attempts, 5xx and
/// network errors retry with `min(1000, 100 * 2^(attempt-1) + jitter(0..100))`
/// ms of backoff; 4xx and malformed responses fail immediately.
const FETCH_MAX_ATTEMPTS: u32 = 4;

fn backoff_delay_ms(attempt: u32) -> u64 {
    let jitter = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0)
        % 100) as u64;
    (100u64 * 2u64.pow(attempt.saturating_sub(1)) + jitter).min(1000)
}

/// Query params reserved for zero-cache (TS `reservedParams`): the configured
/// URL may not already carry them.
const RESERVED_PARAMS: [&str; 2] = ["schema", "appID"];

/// POST `["transform", [...]]` to the API server. Port of `fetchFromAPIServer`:
/// URL allow-check (`urlMatch`), reserved-param guard, composed headers with
/// TS overwrite precedence, and the 4-attempt retry loop with backoff+jitter
/// on 5xx / network errors. Appends the `schema` + `appID` query params.
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

    // URL allow-check at request time (TS fetch.ts). An override the config
    // does not allow fails THIS transform; the connection survives.
    if !ctx
        .allowed_urls
        .iter()
        .any(|pattern| url_pattern_matches(pattern, &ctx.url))
    {
        crate::metrics::record_api_request("url_not_allowed");
        return Err(transform_failed(
            "Internal",
            format!(
                "URL \"{}\" is not allowed by the ZERO_QUERY_URL configuration",
                ctx.url
            ),
        ));
    }

    let mut url = reqwest::Url::parse(&ctx.url)
        .map_err(|e| transform_failed("Internal", format!("invalid userQueryURL: {e}")))?;
    // Reserved-param guard (TS `reservedParams`): the configured URL may not
    // already carry the params zero-cache appends.
    for reserved in RESERVED_PARAMS {
        if url.query_pairs().any(|(k, _)| k == reserved) {
            crate::metrics::record_api_request("config_error");
            return Err(transform_failed(
                "Internal",
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
    // The timeout is NOT optional: `transform_custom_queries` is awaited
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
    crate::metrics::record_api_in_flight(1);
    let result = post_transform_attempts(client, url, &headers, body, &transform_failed).await;
    crate::metrics::record_api_in_flight(-1);
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
                crate::metrics::record_api_attempt("fetch_error", will_retry, attempt_ms, attempt, None);
                if will_retry {
                    tokio::time::sleep(Duration::from_millis(backoff_delay_ms(attempt))).await;
                    attempt += 1;
                    continue;
                }
                break Err((
                    "fetch_error",
                    transform_failed("HTTP", format!("query transform request failed: {e}")),
                ));
            }
            Ok(resp) => {
                let status = resp.status();
                if !status.is_success() {
                    // 5xx can be transient (TS retries them); 4xx fails now.
                    let will_retry = status.is_server_error() && attempt < FETCH_MAX_ATTEMPTS;
                    crate::metrics::record_api_attempt("http_error", will_retry, attempt_ms, attempt, Some(status.as_u16()));
                    if will_retry {
                        tokio::time::sleep(Duration::from_millis(backoff_delay_ms(attempt))).await;
                        attempt += 1;
                        continue;
                    }
                    let preview = resp.text().await.unwrap_or_default();
                    break Err((
                        "http_error",
                        transform_failed(
                            "HTTP",
                            format!("query transform returned {status}: {preview}"),
                        ),
                    ));
                }
                match resp.json::<Value>().await {
                    Ok(v) => {
                        crate::metrics::record_api_attempt("success", false, attempt_ms, attempt, Some(status.as_u16()));
                        break Ok(v);
                    }
                    Err(e) => {
                        crate::metrics::record_api_attempt("parse_error", false, attempt_ms, attempt, Some(status.as_u16()));
                        break Err((
                            "parse_error",
                            transform_failed(
                                "Internal",
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
            crate::metrics::record_api_request("success");
            crate::metrics::record_api_request_duration(request_ms);
            Ok(v)
        }
        Err((result, body)) => {
            crate::metrics::record_api_request(result);
            crate::metrics::record_api_request_duration(request_ms);
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
fn headers_digest(headers: &[(String, String)]) -> String {
    let mut pairs: Vec<&(String, String)> = headers.iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let canonical: String = pairs.iter().map(|(k, v)| format!("{k}={v}\n")).collect();
    format!("{:016x}", rust_cvr::hash::h64(&canonical))
}

fn cache_key(ctx: &CustomQueryContext, id: &str) -> String {
    format!(
        "{}|{}|{}|{}|{id}",
        ctx.url,
        ctx.auth.as_deref().unwrap_or(""),
        ctx.user_id.as_deref().unwrap_or(""),
        headers_digest(&ctx.composed_headers())
    )
}

fn cache_get(ctx: &CustomQueryContext, id: &str) -> Option<TransformedQuery> {
    let cache = TRANSFORM_CACHE.lock().ok()?;
    let (at, q) = cache.get(&cache_key(ctx, id))?;
    if at.elapsed() >= CACHE_TTL {
        return None;
    }
    Some(q.clone())
}

fn cache_set(ctx: &CustomQueryContext, id: &str, q: &TransformedQuery) {
    if let Ok(mut cache) = TRANSFORM_CACHE.lock() {
        cache.insert(cache_key(ctx, id), (Instant::now(), q.clone()));
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
        let out = rt
            .block_on(transform_custom_queries(&ctx, &shard(), &[]))
            .unwrap();
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
        let out = rt
            .block_on(transform_custom_queries(&ctx, &shard(), &specs))
            .unwrap();
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
            cache_key(&with_auth("a"), "1"),
            cache_key(&with_auth("b"), "1")
        );
        assert_ne!(
            cache_key(&with_auth("a"), "1"),
            cache_key(&ctx_at("v"), "1")
        );
        assert_ne!(
            cache_key(&with_auth("a"), "1"),
            cache_key(&with_auth("a"), "2")
        );
        assert_eq!(cache_key(&base, "1"), cache_key(&base, "1"));

        // The pinned userID partitions the cache (TS getCacheKey has userID).
        let mut ua = base.clone();
        ua.user_id = Some("alice".to_string());
        let mut ub = base.clone();
        ub.user_id = Some("bob".to_string());
        assert_ne!(cache_key(&ua, "1"), cache_key(&ub, "1"));

        // Forwarded credentials (cookie/origin/custom headers) must partition
        // the cache: same url+auth+id but a different cookie → different key.
        let mut ca = base.clone();
        ca.cookie = Some("session=A".to_string());
        let mut cb = base.clone();
        cb.cookie = Some("session=B".to_string());
        assert_ne!(cache_key(&ca, "1"), cache_key(&cb, "1"));
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
        assert!(url_pattern_matches(
            "https://api.example.com/query",
            "https://api.example.com/query"
        ));
        assert!(url_pattern_matches(
            "https://api.example.com/query",
            "https://api.example.com/query?tenant=1"
        ));
        assert!(url_pattern_matches(
            "https://api.example.com/*",
            "https://api.example.com/v2/query"
        ));
        assert!(url_pattern_matches(
            "https://*.example.com/query",
            "https://tenant-a.example.com/query"
        ));
        assert!(url_pattern_matches(
            "https://api.example.com/:tenant/query",
            "https://api.example.com/acme/query"
        ));
        // A :param is a single path segment.
        assert!(!url_pattern_matches(
            "https://api.example.com/:tenant/query",
            "https://api.example.com/a/b/query"
        ));
        assert!(!url_pattern_matches(
            "https://api.example.com/query",
            "https://evil.example.com/query"
        ));
        // A port stays literal (':8080' is not a param — digits can't start a
        // param name).
        assert!(url_pattern_matches(
            "http://localhost:8080/query",
            "http://localhost:8080/query"
        ));
        assert!(!url_pattern_matches(
            "http://localhost:8080/query",
            "http://localhost:9090/query"
        ));
    }
}
