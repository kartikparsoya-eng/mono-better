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

use crate::services::view_syncer::connection_context_manager::ConnectionValidation;
use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use rust_cvr::shards::ShardID;
use serde_json::Value;

use crate::auth::read_authorizer::hash_of_ast;
use crate::custom::fetch::{api_error_from_result, get_backoff_delay_ms, url_match};
use crate::protocol::query_server::QueryResponse;
use crate::protocol::{
    ErrorBody, ErrorKind, ErrorOrigin, ErrorReason, JsNumber, TransformFailedHttpBody,
    TransformFailedZeroCacheBody,
};

/// TS `CustomQueryTransformer` cache TTL — 5s (chosen to be shorter than a
/// typical short-lived auth token, so a re-auth re-transforms promptly).
const CACHE_TTL: Duration = Duration::from_secs(5);

/// TS sweeps expired entries on a `setInterval(ttlMs * 2)` — every 10s for the
/// 5s TTL — started lazily on the first `set()` and `unref`ed
/// (shared/src/cache.ts:31-38, `#removeExpired`:56-63). It does NOT sweep per
/// insert.
const CACHE_SWEEP_INTERVAL: Duration = Duration::from_secs(10);

/// Cached per-query transform results, keyed by `url|auth|headers-digest|id`.
/// Mirrors the TS per-connection `TimedCache`, but process-wide — so the key
/// MUST encode the full request identity that scopes authorization (URL, token,
/// AND the forwarded cookie/origin/custom headers). Omitting the headers would
/// let one connection read another's authorization-scoped transform.
struct TransformCache {
    entries: HashMap<String, (Instant, TransformedQuery)>,
    /// When the expiry sweep last ran. TS's sweep is driven by a timer, so it
    /// needs no such field; rust amortises the same cadence onto the insert
    /// path (see `cache_set`).
    last_swept: Instant,
}

/// `parking_lot::Mutex`, not `std::sync::Mutex`: the std lock POISONS on a
/// panic held across it, and both accessors used to read it with `.ok()` and
/// silently degrade — `cache_get` returning `None` forever, i.e. every custom
/// query going to the network for the rest of the process's life, with no log
/// line. Poisoning needs a panic inside a critical section that only does a map
/// operation, so it is near-unreachable; the point is that the fix DELETES the
/// failure mode rather than handling it. TS has no analogue: its cache is a
/// plain `Map` on a single-threaded event loop.
static TRANSFORM_CACHE: LazyLock<parking_lot::Mutex<TransformCache>> = LazyLock::new(|| {
    parking_lot::Mutex::new(TransformCache {
        entries: HashMap::new(),
        last_swept: Instant::now(),
    })
});

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
    /// The connection this context was resolved from. TS passes its whole
    /// `ConnectionContext` (clientID / wsID / revision) into `transform` and
    /// `validate` so a successful UNCACHED transform can `validateConnection`
    /// it with the API server's userID (view-syncer.ts:1984-1992). Not part of
    /// the cache key (TS `getCacheKey`).
    pub client_id: String,
    pub ws_id: String,
    pub revision: u32,
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
#[derive(Debug, Clone)]
pub struct TransformedQuery {
    pub id: String,
    pub ast: Value,
    pub hash: String,
}

/// The per-query outcome of a transform.
#[derive(Debug)]
pub enum CustomTransformed {
    /// A concrete AST was returned.
    Ok(TransformedQuery),
    /// The API server reported a per-query error (`{error, id, name, ...}`);
    /// forwarded to the client as a `transformError` without failing others.
    Errored { id: String, error: Value },
}

/// Port of TS `HashedTransformResponse` (transform-query.ts:43-60), success
/// arm: `{kind: 'success', result, cached: true} | {…, cached: false,
/// validation}`. The `failed` arm is [`transform`]'s `Err(TransformFailed body)`.
#[derive(Debug)]
pub struct HashedTransformResponse {
    pub result: Vec<CustomTransformed>,
    /// Every query was served from the cache — no API round trip, nothing
    /// re-asserted about the connection.
    pub cached: bool,
    /// The API server's validation of this connection; present iff `!cached`.
    pub validation: Option<ConnectionValidation>,
}

/// Port of the `validation` derivation in TS `#requestTransform`
/// (transform-query.ts:214-226): a `QueryResponse` whose `userID !== undefined`
/// is `server-validated` with that userID (`null` = a logged-out identity is
/// STILL server-validated); a `QueryResponse` without `userID`, or a legacy
/// `['transformed', …]` tuple, is `client-fallback`.
fn validation_of(response: &Value) -> ConnectionValidation {
    if response.get("kind").and_then(Value::as_str) == Some("QueryResponse")
        && let Some(user_id) = response.get("userID")
    {
        return ConnectionValidation::ServerValidated {
            validated_user_id: user_id.as_str().map(|s| s.to_string()),
        };
    }
    ConnectionValidation::ClientFallback
}

/// Transform a batch of named queries against the user's query API server.
/// Cached results are reused; only cache-missing queries hit the network.
/// Returns `Err(TransformFailed body)` on a whole-request failure.
pub async fn transform(
    ctx: &CustomQueryContext,
    shard: &ShardID,
    queries: &[CustomQuerySpec],
) -> Result<HashedTransformResponse, Value> {
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
        // TS: `{kind: 'success', result: cachedResponses, cached: true}`.
        return Ok(HashedTransformResponse {
            result: results,
            cached: true,
            validation: None,
        });
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

    let (queries, validation) =
        request_transform(ctx, shard, &body, &query_ids, "transform").await?;

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
            // `request_transform` parsed the response against
            // `queryResponseSchema`: a non-error entry carries an `ast`.
            unreachable!("transformedQuerySchema requires `ast`");
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

    Ok(HashedTransformResponse {
        result: results,
        cached: false,
        validation: Some(validation),
    })
}

/// Force the empty `/query` validation request used by auth maintenance. Port of
/// TS `CustomQueryTransformer.validate` (`transform-query.ts`).
///
/// Kept separate from `transform` because that path short-circuits
/// locally on an empty batch (`to_fetch.is_empty()`) and never hits the API
/// server — but validation MUST make the request so a token revoked/deauthorized
/// at the app layer (still cryptographically valid) is surfaced by the server.
/// Returns the API server's validation of the connection (TS returns the
/// `TransformResponse`, whose `validation` `#validateConnection` records;
/// view-syncer.ts:2753-2757). A 200 that is itself a `TransformFailed` body
/// is `request_transform`'s `Err` (TS `#requestTransform` returns it as-is
/// and the caller throws on `kind === 'TransformFailed'`).
pub async fn validate(
    ctx: &CustomQueryContext,
    shard: &ShardID,
) -> Result<ConnectionValidation, Value> {
    let body = serde_json::json!(["transform", []]);
    let (_, validation) = request_transform(ctx, shard, &body, &[], "validate").await?;
    Ok(validation)
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
/// TS overwrite precedence, the 4-attempt retry loop with backoff+jitter on
/// 5xx / network errors, and the `queryResponseSchema` parse of the body
/// (valita `passthrough`, custom/fetch.ts:258-262). Appends the `schema` +
/// `appID` query params. Then `#requestTransform`'s branching on the parsed
/// reply (transform-query.ts:211-234): `Ok` is TS's `QueryResponse` arm — the
/// transformed queries, still raw JSON, with the connection validation —
/// and `Err` a `TransformFailed` body.
async fn request_transform(
    ctx: &CustomQueryContext,
    shard: &ShardID,
    body: &Value,
    query_ids: &[Value],
    operation: &str,
) -> Result<(Vec<Value>, ConnectionValidation), Value> {
    // Real batch IDs (not `[]`) so the client can attribute the failure to
    // the specific queries. Port of TS `transform-query.ts` catch.
    let query_ids: Vec<String> = query_ids
        .iter()
        .filter_map(|id| id.as_str().map(str::to_string))
        .collect();
    // `apiFailedBody('transform', reason, message)` (custom/fetch.ts:437-451),
    // built through the wire types so every literal (`origin: 'zeroCache'`)
    // is one `errorBodySchema` accepts. A plain `Error` thrown inside
    // `fetchFromAPIServer` takes `#requestTransform`'s catch instead
    // (transform-query.ts:248-254): reason `internal`, message
    // `Failed to ${operation} queries: ${message}`.
    let transform_failed = |reason: ErrorReason, msg: String| -> Value {
        serde_json::to_value(ErrorBody::TransformFailedZeroCache(
            TransformFailedZeroCacheBody {
                kind: ErrorKind::TransformFailed,
                details: None,
                query_ids: query_ids.clone(),
                message: msg,
                origin: ErrorOrigin::ZeroCache,
                reason,
            },
        ))
        .expect("an error body serializes")
    };

    // URL allow-check at request time (TS fetch.ts). An override the config
    // does not allow fails THIS transform; the connection survives.
    if !ctx
        .allowed_urls
        .iter()
        .any(|pattern| url_match(pattern, &ctx.url))
    {
        crate::custom::metrics::record_api_request("url_not_allowed", 0, 0.0, None, None);
        return Err(transform_failed(
            ErrorReason::Internal,
            format!(
                "URL \"{}\" is not allowed by the ZERO_QUERY_URL configuration",
                ctx.url
            ),
        ));
    }

    // TS `new URL(url)` (fetch.ts:176) throws a plain `TypeError('Invalid URL')`.
    let mut url = reqwest::Url::parse(&ctx.url).map_err(|_| {
        transform_failed(
            ErrorReason::Internal,
            format!("Failed to {operation} queries: Invalid URL"),
        )
    })?;
    // Reserved-param guard (TS `reservedParams`): the configured URL may not
    // already carry the params zero-cache appends.
    for reserved in RESERVED_PARAMS {
        if url.query_pairs().any(|(k, _)| k == reserved) {
            crate::custom::metrics::record_api_request("config_error", 0, 0.0, None, None);
            // TS says "push URL" for both sources (fetch.ts:184-186), thrown as
            // a plain `Error`.
            return Err(transform_failed(
                ErrorReason::Internal,
                format!(
                    "Failed to {operation} queries: The push URL cannot contain the reserved query param \"{reserved}\""
                ),
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
    let result =
        post_transform_attempts(client, url, &headers, body, &query_ids, &transform_failed).await;
    crate::custom::metrics::record_api_in_flight(-1);
    let v = result?;

    // transform-query.ts:211-234: `'kind' in transformResponse` is a
    // `QueryResponse` (queries + validation) or a `TransformFailed` body
    // returned as-is; a legacy `['transformed', body]` tuple is a
    // client-fallback response, and `['transformFailed', body]` yields its
    // body. `post_transform_attempts` parsed the reply against
    // `queryResponseSchema`, so the lookups below cannot miss.
    if v.get("kind").is_some() {
        if v.get("kind").and_then(Value::as_str) == Some("QueryResponse") {
            let queries = v
                .get("queries")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            Ok((queries, validation_of(&v)))
        } else {
            Err(v)
        }
    } else if v.get(0).and_then(Value::as_str) == Some("transformed") {
        let queries = v
            .get(1)
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        Ok((queries, ConnectionValidation::ClientFallback))
    } else {
        Err(v.get(1).cloned().unwrap_or(Value::Null))
    }
}

async fn post_transform_attempts(
    client: &reqwest::Client,
    url: reqwest::Url,
    headers: &[(String, String)],
    body: &Value,
    query_ids: &[String],
    transform_failed: &dyn Fn(ErrorReason, String) -> Value,
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
                    // fetch.ts:349-353 `Fetch from API server threw error: …`.
                    transform_failed(
                        ErrorReason::Internal,
                        format!("Fetch from API server threw error: {e}"),
                    ),
                    None,
                    None,
                ));
            }
            Ok(resp) => {
                let status = resp.status();
                if !status.is_success() {
                    // 5xx can be transient (TS retries them); 4xx fails now.
                    let will_retry = status.is_server_error() && attempt < FETCH_MAX_ATTEMPTS;
                    // TS reads the error body BEFORE recording the attempt so
                    // `error_kind`/`error_reason` label it (custom/fetch.ts:
                    // 528-547); a retried attempt is labeled the same way.
                    let preview = resp.text().await.unwrap_or_default();
                    let error = crate::custom::metrics::ApiErrorAttrs::from_body(&preview);
                    crate::custom::metrics::record_api_attempt(
                        "http_error",
                        will_retry,
                        attempt_ms,
                        attempt,
                        Some(status.as_u16()),
                        error.as_ref(),
                    );
                    if will_retry {
                        tokio::time::sleep(Duration::from_millis(get_backoff_delay_ms(attempt)))
                            .await;
                        attempt += 1;
                        continue;
                    }
                    // TS fetch.ts:222 `lc.warn?.('fetch from API server returned non-OK status', {url, status, bodyPreview})`.
                    tracing::warn!(
                        url = %url,
                        status = status.as_u16(),
                        body_preview = %preview,
                        "fetch from API server returned non-OK status"
                    );
                    // Port of the ZeroCache `reason: 'http'` TransformFailed
                    // variant (`error.ts` transformFailedBodySchema): carry the
                    // HTTP `status` (+ `bodyPreview`) so a 401/403 is recognizable
                    // as a server-side auth failure — see `is_auth_error_body`,
                    // used by the auth-maintenance revocation probe.
                    // fetch.ts:242-247: `apiFailedBody(source, HTTP, 'Fetch from API
                    // server returned non-OK status …', response, bodyPreview)`.
                    let failure = serde_json::to_value(ErrorBody::TransformFailedHttp(
                        TransformFailedHttpBody {
                            kind: ErrorKind::TransformFailed,
                            details: None,
                            query_ids: query_ids.to_vec(),
                            message: format!(
                                "Fetch from API server returned non-OK status {}",
                                status.as_u16()
                            ),
                            origin: ErrorOrigin::ZeroCache,
                            reason: ErrorReason::Http,
                            status: JsNumber::from(i64::from(status.as_u16())),
                            body_preview: Some(preview.clone()),
                        },
                    ))
                    .expect("an error body serializes");
                    break Err(("http_error", failure, Some(status.as_u16()), error));
                }
                // fetch.ts:258-262: `response.json()` then
                // `validator.parse(json, {mode: 'passthrough'})` — a body that
                // is not JSON and a body outside `queryResponseSchema` are the
                // same `parse` failure.
                let parsed = match resp.json::<Value>().await {
                    // `getErrorMessage(e)` of the TypeError `parse` throws
                    // (custom/fetch.ts:305): the bare valita message, paths
                    // from the reply root.
                    Ok(v) => {
                        match crate::protocol::valita::deserialize_at::<QueryResponse>(&v, &[], &v)
                        {
                            Ok(_) => Ok(v),
                            Err(issue) => Err(crate::protocol::valita::get_message(&issue, &v)),
                        }
                    }
                    Err(e) => Err(e.to_string()),
                };
                match parsed {
                    Ok(v) => {
                        // fetch.ts:263-285: a 2xx whose body is itself an
                        // error body is recorded as `api_error`, not `success`.
                        let api_error = api_error_from_result(&v);
                        crate::custom::metrics::record_api_attempt(
                            if api_error.is_some() {
                                "api_error"
                            } else {
                                "success"
                            },
                            false,
                            attempt_ms,
                            attempt,
                            Some(status.as_u16()),
                            api_error.as_ref(),
                        );
                        break Ok((v, status.as_u16(), api_error));
                    }
                    Err(e) => {
                        // TS fetch.ts:294 `lc.warn?.('failed to parse response', …)`.
                        tracing::warn!(url = %url, error = %e, "failed to parse response");
                        // fetch.ts:301-305 `apiFailedBody(source, ErrorReason.Parse, …)`.
                        let failure = transform_failed(
                            ErrorReason::Parse,
                            format!("Failed to parse response from API server: {e}"),
                        );
                        let error = crate::custom::metrics::ApiErrorAttrs::from_value(&failure);
                        crate::custom::metrics::record_api_attempt(
                            "parse_error",
                            false,
                            attempt_ms,
                            attempt,
                            Some(status.as_u16()),
                            error.as_ref(),
                        );
                        break Err(("parse_error", failure, Some(status.as_u16()), error));
                    }
                }
            }
        }
    };
    let request_ms = request_started.elapsed().as_secs_f64() * 1000.0;
    // TS `fetchFromAPIServer`'s `finally` (custom/fetch.ts:365-372): one
    // `apiRequests` + `apiRequestDuration` sample carrying the attempt count,
    // the last response's status and the error body's kind/reason.
    match outcome {
        Ok((v, status, api_error)) => {
            crate::custom::metrics::record_api_request(
                if api_error.is_some() {
                    "api_error"
                } else {
                    "success"
                },
                attempt,
                request_ms,
                Some(status),
                api_error.as_ref(),
            );
            Ok(v)
        }
        Err((result, body, status, error)) => {
            crate::custom::metrics::record_api_request(
                result,
                attempt,
                request_ms,
                status,
                error.as_ref(),
            );
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
    let mut cache = TRANSFORM_CACHE.lock();
    let key = get_cache_key(ctx, id);
    match cache.entries.get(&key) {
        Some((at, q)) if at.elapsed() < CACHE_TTL => Some(q.clone()),
        // Expired → evict on read, exactly as TS `TimedCache.get` does
        // (`this.#cache.delete(key); return undefined`, cache.ts:47-50).
        Some(_) => {
            cache.entries.remove(&key);
            None
        }
        None => None,
    }
}

fn cache_set(ctx: &CustomQueryContext, id: &str, q: &TransformedQuery) {
    let mut cache = TRANSFORM_CACHE.lock();
    // The sweep is what keeps the process-wide cache from growing unbounded as
    // rotating short-lived JWTs mint fresh keys that are never re-read. It used
    // to run on EVERY insert — a full O(n) scan of the whole process-wide map,
    // while holding a lock shared by ~1000 CG threads, and `n` is exactly the
    // population those rotating keys grow. TS runs the same scan on a timer at
    // twice the TTL, so match that cadence: at most one sweep per
    // `CACHE_SWEEP_INTERVAL`, which makes the insert path amortised O(1).
    //
    // Residual difference from TS, accepted deliberately: TS's `setInterval`
    // fires even with no cache activity, so an idle process reclaims within
    // 10s, whereas this sweep needs a subsequent insert. Reads still evict
    // expired entries (`cache_get`), the retained set is bounded by what was
    // inserted, and the alternative — a background timer task owning a global
    // — would be a larger rust-only invention than the leak it prevents.
    if cache.last_swept.elapsed() >= CACHE_SWEEP_INTERVAL {
        cache.entries.retain(|_, (at, _)| at.elapsed() < CACHE_TTL);
        cache.last_swept = Instant::now();
    }
    cache
        .entries
        .insert(get_cache_key(ctx, id), (Instant::now(), q.clone()));
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

/// TEST-ONLY: how many entries the transform cache currently holds, and how
/// long since its last expiry sweep. Lets a test observe the sweep SCHEDULE
/// rather than only its effect.
#[doc(hidden)]
pub fn __test_transform_cache_state() -> (usize, std::time::Duration) {
    let cache = TRANSFORM_CACHE.lock();
    (cache.entries.len(), cache.last_swept.elapsed())
}

/// TEST-ONLY: drop every entry and reset the sweep clock, so a test starts from
/// a known state despite the cache being process-wide.
#[doc(hidden)]
pub fn __test_reset_transform_cache() {
    let mut cache = TRANSFORM_CACHE.lock();
    cache.entries.clear();
    cache.last_swept = Instant::now();
}

/// TEST-ONLY: backdate the sweep clock by `by`, so a test can place itself
/// either side of `CACHE_SWEEP_INTERVAL` without sleeping.
#[doc(hidden)]
pub fn __test_backdate_transform_cache_sweep_clock(by: Duration) {
    let mut cache = TRANSFORM_CACHE.lock();
    cache.last_swept = cache
        .last_swept
        .checked_sub(by)
        .expect("backdating past the process start is a test bug");
}

/// TEST-ONLY: insert an entry whose recorded timestamp is `age` in the past, so
/// a test can make an entry EXPIRED (older than `CACHE_TTL`) without sleeping.
#[doc(hidden)]
pub fn __test_insert_aged(ctx: &CustomQueryContext, id: &str, q: &TransformedQuery, age: Duration) {
    let mut cache = TRANSFORM_CACHE.lock();
    let at = Instant::now()
        .checked_sub(age)
        .expect("aging past the process start is a test bug");
    cache
        .entries
        .insert(get_cache_key(ctx, id), (at, q.clone()));
}

/// TEST-ONLY: the sweep interval and TTL, so a test does not hard-code them.
#[doc(hidden)]
pub fn __test_cache_timings() -> (Duration, Duration) {
    (CACHE_TTL, CACHE_SWEEP_INTERVAL)
}

#[cfg(test)]
pub(crate) mod test_support {
    /// Scripted HTTP stub: answers the next `responses.len()` connections, in
    /// order, with `(status, body)`, then exits. Shared by the transform tests
    /// and the view-syncer validation tests.
    pub(crate) fn spawn_http_stub_seq(responses: Vec<(&'static str, &'static str)>) -> String {
        let queue = std::sync::Mutex::new(std::collections::VecDeque::from(responses));
        let n = queue.lock().unwrap().len();
        spawn_http_stub_with(n, move |_req| {
            let (status, body) = queue.lock().unwrap().pop_front().unwrap();
            (status, body.to_string())
        })
    }

    /// Request-aware HTTP stub: answers up to `n` connections, each with
    /// `respond(request_body)`, then exits. Lets a test tell the empty
    /// `["transform",[]]` validation probe apart from a real transform.
    pub(crate) fn spawn_http_stub_with(
        n: usize,
        respond: impl Fn(&str) -> (&'static str, String) + Send + 'static,
    ) -> String {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback stub");
        let addr = listener.local_addr().expect("stub local_addr");
        std::thread::spawn(move || {
            for _ in 0..n {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
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
                let request_body = header_end
                    .map(|end| String::from_utf8_lossy(&buf[end..]).to_string())
                    .unwrap_or_default();
                let (status, body) = respond(&request_body);
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        format!("http://{addr}/query")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The transform cache is PROCESS-WIDE, and so is its sweep clock, so the
    /// three tests below cannot run concurrently with each other — one
    /// resetting the clock invalidates another's backdating. libtest runs tests
    /// in parallel by default, so serialize them explicitly rather than relying
    /// on `--test-threads=1`.
    static CACHE_TEST_GUARD: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    fn probe_ctx(tag: &str) -> CustomQueryContext {
        CustomQueryContext {
            url: format!("https://example.test/{tag}"),
            ..Default::default()
        }
    }

    fn probe_query(id: &str) -> TransformedQuery {
        TransformedQuery {
            id: id.to_string(),
            ast: serde_json::json!({"table": "issue"}),
            hash: "h".to_string(),
        }
    }

    /// The expiry sweep must run on a SCHEDULE, not on every insert.
    ///
    /// TS sweeps on a `setInterval(ttlMs * 2)` started lazily on the first
    /// `set()` (shared/src/cache.ts:31-38, `#removeExpired`:56-63) — every 10s
    /// for the 5s TTL. Rust ran `cache.retain(..)`, a full O(n) scan of the
    /// whole PROCESS-WIDE map, on every insert, while holding a lock shared by
    /// ~1000 CG threads; and `n` is exactly the population that rotating
    /// short-lived JWTs grow. The sweep is now gated on
    /// `CACHE_SWEEP_INTERVAL`, matching TS's cadence and making the insert path
    /// amortised O(1).
    ///
    /// The observable is the sweep CLOCK: a sweep resets `last_swept`, so a
    /// clock that stays backdated across an insert proves no sweep ran.
    ///
    /// Mutation test: make the sweep unconditional again (drop the
    /// `if cache.last_swept.elapsed() >= CACHE_SWEEP_INTERVAL` guard, keeping
    /// the `last_swept = Instant::now()`) and
    /// `an_insert_inside_the_interval_does_not_sweep` fails; delete the sweep
    /// block entirely and `an_insert_past_the_interval_sweeps` fails.
    #[test]
    fn an_insert_inside_the_interval_does_not_sweep() {
        let _serial = CACHE_TEST_GUARD.lock();
        let (_ttl, interval) = __test_cache_timings();
        let ctx = probe_ctx("sweep-inside");
        __test_reset_transform_cache();

        // Sit just INSIDE the interval, so a correctly-scheduled sweep must
        // decline to run.
        let backdate = interval - Duration::from_secs(2);
        __test_backdate_transform_cache_sweep_clock(backdate);

        cache_set(&ctx, "q1", &probe_query("q1"));
        cache_set(&ctx, "q2", &probe_query("q2"));
        cache_set(&ctx, "q3", &probe_query("q3"));

        let (len, since_sweep) = __test_transform_cache_state();
        assert_eq!(len, 3, "the three entries must be cached");
        assert!(
            since_sweep >= backdate - Duration::from_secs(1),
            "no sweep may have run: the clock should still read ~{backdate:?} \
             since the last sweep, but reads {since_sweep:?} — a reset clock \
             means the insert path swept, which is the O(n)-under-a-shared-lock \
             behaviour this replaced"
        );
        __test_reset_transform_cache();
    }

    #[test]
    fn an_insert_past_the_interval_sweeps() {
        let _serial = CACHE_TEST_GUARD.lock();
        let (ttl, interval) = __test_cache_timings();
        let ctx = probe_ctx("sweep-past");
        __test_reset_transform_cache();

        // One EXPIRED entry (older than the TTL) and one fresh one.
        __test_insert_aged(&ctx, "stale", &probe_query("stale"), ttl * 2);
        cache_set(&ctx, "fresh", &probe_query("fresh"));
        assert_eq!(
            __test_transform_cache_state().0,
            2,
            "precondition: both entries are present before any sweep"
        );

        // Cross the interval, then insert: this insert must sweep.
        __test_backdate_transform_cache_sweep_clock(interval + Duration::from_secs(1));
        cache_set(&ctx, "trigger", &probe_query("trigger"));

        let (len, since_sweep) = __test_transform_cache_state();
        assert!(
            since_sweep < Duration::from_secs(2),
            "the sweep must have run and reset the clock; it reads \
             {since_sweep:?} since the last sweep"
        );
        assert_eq!(
            len, 2,
            "the sweep must drop the EXPIRED entry and keep the two live ones \
             (fresh + trigger); got {len}"
        );
        assert!(
            cache_get(&ctx, "stale").is_none(),
            "the expired entry must be gone"
        );
        assert!(
            cache_get(&ctx, "fresh").is_some(),
            "a live entry must survive the sweep"
        );
        __test_reset_transform_cache();
    }

    /// A panic that held the transform-cache lock must not kill the cache.
    ///
    /// It was a `std::sync::Mutex` read with `.lock().ok()?`, so one poisoning
    /// panic made `cache_get` return `None` for every query for the rest of the
    /// process's life — every custom query going to the network, silently and
    /// permanently. `parking_lot::Mutex` cannot poison.
    ///
    /// Mutation test: restore the `StdMutex` + `.lock().ok()?` / `if let Ok(..)`
    /// pair and the post-panic `cache_get` returns `None`, failing the last
    /// assertion.
    #[test]
    fn a_panic_holding_the_transform_cache_lock_does_not_kill_the_cache() {
        let _serial = CACHE_TEST_GUARD.lock();
        let ctx = probe_ctx("poison");
        __test_reset_transform_cache();
        cache_set(&ctx, "q1", &probe_query("q1"));
        assert!(
            cache_get(&ctx, "q1").is_some(),
            "precondition: the entry is cached"
        );

        let panicked = std::thread::spawn(|| {
            let _guard = TRANSFORM_CACHE.lock();
            panic!("poison the transform cache lock");
        })
        .join();
        assert!(
            panicked.is_err(),
            "the probe thread must actually have panicked while holding the \
             lock, or this test proves nothing"
        );

        assert!(
            cache_get(&ctx, "q1").is_some(),
            "after a panic held the lock, the cache MUST still serve. A \
             poisoned std::sync::Mutex read with `.ok()?` returns None for \
             every query, permanently, with no log line"
        );
        __test_reset_transform_cache();
    }

    /// TS `HashedTransformResponse` (transform-query.ts:43-60) + the
    /// `validation` derivation in `#requestTransform` (:214-226): an UNCACHED
    /// transform carries `cached: false` and the API server's validation —
    /// `server-validated` with its `userID` — while a fully cached batch is
    /// `cached: true` with no validation (nothing was re-asserted).
    #[test]
    fn transform_carries_the_api_servers_validation_only_when_uncached() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let url = super::test_support::spawn_http_stub_seq(vec![(
            "200 OK",
            r#"{"kind":"QueryResponse","userID":"u1","queries":[{"id":"qv1","name":"n","ast":{"table":"issue"}}]}"#,
        )]);
        let mut ctx = ctx_at(&url);
        ctx.auth = Some("tok-validation".to_string());
        let specs = vec![CustomQuerySpec {
            id: "qv1".to_string(),
            name: "myQuery".to_string(),
            args: vec![],
        }];
        let first = rt.block_on(transform(&ctx, &shard(), &specs)).unwrap();
        assert!(!first.cached);
        assert!(
            matches!(
                first.validation,
                Some(ConnectionValidation::ServerValidated { validated_user_id: Some(ref u) }) if u == "u1"
            ),
            "got {:?}",
            first.validation
        );
        assert_eq!(first.result.len(), 1);
        // Served from the cache: the stub is exhausted, so a request would fail.
        let second = rt.block_on(transform(&ctx, &shard(), &specs)).unwrap();
        assert!(second.cached);
        assert!(second.validation.is_none());
    }

    /// TS `CustomQueryTransformer.validate` returns the response's validation
    /// (transform-query.ts:110-114 → `#requestTransform`): a `QueryResponse`
    /// with `userID: null` is STILL server-validated (a logged-out identity),
    /// and a legacy `['transformed', …]` tuple is client-fallback.
    #[test]
    fn validate_returns_the_api_servers_validation() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let url = super::test_support::spawn_http_stub_seq(vec![
            (
                "200 OK",
                r#"{"kind":"QueryResponse","userID":null,"queries":[]}"#,
            ),
            ("200 OK", r#"["transformed",[]]"#),
        ]);
        let ctx = ctx_at(&url);
        let v = rt.block_on(validate(&ctx, &shard())).unwrap();
        assert!(
            matches!(
                v,
                ConnectionValidation::ServerValidated {
                    validated_user_id: None
                }
            ),
            "got {v:?}"
        );
        let v = rt.block_on(validate(&ctx, &shard())).unwrap();
        assert!(
            matches!(v, ConnectionValidation::ClientFallback),
            "got {v:?}"
        );
    }

    /// One-query batch (`id: "a"`) against a stub answering `body` with 200.
    async fn transform_one_against(body: &'static str) -> Result<HashedTransformResponse, Value> {
        let url = spawn_http_stub("200 OK", body);
        let ctx = ctx_at(&url);
        let spec = CustomQuerySpec {
            id: "a".to_string(),
            name: "n".to_string(),
            args: vec![],
        };
        transform(&ctx, &shard(), &[spec]).await
    }

    /// `#requestTransform` (transform-query.ts:211-229): a `QueryResponse`
    /// with a `userID` is server-validated; the legacy `['transformed', …]`
    /// tuple is a client-fallback response carrying its queries.
    #[tokio::test]
    async fn transform_handles_modern_and_legacy_responses() {
        let ok = transform_one_against(
            r#"{"kind":"QueryResponse","userID":"u1","queries":[{"id":"a","name":"n","ast":{"table":"issue"}}]}"#,
        )
        .await
        .expect("a QueryResponse");
        assert!(
            matches!(&ok.validation, Some(ConnectionValidation::ServerValidated { validated_user_id }) if validated_user_id.as_deref() == Some("u1")),
            "got {:?}",
            ok.validation
        );
        assert_eq!(ok.result.len(), 1);

        let ok = transform_one_against(
            r#"["transformed",[{"id":"a","name":"n","ast":{"table":"issue"}},{"id":"b","name":"n","ast":{"table":"issue"}}]]"#,
        )
        .await
        .expect("a legacy transformed tuple");
        assert!(matches!(
            ok.validation,
            Some(ConnectionValidation::ClientFallback)
        ));
        assert_eq!(ok.result.len(), 2);
    }

    /// `fetchFromAPIServer` (custom/fetch.ts:258-262) parses the body against
    /// `queryResponseSchema`: a 200 that fails it is a `parse` failure of the
    /// whole batch (fetch.ts:301-305, then transform-query.ts:240-245 adds
    /// the batch ids). Before the port the entry was accepted and its AST
    /// hashed and hydrated.
    #[tokio::test]
    async fn transform_fails_the_batch_when_the_response_is_outside_query_response_schema() {
        let err = transform_one_against(
            r#"{"kind":"QueryResponse","queries":[{"id":"a","name":"n","ast":{"table":5}}]}"#,
        )
        .await
        .expect_err("a non-string table must fail the parse");
        assert_eq!(err["kind"], "TransformFailed");
        assert_eq!(err["origin"], "zeroCache");
        assert_eq!(err["reason"], "parse");
        // The body must be one `errorBodySchema` accepts — it is sent to the
        // client as-is (`["error", body]`), where the client parses it.
        <ErrorBody as serde::Deserialize>::deserialize(&err)
            .expect("a TransformFailed body the client can parse");
        assert!(
            err["message"]
                .as_str()
                .unwrap()
                .starts_with("Failed to parse response from API server: "),
            "{err}"
        );
        assert_eq!(err["queryIDs"], serde_json::json!(["a"]));
    }

    /// valita `passthrough` (custom/fetch.ts:260): unknown keys anywhere in
    /// the response are kept, not rejected.
    #[tokio::test]
    async fn transform_keeps_unknown_keys_in_the_response() {
        let ok = transform_one_against(
            r#"{"kind":"QueryResponse","extra":1,"queries":[{"id":"a","name":"n","extra":2,"ast":{"table":"issue","extra":3}}]}"#,
        )
        .await
        .expect("unknown keys pass through");
        assert_eq!(ok.result.len(), 1);
        match &ok.result[0] {
            CustomTransformed::Ok(q) => assert_eq!(q.ast["extra"], 3),
            CustomTransformed::Errored { .. } => panic!("not an errored query"),
        }
    }

    /// custom/fetch.ts:301-305: a body that is not JSON is the same `parse`
    /// failure. Before the port it was reported as reason `internal`.
    #[tokio::test]
    async fn transform_reports_a_non_json_body_as_a_parse_failure() {
        let err = transform_one_against("not json")
            .await
            .expect_err("a non-JSON body must fail");
        assert_eq!(err["reason"], "parse");
        assert!(
            err["message"]
                .as_str()
                .unwrap()
                .starts_with("Failed to parse response from API server: "),
            "{err}"
        );
        assert_eq!(err["queryIDs"], serde_json::json!(["a"]));
    }

    /// custom-queries/transform-query.ts:236: a legacy tuple yields `transformResponse[1]`,
    /// so `['transformFailed', body]` yields its body. Before the port the whole tuple was the error.
    #[tokio::test]
    async fn transform_returns_the_body_of_a_legacy_transform_failed_tuple() {
        let err = transform_one_against(
            r#"["transformFailed",{"kind":"TransformFailed","origin":"server","reason":"internal","message":"boom","queryIDs":["a"]}]"#,
        )
        .await
        .expect_err("a legacy transformFailed tuple must fail");
        assert_eq!(
            err,
            serde_json::json!({"kind":"TransformFailed","origin":"server","reason":"internal","message":"boom","queryIDs":["a"]})
        );
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
            .expect_err("transform should fail for a disallowed URL");
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
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback stub");
        let addr = listener.local_addr().expect("stub local_addr");
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
        let ok_url = spawn_http_stub("200 OK", r#"{"kind":"QueryResponse","queries":[]}"#);
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
        assert_eq!(err["origin"], "zeroCache");
        assert_eq!(err["reason"], "http");
        assert_eq!(err["status"], 401);
        assert_eq!(
            err["message"],
            "Fetch from API server returned non-OK status 401"
        );
        <ErrorBody as serde::Deserialize>::deserialize(&err)
            .expect("a TransformFailed body the client can parse");
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
        assert!(out.result.is_empty());
        assert!(out.cached && out.validation.is_none());
    }

    #[test]
    fn cached_queries_skip_the_network() {
        // Seed the cache so a query resolves without a request. The bogus URL
        // proves no network call happens (it would error otherwise).
        //
        // The cache is process-wide and libtest runs tests in parallel, so any
        // test that depends on cache CONTENTS has to hold the same guard as
        // the sweep tests — otherwise their `__test_reset_transform_cache()`
        // wipes the entry seeded below and this test falls through to the
        // network it is asserting never happens.
        let _serial = CACHE_TEST_GUARD.lock();
        __test_reset_transform_cache();
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
            .block_on(transform(&ctx, &shard(), &specs))
            .unwrap()
            .result;
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
            client_id: String::new(),
            ws_id: String::new(),
            revision: 0,
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
