//! HTTP server — axum-based endpoints for /statz, /metrics, /heapz, /notify/:cg_id.
//!
//! The HTTP server runs on the tokio runtime. It serves:
//! - `GET /statz` — server statistics (active CGs, connections, memory)
//! - `GET /metrics` — Prometheus text-format metrics (scraped by the ART G17
//!   telemetry gate; `zero_sync_*` counters + latency histograms)
//! - `GET /heapz` — heap snapshot placeholder (V8 compatibility)
//! - `POST /notify/:cg_id` — change-streamer notification endpoint
//!
//! Notifications are forwarded to the appropriate CG thread via a channel.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
};
use serde::Serialize;
use serde_json::{Value, json};

use crate::router::ConnectionRouter;

/// Server statistics for /statz endpoint.
#[derive(Debug, Default, Serialize)]
pub struct ServerStats {
    pub active_client_groups: u64,
    pub active_connections: u64,
    pub total_messages_received: u64,
    pub total_messages_sent: u64,
    pub uptime_ms: u64,
}

/// Shared state for the HTTP server.
pub struct HttpServerState {
    pub router: Arc<ConnectionRouter>,
    pub stats: Arc<Mutex<ServerStats>>,
    pub start_time: std::time::Instant,
    /// Shared secret for the `/notify` endpoints (`NOTIFY_AUTH_TOKEN`). The
    /// HTTP port is bound on all interfaces (metrics are scraped externally),
    /// which leaves `/notify` reachable by any peer: one unauthenticated POST
    /// triggers a full advance cycle on every hosted CG, and a forged
    /// watermark/commit-time poisons the serving-lag histogram. The dispatcher
    /// generates a per-process token and passes it via env; when set, both
    /// notify routes require it in the `x-notify-auth` header. Unset (manual /
    /// standalone runs) preserves the open behavior.
    pub notify_auth_token: Option<String>,
}

/// Notify-route guard: shared-secret check (when configured) + `state` check.
/// Returns an error response, or `None` when the request may proceed.
fn check_notify_request(
    state: &HttpServerState,
    headers: &axum::http::HeaderMap,
    notification: &Value,
) -> Option<(StatusCode, Json<Value>)> {
    if let Some(expected) = &state.notify_auth_token {
        let presented = headers.get("x-notify-auth").and_then(|v| v.to_str().ok());
        if presented != Some(expected.as_str()) {
            return Some((
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "missing or invalid x-notify-auth"})),
            ));
        }
    }
    // The bridge always sends `state: "version-ready"`. Reject any OTHER state
    // outright; a missing state (legacy/manual poke) is still accepted.
    if let Some(s) = notification.get("state").and_then(Value::as_str)
        && s != "version-ready"
    {
        return Some((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("unsupported notification state: {s}")})),
        ));
    }
    None
}

/// Bind the HTTP TCP listener without serving, so the caller can confirm the
/// port is bound (and emit its process-ready signal) before serving begins.
pub async fn bind_http_listener(addr: SocketAddr) -> tokio::net::TcpListener {
    tracing::info!("HTTP server listening on {}", addr);
    tokio::net::TcpListener::bind(addr).await.unwrap()
}

/// Serve the axum app on an already-bound listener (see `bind_http_listener`).
pub async fn serve_http(listener: tokio::net::TcpListener, router: Arc<ConnectionRouter>) {
    let state = Arc::new(HttpServerState {
        router,
        stats: Arc::new(Mutex::new(ServerStats::default())),
        start_time: std::time::Instant::now(),
        notify_auth_token: std::env::var("NOTIFY_AUTH_TOKEN")
            .ok()
            .filter(|t| !t.is_empty()),
    });

    let app = Router::new()
        .route("/statz", get(statz_handler))
        .route("/metrics", get(metrics_handler))
        .route("/heapz", get(heapz_handler))
        // Live-object census across all three Rust crates (leak hunt). Poll with
        // `curl http://<http-port>/census` during a load run to see which
        // counter climbs.
        .route("/census", get(census_handler))
        // Global commit notification — the replicator POSTs here on each commit;
        // every hosted CG advances to the new replica head.
        .route("/notify", post(notify_broadcast_handler))
        // Targeted notification for a single client group (kept for completeness).
        .route("/notify/:cg_id", post(notify_handler))
        .with_state(state);

    axum::serve(listener, app).await.unwrap();
}

/// Run the HTTP server on the given address (bind + serve).
pub async fn run_http_server(addr: SocketAddr, router: Arc<ConnectionRouter>) {
    let listener = bind_http_listener(addr).await;
    serve_http(listener, router).await;
}

/// GET /statz — return server statistics.
async fn statz_handler(State(state): State<Arc<HttpServerState>>) -> (StatusCode, Json<Value>) {
    let stats = state.stats.lock().unwrap();
    let uptime_ms = state.start_time.elapsed().as_millis() as u64;
    let active_cgs = state.router.cg_count();

    let response = json!({
        "activeClientGroups": active_cgs,
        "activeConnections": stats.active_connections,
        "totalMessagesReceived": stats.total_messages_received,
        "totalMessagesSent": stats.total_messages_sent,
        "uptimeMs": uptime_ms,
        "metrics": state.router.metrics_snapshot(),
    });

    (StatusCode::OK, Json(response))
}

/// GET /metrics — Prometheus text-format metrics. Scraped by the ART G17
/// telemetry gate; exposes `zero_sync_*` counters + hydration/advance latency
/// histograms (TS pushes OTLP; we expose a pull endpoint — same metric names).
async fn metrics_handler(State(state): State<Arc<HttpServerState>>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        state.router.metrics_prometheus(),
    )
}

/// GET /census — live-object census aggregated across the three Rust crates.
///
/// Plaintext, one line per crate. Each crate's `live_count::snapshot()` renders
/// its own `AtomicI64` census (inc on construct / dec on Drop). Poll this during
/// a load run to attribute a process RSS climb: a `syncer: cg=N` that never
/// returns to zero after clients disconnect means a client-group task is being
/// retained (and with it its `SyncEngine` → IVM graph + CVR store).
async fn census_handler() -> impl IntoResponse {
    let body = format!(
        "syncer: {}\ncvr:    {}\nivm:    {}\n",
        crate::live_count::snapshot(),
        rust_cvr::live_count::snapshot(),
        rust_ivm::live_count::snapshot(),
    );
    (
        StatusCode::OK,
        [("content-type", "text/plain; charset=utf-8")],
        body,
    )
}

/// GET /heapz — heap snapshot placeholder.
/// Returns a minimal V8-style heap snapshot for compatibility.
async fn heapz_handler(State(state): State<Arc<HttpServerState>>) -> (StatusCode, Json<Value>) {
    let stats = state.stats.lock().unwrap();
    let response = json!({
        "type": "heap_snapshot",
        "timestamp": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0),
        "stats": {
            "activeClientGroups": state.router.cg_count(),
            "activeConnections": stats.active_connections,
        },
    });

    (StatusCode::OK, Json(response))
}

/// POST /notify — global commit notification from the replicator.
///
/// The TS replicator/change-streamer POSTs here on each commit (the Rust analog
/// of the in-process `version-ready` `Subscription<ReplicaState>` in TS). Every
/// hosted CG thread advances to the new replica head and pokes its clients.
async fn notify_broadcast_handler(
    State(state): State<Arc<HttpServerState>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<Value>) {
    // The body is optional; default to an empty object when absent/blank. When
    // present it must be valid JSON (e.g. `{"state":"version-ready"}`).
    let notification: Value = if body.is_empty() {
        json!({})
    } else {
        match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": format!("invalid json: {}", e)})),
                );
            }
        }
    };
    if let Some(rejection) = check_notify_request(&state, &headers, &notification) {
        return rejection;
    }
    let notified = state.router.broadcast_notification(notification);
    tracing::debug!("broadcast notification to {notified} CG thread(s)");
    (
        StatusCode::OK,
        Json(json!({"ok": true, "notified": notified})),
    )
}

/// POST /notify/:cg_id — change-streamer notification endpoint.
///
/// The change-streamer sends a POST request when new data is available.
/// The body contains the new version information. We forward this to the
/// appropriate CG thread via a channel.
async fn notify_handler(
    State(state): State<Arc<HttpServerState>>,
    Path(cg_id): Path<String>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<Value>) {
    tracing::debug!("received notification for CG {}", cg_id);

    // Parse the notification body
    let body_str = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid utf8"})),
            );
        }
    };

    let notification: Value = match serde_json::from_str(body_str) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("invalid json: {}", e)})),
            );
        }
    };

    if let Some(rejection) = check_notify_request(&state, &headers, &notification) {
        return rejection;
    }

    // Forward to the CG thread
    if state.router.send_notification(&cg_id, notification) {
        (StatusCode::OK, Json(json!({"ok": true})))
    } else {
        tracing::debug!("no CG thread found for {}", cg_id);
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "client group not found"})),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    /// The `/census` handler returns a 200 text/plain body with one line per
    /// Rust crate (syncer/cvr/ivm), each rendering that crate's live-object
    /// census. This is the endpoint polled during a load run to watch which
    /// counter climbs.
    #[tokio::test]
    async fn census_handler_returns_all_three_crates() {
        let resp = census_handler().await.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(ct.starts_with("text/plain"), "content-type was {ct}");
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(body.contains("syncer:"), "body: {body}");
        assert!(body.contains("cvr:"), "body: {body}");
        assert!(body.contains("ivm:"), "body: {body}");
        // The syncer line renders our own census statics.
        assert!(body.contains("cg="), "body: {body}");
    }
}
