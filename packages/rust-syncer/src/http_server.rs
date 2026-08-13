//! HTTP server — axum-based endpoints for /statz, /heapz, /notify/:cg_id.
//!
//! The HTTP server runs on the tokio runtime. It serves:
//! - `GET /statz` — server statistics (active CGs, connections, memory)
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
    response::Json,
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
    });

    let app = Router::new()
        .route("/statz", get(statz_handler))
        .route("/heapz", get(heapz_handler))
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
