//! HTTP server — axum-based endpoints for /statz, /heapz, /notify/:cg_id.
//!
//! The HTTP server runs on the tokio runtime. It serves:
//! - `GET /statz` — server statistics (active CGs, connections, memory)
//! - `GET /heapz` — heap snapshot placeholder (V8 compatibility)
//! - `POST /notify/:cg_id` — change-streamer notification endpoint
//!
//! Notifications are forwarded to the appropriate CG thread via a channel.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

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

/// Run the HTTP server on the given address.
pub async fn run_http_server(
    addr: SocketAddr,
    router: Arc<ConnectionRouter>,
) {
    let state = Arc::new(HttpServerState {
        router,
        stats: Arc::new(Mutex::new(ServerStats::default())),
        start_time: std::time::Instant::now(),
    });

    let app = Router::new()
        .route("/statz", get(statz_handler))
        .route("/heapz", get(heapz_handler))
        .route("/notify/:cg_id", post(notify_handler))
        .with_state(state);

    tracing::info!("HTTP server listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

/// GET /statz — return server statistics.
async fn statz_handler(
    State(state): State<Arc<HttpServerState>>,
) -> (StatusCode, Json<Value>) {
    let stats = state.stats.lock().unwrap();
    let uptime_ms = state.start_time.elapsed().as_millis() as u64;
    let active_cgs = state.router.cg_count();

    let response = json!({
        "activeClientGroups": active_cgs,
        "activeConnections": stats.active_connections,
        "totalMessagesReceived": stats.total_messages_received,
        "totalMessagesSent": stats.total_messages_sent,
        "uptimeMs": uptime_ms,
    });

    (StatusCode::OK, Json(response))
}

/// GET /heapz — heap snapshot placeholder.
/// Returns a minimal V8-style heap snapshot for compatibility.
async fn heapz_handler(
    State(state): State<Arc<HttpServerState>>,
) -> (StatusCode, Json<Value>) {
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
