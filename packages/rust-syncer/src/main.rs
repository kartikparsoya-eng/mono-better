//! rust-syncer — full Rust syncer binary entry point.
//!
//! Replaces the TS syncer worker process. Launched by `main.ts` when
//! `ZERO_SYNCER=rust` is set.
//!
//! Configuration is via environment variables:
//! - `PORT` — WebSocket listen port
//! - `HTTP_PORT` — HTTP server port (for /statz, /heapz, /notify)
//! - `REPLICA_FILE` — SQLite replica file path
//! - `CVR_PG_URI` — CVR Postgres connection string
//! - `TASK_ID` — Task ID
//! - `SHARD` — Shard ID
//! - `AUTH_JWK` — JWT JWK (optional)
//! - `AUTH_JWKS_URL` — JWT JWKS URL (optional)
//! - `AUTH_SECRET` — JWT secret (optional)
//! - `MUTAGEN_URL` — Mutagen service URL (optional)
//! - `PUSHER_URL` — Pusher service URL (optional)
//! - `MAX_CLIENT_GROUPS` — Maximum number of client groups

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use rust_syncer::http_server::run_http_server;
use rust_syncer::router::{ConnectionRouter, CGServicesFactory};
use rust_syncer::ws_server::{run_ws_server, WsServerConfig};

/// Configuration parsed from environment variables.
pub struct SyncerConfig {
    pub ws_port: u16,
    pub http_port: u16,
    pub replica_file: String,
    pub cvr_pg_uri: String,
    pub task_id: String,
    pub shard: String,
    pub auth_jwk: Option<String>,
    pub auth_jwks_url: Option<String>,
    pub auth_secret: Option<String>,
    pub mutagen_url: Option<String>,
    pub pusher_url: Option<String>,
    pub max_client_groups: usize,
}

impl SyncerConfig {
    pub fn from_env() -> Self {
        Self {
            ws_port: env::var("PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(8080),
            http_port: env::var("HTTP_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(8081),
            replica_file: env::var("REPLICA_FILE").unwrap_or_else(|_| "replica.db".to_string()),
            cvr_pg_uri: env::var("CVR_PG_URI").unwrap_or_else(|_| {
                "postgres://localhost/zero".to_string()
            }),
            task_id: env::var("TASK_ID").unwrap_or_else(|_| "task-0".to_string()),
            shard: env::var("SHARD").unwrap_or_else(|_| "0".to_string()),
            auth_jwk: env::var("AUTH_JWK").ok(),
            auth_jwks_url: env::var("AUTH_JWKS_URL").ok(),
            auth_secret: env::var("AUTH_SECRET").ok(),
            mutagen_url: env::var("MUTAGEN_URL").ok(),
            pusher_url: env::var("PUSHER_URL").ok(),
            max_client_groups: env::var("MAX_CLIENT_GROUPS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100),
        }
    }
}

fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = SyncerConfig::from_env();

    tracing::info!(
        "Starting rust-syncer: ws_port={}, http_port={}, shard={}, task_id={}",
        config.ws_port,
        config.http_port,
        config.shard,
        config.task_id
    );

    // Create the connection router
    // For now, we use a placeholder services factory and auth validator.
    // Phase 6 will provide the full implementation with Engine + CVRStore.
    let router = Arc::new(ConnectionRouter::new(
        Arc::new(PlaceholderServicesFactory {}),
        Arc::new(PlaceholderAuthValidator),
    ));

    // Create the tokio runtime
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Start the HTTP server
    let http_router = router.clone();
    let http_addr: SocketAddr = format!("0.0.0.0:{}", config.http_port).parse().unwrap();
    runtime.spawn(async move {
        run_http_server(http_addr, http_router).await;
    });

    // Start the WebSocket server
    let ws_router = router.clone();
    let ws_config = WsServerConfig {
        port: config.ws_port,
        max_payload_bytes: 1024 * 1024 * 16, // 16MB
        compression: false,
    };

    runtime.block_on(async move {
        let ws_router2 = ws_router.clone();
        run_ws_server(ws_config, move |ctx| {
            // Route the connection to the appropriate CG thread
            let cg_id = ctx.params.client_group_id.clone();
            let router = ws_router2.clone();
            tokio::spawn(async move {
                router.handle_connection(ctx).await;
            });
        }).await
    });

    // Send ready message to parent process (TS ProcessManager)
    // Format matches TS: JSON array ["ready", {"ready": true}]
    println!("[\"ready\", {{\"ready\": true}}]");
}

/// Placeholder services factory — will be replaced with full implementation.
struct PlaceholderServicesFactory {}

impl CGServicesFactory for PlaceholderServicesFactory {
    fn create_view_syncer(&self, _cg_id: &str) -> Arc<dyn rust_syncer::ViewSyncerDispatch> {
        Arc::new(PlaceholderViewSyncer)
    }

    fn create_conn_context_manager(
        &self,
        _cg_id: &str,
    ) -> Arc<dyn rust_syncer::ConnContextManagerDispatch> {
        Arc::new(PlaceholderConnContextManager)
    }

    fn create_mutagen(&self, _cg_id: &str) -> Option<Arc<dyn rust_syncer::MutagenDispatch>> {
        None
    }

    fn create_pusher(&self, _cg_id: &str) -> Option<Arc<dyn rust_syncer::PusherDispatch>> {
        None
    }
}

struct PlaceholderAuthValidator;

#[async_trait::async_trait]
impl rust_syncer::AuthValidator for PlaceholderAuthValidator {
    async fn validate_auth(
        &self,
        _client_group_id: &str,
        _client_id: &str,
        _user_id: Option<&str>,
        _auth: Option<&str>,
    ) -> Result<(), rust_syncer::protocol::ErrorBody> {
        Ok(())
    }
}

struct PlaceholderViewSyncer;

impl rust_syncer::ViewSyncerDispatch for PlaceholderViewSyncer {
    fn change_desired_queries(&self, _selector: &rust_syncer::ConnectionSelector, _msg: &str) {}
    fn update_auth(&self, _selector: &rust_syncer::ConnectionSelector, _msg: &str, _changed: bool) {}
    fn delete_clients(&self, _selector: &rust_syncer::ConnectionSelector, _msg: &str) -> Vec<String> {
        Vec::new()
    }
    fn init_connection(&self, _selector: &rust_syncer::ConnectionSelector, _msg: &str) -> bool {
        true
    }
    fn inspect(&self, _selector: &rust_syncer::ConnectionSelector, _msg: &str) {}
}

struct PlaceholderConnContextManager;

impl rust_syncer::ConnContextManagerDispatch for PlaceholderConnContextManager {
    fn must_get_connection_context(
        &self,
        _selector: &rust_syncer::ConnectionSelector,
    ) -> rust_syncer::ConnContextInfo {
        rust_syncer::ConnContextInfo {
            auth: None,
            revision: 0,
        }
    }

    fn init_connection(&self, _selector: &rust_syncer::ConnectionSelector, _body: &serde_json::Value) {}

    fn update_auth(&self, _selector: &rust_syncer::ConnectionSelector, _body: &serde_json::Value) -> bool {
        true
    }
}
