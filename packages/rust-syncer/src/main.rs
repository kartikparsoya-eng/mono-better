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
//! - `ZERO_APP_ID` / `APP_ID` — Application id (schema prefix); default `zero`
//! - `ZERO_ADMIN_PASSWORD` — Inspector protocol admin password (optional)
//! - `ZERO_SERVER_VERSION` — Server version for the inspector `version` op
//!   (defaults to the crate version)

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use rust_syncer::http_server::{bind_http_listener, serve_http};
use rust_syncer::router::{CGServicesFactory, ConnectionRouter};
use rust_syncer::ws_server::{WsServerConfig, bind_ws_listener, serve_ws};

/// Configuration parsed from environment variables.
pub struct SyncerConfig {
    pub ws_port: u16,
    pub http_port: u16,
    pub replica_file: String,
    pub cvr_pg_uri: String,
    pub task_id: String,
    pub shard: String,
    pub app_id: String,
    pub auth_jwk: Option<String>,
    pub auth_jwks_url: Option<String>,
    pub auth_secret: Option<String>,
    pub auth_issuer: Option<String>,
    pub auth_audience: Option<String>,
    pub mutagen_url: Option<String>,
    pub pusher_url: Option<String>,
    pub max_client_groups: usize,
    pub admin_password: Option<String>,
    pub server_version: String,
    /// Max CVR Postgres connections for this worker (parity with the TS
    /// `--cvr-max-conns-per-worker` flag: whole budget divided across syncers).
    pub cvr_max_conns: u32,
    /// Interval (ms) between periodic JWT re-validation + query re-transform for
    /// live connections (TS `--auth-revalidate-interval-seconds`, default 300s).
    /// `0` disables periodic auth maintenance.
    pub revalidate_interval_ms: Option<i64>,
}

impl SyncerConfig {
    pub fn from_env() -> Self {
        Self {
            ws_port: env::var("PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8080),
            http_port: env::var("HTTP_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8081),
            replica_file: env::var("REPLICA_FILE").unwrap_or_else(|_| "replica.db".to_string()),
            cvr_pg_uri: env::var("CVR_PG_URI")
                .unwrap_or_else(|_| "postgres://localhost/zero".to_string()),
            task_id: env::var("TASK_ID").unwrap_or_else(|_| "task-0".to_string()),
            shard: env::var("SHARD").unwrap_or_else(|_| "0".to_string()),
            app_id: env::var("ZERO_APP_ID")
                .or_else(|_| env::var("APP_ID"))
                .unwrap_or_else(|_| "zero".to_string()),
            auth_jwk: env::var("AUTH_JWK").ok(),
            auth_jwks_url: env::var("AUTH_JWKS_URL").ok(),
            auth_secret: env::var("AUTH_SECRET").ok(),
            auth_issuer: env::var("AUTH_ISSUER").ok(),
            auth_audience: env::var("AUTH_AUDIENCE").ok(),
            mutagen_url: env::var("MUTAGEN_URL").ok(),
            pusher_url: env::var("PUSHER_URL").ok(),
            max_client_groups: env::var("MAX_CLIENT_GROUPS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100),
            admin_password: env::var("ZERO_ADMIN_PASSWORD")
                .ok()
                .filter(|s| !s.is_empty()),
            server_version: env::var("ZERO_SERVER_VERSION")
                .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string()),
            cvr_max_conns: env::var("CVR_MAX_CONNS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
            // TS default: 300s. `0` (or a negative) disables it.
            revalidate_interval_ms: {
                let secs = env::var("AUTH_REVALIDATE_INTERVAL_SECONDS")
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(300);
                (secs > 0).then_some(secs * 1000)
            },
        }
    }
}

/// Resolve when the process receives a shutdown signal (Ctrl-C / SIGINT, or
/// SIGTERM from the ProcessManager). Used to trigger a graceful drain.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = term => {},
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

    let config = Arc::new(SyncerConfig::from_env());

    tracing::info!(
        "Starting rust-syncer: ws_port={}, http_port={}, shard={}, task_id={}",
        config.ws_port,
        config.http_port,
        config.shard,
        config.task_id
    );

    // Create the tokio runtime first — its handle is injected into each CG's
    // SyncEngine for the `block_on` PG I/O edge (the CG threads have no ambient
    // runtime of their own).
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Shared process metrics — the same Arc is handed to the router (read by
    // `/statz`) and to every CG's SyncEngineConfig (written on the hot path).
    let metrics = Arc::new(rust_syncer::metrics::Metrics::default());

    // Create the connection router with the real per-CG services factory and a
    // real JWT auth validator (secret/jwk/jwksUrl from config).
    let router = Arc::new(ConnectionRouter::new(
        Arc::new(RealServicesFactory {
            config: config.clone(),
            tokio_handle: runtime.handle().clone(),
            metrics: metrics.clone(),
        }),
        Arc::new(rust_syncer::JwtAuthValidator {
            jwk: config.auth_jwk.clone(),
            secret: config.auth_secret.clone(),
            jwks_url: config.auth_jwks_url.clone(),
            issuer: config.auth_issuer.clone(),
            audience: config.auth_audience.clone(),
        }),
        metrics.clone(),
    ));

    // Bind BOTH listeners eagerly so the process is genuinely accepting on its
    // WS + HTTP ports before we announce readiness. The TS dispatcher reverse-
    // proxies client upgrades to the WS port and POSTs commit notifications to
    // the HTTP port, and `ProcessManager.allWorkersReady()` gates the dispatcher
    // on the ready signal — so readiness MUST come after the binds, not (as
    // before) after the accept loop exits at shutdown.
    let http_router = router.clone();
    let http_addr: SocketAddr = format!("0.0.0.0:{}", config.http_port).parse().unwrap();
    let ws_router = router.clone();
    let ws_config = WsServerConfig {
        port: config.ws_port,
        max_payload_bytes: 1024 * 1024 * 16, // 16MB
        compression: false,
    };

    let (http_listener, ws_listener) = runtime.block_on(async {
        let http_listener = bind_http_listener(http_addr).await;
        let ws_listener = bind_ws_listener(ws_config.port)
            .await
            .expect("bind WebSocket port");
        (http_listener, ws_listener)
    });

    // Serve HTTP in the background now that its listener is bound.
    runtime.spawn(async move {
        serve_http(http_listener, http_router).await;
    });

    // Both ports are bound → announce readiness to the parent ProcessManager.
    // Format matches TS: JSON array ["ready", {"ready": true}]. Flush stdout
    // (it is piped, so line-buffered) so the parent sees it immediately.
    println!("[\"ready\", {{\"ready\": true}}]");
    use std::io::Write as _;
    let _ = std::io::stdout().flush();

    let _ = runtime.block_on(async move {
        let ws_router2 = ws_router.clone();
        let server = serve_ws(ws_listener, move |ctx| {
            // Route the connection to the appropriate CG thread
            let router = ws_router2.clone();
            tokio::spawn(async move {
                router.handle_connection(ctx).await;
            });
        });
        tokio::pin!(server);
        // Serve until the accept loop ends OR a shutdown signal arrives, then
        // drain: `router.shutdown()` fails every connection with a Rehome error
        // so clients reconnect elsewhere (TS `Syncer.drain` / view-syncer
        // `#cleanup`), and joins the CG threads.
        let result = tokio::select! {
            res = &mut server => res,
            () = shutdown_signal() => {
                tracing::info!("shutdown signal received; draining connections");
                ws_router.shutdown().await;
                Ok(())
            }
        };
        result
    });
}

/// Per-CG services factory. Builds a real `SyncEngine` config from the process
/// config (replica path, CVR Postgres, shard). Mutagen/pusher are intentionally
/// absent (mutations are HTTP-direct — see `create_mutagen`); the
/// connection-context dispatch is a light placeholder (the CG-thread path owns
/// auth state directly, see `router.rs`).
struct RealServicesFactory {
    config: Arc<SyncerConfig>,
    tokio_handle: tokio::runtime::Handle,
    metrics: Arc<rust_syncer::metrics::Metrics>,
}

impl CGServicesFactory for RealServicesFactory {
    fn create_view_syncer(&self, _cg_id: &str) -> Arc<dyn rust_syncer::ViewSyncerDispatch> {
        Arc::new(PlaceholderViewSyncer)
    }

    fn create_conn_context_manager(
        &self,
        _cg_id: &str,
    ) -> Arc<dyn rust_syncer::ConnContextManagerDispatch> {
        Arc::new(PlaceholderConnContextManager)
    }

    // Mutations are HANDLED OUT-OF-BAND, not by the Rust syncer. Clients push
    // mutations over HTTP directly to the TS mutation endpoint (mutagen → PG for
    // CRUD; pusher → userPushURL for custom). Their results flow back to clients
    // through the CVR's internal `lmids` (→ `lastMutationIDChanges`) and
    // `mutationResults` (→ `mutationsPatch`) queries, which the SyncEngine
    // already hydrates and pokes. The Rust syncer therefore never processes a WS
    // `push`: with no mutagen/pusher wired, `SyncerWsMessageHandler` rejects a
    // stray WS push with the TS-faithful "must set ZERO_MUTATE_URL" / "legacy
    // CRUD disabled" error. Returning `None` here is the intended production
    // configuration, not an unfinished stub.
    fn create_mutagen(&self, _cg_id: &str) -> Option<Arc<dyn rust_syncer::MutagenDispatch>> {
        None
    }

    fn create_pusher(&self, _cg_id: &str) -> Option<Arc<dyn rust_syncer::PusherDispatch>> {
        None
    }

    fn create_sync_engine_config(&self, cg_id: &str) -> rust_syncer::SyncEngineConfig {
        let shard_num = self.config.shard.parse::<u32>().unwrap_or(0);
        // Read the syncable table specs + read-permissions from the replica.
        let tables = match rust_syncer::compute_table_specs_from_path(&self.config.replica_file) {
            Ok(specs) => {
                tracing::info!(
                    "CG {cg_id}: loaded {} table specs from replica",
                    specs.len()
                );
                specs
            }
            Err(e) => {
                tracing::error!("CG {cg_id}: failed to read replica table specs: {e}");
                Vec::new()
            }
        };
        let load_result: Result<Option<serde_json::Value>, String> =
            rusqlite::Connection::open(&self.config.replica_file)
                .map_err(|e| e.to_string())
                .and_then(|conn| rust_syncer::load_permissions(&conn, &self.config.app_id))
                .map(|loaded| loaded.permissions);
        match &load_result {
            Ok(Some(_)) => tracing::info!("CG {cg_id}: loaded read-permissions from replica"),
            Ok(None) => {
                tracing::warn!("CG {cg_id}: no read-permissions deployed — queries pass through")
            }
            // Fail CLOSED: an existing-but-unloadable permissions doc must not
            // silently disable authorization. `resolve_permissions` substitutes a
            // deny-all config so no unauthorized row is served.
            Err(e) => tracing::error!(
                "CG {cg_id}: failed to load permissions ({e}); denying all client queries (fail-closed)"
            ),
        }
        // Re-read the hash separately for hot-reload detection. It shares the
        // load path but we keep it even when `resolve_permissions` substitutes
        // deny-all on error: a later successful read with a real hash then
        // differs from this seed and triggers a self-healing reload.
        let permissions_hash: Option<String> =
            rusqlite::Connection::open(&self.config.replica_file)
                .ok()
                .and_then(|conn| rust_syncer::load_permissions(&conn, &self.config.app_id).ok())
                .and_then(|loaded| loaded.hash);
        let permissions = rust_syncer::resolve_permissions(load_result);
        let app_id = self.config.app_id.clone();
        rust_syncer::SyncEngineConfig {
            tables,
            replica_path: Some(self.config.replica_file.clone()),
            app_id: app_id.clone(),
            shard: rust_cvr::types::ShardID {
                app_id: app_id.clone(),
                shard_num,
            },
            cvr_pg: Some(rust_syncer::CvrPgConfig {
                pg_uri: self.config.cvr_pg_uri.clone(),
                schema: format!("{}_{}/cvr", app_id, self.config.shard),
                cvr_id: cg_id.to_string(),
                task_id: self.config.task_id.clone(),
                max_conns: self.config.cvr_max_conns,
            }),
            permissions,
            permissions_hash,
            revalidate_interval_ms: self.config.revalidate_interval_ms,
            tokio_handle: self.tokio_handle.clone(),
            admin_password: self.config.admin_password.clone(),
            server_version: self.config.server_version.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

struct PlaceholderViewSyncer;

impl rust_syncer::ViewSyncerDispatch for PlaceholderViewSyncer {
    fn change_desired_queries(&self, _selector: &rust_syncer::ConnectionSelector, _msg: &str) {}
    fn update_auth(&self, _selector: &rust_syncer::ConnectionSelector, _msg: &str, _changed: bool) {
    }
    fn delete_clients(
        &self,
        _selector: &rust_syncer::ConnectionSelector,
        _msg: &str,
    ) -> Vec<String> {
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

    fn init_connection(
        &self,
        _selector: &rust_syncer::ConnectionSelector,
        _body: &serde_json::Value,
    ) {
    }

    fn update_auth(
        &self,
        _selector: &rust_syncer::ConnectionSelector,
        _body: &serde_json::Value,
    ) -> bool {
        true
    }
}
