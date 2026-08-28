//! rust-syncer — full Rust syncer binary for zero-cache.
//!
//! Replaces the entire TS syncer process (syncer.ts, dispatcher.ts,
//! view-syncer.ts, connection.ts, etc.) with a single Rust binary.
//! See `packages/zero-cache/docs/rust-cvr-port/89-full-rust-syncer.md`.
//!
//! ## File structure
//! The module tree mirrors the TS source layout 1:1 (`auth/`, `workers/`,
//! `services/view_syncer/`, `custom_queries/`, `db/`) so each Rust file maps to
//! its TS origin. The one documented exception is the per-CG **actor core**
//! (`router.rs`): TS's separate `ViewSyncerService` (view-syncer.ts),
//! `ConnectionContextManager` (connection-context-manager.ts) and `Syncer`
//! (syncer.ts) classes map to `ViewSyncerService` + `Syncer` for the
//! single-threaded-per-CG `spawn_local` model — they cannot split into 1:1 files
//! without un-fusing the structs (a rewrite). The remaining top-level files
//! (`ws_server`, `ws_sink`, `http_server`, `otel`, `metrics`, `protocol`,
//! `sync_engine`, `live_count`, `trace`) are Rust-only transport /
//! observability / process infra with no single TS origin.

// TS-mirrored subtrees.
pub mod auth;
pub mod custom_queries;
pub mod db;
pub mod services;
pub mod workers;

// Rust-only infra + the fused per-CG actor core (router).
pub mod http_server;
pub mod live_count;
pub mod observability;
pub use observability::metrics;
pub mod custom;
pub mod otel;
pub mod protocol;
pub mod trace;
pub mod ws_server;
pub mod ws_sink;

pub use auth::jwt::{JwtAuthValidator, decode_jwt_claims};
pub use auth::load_permissions::{
    LoadedPermissions, PermissionsReload, deny_all_permissions, load_permissions,
    reload_permissions_if_changed, resolve_permissions,
};
pub use auth::read_authorizer::{hash_of_ast, transform_and_hash_query, transform_query};
pub use db::lite_tables::{
    ReplicaVersions, compute_table_specs_from_path, compute_zql_specs, read_replica_versions,
    read_replica_versions_from_path, validate_client_schema,
};
pub use http_server::{
    HttpServerState, ServerStats, bind_http_listener, run_http_server, serve_http,
};
pub use protocol::*;
pub use services::mutagen::pusher::PusherService;
pub use services::view_syncer::connection_context_manager::{
    Auth, CCMError, ConnectParamsForRegistration, ConnectionContextManager, ConnectionFetchContext,
    ConnectionState, ConnectionValidation, FetchConfig, HeaderOptions, InitConnectionBody,
    JwtPayload, MaintenanceKind, MaintenancePlan, UpdateAuthBody, UserState, ValidationResult,
    auth_equals, resolve_auth,
};
pub use services::view_syncer::drain_coordinator::DrainCoordinator;
pub use services::view_syncer::pipeline_driver::{
    AdvanceOutcome, IvmColumnSchema, IvmPipelines, IvmTableSpec, parse_ts_ast,
};
pub use services::view_syncer::view_syncer::{
    AuthValidator, CGServicesFactory, CvrPgConfig, SyncEngineConfig,
};
pub use services::view_syncer::view_syncer::{SyncResult, ViewSyncerService};
pub use workers::cg_executor::{CGHandle, CGMessage};
pub use workers::connect_params::{ConnectParams, ConnectParamsError, get_connect_params};
pub use workers::connection::{
    Connection, HandlerResult, LogLevel, MessageHandler, classify_error_log_level,
};
pub use workers::syncer::{ConnectionSinks, GroupAuthState, Syncer};
pub use workers::syncer_ws_message_handler::{
    ConnContextInfo, ConnContextManagerDispatch, ConnectionSelector, MutagenDispatch,
    PushRelayHeaders, PusherDispatch, SyncerWsMessageHandler, ViewSyncerDispatch,
};
pub use ws_server::{
    ConnectionContext, WsServerConfig, accept_connection, accept_connection_with_limit,
    bind_ws_listener, run_ws_server, serve_ws, serve_ws_with_config,
};
pub use ws_sink::{DirectWebSocketSink, WsCommand};
