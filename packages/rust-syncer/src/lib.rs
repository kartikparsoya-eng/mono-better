//! rust-syncer — full Rust syncer binary for zero-cache.
//!
//! Replaces the entire TS syncer process (syncer.ts, dispatcher.ts,
//! view-syncer.ts, connection.ts, etc.) with a single Rust binary.
//! See `packages/zero-cache/docs/rust-cvr-port/89-full-rust-syncer.md`.

pub mod auth;
pub mod connect_params;
pub mod connection;
pub mod connection_context;
pub mod custom_query;
pub mod drain;
pub mod http_server;
pub mod message_handler;
pub mod metrics;
pub mod permissions;
pub mod pipeline_driver;
pub mod protocol;
pub mod replica_schema;
pub mod router;
pub mod sync_engine;
// NOTE: the former `view_syncer` module (the placeholder `RustViewSyncer` +
// `PipelineDriver`/`CVRStoreOps` traits) was removed. The real dispatch lives on
// the CG thread: `router::CgState` owns a `sync_engine::SyncEngine`, which drives
// `rust-ivm` (via `pipeline_driver::IvmPipelines`) and `rust-cvr`. One path.
pub mod ws_server;
pub mod ws_sink;

pub use auth::{JwtAuthValidator, decode_jwt_claims};
pub use connect_params::{ConnectParams, ConnectParamsError, get_connect_params};
pub use connection::{
    Connection, HandlerResult, LogLevel, MessageHandler, classify_error_log_level,
};
pub use connection_context::{
    Auth, CCMError, ConnectParamsForRegistration, ConnectionContextManager, ConnectionFetchContext,
    ConnectionState, ConnectionValidation, FetchConfig, HeaderOptions, InitConnectionBody,
    JwtPayload, MaintenanceKind, MaintenancePlan, UpdateAuthBody, UserState, ValidationResult,
    auth_equals, resolve_auth,
};
pub use drain::DrainCoordinator;
pub use http_server::{
    HttpServerState, ServerStats, bind_http_listener, run_http_server, serve_http,
};
pub use message_handler::{
    ConnContextInfo, ConnContextManagerDispatch, ConnectionSelector, MutagenDispatch,
    PusherDispatch, SyncerWsMessageHandler, ViewSyncerDispatch,
};
pub use permissions::{
    LoadedPermissions, PermissionsReload, deny_all_permissions, hash_of_ast, load_permissions,
    reload_permissions_if_changed, resolve_permissions, transform_and_hash, transform_query,
};
pub use pipeline_driver::{
    AdvanceOutcome, IvmColumnSchema, IvmPipelines, IvmTableSpec, parse_ts_ast,
};
pub use protocol::*;
pub use replica_schema::{
    ReplicaVersions, compute_table_specs, compute_table_specs_from_path, read_replica_versions,
    read_replica_versions_from_path, validate_client_schema,
};
pub use router::{
    AuthValidator, CGHandle, CGMessage, CGServicesFactory, ConnectionRouter, CvrPgConfig,
    GroupAuthState, SyncEngineConfig,
};
pub use sync_engine::{SyncEngine, SyncResult, parse_existing_rows};
pub use ws_server::{
    ConnectionContext, WsServerConfig, accept_connection, accept_connection_with_limit,
    bind_ws_listener, run_ws_server, serve_ws, serve_ws_with_config,
};
pub use ws_sink::{DirectWebSocketSink, WsCommand};
