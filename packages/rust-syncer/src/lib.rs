//! rust-syncer — full Rust syncer binary for zero-cache.
//!
//! Replaces the entire TS syncer process (syncer.ts, dispatcher.ts,
//! view-syncer.ts, connection.ts, etc.) with a single Rust binary.
//! See `packages/zero-cache/docs/rust-cvr-port/89-full-rust-syncer.md`.

pub mod connect_params;
pub mod connection;
pub mod connection_context;
pub mod drain;
pub mod http_server;
pub mod message_handler;
pub mod protocol;
pub mod router;
pub mod view_syncer;
pub mod ws_server;
pub mod ws_sink;

pub use connect_params::{get_connect_params, ConnectParams, ConnectParamsError};
pub use connection::{Connection, HandlerResult, MessageHandler, LogLevel, classify_error_log_level};
pub use connection_context::{
    Auth, CCMError, ConnectionContextManager,
    ConnectionState, ConnectionFetchContext,
    ConnectionValidation, FetchConfig, HeaderOptions,
    InitConnectionBody, JwtPayload, MaintenanceKind, MaintenancePlan,
    ConnectParamsForRegistration, UpdateAuthBody, UserState, ValidationResult,
    auth_equals, resolve_auth,
};
pub use drain::DrainCoordinator;
pub use message_handler::{
    ConnContextInfo, ConnContextManagerDispatch, ConnectionSelector,
    MutagenDispatch, PusherDispatch, SyncerWsMessageHandler, ViewSyncerDispatch,
};
pub use protocol::*;
pub use router::{
    AuthValidator, CGHandle, CGMessage, CGServicesFactory, ConnectionRouter,
    GroupAuthState,
};
pub use view_syncer::{
    AdvanceNotification, AdvanceParams, AdvanceResult, AdvanceSyncResult,
    CVRQuery, CVRSnapshot, CVRStoreOps, CVRVersion, HydrateParams,
    HydrateResult, InspectorDelegate, PipelineDriver, RustViewSyncer, TTLClock,
    TransformMode, has_expired_queries, is_expired,
};
pub use http_server::{run_http_server, ServerStats, HttpServerState};
pub use ws_server::{run_ws_server, accept_connection, ConnectionContext, WsServerConfig};
pub use ws_sink::{DirectWebSocketSink, WsCommand};
