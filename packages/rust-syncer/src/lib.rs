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

/// Rust-only invention I-13 (parity/INVENTIONS.md): process-wide mimalloc for
/// Rust AND SQLite allocations. No TS twin — see the module doc.
pub mod alloc;

// TS-mirrored subtrees.
pub mod ast_to_zql;
pub mod auth;
pub mod custom_queries;
pub mod db;
pub mod services;
// Fold of `shared/src/tdigest.ts` (+ centroid.ts / binary-search.ts) — the
// inspector server-metrics histogram; see `server/inspector_delegate.rs`.
pub mod tdigest;
pub mod workers;

// Rust-only infra + the fused per-CG actor core (router).
pub mod http_server;
pub mod live_count;
pub mod observability;
pub use observability::metrics;
pub mod config;
pub mod custom;
pub mod server;
pub use server::otel_start as otel;
pub mod protocol;
pub mod trace;
pub mod ws_server;
pub mod ws_sink;

/// A global default subscriber that swallows every event but declares itself
/// interested in ALL of them.
///
/// This is the piece that makes log capture reliable. `tracing` caches each
/// callsite's `Interest` process-globally, and with no global default
/// installed it computes that interest from `Dispatch::none()` — which is
/// interested in nothing. The first thread to reach a callsite therefore
/// caches it as disabled FOREVER, and a thread-local capture subscriber
/// installed later is never offered the event. The symptom is a capture buffer
/// missing exactly the lines whose callsites some other test happened to touch
/// first, while lines from callsites this test reached first come through
/// normally — which is why it looked like a subscriber-scoping bug rather than
/// a caching one, and why it only appeared under `cargo test`'s parallelism.
///
/// Installing this once makes every callsite `Interest::always()`, so interest
/// is never cached as disabled and each event is dispatched to whatever
/// dispatcher is current on the emitting thread — the per-test
/// `set_default`/`with_default` subscriber when there is one, this no-op
/// otherwise.
#[cfg(test)]
struct AlwaysInterested;

#[cfg(test)]
impl tracing::Subscriber for AlwaysInterested {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::always()
    }
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

/// Install [`AlwaysInterested`] as the process-global default, exactly once.
#[cfg(test)]
pub(crate) fn ensure_permissive_global_subscriber() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // A global default may already exist (another harness installed one);
        // failing to set ours is fine as long as SOMETHING global is there.
        let _ = tracing::subscriber::set_global_default(AlwaysInterested);
    });
}

pub use auth::jwt::{JwtAuthValidator, decode_jwt_claims};
pub use auth::load_permissions::{
    LoadedPermissions, PermissionsReload, deny_all_permissions, load_permissions,
    reload_permissions_if_changed, resolve_permissions,
};
pub use auth::read_authorizer::{
    hash_of_ast, hash_of_name_and_args, transform_and_hash_query, transform_query,
};
pub use db::lite_tables::{
    ReplicaVersions, ZqlSpecOptions, compute_table_specs_from_path, compute_zql_specs,
    read_replica_versions, read_replica_versions_from_path,
};
pub use db::specs::{LiteColumnSpec, LiteTableSpec};
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
