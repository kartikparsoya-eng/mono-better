//! rust-syncer — full Rust syncer binary for zero-cache.
//!
//! Replaces the entire TS syncer process (syncer.ts, dispatcher.ts,
//! view-syncer.ts, connection.ts, etc.) with a single Rust binary.
//! See `packages/zero-cache/docs/rust-cvr-port/89-full-rust-syncer.md`.

pub mod connect_params;
pub mod protocol;
pub mod ws_server;
pub mod ws_sink;

pub use connect_params::{get_connect_params, ConnectParams, ConnectParamsError};
pub use protocol::*;
pub use ws_server::{run_ws_server, accept_connection, ConnectionContext, WsServerConfig};
pub use ws_sink::{DirectWebSocketSink, WsCommand};
