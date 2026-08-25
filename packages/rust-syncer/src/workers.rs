//! `workers/` — port of `zero-cache/src/workers/`: the syncer worker's connect-
//! param parsing, connection lifecycle, WS message handler, and the serving-lag
//! statistics from `syncer.ts`. The `Syncer` dispatch itself is fused into the
//! per-CG actor core (`router.rs`), a documented "Rust in the right place".
pub mod connect_params;
pub mod connection;
pub mod syncer;
pub mod syncer_ws_message_handler;
