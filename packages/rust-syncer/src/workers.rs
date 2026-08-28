//! `workers/` — port of `zero-cache/src/workers/`: connect-param parsing, the
//! Connection lifecycle, the Syncer (connection management + serving-lag
//! statistics, `syncer.ts`), and the WS message handler. `cg_executor` is the
//! RUST-ONLY scheduling substrate those ported seats run on (no TS twin —
//! INVENTIONS.md I-1/doc 91).
pub mod cg_executor;
pub mod connect_params;
pub mod connection;
pub mod syncer;
pub mod syncer_ws_message_handler;
