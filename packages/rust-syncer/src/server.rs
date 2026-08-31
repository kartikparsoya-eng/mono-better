//! `server/` — port of `zero-cache/src/server/`: the syncer worker's
//! bootstrap seat (`syncer.ts` — the per-CG services factory) and the OTLP
//! metrics export startup (`otel-start.ts`). The process entry (`main.ts` +
//! `runner/`) remains `src/main.rs` (the bin); the HTTP/WS dispatch
//! (`worker-dispatcher.ts`) is `http_server.rs`/`ws_server.rs` (invention-
//! heavy: I-1/I-4 reader/writer tasks; documented in INVENTIONS.md).

pub mod inspector_delegate;
pub mod otel_start;
pub mod syncer;
