//! Env-gated event trace harness for debugging the CVR flush/poke pipeline.
//!
//! Enable with `CVR_TRACE=1`. Zero cost when off (one cached bool check).
//! Instrument a meaningful CVR event with:
//!     crate::tracer::note("CVRStore", "flush start rows=42");
//! and read the flow as a top-to-bottom log of store flushes, loads, updater
//! batches, and poke start/end — the events that move (or fail to move) a CVR
//! version forward.
//!
//! Mirrors `packages/rust-ivm/src/ivm/trace.rs`. Not part of the production
//! path.

use std::sync::OnceLock;

static ENABLED: OnceLock<bool> = OnceLock::new();

#[inline]
pub fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var("CVR_TRACE").is_ok())
}

/// Log a free-form event against a named component.
#[inline]
pub fn note(op: &str, msg: &str) {
    if enabled() {
        eprintln!("[cvr-trace] {op:16} {msg}");
    }
}

/// Log an event a component RECEIVED (e.g. an updater batch).
#[inline]
pub fn recv(op: &str, msg: &str) {
    if enabled() {
        eprintln!("[cvr-trace] {op:16} recv  {msg}");
    }
}

/// Log an event a component EMITTED (e.g. a poke to the client).
#[inline]
pub fn emit(op: &str, msg: &str) {
    if enabled() {
        eprintln!("[cvr-trace] {op:16} EMIT  {msg}");
    }
}
