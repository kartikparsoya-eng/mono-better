//! Env-gated event trace harness for debugging the syncer's connection/advance
//! lifecycle.
//!
//! Enable with `SYNCER_TRACE=1`. Zero cost when off (one cached bool check).
//! Instrument key lifecycle events with:
//!     crate::trace::note("conn-open", &format!("cg={cg} client={id}"));
//! and read the flow as a top-to-bottom log of connection open/close, hydrate
//! start/end (with elapsed), advance start/end, and poke.
//!
//! Intended for correlating a climbing `/census` counter with the event that
//! failed to tear something down (e.g. an advance that never completes, or a
//! connection whose close never fires). Not part of the production path.

use std::sync::OnceLock;

static ENABLED: OnceLock<bool> = OnceLock::new();

#[inline]
pub fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var("SYNCER_TRACE").is_ok())
}

/// Log a free-form lifecycle event. `op` is a short event tag (e.g.
/// `"conn-open"`, `"hydrate-end"`); `msg` carries the context (ids, elapsed).
#[inline]
pub fn note(op: &str, msg: &str) {
    if enabled() {
        eprintln!("[syncer-trace] {op:16} {msg}");
    }
}
