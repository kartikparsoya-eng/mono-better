//! Port of `packages/zero-cache/src/services/view-syncer/ttl-clock.ts` — the
//! opaque TTLClock timestamp. TS brands it as a number; Rust uses `i64` directly.

/// TTLClock is an opaque number in TS. In Rust we use i64 directly.
pub type TTLClock = i64;
