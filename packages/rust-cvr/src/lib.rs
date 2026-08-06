//! The Rust port of the zero-cache view-syncer's CVR stack.
//!
//! Phase A scope (this commit):
//! - `hash` — TS-parity for `packages/shared/src/hash.ts`
//! - `row_key` — TS-parity for `packages/zero-cache/src/types/row-key.ts`
//! - `row_set_signature` — TS-parity for
//!   `packages/zero-cache/src/services/view-syncer/row-set-signature.ts`
//!
//! Parity contract: every public function must be byte-identical (same return
//! value for same inputs) to the TS implementation, including error behavior
//! where applicable.
//!
//! See `packages/zero-cache/docs/rust-cvr-port/` for the rollout plan.

pub mod hash;
pub mod row_key;
pub mod row_set_signature;

#[cfg(test)]
mod parity_check;
