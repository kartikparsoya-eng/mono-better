//! Port of `packages/zero-protocol/src/pong.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use super::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PongBody {}

/// Create a `["pong", {}]` message.
pub fn pong_message() -> Value {
    downstream_message("pong", &PongBody {})
}
