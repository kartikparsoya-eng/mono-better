//! Port of `packages/zero-protocol/src/error-reason-enum.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use serde::{Deserialize, Serialize};

// ErrorReason values are lowercase/mixed ("database", "parse", "oooMutation", etc.).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorReason {
    #[serde(rename = "database")]
    Database,
    #[serde(rename = "parse")]
    Parse,
    #[serde(rename = "oooMutation")]
    OutOfOrderMutation,
    #[serde(rename = "unsupportedPushVersion")]
    UnsupportedPushVersion,
    #[serde(rename = "internal")]
    Internal,
    #[serde(rename = "http")]
    Http,
    #[serde(rename = "timeout")]
    Timeout,
}
