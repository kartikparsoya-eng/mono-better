//! Port of `packages/zero-protocol/src/error-origin-enum.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use serde::{Deserialize, Serialize};

// ErrorOrigin values are lowercase/mixed ("client", "server", "zeroCache").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ErrorOrigin {
    Client,
    Server,
    ZeroCache,
}
