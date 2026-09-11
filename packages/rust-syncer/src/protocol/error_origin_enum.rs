//! Port of `packages/zero-protocol/src/error-origin-enum.ts` — serde
//! equivalents of the valita schemas.

use serde::{Deserialize, Serialize};

// ErrorOrigin values are lowercase/mixed ("client", "server", "zeroCache").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ErrorOrigin {
    Client,
    Server,
    ZeroCache,
}
