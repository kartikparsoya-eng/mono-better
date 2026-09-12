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

impl std::fmt::Display for ErrorOrigin {
    /// The wire spelling — the enum's VALUE, not its member name. TS declares
    /// `Client = 'client'`, `Server = 'server'` and `ZeroCache = 'zeroCache'`
    /// (error-origin-enum.ts:1-3), and it is that string, not the member name,
    /// that `${errorBody.origin}` interpolates (connection.ts:333).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ErrorOrigin::Client => "client",
            ErrorOrigin::Server => "server",
            ErrorOrigin::ZeroCache => "zeroCache",
        })
    }
}
