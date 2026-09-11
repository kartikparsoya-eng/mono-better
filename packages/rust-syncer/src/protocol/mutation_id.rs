//! Port of `packages/zero-protocol/src/mutation-id.ts` — serde
//! equivalents of the valita schemas.

use serde::{Deserialize, Serialize};

/// `mutationIDSchema` in both valita parse modes (see `ast.rs` for why one
/// schema body is stamped out twice): [`strict`] for the upstream `push`
/// message, [`passthrough`] for the legacy `pushErrorSchema` bodies
/// `apiErrorFromResult` reads back from the API server (custom/fetch.ts:475).
macro_rules! mutation_id_schema {
    ($(#[$unknown:meta])*) => {
        // TS `mutationIDSchema.id` is `v.number()` — a JS number (f64), not an
        // i64. `Eq` is gone with it: f64 is not `Eq`, and TS compares these as
        // JS numbers anyway.
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        $(#[$unknown])*
        pub struct MutationID {
            pub id: crate::protocol::JsNumber,
            #[serde(rename = "clientID")]
            pub client_id: String,
        }
    };
}

/// `mutationIDSchema` as parsed at the upstream boundary (valita `strict`).
pub mod strict {
    use super::*;
    mutation_id_schema!(#[serde(deny_unknown_fields)]);
}

/// `mutationIDSchema` as parsed from the API server (valita `passthrough`).
pub mod passthrough {
    use super::*;
    mutation_id_schema!();
}

pub use strict::*;
