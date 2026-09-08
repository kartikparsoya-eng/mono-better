//! Port of `packages/zero-protocol/src/delete-clients.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use serde::{Deserialize, Serialize};

// deleteClientsBodySchema uses clientIDs/clientGroupIDs (capital IDs)
// valita `v.object` rejects unknown keys (M13 R3), and `.optional()` is
// absent-or-value, never an explicit `null` (M13 R4).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteClientsBody {
    #[serde(
        rename = "clientIDs",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::protocol::optional_no_null"
    )]
    pub client_ids: Option<Vec<String>>,
    #[serde(
        rename = "clientGroupIDs",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::protocol::optional_no_null"
    )]
    pub client_group_ids: Option<Vec<String>>,
}
