//! Port of `packages/zero-protocol/src/delete-clients.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use serde::{Deserialize, Serialize};

// deleteClientsBodySchema uses clientIDs/clientGroupIDs (capital IDs)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeleteClientsBody {
    #[serde(rename = "clientIDs")]
    pub client_ids: Option<Vec<String>>,
    #[serde(rename = "clientGroupIDs")]
    pub client_group_ids: Option<Vec<String>>,
}
