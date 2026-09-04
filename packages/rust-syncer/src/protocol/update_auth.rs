//! Port of `packages/zero-protocol/src/update-auth.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use serde::{Deserialize, Serialize};

// valita `v.object` rejects unknown keys where serde ignores them (M13 R3).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateAuthBody {
    pub auth: String,
}
