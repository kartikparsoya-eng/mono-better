//! Port of `packages/zero-protocol/src/update-auth.ts` — serde
//! equivalents of the valita schemas.

use serde::{Deserialize, Serialize};

// valita `v.object` rejects unknown keys where serde ignores them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateAuthBody {
    pub auth: String,
}
