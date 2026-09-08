//! Port of `packages/zero-protocol/src/change-desired-queries.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use super::*;
use serde::{Deserialize, Serialize};

// changeDesiredQueriesBodySchema uses desiredQueriesPatch
// valita `v.object` rejects unknown keys (M13 R3).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChangeDesiredQueriesBody {
    pub desired_queries_patch: UpQueriesPatch,
    // `.optional()` is absent-or-value, never `null` (M13 R4).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::protocol::optional_no_null"
    )]
    pub traceparent: Option<String>,
}
