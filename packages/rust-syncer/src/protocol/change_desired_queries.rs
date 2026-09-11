//! Port of `packages/zero-protocol/src/change-desired-queries.ts` — serde
//! equivalents of the valita schemas.

use super::*;
use serde::{Deserialize, Serialize};

// changeDesiredQueriesBodySchema uses desiredQueriesPatch
// valita `v.object` rejects unknown keys.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChangeDesiredQueriesBody {
    #[serde(deserialize_with = "crate::protocol::queries_patch::strict_up_queries_patch")]
    pub desired_queries_patch: UpQueriesPatch,
    // `.optional()` is absent-or-value, never `null`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::protocol::optional_no_null"
    )]
    pub traceparent: Option<String>,
}
