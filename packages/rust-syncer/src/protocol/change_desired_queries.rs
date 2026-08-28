//! Port of `packages/zero-protocol/src/change-desired-queries.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use super::*;
use serde::{Deserialize, Serialize};

// changeDesiredQueriesBodySchema uses desiredQueriesPatch
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeDesiredQueriesBody {
    pub desired_queries_patch: UpQueriesPatch,
    pub traceparent: Option<String>,
}
