//! Port of `zero-protocol/src/mutation.ts` — the response half zero-cache
//! parses back from the API server (`mutationResponseSchema`, through
//! `mutateResponseSchema`), in valita `passthrough` mode (custom/fetch.ts:260).
//!
//! The `mutationResultSchema` members are defined in rust-cvr's
//! `client_handler.rs`: that crate parses them too (`mutationRowSchema`) and
//! cannot depend on this one, so the single definition lives there and is
//! re-exported here under the TS names.

use serde::Deserialize;

pub use rust_cvr::client_handler::{
    AppError, AppErrorLiteral, MutationError, MutationOk, MutationResult, ZeroError, ZeroErrorKind,
};

use super::mutation_id::passthrough::MutationID;

/// `mutationResponseSchema` (mutation.ts:144-147).
#[derive(Debug, Deserialize)]
pub struct MutationResponse {
    pub id: MutationID,
    pub result: MutationResult,
}
