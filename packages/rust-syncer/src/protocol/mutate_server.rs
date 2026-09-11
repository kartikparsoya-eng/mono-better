//! Port of `zero-protocol/src/mutate-server.ts` — the API server's reply to a
//! push, as zero-cache parses it: `fetchFromAPIServer(mutateResponseSchema,
//! 'push', …)` (mutagen/pusher.ts:522-533) in valita `passthrough` mode
//! (custom/fetch.ts:260), so unknown keys are kept and no object here carries
//! `deny_unknown_fields`.

use serde::Deserialize;

use super::error::PushFailedBody;
use super::mutation::MutationResponse;
use super::nullable_optional;
use super::push::PushError;

/// `legacyPushSuccessSchema` (mutate-server.ts:6-8).
#[derive(Debug, Deserialize)]
pub struct LegacyPushSuccess {
    pub mutations: Vec<MutationResponse>,
}

/// `legacyPushResponseSchema` (mutate-server.ts:10-14):
/// `legacyPushSuccess | pushError | pushFailedBody`, tried in order.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum LegacyPushResponse {
    Success(LegacyPushSuccess),
    Error(PushError),
    Failed(PushFailedBody),
}

/// `v.literal('MutateResponse')` (mutate-server.ts:17).
#[derive(Debug, Clone, Copy, Deserialize)]
pub enum MutateResponseKind {
    #[serde(rename = "MutateResponse")]
    MutateResponse,
}

/// `mutateSuccessSchema` (mutate-server.ts:16-20).
#[derive(Debug, Deserialize)]
pub struct MutateSuccess {
    pub kind: MutateResponseKind,
    /// `v.string().nullable().optional()`: absent, `null`, or a string; TS
    /// treats present-but-`null` as server-validated with no user
    /// (pusher.ts:552-559).
    #[serde(default, rename = "userID", deserialize_with = "nullable_optional")]
    pub user_id: Option<Option<String>>,
    pub mutations: Vec<MutationResponse>,
}

/// `mutateResponseSchema` (mutate-server.ts:23-28):
/// `mutateSuccess | pushFailedBody | legacyPushResponse` (the last for
/// backwards compatibility), tried in order like valita's union.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum MutateResponse {
    Success(MutateSuccess),
    Failed(PushFailedBody),
    Legacy(LegacyPushResponse),
}
