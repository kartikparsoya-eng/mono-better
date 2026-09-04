//! Port of `packages/zero-protocol/src/pull.ts` — the upstream pull request
//! body. Rust previously kept the `pull` body as a raw `Value` and validated
//! NOTHING, so every wrong type, missing field and null field sailed through
//! where TS rejected it against `pullRequestBodySchema` (M13 R2).

use serde::{Deserialize, Serialize};

use super::version::NullableVersion;

/// Port of TS `pullRequestBodySchema` (pull.ts:5).
///
/// `cookie` is `nullableVersionSchema` = `v.union(v.string(), v.null())`
/// (version.ts:4): REQUIRED to be present, but permitted to be null. It cannot
/// be a plain `Option<String>` — serde's derive treats a MISSING key as `None`
/// for any `Option` field, which would accept a body with no `cookie` at all
/// where TS rejects it. [`NullableVersion`] is a newtype precisely so the key
/// stays mandatory.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullRequestBody {
    #[serde(rename = "clientGroupID")]
    pub client_group_id: String,
    pub cookie: NullableVersion,
    #[serde(rename = "requestID")]
    pub request_id: String,
}
