//! Port of the shard identity + schema-name helpers (TS `zero-cache/src/types/shards.ts`).

use serde::{Deserialize, Serialize};

/// ShardID — {appID, shardNum}
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardID {
    pub app_id: String,
    pub shard_num: u32,
}

/// Port of TS `ALLOWED_APP_ID_CHARACTERS = /^[a-z0-9_]+$/` (shards.ts:45),
/// as the regex source (documentation) + `allowed_app_id_characters` (the
/// `.test(id)`).
pub const ALLOWED_APP_ID_CHARACTERS: &str = "^[a-z0-9_]+$";

/// Port of TS `INVALID_APP_ID_MESSAGE` (shards.ts:47-48).
pub const INVALID_APP_ID_MESSAGE: &str =
    "The App ID may only consist of lower-case letters, numbers, and the underscore character";

/// `ALLOWED_APP_ID_CHARACTERS.test(appID)`.
pub fn allowed_app_id_characters(app_id: &str) -> bool {
    !app_id.is_empty()
        && app_id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// Port of TS `check(shard)` (shards.ts:50-57): `throw new Error(
/// INVALID_APP_ID_MESSAGE)` → `Err`. (`shardNum` is a `u32` by type.)
pub fn check(shard: &ShardID) -> Result<(), String> {
    if !allowed_app_id_characters(&shard.app_id) {
        return Err(INVALID_APP_ID_MESSAGE.to_string());
    }
    Ok(())
}

/// Port of TS `appSchema({appID})` (shards.ts:59-62): the app's own PG schema
/// name is the appID itself.
pub fn app_schema(shard: &ShardID) -> String {
    shard.app_id.clone()
}

pub fn upstream_schema(shard: &ShardID) -> String {
    format!("{}_{}", shard.app_id, shard.shard_num)
}

pub fn cvr_schema(shard: &ShardID) -> String {
    // TS `cvrSchema` (shards.ts) is `${appID}_${shardNum}/cvr` — a SLASH, not an
    // underscore. The real Rust path (rust-syncer main.rs, seq_replay.rs) already
    // builds `{app}_{shard}/cvr`; this helper had drifted to `_cvr`, which would
    // point at the wrong PG schema if ever wired in. Pinned by parity_check.rs.
    format!("{}_{}/cvr", shard.app_id, shard.shard_num)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TS `check` throws `INVALID_APP_ID_MESSAGE` for anything outside
    /// `/^[a-z0-9_]+$/` (shards.ts:45-54); rust accepted any string.
    #[test]
    fn check_rejects_app_ids_outside_the_ts_charset() {
        let ok = |id: &str| {
            check(&ShardID {
                app_id: id.to_string(),
                shard_num: 0,
            })
        };
        assert_eq!(ok("zero"), Ok(()));
        assert_eq!(ok("my_app_2"), Ok(()));
        for bad in ["My-App", "Zero", "app.id", "", "app id", "zé"] {
            assert_eq!(ok(bad), Err(INVALID_APP_ID_MESSAGE.to_string()), "{bad:?}");
        }
        assert_eq!(ALLOWED_APP_ID_CHARACTERS, "^[a-z0-9_]+$");
    }
}
