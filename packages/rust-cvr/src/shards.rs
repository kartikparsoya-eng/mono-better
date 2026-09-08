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

/// TS `check(shard)` THROWS (shards.ts:50-57); every schema-name helper below
/// calls it first. A throw from a synchronous helper is a panic here — the
/// message is TS's `INVALID_APP_ID_MESSAGE` verbatim. Unreachable in
/// production: `zero_config.rs` `validated_app_id` rejects the appID at config
/// load (zero-config.ts:35-38), exactly as TS's `parseOptions` does.
fn must_check(shard: &ShardID) {
    if let Err(message) = check(shard) {
        panic!("{message}");
    }
}

/// Port of TS `appSchema({appID})` (shards.ts:59-62): `check({appID, shardNum:
/// 0})`, then the app's own PG schema name is the appID itself.
pub fn app_schema(shard: &ShardID) -> String {
    must_check(&ShardID {
        app_id: shard.app_id.clone(),
        shard_num: 0,
    });
    shard.app_id.clone()
}

/// Port of TS `upstreamSchema(shard)` (shards.ts:64-67).
pub fn upstream_schema(shard: &ShardID) -> String {
    must_check(shard);
    format!("{}_{}", shard.app_id, shard.shard_num)
}

/// Port of TS `cvrSchema(shard)` (shards.ts:74-77).
pub fn cvr_schema(shard: &ShardID) -> String {
    must_check(shard);
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

    /// TS `appSchema` / `upstreamSchema` / `cvrSchema` each call `check(shard)`
    /// FIRST and throw `INVALID_APP_ID_MESSAGE` (shards.ts:59-77); rust ported
    /// `check` (59cef76c2) but the three helpers skipped it. Non-vacuous: drop
    /// `must_check` from `cvr_schema` and this returns `Bad-App_0/cvr`.
    #[test]
    #[should_panic(
        expected = "The App ID may only consist of lower-case letters, numbers, and the underscore character"
    )]
    fn schema_helpers_check_the_app_id_like_ts() {
        let bad = ShardID {
            app_id: "Bad-App".to_string(),
            shard_num: 0,
        };
        let _ = cvr_schema(&bad);
    }

    #[test]
    fn schema_helpers_render_a_valid_shard() {
        let shard = ShardID {
            app_id: "zero".to_string(),
            shard_num: 3,
        };
        assert_eq!(app_schema(&shard), "zero");
        assert_eq!(upstream_schema(&shard), "zero_3");
        assert_eq!(cvr_schema(&shard), "zero_3/cvr");
    }
}
