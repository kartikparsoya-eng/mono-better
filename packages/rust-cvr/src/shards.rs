//! Port of the shard identity + schema-name helpers (TS `zero-cache/src/types/shards.ts`).

use serde::{Deserialize, Serialize};

/// ShardID — {appID, shardNum}
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardID {
    pub app_id: String,
    pub shard_num: u32,
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
