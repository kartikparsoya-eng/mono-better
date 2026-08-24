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
    format!("{}_{}_cvr", shard.app_id, shard.shard_num)
}
