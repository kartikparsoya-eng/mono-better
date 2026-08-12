//! Port of the CVR type system from `schema/types.ts` and `cvr.ts`.
//!
//! All types use `serde_json::Value` for JSON-shaped fields (AST, rowKey, contents, args)
//! since the Rust port does not interpret these — it only stores, passes, and compares them.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::version::CVRVersion;

/// TTLClock is an opaque number in TS. In Rust we use i64 directly.
pub type TTLClock = i64;

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

/// RowID — re-exported from row_key module (schema, table, rowKey: Map<String, Value>)
pub use crate::row_key::RowID;

/// RefCounts: query hash → count. Using BTreeMap for deterministic ordering.
pub type RefCounts = BTreeMap<String, i64>;

/// RowRecord — a row's CVR metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowRecord {
    pub id: RowID,
    pub row_version: String,
    pub patch_version: CVRVersion,
    /// None = tombstone (row removed from view)
    pub ref_counts: Option<RefCounts>,
}

/// RowUpdate — what the replicator sends for a row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contents: Option<Value>,
    pub ref_counts: RefCounts,
}

/// ClientRecord — per-client metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientRecord {
    pub id: String,
    pub desired_query_ids: Vec<String>,
}

/// ClientState — per-client, per-query state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientState {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inactivated_at: Option<TTLClock>,
    pub ttl: i64,
    pub version: CVRVersion,
}

/// AST is stored as opaque JSON.
pub type AST = Value;

/// ClientSchema is stored as opaque JSON.
pub type ClientSchema = Value;

/// QueryRecord — discriminated union (internal/client/custom).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum QueryRecord {
    #[serde(rename = "internal")]
    Internal(InternalQueryRecord),
    #[serde(rename = "client")]
    Client(ClientQueryRecord),
    #[serde(rename = "custom")]
    Custom(CustomQueryRecord),
}

/// Fields common to all query variants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaseQueryRecord {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transformation_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transformation_version: Option<CVRVersion>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_set_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InternalQueryRecord {
    #[serde(flatten)]
    pub base: BaseQueryRecord,
    pub ast: AST,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientQueryRecord {
    #[serde(flatten)]
    pub base: BaseQueryRecord,
    pub ast: AST,
    pub client_state: BTreeMap<String, ClientState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch_version: Option<CVRVersion>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CustomQueryRecord {
    #[serde(flatten)]
    pub base: BaseQueryRecord,
    pub name: String,
    pub args: Vec<Value>,
    pub client_state: BTreeMap<String, ClientState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch_version: Option<CVRVersion>,
}

// Convenience accessors on QueryRecord
impl QueryRecord {
    pub fn id(&self) -> &str {
        match self {
            QueryRecord::Internal(r) => &r.base.id,
            QueryRecord::Client(r) => &r.base.id,
            QueryRecord::Custom(r) => &r.base.id,
        }
    }

    pub fn base(&self) -> &BaseQueryRecord {
        match self {
            QueryRecord::Internal(r) => &r.base,
            QueryRecord::Client(r) => &r.base,
            QueryRecord::Custom(r) => &r.base,
        }
    }

    pub fn base_mut(&mut self) -> &mut BaseQueryRecord {
        match self {
            QueryRecord::Internal(r) => &mut r.base,
            QueryRecord::Client(r) => &mut r.base,
            QueryRecord::Custom(r) => &mut r.base,
        }
    }

    pub fn is_internal(&self) -> bool {
        matches!(self, QueryRecord::Internal(_))
    }

    pub fn client_state(&self) -> Option<&BTreeMap<String, ClientState>> {
        match self {
            QueryRecord::Internal(_) => None,
            QueryRecord::Client(r) => Some(&r.client_state),
            QueryRecord::Custom(r) => Some(&r.client_state),
        }
    }

    pub fn client_state_mut(&mut self) -> Option<&mut BTreeMap<String, ClientState>> {
        match self {
            QueryRecord::Internal(_) => None,
            QueryRecord::Client(r) => Some(&mut r.client_state),
            QueryRecord::Custom(r) => Some(&mut r.client_state),
        }
    }

    pub fn patch_version(&self) -> Option<&CVRVersion> {
        match self {
            QueryRecord::Internal(_) => None,
            QueryRecord::Client(r) => r.patch_version.as_ref(),
            QueryRecord::Custom(r) => r.patch_version.as_ref(),
        }
    }

    pub fn patch_version_mut(&mut self) -> &mut Option<CVRVersion> {
        match self {
            QueryRecord::Internal(_) => panic!("internal queries have no patch_version"),
            QueryRecord::Client(r) => &mut r.patch_version,
            QueryRecord::Custom(r) => &mut r.patch_version,
        }
    }
}

/// The mutable CVR type (matches TS `CVR`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CVR {
    pub id: String,
    pub version: CVRVersion,
    pub last_active: i64,
    pub ttl_clock: TTLClock,
    pub replica_version: Option<String>,
    pub clients: BTreeMap<String, ClientRecord>,
    pub queries: BTreeMap<String, QueryRecord>,
    pub client_schema: Option<ClientSchema>,
    pub profile_id: Option<String>,
}

/// Patches — sent to clients to update their view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Patch {
    #[serde(rename = "row")]
    Row(RowPatch),
    #[serde(rename = "query")]
    Query(QueryPatch),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum RowPatch {
    #[serde(rename = "put")]
    Put { id: RowID, contents: Value },
    #[serde(rename = "del")]
    Del { id: RowID },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum QueryPatch {
    #[serde(rename = "put")]
    Put {
        id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        client_id: Option<String>,
    },
    #[serde(rename = "del")]
    Del {
        id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        client_id: Option<String>,
    },
}

/// Patch tagged with the version it applies to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PatchToVersion {
    pub patch: Patch,
    pub to_version: CVRVersion,
}

/// RowPatchInfo — internal tracking for dedup.
#[derive(Debug, Clone, PartialEq)]
pub struct RowPatchInfo {
    /// None for a row-del
    pub row_version: Option<String>,
    pub to_version: CVRVersion,
}

/// Desired query spec — what the client wants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DesiredQuerySpec {
    pub hash: String,
    pub ast: Option<AST>,
    pub name: Option<String>,
    pub args: Option<Vec<Value>>,
    pub ttl: Option<i64>, // milliseconds; None = DEFAULT_TTL_MS
}

/// CVR flush stats returned by flush().
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct CVRFlushStats {
    pub rows: usize,
    pub queries: usize,
    pub clients: usize,
    pub desires: usize,
    pub query_partials: usize,
}

/// Store operations collected by the updater for TS to replay.
/// Mirrors the CVRStore method calls that the TS updaters make inline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StoreOp {
    InsertClient(ClientRecord),
    PutQuery(QueryRecord),
    PutDesiredQuery {
        version: CVRVersion,
        query_id: String,
        client_id: String,
        deleted: bool,
        inactivated_at: Option<TTLClock>,
        ttl: i64,
    },
    PutInstance(CVR),
    DeleteClient(String),
    UpdateQuery(QueryRecord),
    MarkQueryAsDeleted {
        version: CVRVersion,
        patch: QueryPatch,
    },
    PutRowRecord(RowRecord),
    DelRowRecord(RowID),
    UpdateRowSetSignature {
        query_id: String,
        hex: String,
    },
}

pub const CLIENT_LMID_QUERY_ID: &str = "lmids";
pub const CLIENT_MUTATION_RESULTS_QUERY_ID: &str = "mutationResults";
