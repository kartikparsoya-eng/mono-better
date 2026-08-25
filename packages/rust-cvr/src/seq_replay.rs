//! Shared engine for the CVR *sequence* differential (parity tooling).
//!
//! Replays a language-neutral "program" (see `agentic/parity/seq/gen.mjs`) of
//! config-driven CVR transactions against the REAL Rust `CVRStore` +
//! `CVRConfigDrivenUpdater` over Postgres and produces a canonical trace, so the
//! output can be diffed against the TS driver (`run-ts.mjs`). Used by both the
//! `cvr_seq_replay` binary (dev-time `diff.mjs` / `fuzz.mjs`) and the CI gate
//! (`tests/seq_diff_pg_test.rs`), which asserts the Rust trace equals a frozen TS
//! golden. Kept in the lib so both callers share one implementation.

use crate::client_handler::PatchToVersion;
use crate::cvr::{
    CVRConfigDrivenUpdater, CVRQueryDrivenUpdater, DesiredQuerySpec, RefCounts, RowRecordMap,
    RowUpdate,
};
use crate::cvr_store::CVRStoreHandle;
use crate::row_key::row_id_string;
use crate::schema::types::{AST, RowID, RowRecord, version_from_string, version_string};
use crate::shards::ShardID;
use crate::ttl_clock::TTLClock;
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use std::sync::Arc;

/// The schema the captured TS DDL (`flush-schema.sql`) hardcodes. Programs must
/// use shard roze/1 to match.
pub const SCHEMA: &str = "roze_1/cvr";
pub const TASK_ID: &str = "seq-task";
const DDL: &str = include_str!("../agentic/parity/flush-schema.sql");

#[derive(Deserialize)]
pub struct Program {
    #[serde(rename = "cvrId")]
    pub cvr_id: String,
    pub shard: Shard,
    #[serde(rename = "connectTime")]
    pub connect_time: f64,
    pub transactions: Vec<Txn>,
}

#[derive(Deserialize)]
pub struct Shard {
    #[serde(rename = "appID")]
    pub app_id: String,
    #[serde(rename = "shardNum")]
    pub shard_num: u32,
}

/// A transaction is either config-driven (default; `ops`) or query-driven
/// (`kind: "query"`; trackQueries -> received -> deleteUnreferencedRows). The
/// config fields default so the existing config-only corpus parses unchanged.
#[derive(Deserialize)]
pub struct Txn {
    #[serde(default = "default_kind")]
    pub kind: String,
    #[serde(rename = "lastActive")]
    pub last_active: i64,
    #[serde(rename = "ttlClock")]
    pub ttl_clock: TTLClock,
    #[serde(default)]
    pub ops: Vec<Op>,
    // query-driven fields
    #[serde(default, rename = "stateVersion")]
    pub state_version: Option<String>,
    #[serde(default, rename = "replicaVersion")]
    pub replica_version: Option<String>,
    #[serde(default)]
    pub track: Option<Track>,
    #[serde(default)]
    pub received: Vec<ReceivedRow>,
    #[serde(default, rename = "deleteUnreferenced")]
    pub delete_unreferenced: bool,
}

fn default_kind() -> String {
    "config".to_string()
}

#[derive(Deserialize)]
pub struct Track {
    /// [id, transformationHash] pairs.
    pub executed: Vec<(String, String)>,
    #[serde(default)]
    pub removed: Vec<String>,
}

#[derive(Deserialize)]
pub struct ReceivedRow {
    pub id: RowIdJson,
    pub contents: Value,
    pub version: String,
    #[serde(rename = "refCounts")]
    pub ref_counts: Value,
}

#[derive(Deserialize)]
pub struct RowIdJson {
    pub schema: String,
    pub table: String,
    #[serde(rename = "rowKey")]
    pub row_key: serde_json::Map<String, Value>,
}

#[derive(Deserialize)]
#[serde(tag = "op")]
pub enum Op {
    #[serde(rename = "ensureClient")]
    EnsureClient {
        #[serde(rename = "clientID")]
        client_id: String,
    },
    #[serde(rename = "putDesiredQueries")]
    PutDesiredQueries {
        #[serde(rename = "clientID")]
        client_id: String,
        queries: Vec<QSpec>,
    },
    #[serde(rename = "markDesiredInactive")]
    MarkDesiredInactive {
        #[serde(rename = "clientID")]
        client_id: String,
        hashes: Vec<String>,
    },
    #[serde(rename = "deleteDesired")]
    DeleteDesired {
        #[serde(rename = "clientID")]
        client_id: String,
        hashes: Vec<String>,
    },
    #[serde(rename = "clearDesired")]
    ClearDesired {
        #[serde(rename = "clientID")]
        client_id: String,
    },
    #[serde(rename = "deleteClient")]
    DeleteClient {
        #[serde(rename = "clientID")]
        client_id: String,
    },
    #[serde(rename = "setClientSchema")]
    SetClientSchema { schema: Value },
    #[serde(rename = "setProfileID")]
    SetProfileID {
        #[serde(rename = "profileID")]
        profile_id: String,
    },
}

#[derive(Deserialize)]
pub struct QSpec {
    hash: String,
    #[serde(default)]
    ast: Option<AST>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    args: Option<Vec<Value>>,
    #[serde(default)]
    ttl: Option<i64>,
}

/// DROP + recreate the CVR schema from the exact captured TS DDL.
pub async fn reset_schema(pool: &PgPool) {
    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE"#))
        .execute(pool)
        .await
        .expect("drop schema");
    sqlx::raw_sql(DDL)
        .execute(pool)
        .await
        .expect("create schema");
}

/// Replay a program against a FRESH schema and return the canonical trace Value:
/// `{cvrId, transactions:[{patches:[str], flushed:bool, version:str, db:{...}}]}`.
pub async fn run(pool: &PgPool, prog: &Program) -> Value {
    let shard = ShardID {
        app_id: prog.shard.app_id.clone(),
        shard_num: prog.shard.shard_num,
    };
    assert_eq!(
        format!("{}_{}/cvr", shard.app_id, shard.shard_num),
        SCHEMA,
        "program shard must be roze/1 to match flush-schema.sql"
    );
    reset_schema(pool).await;

    let mut trace_txns: Vec<Value> = Vec::new();
    for tx in &prog.transactions {
        // Fresh store per transaction: clean load -> mutate -> flush against PG.
        let mut store = CVRStoreHandle::new(
            pool.clone(),
            SCHEMA.to_string(),
            prog.cvr_id.clone(),
            TASK_ID.to_string(),
        );
        let loaded = store.load(prog.connect_time).await.expect("load");
        let orig_version = loaded.cvr.version.clone();

        let (patches, cvr_final): (Vec<String>, crate::cvr::CVR) = if tx.kind == "query" {
            // ── query-driven: trackQueries -> received -> deleteUnreferencedRows ──
            // The existing rows are loaded from PG exactly as TS's `getRowRecords`
            // does (non-tombstone rows for this CG); `received` merges refCounts
            // against them and `deleteUnreferencedRows` GCs rows whose executed-
            // query refs go to zero.
            let state_version = tx
                .state_version
                .clone()
                .expect("query txn needs stateVersion");
            let replica_version = tx
                .replica_version
                .clone()
                .unwrap_or_else(|| "01".to_string());
            let existing = load_existing_rows(pool, &prog.cvr_id).await;
            let mut updater =
                CVRQueryDrivenUpdater::new(loaded.cvr, state_version, replica_version, None);

            let mut patches: Vec<String> = Vec::new();
            let track = tx.track.as_ref().expect("query txn needs track");
            let executed: Vec<(&str, &str)> = track
                .executed
                .iter()
                .map(|(a, b)| (a.as_str(), b.as_str()))
                .collect();
            let removed: Vec<&str> = track.removed.iter().map(|s| s.as_str()).collect();
            let (_v, tq_patches) = updater.track_queries(&executed, &removed);
            for p in tq_patches {
                patches.push(canon_patch(&p));
            }

            let mut rows: std::collections::HashMap<String, (RowID, RowUpdate)> =
                std::collections::HashMap::new();
            for r in &tx.received {
                let id = RowID {
                    schema: r.id.schema.clone(),
                    table: r.id.table.clone(),
                    row_key: r.id.row_key.clone(),
                };
                let id_str = row_id_string(&id);
                let ref_counts: RefCounts =
                    serde_json::from_value(r.ref_counts.clone()).expect("refCounts");
                rows.insert(
                    id_str,
                    (
                        id,
                        RowUpdate {
                            version: Some(r.version.clone()),
                            contents: Some(Arc::new(r.contents.clone())),
                            ref_counts,
                        },
                    ),
                );
            }
            push_patches(&mut patches, updater.received(&rows, &existing));
            if tx.delete_unreferenced {
                push_patches(
                    &mut patches,
                    updater.delete_unreferenced_rows(existing.values()),
                );
            }

            let (cvr_final, _stats) =
                updater.flush(prog.connect_time as i64, tx.last_active, tx.ttl_clock);
            let ops = updater.base.drain_store_ops();
            store.apply_store_ops(ops);
            (patches, cvr_final)
        } else {
            // ── config-driven ──
            let mut updater = CVRConfigDrivenUpdater::new(loaded.cvr, shard.clone());
            let mut patches: Vec<String> = Vec::new();
            for op in &tx.ops {
                match op {
                    Op::EnsureClient { client_id } => {
                        updater.ensure_client(client_id);
                    }
                    Op::PutDesiredQueries { client_id, queries } => {
                        let specs: Vec<DesiredQuerySpec> = queries
                            .iter()
                            .map(|q| DesiredQuerySpec {
                                hash: q.hash.clone(),
                                ast: q.ast.clone(),
                                name: q.name.clone(),
                                args: q.args.clone(),
                                ttl: q.ttl,
                            })
                            .collect();
                        push_patches(&mut patches, updater.put_desired_queries(client_id, &specs));
                    }
                    Op::MarkDesiredInactive { client_id, hashes } => push_patches(
                        &mut patches,
                        updater.mark_desired_queries_as_inactive(client_id, hashes, tx.ttl_clock),
                    ),
                    Op::DeleteDesired { client_id, hashes } => push_patches(
                        &mut patches,
                        updater.delete_desired_queries(client_id, hashes),
                    ),
                    Op::ClearDesired { client_id } => {
                        push_patches(&mut patches, updater.clear_desired_queries(client_id))
                    }
                    Op::DeleteClient { client_id } => {
                        push_patches(&mut patches, updater.delete_client(client_id, tx.ttl_clock))
                    }
                    // No patches — these queue a PutInstance (surfaces in the
                    // instances dump's clientSchema / profileID columns).
                    Op::SetClientSchema { schema } => {
                        updater
                            .set_client_schema(schema.clone())
                            .expect("set_client_schema");
                    }
                    Op::SetProfileID { profile_id } => {
                        updater.set_profile_id(profile_id);
                    }
                }
            }
            let (cvr_final, _stats) =
                updater.flush(prog.connect_time as i64, tx.last_active, tx.ttl_clock);
            let ops = updater.base.drain_store_ops();
            store.apply_store_ops(ops);
            (patches, cvr_final)
        };

        // Snapshot for the flush's no-op pruning — the same non-tombstone
        // row-record set TS's `getRowRecords` cache holds at flush time (a
        // previously-flushed row must be visible so a tombstone transition is
        // NOT pruned as "absent").
        let existing_at_flush = load_existing_rows(pool, &prog.cvr_id).await;
        let flushed = store
            .flush(
                &orig_version,
                &cvr_final,
                prog.connect_time,
                &existing_at_flush,
            )
            .await
            .expect("flush");

        // No-op flush => TS returns the ORIGINAL cvr (version unchanged).
        let (version, flushed_bool) = match flushed {
            Some(_) => (version_string(&cvr_final.version), true),
            None => (version_string(&orig_version), false),
        };

        trace_txns.push(json!({
            "patches": patches,
            "flushed": flushed_bool,
            "version": version,
            "db": dump(pool).await,
        }));
    }

    json!({ "cvrId": prog.cvr_id, "transactions": trace_txns })
}

fn push_patches(acc: &mut Vec<String>, patches: Vec<PatchToVersion>) {
    for p in patches {
        acc.push(canon_patch(&p));
    }
}

/// Canonical, language-neutral rendering of a returned patch — kind:op:id:clientID
/// @version. `PatchToVersion` / its inner `Patch` are INTERNAL types (serialized
/// snake_case here; the wire DTO is `QueryPatchEntry`), so we compare the
/// client-facing MEANING, not the internal field naming. Matches run-ts.mjs.
fn canon_patch(p: &PatchToVersion) -> String {
    let v = version_string(&p.to_version);
    let pv = serde_json::to_value(&p.patch).unwrap();
    let ty = pv.get("type").and_then(|x| x.as_str()).unwrap_or("?");
    let op = pv.get("op").and_then(|x| x.as_str()).unwrap_or("?");
    // `id` is a string for query patches and a RowID object for row patches;
    // canonicalize (sort keys) before stringify so object key order can't diverge.
    let id = pv
        .get("id")
        .map(|x| canonicalize(x).to_string())
        .unwrap_or_default();
    let cid = pv.get("client_id").and_then(|x| x.as_str()).unwrap_or("");
    format!("{ty}:{op}:{id}:{cid}@{v}")
}

/// Load the existing (non-tombstone) row records for this CG from PG — the same
/// set TS's `getRowRecords` returns (row-record-cache.ts `load`:
/// `refCounts IS NOT NULL`). The query-driven updater merges received refCounts
/// against these and GCs unreferenced rows.
async fn load_existing_rows(pool: &PgPool, cvr_id: &str) -> RowRecordMap {
    let rows = sqlx::query(&format!(
        r#"SELECT "schema","table","rowKey","rowVersion","patchVersion","refCounts"
           FROM "{SCHEMA}".rows
           WHERE "clientGroupID" = $1 AND "refCounts" IS NOT NULL"#
    ))
    .bind(cvr_id)
    .fetch_all(pool)
    .await
    .expect("load existing rows");

    let mut map: RowRecordMap = std::collections::HashMap::new();
    for r in &rows {
        let row_key = r
            .get::<Value, _>("rowKey")
            .as_object()
            .expect("rowKey object")
            .clone();
        let id = RowID {
            schema: r.get::<String, _>("schema"),
            table: r.get::<String, _>("table"),
            row_key,
        };
        let ref_counts: RefCounts =
            serde_json::from_value(r.get::<Value, _>("refCounts")).expect("refCounts");
        let record = RowRecord {
            id: id.clone(),
            row_version: r.get::<String, _>("rowVersion"),
            patch_version: version_from_string(&r.get::<String, _>("patchVersion")),
            ref_counts: Some(ref_counts),
        };
        map.insert(row_id_string(&id), record);
    }
    map
}

/// Canonical DB dump — the shared oracle. Mirrors run-ts.mjs's SELECTs.
async fn dump(pool: &PgPool) -> Value {
    let instances = sqlx::query(&format!(
        r#"SELECT version, "replicaVersion", "ttlClock", "clientSchema", "profileID"
           FROM "{SCHEMA}".instances ORDER BY version"#
    ))
    .fetch_all(pool)
    .await
    .expect("dump instances")
    .iter()
    .map(|r| {
        json!({
            "version": r.get::<String, _>("version"),
            "replicaVersion": r.get::<Option<String>, _>("replicaVersion"),
            "ttlClock": r.get::<f64, _>("ttlClock"),
            "clientSchema": r.get::<Option<Value>, _>("clientSchema"),
            "profileID": r.get::<Option<String>, _>("profileID"),
        })
    })
    .collect::<Vec<_>>();

    let clients = sqlx::query(&format!(
        r#"SELECT "clientID" FROM "{SCHEMA}".clients ORDER BY "clientID""#
    ))
    .fetch_all(pool)
    .await
    .expect("dump clients")
    .iter()
    .map(|r| json!({ "clientID": r.get::<String, _>("clientID") }))
    .collect::<Vec<_>>();

    let queries = sqlx::query(&format!(
        r#"SELECT "queryHash","clientAST","queryName","queryArgs","patchVersion",
                  "transformationHash","transformationVersion","internal","deleted"
           FROM "{SCHEMA}".queries ORDER BY "queryHash""#
    ))
    .fetch_all(pool)
    .await
    .expect("dump queries")
    .iter()
    .map(|r| {
        json!({
            "queryHash": r.get::<String, _>("queryHash"),
            "clientAST": r.get::<Option<Value>, _>("clientAST"),
            "queryName": r.get::<Option<String>, _>("queryName"),
            "queryArgs": r.get::<Option<Value>, _>("queryArgs"),
            "patchVersion": r.get::<Option<String>, _>("patchVersion"),
            "transformationHash": r.get::<Option<String>, _>("transformationHash"),
            "transformationVersion": r.get::<Option<String>, _>("transformationVersion"),
            "internal": r.get::<Option<bool>, _>("internal"),
            "deleted": r.get::<Option<bool>, _>("deleted"),
        })
    })
    .collect::<Vec<_>>();

    let desires = sqlx::query(&format!(
        r#"SELECT "clientID","queryHash","patchVersion","deleted","ttlMs","inactivatedAtMs"
           FROM "{SCHEMA}".desires ORDER BY "clientID","queryHash""#
    ))
    .fetch_all(pool)
    .await
    .expect("dump desires")
    .iter()
    .map(|r| {
        json!({
            "clientID": r.get::<String, _>("clientID"),
            "queryHash": r.get::<String, _>("queryHash"),
            "patchVersion": r.get::<String, _>("patchVersion"),
            "deleted": r.get::<Option<bool>, _>("deleted"),
            "ttlMs": r.get::<Option<f64>, _>("ttlMs"),
            "inactivatedAtMs": r.get::<Option<f64>, _>("inactivatedAtMs"),
        })
    })
    .collect::<Vec<_>>();

    let rows = sqlx::query(&format!(
        r#"SELECT "schema","table","rowKey","rowVersion","patchVersion","refCounts"
           FROM "{SCHEMA}".rows ORDER BY "table","rowKey"::text"#
    ))
    .fetch_all(pool)
    .await
    .expect("dump rows")
    .iter()
    .map(|r| {
        json!({
            "schema": r.get::<String, _>("schema"),
            "table": r.get::<String, _>("table"),
            "rowKey": r.get::<Value, _>("rowKey"),
            "rowVersion": r.get::<String, _>("rowVersion"),
            "patchVersion": r.get::<String, _>("patchVersion"),
            "refCounts": r.get::<Option<Value>, _>("refCounts"),
        })
    })
    .collect::<Vec<_>>();

    json!({
        "instances": instances,
        "clients": clients,
        "queries": queries,
        "desires": desires,
        "rows": rows,
    })
}

/// Canonicalize a trace so TS-vs-Rust comparison ignores non-semantic differences:
/// object key order, integer-valued float representation (`-1.0` vs `-1`), and
/// array order (DB dumps are row *sets*; patch order is a TS `intersection`
/// optimization artifact, not a contract). Matches diff.mjs's `canon`.
pub fn canonicalize(v: &Value) -> Value {
    match v {
        Value::Array(a) => {
            let mut items: Vec<Value> = a.iter().map(canonicalize).collect();
            items.sort_by_key(|x| x.to_string());
            Value::Array(items)
        }
        Value::Object(o) => {
            let mut keys: Vec<&String> = o.keys().collect();
            keys.sort();
            let mut m = serde_json::Map::new();
            for k in keys {
                m.insert(k.clone(), canonicalize(&o[k]));
            }
            Value::Object(m)
        }
        Value::Number(n) => {
            // Coerce integer-valued floats to integers (Rust f64 columns emit
            // `X.0`; TS emits `X`).
            if let Some(f) = n.as_f64()
                && f.fract() == 0.0
                && f.abs() < 9.007e15
            {
                return json!(f as i64);
            }
            v.clone()
        }
        _ => v.clone(),
    }
}
