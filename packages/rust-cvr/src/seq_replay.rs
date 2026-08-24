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
use crate::cvr::{CVRConfigDrivenUpdater, DesiredQuerySpec};
use crate::cvr_store::CVRStoreHandle;
use crate::schema::types::{AST, version_string};
use crate::shards::ShardID;
use crate::ttl_clock::TTLClock;
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

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

#[derive(Deserialize)]
pub struct Txn {
    #[serde(rename = "lastActive")]
    pub last_active: i64,
    #[serde(rename = "ttlClock")]
    pub ttl_clock: TTLClock,
    pub ops: Vec<Op>,
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
            }
        }

        let (cvr_final, _stats) =
            updater.flush(prog.connect_time as i64, tx.last_active, tx.ttl_clock);
        let ops = updater.base.drain_store_ops();
        store.apply_store_ops(ops);
        let flushed = store
            .flush(&orig_version, &cvr_final, prog.connect_time)
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
    let id = pv.get("id").map(|x| x.to_string()).unwrap_or_default();
    let cid = pv.get("client_id").and_then(|x| x.as_str()).unwrap_or("");
    format!("{ty}:{op}:{id}:{cid}@{v}")
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
