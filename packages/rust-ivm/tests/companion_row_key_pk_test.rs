//! Regression: the hydrate-time scalar-EXISTS *companion* row must emit with
//! its table's REAL primary key (e.g. channel_participants `id`), NEVER an
//! empty `{}` rowKey (which crashes the client at `toPrimaryKeyString`, the
//! prod "Got undefined").
//!
//! Root cause of the latent gap: the hydrate-time companion emission
//! (engine::add_queries_streaming, "Companion rows" block) keyed the row by the
//! top-level `primary_keys` map ONLY. The main-hydrate Streamer path, by
//! contrast, falls back to the source pipeline's OWN `schema.primary_key`
//! (streamer/mod.rs) — so it is always well-formed. The companion path had no
//! such fallback: if a companion table were ever absent from the map it would
//! have emitted `rowKey:"{}"`.
//!
//! In normal wiring this asymmetry is UNREACHABLE: a companion is only produced
//! for a table that passed `is_simple_subquery`, which requires the table's
//! unique keys, which the driver/napi register together with the table's source
//! and PK. But the two maps (`primary_keys` vs `unique_keys`) are written in
//! separate places, so the invariant was implicit, not enforced at the emission
//! site. The fix captures the companion table's PK from its own pipeline schema
//! at resolve time and uses it as the always-available fallback — faithful to
//! TS, which keys the EXISTS companion row by the subquery table's own PK.
//!
//! This test forces the asymmetry (source registered, but the table dropped
//! from the top-level `primary_keys` map via a test-only hook) and asserts the
//! emitted companion rowKey CONTAINS the real `id`. BEFORE the fix this panicked
//! (empty-PK assertion) / emitted `{}`; AFTER the fix it emits `{"id":"cp0"}`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rusqlite::Connection;

use rust_ivm::builder::ast::{
    Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery, SimpleCondition, ValuePosition,
};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::snapshotter::Snapshotter;
use rust_ivm::sqlite::table_source::TableSource;
use rust_ivm::streamer::RowChange;

fn create_replica(path: &str) {
    for p in [path, &format!("{}-wal", path), &format!("{}-shm", path)] {
        let _ = std::fs::remove_file(p);
    }
    let conn = Connection::open(path).unwrap();
    let _: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    conn.execute_batch(
        "CREATE TABLE \"_zero.replicationState\" (stateVersion TEXT PRIMARY KEY);
         INSERT INTO \"_zero.replicationState\" (stateVersion) VALUES ('v1');
         CREATE TABLE channels (id TEXT PRIMARY KEY, visibility TEXT);
         CREATE TABLE conversations (conversationId TEXT PRIMARY KEY, channelId TEXT, createdAt INTEGER);
         CREATE TABLE channel_participants (id TEXT PRIMARY KEY, channelId TEXT, userId TEXT, role TEXT);
         CREATE UNIQUE INDEX cp_channel_user ON channel_participants(channelId, userId);
         INSERT INTO channels VALUES ('ch1','PRIVATE');
         INSERT INTO conversations VALUES ('conv0','ch1',1);
         INSERT INTO channel_participants VALUES ('cp0','ch1','me','ADMIN');",
    )
    .unwrap();
    drop(conn);
}

fn s(v: &str) -> rust_ivm::ivm::data::Value {
    rust_ivm::ivm::data::Value::Str(v.into())
}
fn simple(col: &str, op: &str, val: rust_ivm::ivm::data::Value) -> Condition {
    Condition::Simple(SimpleCondition {
        op: op.to_string(),
        left: ValuePosition::Column {
            name: col.to_string(),
        },
        right: ValuePosition::Literal { value: val },
    })
}

/// The conversation ACL's nested scalar-exists AST (channel_participants keyed
/// on the (channelId,userId) unique index).
fn nested_scalar_exists_ast() -> Ast {
    let participants_scalar = CorrelatedSubqueryCondition {
        related: RelatedSubquery {
            subquery: Box::new(Ast {
                table: "channel_participants".to_string(),
                alias: Some("zsubq_participants".to_string()),
                where_clause: Some(Condition::And(vec![
                    simple("userId", "=", s("me")),
                    simple("channelId", "=", s("ch1")),
                ])),
                ..Default::default()
            }),
            relationship_name: "zsubq_participants".to_string(),
            parent_key: vec!["id".to_string()],
            child_key: vec!["channelId".to_string()],
            hidden: false,
            system: None,
        },
        op: "EXISTS".to_string(),
        flip: Some(false),
        scalar: true,
        plan_id: None,
    };

    let channel_exists = CorrelatedSubqueryCondition {
        related: RelatedSubquery {
            subquery: Box::new(Ast {
                table: "channels".to_string(),
                alias: Some("zsubq_channel".to_string()),
                where_clause: Some(Condition::CorrelatedSubquery(participants_scalar)),
                ..Default::default()
            }),
            relationship_name: "zsubq_channel".to_string(),
            parent_key: vec!["channelId".to_string()],
            child_key: vec!["id".to_string()],
            hidden: false,
            system: None,
        },
        op: "EXISTS".to_string(),
        flip: Some(false),
        scalar: false,
        plan_id: None,
    };

    Ast {
        table: "conversations".to_string(),
        where_clause: Some(Condition::CorrelatedSubquery(channel_exists)),
        ..Default::default()
    }
}

fn build_engine(path: &str) -> Engine {
    let pks: HashMap<String, Vec<String>> = [
        ("channels".to_string(), vec!["id".to_string()]),
        (
            "conversations".to_string(),
            vec!["conversationId".to_string()],
        ),
        ("channel_participants".to_string(), vec!["id".to_string()]),
    ]
    .into_iter()
    .collect();

    let mut snap = Snapshotter::with_read_pool(path, "companion", None, 0, None);
    snap.init().unwrap();
    let curr = snap.current_conn().unwrap();

    let col = |names: &[&str]| -> HashMap<String, ColumnType> {
        names
            .iter()
            .map(|n| (n.to_string(), ColumnType::String { optional: false }))
            .collect()
    };

    let ch = TableSource::new(
        curr.clone(),
        "channels",
        col(&["id", "visibility"]),
        vec!["id".to_string()],
    );
    let cv = TableSource::new(
        curr.clone(),
        "conversations",
        col(&["conversationId", "channelId", "createdAt"]),
        vec!["conversationId".to_string()],
    );
    let cp = TableSource::new(
        curr.clone(),
        "channel_participants",
        col(&["id", "channelId", "userId", "role"]),
        vec!["id".to_string()],
    );

    let mut eng = Engine::new(pks);
    eng.register_source(Rc::new(RefCell::new(ch)));
    eng.register_source(Rc::new(RefCell::new(cv)));
    eng.register_source(Rc::new(RefCell::new(cp)));
    eng.set_unique_keys("channels", vec![vec!["id".to_string()]]);
    eng.set_unique_keys("conversations", vec![vec!["conversationId".to_string()]]);
    eng.set_unique_keys(
        "channel_participants",
        vec![
            vec!["id".to_string()],
            vec!["channelId".to_string(), "userId".to_string()],
        ],
    );
    eng
}

/// Collect the emitted companion (channel_participants) row keys' `id` values.
fn hydrate_companion_ids(eng: &mut Engine) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    eng.add_queries_streaming(
        &[QuerySpec {
            query_id: "q".into(),
            ast: nested_scalar_exists_ast(),
        }],
        |rc: &RowChange| {
            if rc.table == "channel_participants" {
                match rc.row_key.get("id") {
                    Some(rust_ivm::ivm::data::Value::Str(v)) => ids.push(v.to_string()),
                    // Empty/absent PK in the row key is exactly the bug: an
                    // empty `{}` rowKey (client "Got undefined"). Record a
                    // sentinel so the assertion fails loudly if it ever happens.
                    _ => ids.push(format!("<BAD:{:?}>", rc.row_key)),
                }
            }
        },
    );
    ids
}

/// Baseline: normal wiring (map HAS channel_participants) — the companion row
/// key is well-formed and carries the real `id`.
#[test]
fn companion_row_key_carries_real_pk_normal_wiring() {
    let path = "/tmp/rust-ivm-companion-pk-normal.db";
    create_replica(path);
    let mut eng = build_engine(path);
    let ids = hydrate_companion_ids(&mut eng);
    eprintln!("[normal] companion ids = {ids:?}");
    assert!(
        ids.contains(&"cp0".to_string()),
        "companion channel_participants row must emit its real id 'cp0', got {ids:?}"
    );
    assert!(
        !ids.iter().any(|i| i.starts_with("<BAD:")),
        "no companion row key may be empty/malformed, got {ids:?}"
    );
}

/// Fault-injection: force the (normally-impossible) asymmetry where the source
/// is registered but the table is absent from the top-level `primary_keys` map.
/// BEFORE the fix the emission path had no schema fallback → it panicked on the
/// empty-PK assertion / would have emitted `rowKey:"{}"`. AFTER the fix it falls
/// back to the companion pipeline schema's own PK and emits `{"id":"cp0"}`,
/// exactly like TS.
#[test]
fn companion_row_key_uses_schema_pk_when_map_missing() {
    let path = "/tmp/rust-ivm-companion-pk-missing.db";
    create_replica(path);
    let mut eng = build_engine(path);
    // Simulate the asymmetry: drop only the map entry; source + unique keys stay.
    eng.__test_drop_primary_key("channel_participants");

    let ids = hydrate_companion_ids(&mut eng);
    eprintln!("[map-missing] companion ids = {ids:?}");
    assert_eq!(
        ids,
        vec!["cp0".to_string()],
        "with the map entry dropped, the companion row must STILL emit its real \
         id via the pipeline schema's primary key (faithful to TS); an empty or \
         malformed key here is the prod 'Got undefined' crash. got {ids:?}"
    );
}
