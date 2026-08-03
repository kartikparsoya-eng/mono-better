//! Root-cause proof for the ART G8 gap: the conversation ACL injects a nested
//! *scalar* EXISTS on channel_participants keyed on the (channelId, userId)
//! UNIQUE index:
//!
//!   conversations
//!     .whereExists('channel', ch =>
//!        ch.whereExists('participants',
//!           p => p.where('userId', me).where('channelId', X), { scalar: true }))
//!
//! A scalar subquery is pre-resolved to a literal ONLY when `is_simple_subquery`
//! finds a unique index all of whose columns are equality-constrained. If the
//! engine only knows the PK ([id]) — which is what the driver currently passes,
//! omitting secondary unique keys — resolution FAILS, the scalar degrades to a
//! live per-parent Exists, and the matched participant (cp0 = me) streams only
//! as a HIDDEN companion (client discards it) → G8 "only_mirror".
//!
//! With the (channelId, userId) unique key known, the scalar resolves once and
//! the matched row is emitted VISIBLE (is_hidden=false) → present on the client,
//! matching TS.

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
         INSERT INTO conversations VALUES ('conv0','ch1',1),('conv1','ch1',2),('conv2','ch1',3),('conv3','ch1',4);
         INSERT INTO channel_participants VALUES ('cp0','ch1','me','ADMIN');
         INSERT INTO channel_participants VALUES ('cp1','ch1','u1','MEMBER');
         INSERT INTO channel_participants VALUES ('cp2','ch1','u2','MEMBER');
         INSERT INTO channel_participants VALUES ('cp3','ch1','u3','MEMBER');",
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

/// The conversation ACL's nested scalar-exists AST.
fn nested_scalar_exists_ast() -> Ast {
    // innermost: channel_participants WHERE userId=me AND channelId=ch1
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
            parent_key: vec!["id".to_string()], // channels.id
            child_key: vec!["channelId".to_string()], // channel_participants.channelId
            hidden: false,
            system: None,
        },
        op: "EXISTS".to_string(),
        flip: Some(false),
        scalar: true, // <- the scalar flag from the ACL
        plan_id: None,
    };

    // middle: channels WHERE EXISTS_scalar(participants)
    let channel_exists = CorrelatedSubqueryCondition {
        related: RelatedSubquery {
            subquery: Box::new(Ast {
                table: "channels".to_string(),
                alias: Some("zsubq_channel".to_string()),
                where_clause: Some(Condition::CorrelatedSubquery(participants_scalar)),
                ..Default::default()
            }),
            relationship_name: "zsubq_channel".to_string(),
            parent_key: vec!["channelId".to_string()], // conversations.channelId
            child_key: vec!["id".to_string()],         // channels.id
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

/// Hydrate; return (visible cp ids, hidden cp emission count).
fn hydrate(path: &str, unique_keys_for_cp: Vec<Vec<String>>) -> (Vec<String>, usize) {
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

    let mut snap = Snapshotter::new(path, "repro", None);
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
    // channels/conversations: PK-only unique keys (enough for their own resolution).
    eng.set_unique_keys("channels", vec![vec!["id".to_string()]]);
    eng.set_unique_keys("conversations", vec![vec!["conversationId".to_string()]]);
    eng.set_unique_keys("channel_participants", unique_keys_for_cp);

    let mut visible: Vec<String> = Vec::new();
    let mut hidden = 0usize;
    eng.add_queries_streaming(
        &[QuerySpec {
            query_id: "q".into(),
            ast: nested_scalar_exists_ast(),
        }],
        |rc: &RowChange| {
            if rc.table == "channel_participants" {
                if rc.is_hidden {
                    hidden += 1;
                } else if let Some(row) = rc.row.as_ref()
                    && let Some(rust_ivm::ivm::data::Value::Str(v)) = row.get("id")
                {
                    visible.push(v.to_string());
                }
            }
        },
    );
    visible.sort();
    (visible, hidden)
}

/// The bug: with only the PK ([id]) — what the driver passes today — the scalar
/// can't resolve, cp0 streams only hidden, client-visible set is EMPTY.
#[test]
fn scalar_unresolved_with_pk_only_drops_cp0_from_visible() {
    let path = "/tmp/rust-ivm-repro-scalar-pk.db";
    create_replica(path);
    let (visible, hidden) = hydrate(path, vec![vec!["id".to_string()]]);
    eprintln!("[PK-only] visible={visible:?} hidden_emissions={hidden}");
    // Documents the current bug: cp0 is NOT client-visible (only hidden companions).
    assert!(
        !visible.contains(&"cp0".to_string()),
        "PK-only: expected cp0 to be dropped from visible (bug), got {visible:?}"
    );
}

/// The fix: with the (channelId, userId) unique key known, the scalar resolves
/// once and cp0 is emitted VISIBLE exactly once — matching TS.
#[test]
fn scalar_resolves_with_unique_key_emits_cp0_visible_once() {
    let path = "/tmp/rust-ivm-repro-scalar-uk.db";
    create_replica(path);
    let (visible, _hidden) = hydrate(
        path,
        vec![
            vec!["id".to_string()],
            vec!["channelId".to_string(), "userId".to_string()],
        ],
    );
    eprintln!("[with-UK] visible={visible:?}");
    assert_eq!(
        visible,
        vec!["cp0".to_string()],
        "with unique key: cp0 must be visible exactly once"
    );
}
