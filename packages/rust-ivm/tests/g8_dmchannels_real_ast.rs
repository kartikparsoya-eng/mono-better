//! Real-path regression guard using the EXACT transformed AST of
//! `dmChannelsLatestMessagesPaginated` (dumped from the xyne backend), replayed
//! over a SQLite TableSource.
//!
//! Root `channel_stats` is gated by a nested permission EXISTS:
//!   EXISTS(channel c WHERE c.id = channelId AND
//!            ( c.visibility = 'PUBLIC'
//!              OR EXISTS(participants p WHERE p.channelId = c.id AND p.userId = me) ))
//! i.e. EXISTS-within-OR-within-EXISTS. A PRIVATE, cross-workspace DM where the
//! querying user is NOT a participant makes the inner EXISTS(participants userId=me)
//! false → the whole gate is false → the DM channel must NOT be emitted. rust
//! evaluates this correctly (this is the exact shape the ART G8 diff pointed at;
//! the diff itself was a transient in the full concurrent run, not an eval bug).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rusqlite::Connection;

use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::sqlite::table_source::TableSource;
use rust_ivm::streamer::RowChange;

// Exact AST dumped from the backend query fn for user cms5zzgo (ws cms5zzf8s),
// with literals renamed to the short test ids used below.
const DM_CHANNELS_AST: &str = r#"
{"table":"channel_stats","where":{"type":"and","conditions":[
  {"type":"correlatedSubquery","op":"EXISTS","related":{"system":"client",
    "correlation":{"parentField":["channelId"],"childField":["id"]},
    "subquery":{"table":"channels","alias":"zsubq_channel","where":{"type":"or","conditions":[
      {"type":"simple","left":{"type":"column","name":"scopeType"},"right":{"type":"literal","value":"DM"},"op":"="},
      {"type":"simple","left":{"type":"column","name":"scopeType"},"right":{"type":"literal","value":"GROUP_DM"},"op":"="}
    ]}}}},
  {"type":"correlatedSubquery","op":"EXISTS","related":{"system":"client",
    "correlation":{"parentField":["channelId"],"childField":["id"]},
    "subquery":{"table":"channels","alias":"zsubq_channel","where":{"type":"or","conditions":[
      {"type":"simple","left":{"type":"column","name":"visibility"},"right":{"type":"literal","value":"PUBLIC"},"op":"="},
      {"type":"correlatedSubquery","op":"EXISTS","related":{"system":"client",
        "correlation":{"parentField":["id"],"childField":["channelId"]},
        "subquery":{"table":"channel_participants","alias":"zsubq_participants","where":{
          "type":"simple","left":{"type":"column","name":"userId"},"right":{"type":"literal","value":"me"},"op":"="}}}}
    ]}}}}
]},
"orderBy":[["lastActivityAt","desc"],["channelId","desc"]],"limit":50,
"related":[{"system":"client","correlation":{"parentField":["channelId"],"childField":["id"]},
  "subquery":{"table":"channels","alias":"channel","related":[
    {"system":"client","correlation":{"parentField":["id"],"childField":["channelId"]},
      "subquery":{"table":"conversations","alias":"conversations","orderBy":[["createdAt","desc"]],"limit":1}}]}}]}
"#;

fn seed() -> Rc<RefCell<Connection>> {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE channel_stats (channelId TEXT PRIMARY KEY, lastActivityAt INTEGER NOT NULL);
        CREATE TABLE channels (id TEXT PRIMARY KEY, scopeType TEXT NOT NULL, visibility TEXT NOT NULL, workspaceId TEXT NOT NULL);
        CREATE TABLE channel_participants (id TEXT PRIMARY KEY, channelId TEXT NOT NULL, userId TEXT NOT NULL);
        CREATE TABLE conversations (id TEXT PRIMARY KEY, channelId TEXT NOT NULL, createdAt INTEGER NOT NULL);

        -- dm1: PRIVATE cross-workspace DM, me NOT a participant -> LEAK target
        INSERT INTO channels VALUES ('dm1','DM','PRIVATE','wsother');
        INSERT INTO channel_stats VALUES ('dm1', 100);
        INSERT INTO channel_participants VALUES ('cp_other','dm1','other');

        -- mine_dm: DM the ART user participates in -> must be emitted
        INSERT INTO channels VALUES ('mine_dm','DM','PRIVATE','wsme');
        INSERT INTO channel_stats VALUES ('mine_dm', 200);
        INSERT INTO channel_participants VALUES ('cp_me','mine_dm','me');

        INSERT INTO conversations VALUES ('conv_dm1','dm1',1);
        INSERT INTO conversations VALUES ('conv_mine','mine_dm',1);
        "#,
    )
    .unwrap();
    Rc::new(RefCell::new(conn))
}

fn cols(names: &[&str]) -> HashMap<String, ColumnType> {
    names
        .iter()
        .map(|n| (n.to_string(), ColumnType::String { optional: false }))
        .collect()
}

#[test]
fn dm_channels_real_ast_excludes_cross_workspace_dm() {
    let db = seed();

    let sources = [
        (
            "channel_stats",
            cols(&["channelId", "lastActivityAt"]),
            vec!["channelId".to_string()],
        ),
        (
            "channels",
            cols(&["id", "scopeType", "visibility", "workspaceId"]),
            vec!["id".to_string()],
        ),
        (
            "channel_participants",
            cols(&["id", "channelId", "userId"]),
            vec!["id".to_string()],
        ),
        (
            "conversations",
            cols(&["id", "channelId", "createdAt"]),
            vec!["id".to_string()],
        ),
    ];

    let mut uniq = HashMap::new();
    let mut eng = {
        for (name, _, pk) in &sources {
            uniq.insert(name.to_string(), pk.clone());
        }
        Engine::new(uniq.clone())
    };
    for (name, c, pk) in sources {
        let ts = TableSource::new(db.clone(), name, c, pk.clone());
        eng.register_source(Rc::new(RefCell::new(ts)));
        eng.set_unique_keys(name, vec![pk]);
    }

    let ast = rust_ivm::replay::json_to_ast(&serde_json::from_str(DM_CHANNELS_AST).unwrap());

    let emitted = Rc::new(RefCell::new(Vec::<String>::new()));
    let sink = emitted.clone();
    eng.add_queries_streaming(
        &[QuerySpec {
            query_id: "q".into(),
            ast,
        }],
        move |rc: &RowChange| {
            if rc.table == "channels"
                && !rc.is_hidden
                && rc.change_type == rust_ivm::ivm::change::ChangeType::Add
                && let Some(rust_ivm::ivm::data::Value::Str(s)) = rc.row_key.get("id")
            {
                sink.borrow_mut().push(s.to_string());
            }
        },
    );

    let mut ids = emitted.borrow().clone();
    ids.sort();
    ids.dedup();
    println!("dmChannels emitted (non-hidden) channel ids: {ids:?}");

    assert!(
        ids.contains(&"mine_dm".to_string()),
        "mine_dm (me participates) must be emitted"
    );
    assert!(
        !ids.contains(&"dm1".to_string()),
        "dm1 (private cross-ws DM, me NOT a participant) must NOT leak — got {ids:?}"
    );
}
