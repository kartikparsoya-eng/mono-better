//! Real-path regression guard for `userVisibleChannelsV2` (exact backend AST):
//!   channel_user_status WHERE userId = me AND isClosed = false AND isDeleted = false
//!     .related('channel', ch => ch.related('channelStats'))
//! A user with ZERO status rows must get nothing. The DM's only status row belongs
//! to another user; the root `userId = me` + boolean filters must exclude it so its
//! related `channel` is never fetched. isClosed/isDeleted are declared boolean and
//! stored as 0/1 to exercise the SQLite->Value coercion. rust evaluates this
//! correctly; the ART G8 diff was a transient, not this path.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rusqlite::Connection;

use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::sqlite::table_source::TableSource;
use rust_ivm::streamer::RowChange;

const V2_AST: &str = r#"
{"table":"channel_user_status","where":{"type":"and","conditions":[
  {"type":"simple","left":{"type":"column","name":"userId"},"right":{"type":"literal","value":"me"},"op":"="},
  {"type":"simple","left":{"type":"column","name":"isClosed"},"right":{"type":"literal","value":false},"op":"="},
  {"type":"simple","left":{"type":"column","name":"isDeleted"},"right":{"type":"literal","value":false},"op":"="}
]},
"related":[{"system":"client","correlation":{"parentField":["channelId"],"childField":["id"]},
  "subquery":{"table":"channels","alias":"channel","related":[
    {"system":"client","correlation":{"parentField":["id"],"childField":["channelId"]},
      "subquery":{"table":"channel_stats","alias":"channelStats"}}]}}]}
"#;

fn seed() -> Rc<RefCell<Connection>> {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE channel_user_status (id TEXT PRIMARY KEY, userId TEXT NOT NULL, channelId TEXT NOT NULL, isClosed INTEGER NOT NULL, isDeleted INTEGER NOT NULL);
        CREATE TABLE channels (id TEXT PRIMARY KEY, scopeType TEXT NOT NULL, visibility TEXT NOT NULL, workspaceId TEXT NOT NULL);
        CREATE TABLE channel_stats (channelId TEXT PRIMARY KEY, lastActivityAt INTEGER NOT NULL);

        -- Only OTHER user has a status row (on the private DM). ME has none.
        INSERT INTO channel_user_status VALUES ('cus_other','other','dm1',0,0);
        INSERT INTO channels VALUES ('dm1','DM','PRIVATE','wsother');
        INSERT INTO channel_stats VALUES ('dm1',100);
        "#,
    )
    .unwrap();
    Rc::new(RefCell::new(conn))
}

#[test]
fn user_visible_v2_excludes_other_users_status_row() {
    let db = seed();

    let sources: Vec<(&str, HashMap<String, ColumnType>, Vec<String>)> = vec![
        (
            "channel_user_status",
            HashMap::from([
                ("id".to_string(), ColumnType::String { optional: false }),
                ("userId".to_string(), ColumnType::String { optional: false }),
                (
                    "channelId".to_string(),
                    ColumnType::String { optional: false },
                ),
                (
                    "isClosed".to_string(),
                    ColumnType::Boolean { optional: false },
                ),
                (
                    "isDeleted".to_string(),
                    ColumnType::Boolean { optional: false },
                ),
            ]),
            vec!["id".to_string()],
        ),
        (
            "channels",
            HashMap::from([
                ("id".to_string(), ColumnType::String { optional: false }),
                (
                    "scopeType".to_string(),
                    ColumnType::String { optional: false },
                ),
                (
                    "visibility".to_string(),
                    ColumnType::String { optional: false },
                ),
                (
                    "workspaceId".to_string(),
                    ColumnType::String { optional: false },
                ),
            ]),
            vec!["id".to_string()],
        ),
        (
            "channel_stats",
            HashMap::from([
                (
                    "channelId".to_string(),
                    ColumnType::String { optional: false },
                ),
                (
                    "lastActivityAt".to_string(),
                    ColumnType::Number { optional: false },
                ),
            ]),
            vec!["channelId".to_string()],
        ),
    ];

    let mut uniq = HashMap::new();
    for (n, _, pk) in &sources {
        uniq.insert(n.to_string(), pk.clone());
    }
    let mut eng = Engine::new(uniq);
    for (name, c, pk) in sources {
        let ts = TableSource::new(db.clone(), name, c, pk.clone());
        eng.register_source(Rc::new(RefCell::new(ts)));
        eng.set_unique_keys(name, vec![pk]);
    }

    let ast = rust_ivm::replay::json_to_ast(&serde_json::from_str(V2_AST).unwrap());

    let emitted = Rc::new(RefCell::new(Vec::<String>::new()));
    let sink = emitted.clone();
    eng.add_queries_streaming(
        &[QuerySpec {
            query_id: "q".into(),
            ast,
        }],
        move |rc: &RowChange| {
            if !rc.is_hidden && rc.change_type == rust_ivm::ivm::change::ChangeType::Add {
                let key = rc.row_key.get("id").or_else(|| rc.row_key.get("channelId"));
                if let Some(rust_ivm::ivm::data::Value::Str(s)) = key {
                    sink.borrow_mut().push(format!("{}:{}", rc.table, s));
                }
            }
        },
    );

    let mut ids = emitted.borrow().clone();
    ids.sort();
    ids.dedup();
    println!("V2 emitted rows: {ids:?}");

    assert!(
        !ids.iter().any(|s| s == "channels:dm1"),
        "the private DM (only OTHER user has a status row) must NOT leak — got {ids:?}"
    );
    assert!(
        ids.is_empty(),
        "ART user has no status rows; nothing may be emitted — got {ids:?}"
    );
}
