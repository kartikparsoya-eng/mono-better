//! Real-path regression guard for `userVisibleEmailChannels`:
//!   channel_user_status WHERE userId = me AND isClosed = false AND isDeleted = false
//!     .related('channel', ch => ch.where('type','IN',['EMAIL','SLACK','APP','CALL'])
//!                                  .related('channelStats'))
//!
//! A user's OWN "Saved messages" self-DM (scopeType=DM, type=DEFAULT) has a status
//! row (passes the root filter) but must be excluded by the related('channel')
//! subquery's `type IN [...]` WHERE. This pins that the related-subquery (Join)
//! child WHERE filter (`type IN [array]`) IS enforced. rust evaluates this
//! correctly; the ART G8 diff pointed here but was a transient, not an eval bug.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rusqlite::Connection;

use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::sqlite::table_source::TableSource;
use rust_ivm::streamer::RowChange;

const EMAIL_AST: &str = r#"
{"table":"channel_user_status","where":{"type":"and","conditions":[
  {"type":"simple","left":{"type":"column","name":"userId"},"right":{"type":"literal","value":"me"},"op":"="},
  {"type":"simple","left":{"type":"column","name":"isClosed"},"right":{"type":"literal","value":false},"op":"="},
  {"type":"simple","left":{"type":"column","name":"isDeleted"},"right":{"type":"literal","value":false},"op":"="}
]},
"related":[{"system":"client","correlation":{"parentField":["channelId"],"childField":["id"]},
  "subquery":{"table":"channels","alias":"channel",
    "where":{"type":"simple","left":{"type":"column","name":"type"},"right":{"type":"literal","value":["EMAIL","SLACK","APP","CALL"]},"op":"IN"},
    "related":[{"system":"client","correlation":{"parentField":["id"],"childField":["channelId"]},
      "subquery":{"table":"channel_stats","alias":"channelStats"}}]}}]}
"#;

fn seed() -> Rc<RefCell<Connection>> {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE channel_user_status (id TEXT PRIMARY KEY, userId TEXT NOT NULL, channelId TEXT NOT NULL, isClosed INTEGER NOT NULL, isDeleted INTEGER NOT NULL);
        CREATE TABLE channels (id TEXT PRIMARY KEY, type TEXT NOT NULL, scopeType TEXT NOT NULL);
        CREATE TABLE channel_stats (channelId TEXT PRIMARY KEY, lastActivityAt INTEGER NOT NULL);

        -- me has status rows on BOTH channels
        INSERT INTO channel_user_status VALUES ('cus_dm','me','dm1',0,0);
        INSERT INTO channel_user_status VALUES ('cus_email','me','email1',0,0);

        -- dm1: the "Saved messages" self-DM, type=DEFAULT -> must be filtered out by `type IN [EMAIL,...]`
        INSERT INTO channels VALUES ('dm1','DEFAULT','DM');
        -- email1: type=EMAIL -> must be emitted
        INSERT INTO channels VALUES ('email1','EMAIL','DEFAULT');

        INSERT INTO channel_stats VALUES ('dm1',100);
        INSERT INTO channel_stats VALUES ('email1',200);
        "#,
    )
    .unwrap();
    Rc::new(RefCell::new(conn))
}

fn strcols(names: &[&str]) -> HashMap<String, ColumnType> {
    names
        .iter()
        .map(|n| (n.to_string(), ColumnType::String { optional: false }))
        .collect()
}

#[test]
fn email_channels_related_type_in_filter_excludes_default_dm() {
    let db = seed();

    let sources: Vec<(&str, HashMap<String, ColumnType>, Vec<String>)> = vec![
        (
            "channel_user_status",
            {
                let mut m = strcols(&["id", "userId", "channelId"]);
                m.insert(
                    "isClosed".to_string(),
                    ColumnType::Boolean { optional: false },
                );
                m.insert(
                    "isDeleted".to_string(),
                    ColumnType::Boolean { optional: false },
                );
                m
            },
            vec!["id".to_string()],
        ),
        (
            "channels",
            strcols(&["id", "type", "scopeType"]),
            vec!["id".to_string()],
        ),
        (
            "channel_stats",
            {
                let mut m = strcols(&["channelId"]);
                m.insert(
                    "lastActivityAt".to_string(),
                    ColumnType::Number { optional: false },
                );
                m
            },
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

    let ast = rust_ivm::replay::json_to_ast(&serde_json::from_str(EMAIL_AST).unwrap());

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
            {
                if let Some(rust_ivm::ivm::data::Value::Str(s)) = rc.row_key.get("id") {
                    sink.borrow_mut().push(s.to_string());
                }
            }
        },
    );

    let mut ids = emitted.borrow().clone();
    ids.sort();
    ids.dedup();
    println!("userVisibleEmailChannels emitted channel ids: {ids:?}");

    assert!(
        ids.contains(&"email1".to_string()),
        "email1 (type=EMAIL) must be emitted"
    );
    assert!(
        !ids.contains(&"dm1".to_string()),
        "dm1 (type=DEFAULT) must be filtered by `type IN [EMAIL,...]` — got {ids:?}"
    );
}
