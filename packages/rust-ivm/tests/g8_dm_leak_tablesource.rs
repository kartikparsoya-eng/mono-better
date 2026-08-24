//! Real-path (TableSource / SQLite) regression guard for the `userAllChannels`
//! channels read-permission (channels-acl.ts canSelect, member branch):
//!   channels WHERE workspaceId = <me-ws>
//!     AND ( visibility = 'PUBLIC' OR EXISTS(participants p WHERE p.userId = me) )
//!
//! A PRIVATE channel in a DIFFERENT workspace (me not a participant) must be
//! excluded by the top-level `workspaceId = me-ws` conjunct. This pins the
//! TableSource/SQL path that ART exercises. rust evaluates it correctly; the ART
//! G8 1-row diff was a transient in the full concurrent run, not an eval bug.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rusqlite::Connection;

use rust_ivm::builder::ast::{
    Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery, SimpleCondition, ValuePosition,
};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::sqlite::table_source::TableSource;
use rust_ivm::streamer::RowChange;

fn seed() -> Rc<RefCell<Connection>> {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE channels (id TEXT PRIMARY KEY, workspaceId TEXT NOT NULL, visibility TEXT NOT NULL);
        CREATE TABLE channel_participants (id TEXT PRIMARY KEY, channelId TEXT NOT NULL, userId TEXT NOT NULL);

        -- me-ws = wsme (EMPTY, like the real ART user's workspace), other-ws = wsother.
        -- Every channel is in wsother, so `WHERE workspaceId = wsme` must return NOTHING.
        INSERT INTO channels VALUES ('dm1','wsother','PRIVATE');            -- LEAK target
        INSERT INTO channels VALUES ('pub_other','wsother','PUBLIC');       -- excluded by workspaceId

        INSERT INTO channel_participants VALUES ('cp_other','dm1','other');
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

fn simple(col: &str, op: &str, val: &str) -> Condition {
    Condition::Simple(SimpleCondition {
        op: op.to_string(),
        left: ValuePosition::Column {
            name: col.to_string(),
        },
        right: ValuePosition::Literal {
            value: rust_ivm::ivm::data::Value::Str(val.into()),
        },
    })
}

fn user_all_channels_ast() -> Ast {
    let participants = RelatedSubquery {
        subquery: Box::new(Ast {
            schema: None,
            table: "channel_participants".to_string(),
            alias: Some("participants".to_string()),
            where_clause: Some(simple("userId", "=", "me")),
            related: Vec::new(),
            limit: None,
            order_by: None,
            start: None,
        }),
        relationship_name: "participants".to_string(),
        parent_key: vec!["id".to_string()],
        child_key: vec!["channelId".to_string()],
        hidden: false,
        system: None,
    };
    Ast {
        schema: None,
        table: "channels".to_string(),
        alias: None,
        where_clause: Some(Condition::And(vec![
            simple("workspaceId", "=", "wsme"),
            Condition::Or(vec![
                simple("visibility", "=", "PUBLIC"),
                Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
                    related: participants,
                    op: "EXISTS".to_string(),
                    flip: Some(false),
                    scalar: false,
                    plan_id: None,
                }),
            ]),
        ])),
        related: Vec::new(),
        limit: None,
        order_by: None,
        start: None,
    }
}

#[test]
fn user_all_channels_tablesource_excludes_cross_workspace_dm() {
    let db = seed();

    let chs = TableSource::new(
        db.clone(),
        "channels",
        cols(&["id", "workspaceId", "visibility"]),
        vec!["id".to_string()],
    );
    let cps = TableSource::new(
        db.clone(),
        "channel_participants",
        cols(&["id", "channelId", "userId"]),
        vec!["id".to_string()],
    );

    let mut eng = Engine::new(HashMap::from([
        ("channels".to_string(), vec!["id".to_string()]),
        ("channel_participants".to_string(), vec!["id".to_string()]),
    ]));
    eng.register_source(Rc::new(RefCell::new(chs)));
    eng.register_source(Rc::new(RefCell::new(cps)));
    eng.set_unique_keys("channels", vec![vec!["id".to_string()]]);
    eng.set_unique_keys("channel_participants", vec![vec!["id".to_string()]]);

    let emitted = Rc::new(RefCell::new(Vec::<String>::new()));
    let sink = emitted.clone();
    eng.add_queries_streaming(
        &[QuerySpec {
            query_id: "q".into(),
            ast: user_all_channels_ast(),
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
    println!("TableSource userAllChannels emitted channel ids: {ids:?}");

    // wsme has no channels → the query result must be empty.
    assert!(
        ids.is_empty(),
        "workspaceId=wsme has no channels; nothing may be emitted — got {ids:?}"
    );
}
