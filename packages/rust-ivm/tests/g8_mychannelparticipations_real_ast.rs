//! Real-path regression guard using the EXACT transformed AST of
//! `myChannelParticipations` (dumped from the xyne backend via the ART diff
//! oracle), replayed over a SQLite TableSource.
//!
//! The app query is trivially `channel_participants WHERE userId=me AND
//! role=ADMIN`. The xyne `ChannelParticipantsACL.canSelect` then wraps it with a
//! read rule applied through the ordinary client query builder (system:client):
//!
//!   OR[ userId = me,
//!       EXISTS(channels zsubq_channel WHERE workspaceId = my-ws AND
//!                OR[ visibility = 'PUBLIC',
//!                    EXISTS(channel_participants zsubq_participants
//!                             WHERE userId = me) ]) ]
//!
//! Because the ROOT already filters `userId = me`, the first OR branch
//! (`userId = me`) is ALWAYS true, so the `EXISTS(channels …)` branch is
//! redundant: every qualifying participant row passes the OR via the cheap
//! equality branch and NEVER needs the exists join. TS (zero-cache 1.9
//! pipeline-driver) therefore attaches NO `channels` relationship to those rows
//! and streams ZERO `channels` rows to the CVR (verified live: the real TS 1.9
//! mirror emits 9 channel_participants and 0 channels for this query, while the
//! rust syncer emitted 9 channels — the ART G8 divergence).
//!
//! This test pins the wire-visible contract: for a participant row that passes
//! the OR via `userId = me`, the redundant EXISTS(channels) branch must NOT emit
//! any `channels` RowChange to the CVR (hidden or not). Over-emitting is the G8
//! bug.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rusqlite::Connection;

use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::sqlite::table_source::TableSource;
use rust_ivm::streamer::RowChange;

// Exact transformed AST of myChannelParticipations for user `me` in ws `myws`
// (literals renamed to the short test ids used below).
const MCP_AST: &str = r#"
{"table":"channel_participants","where":{"type":"and","conditions":[
  {"type":"simple","left":{"type":"column","name":"userId"},"right":{"type":"literal","value":"me"},"op":"="},
  {"type":"simple","left":{"type":"column","name":"role"},"right":{"type":"literal","value":"ADMIN"},"op":"="},
  {"type":"or","conditions":[
    {"type":"simple","left":{"type":"column","name":"userId"},"right":{"type":"literal","value":"me"},"op":"="},
    {"type":"correlatedSubquery","op":"EXISTS","related":{"system":"client",
      "correlation":{"parentField":["channelId"],"childField":["id"]},
      "subquery":{"table":"channels","alias":"zsubq_channel","where":{"type":"and","conditions":[
        {"type":"simple","left":{"type":"column","name":"workspaceId"},"right":{"type":"literal","value":"myws"},"op":"="},
        {"type":"or","conditions":[
          {"type":"simple","left":{"type":"column","name":"visibility"},"right":{"type":"literal","value":"PUBLIC"},"op":"="},
          {"type":"correlatedSubquery","op":"EXISTS","related":{"system":"client",
            "correlation":{"parentField":["id"],"childField":["channelId"]},
            "subquery":{"table":"channel_participants","alias":"zsubq_participants","where":{
              "type":"simple","left":{"type":"column","name":"userId"},"right":{"type":"literal","value":"me"},"op":"="}}}}
        ]}
      ]}}}}
  ]}
]}}
"#;

fn seed() -> Rc<RefCell<Connection>> {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE channel_participants (id TEXT PRIMARY KEY, channelId TEXT NOT NULL, userId TEXT NOT NULL, role TEXT NOT NULL);
        CREATE TABLE channels (id TEXT PRIMARY KEY, workspaceId TEXT NOT NULL, visibility TEXT NOT NULL);

        -- chA: PUBLIC channel in-ws; `me` is its ADMIN participant.
        INSERT INTO channels VALUES ('chA','myws','PUBLIC');
        INSERT INTO channel_participants VALUES ('p_me_A','chA','me','ADMIN');

        -- chB: PRIVATE channel in-ws; `me` is its ADMIN participant (passes the
        -- inner EXISTS(participants userId=me) branch too).
        INSERT INTO channels VALUES ('chB','myws','PRIVATE');
        INSERT INTO channel_participants VALUES ('p_me_B','chB','me','ADMIN');
        "#,
    )
    .unwrap();
    // ANALYZE so the scanstatus cost model has stat data (the prod path; the
    // COUNT(*) fallback that once planned this without ANALYZE was removed).
    conn.execute_batch("ANALYZE;").unwrap();
    Rc::new(RefCell::new(conn))
}

fn cols(names: &[&str]) -> HashMap<String, ColumnType> {
    names
        .iter()
        .map(|n| (n.to_string(), ColumnType::String { optional: false }))
        .collect()
}

#[test]
fn mychannelparticipations_redundant_or_exists_emits_no_channels() {
    let db = seed();

    let sources = [
        (
            "channel_participants",
            cols(&["id", "channelId", "userId", "role"]),
            vec!["id".to_string()],
        ),
        (
            "channels",
            cols(&["id", "workspaceId", "visibility"]),
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
    // Enable flip planning exactly as the syncer's `build_engine` does for the
    // replica-backed path (parity with TS `buildPipeline` → `planQuery`). This
    // is the production fix under test: WITHOUT it the engine builds the
    // redundant OR-EXISTS non-flipped and over-emits the channels backing rows.
    eng.set_cost_model_conn(db.clone());
    // Real prod path: the scanstatus cost model needs table specs (TS
    // createSQLiteCostModel parity). The removed COUNT(*) fallback used to plan
    // this without specs; now no specs ⇒ unplanned ⇒ over-emit, so pin specs.
    let specs: HashMap<String, HashMap<String, ColumnType>> = sources
        .iter()
        .map(|(n, c, _)| (n.to_string(), c.clone()))
        .collect();
    eng.set_cost_model_table_specs(specs);
    for (name, c, pk) in sources {
        let ts = TableSource::new(db.clone(), name, c, pk.clone());
        eng.register_source(Rc::new(RefCell::new(ts)));
        eng.set_unique_keys(name, vec![pk]);
    }

    let ast = rust_ivm::replay::json_to_ast(&serde_json::from_str(MCP_AST).unwrap());

    let participants = Rc::new(RefCell::new(Vec::<String>::new()));
    let channels_all = Rc::new(RefCell::new(Vec::<String>::new()));
    let channels_hidden = Rc::new(RefCell::new(Vec::<String>::new()));
    let p_sink = participants.clone();
    let c_all = channels_all.clone();
    let c_hidden = channels_hidden.clone();
    eng.add_queries_streaming(
        &[QuerySpec {
            query_id: "q".into(),
            ast,
        }],
        move |rc: &RowChange| {
            if rc.change_type != rust_ivm::ivm::change::ChangeType::Add {
                return;
            }
            if rc.table == "channel_participants"
                && let Some(rust_ivm::ivm::data::Value::Str(s)) = rc.row_key.get("id")
            {
                p_sink.borrow_mut().push(s.to_string());
            }
            if rc.table == "channels"
                && let Some(rust_ivm::ivm::data::Value::Str(s)) = rc.row_key.get("id")
            {
                c_all.borrow_mut().push(s.to_string());
                if rc.is_hidden {
                    c_hidden.borrow_mut().push(s.to_string());
                }
            }
        },
    );

    let mut prows = participants.borrow().clone();
    prows.sort();
    prows.dedup();
    let mut crows = channels_all.borrow().clone();
    crows.sort();
    crows.dedup();
    let mut chid = channels_hidden.borrow().clone();
    chid.sort();
    chid.dedup();
    println!("MCP emitted channel_participants: {prows:?}");
    println!("MCP emitted channels (ALL): {crows:?}");
    println!("MCP emitted channels (is_hidden=true): {chid:?}");

    // The 2 admin participant rows are the query RESULT and must be emitted.
    assert!(
        prows.contains(&"p_me_A".to_string()) && prows.contains(&"p_me_B".to_string()),
        "both admin participant rows must be emitted — got {prows:?}"
    );

    // TS 1.9 emits ZERO channels for this query: every participant passes the
    // OR via `userId = me`, so the redundant EXISTS(channels) branch never
    // attaches a `channels` relationship. rust must match — emit no channels row
    // to the CVR wire.
    assert!(
        crows.is_empty(),
        "redundant OR-EXISTS(channels) must emit NO channels rows (TS emits 0); \
         rust over-emitted {crows:?} (G8)"
    );
}
