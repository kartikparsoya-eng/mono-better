//! SQLite-backed reproduction of the ART G8 gap. MemorySource does NOT drop the
//! position-0 row (see repro_channel_participants_test), and parallel==serial
//! (see read_parallel_bench_test), so this exercises the real TableSource path
//! with the exact 4-participant shape (cp0 = smallest id = me/ADMIN) at
//! pool_lanes 0 (serial) and 2 (parallel), across several query shapes.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rusqlite::Connection;

use rust_ivm::builder::ast::{
    Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery, SimpleCondition, ValuePosition,
};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::Source;
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
         CREATE TABLE channel_participants (id TEXT PRIMARY KEY, channelId TEXT, userId TEXT, role TEXT);
         INSERT INTO channels VALUES ('ch1','PRIVATE');
         -- cp0 is the smallest id: the ADMIN / querying-user row (position 0).
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

fn participants_rel(
    alias: &str,
    where_clause: Option<Condition>,
    limit: Option<usize>,
) -> RelatedSubquery {
    RelatedSubquery {
        subquery: Box::new(Ast {
            table: "channel_participants".to_string(),
            alias: Some(alias.to_string()),
            where_clause,
            limit,
            ..Default::default()
        }),
        relationship_name: alias.to_string(),
        parent_key: vec!["id".to_string()],
        child_key: vec!["channelId".to_string()],
        hidden: false,
        system: None,
    }
}

/// Hydrate `ast` against the replica at the given pool lanes; return visible
/// (non-hidden) channel_participants ids in emission order.
fn hydrate_cp_ids(path: &str, pool_lanes: usize, ast: Ast) -> Vec<String> {
    let pks: HashMap<String, Vec<String>> = [
        ("channels".to_string(), vec!["id".to_string()]),
        ("channel_participants".to_string(), vec!["id".to_string()]),
    ]
    .into_iter()
    .collect();

    let mut snap = Snapshotter::with_read_pool(path, "repro", None, pool_lanes, None);
    snap.init().unwrap();
    let curr_conn = snap.current_conn().unwrap();

    let ch_cols: HashMap<String, ColumnType> = [
        ("id".to_string(), ColumnType::String { optional: false }),
        (
            "visibility".to_string(),
            ColumnType::String { optional: false },
        ),
    ]
    .into_iter()
    .collect();
    let cp_cols: HashMap<String, ColumnType> = [
        ("id".to_string(), ColumnType::String { optional: false }),
        (
            "channelId".to_string(),
            ColumnType::String { optional: false },
        ),
        ("userId".to_string(), ColumnType::String { optional: false }),
        ("role".to_string(), ColumnType::String { optional: false }),
    ]
    .into_iter()
    .collect();

    let mut ch_src = TableSource::new(
        curr_conn.clone(),
        "channels",
        ch_cols,
        vec!["id".to_string()],
    );
    let mut cp_src = TableSource::new(
        curr_conn.clone(),
        "channel_participants",
        cp_cols,
        vec!["id".to_string()],
    );
    if pool_lanes > 0 {
        ch_src.set_read_pool(snap.read_pool());
        cp_src.set_read_pool(snap.read_pool());
    }

    let mut eng = Engine::new(pks);
    eng.register_source(Rc::new(RefCell::new(ch_src)));
    eng.register_source(Rc::new(RefCell::new(cp_src)));

    let mut ids: Vec<String> = Vec::new();
    eng.add_queries_streaming(
        &[QuerySpec {
            query_id: "q".into(),
            ast,
        }],
        |rc: &RowChange| {
            if rc.table == "channel_participants"
                && !rc.is_hidden
                && let Some(row) = rc.row.as_ref()
                && let Some(rust_ivm::ivm::data::Value::Str(v)) = row.get("id")
            {
                ids.push(v.to_string());
            }
        },
    );
    ids
}

fn channels_related_ast(child: RelatedSubquery, where_clause: Option<Condition>) -> Ast {
    Ast {
        table: "channels".to_string(),
        where_clause,
        related: vec![child],
        ..Default::default()
    }
}

fn check(label: &str, path: &str, make_ast: impl Fn() -> Ast) {
    let mut serial = hydrate_cp_ids(path, 0, make_ast());
    let mut parallel = hydrate_cp_ids(path, 2, make_ast());
    serial.sort();
    parallel.sort();
    eprintln!("[{label}] serial={serial:?} parallel={parallel:?}");
    assert_eq!(serial, parallel, "[{label}] parallel must match serial");
    assert!(
        serial.contains(&"cp0".to_string()),
        "[{label}] cp0 (position-0/me) MUST be present, got {serial:?}"
    );
}

// B: channels.related('participants')  -> expect all 4 incl cp0
#[test]
fn repro_sql_b_related_only() {
    let path = "/tmp/rust-ivm-repro-cp-b.db";
    create_replica(path);
    check("B related", path, || {
        channels_related_ast(
            participants_rel("participants", None, None),
            Some(simple("id", "=", s("ch1"))),
        )
    });
}

// D: channels.related('participants', p.where(userId=me).one())  -> expect cp0
#[test]
fn repro_sql_d_my_membership_one() {
    let path = "/tmp/rust-ivm-repro-cp-d.db";
    create_replica(path);
    check("D my-membership .one()", path, || {
        channels_related_ast(
            participants_rel(
                "participants",
                Some(simple("userId", "=", s("me"))),
                Some(1usize),
            ),
            Some(simple("id", "=", s("ch1"))),
        )
    });
}

// C: channels.whereExists('participants', p.where(userId=me)).related('participants') -> expect all 4
#[test]
fn repro_sql_c_exists_plus_related() {
    let path = "/tmp/rust-ivm-repro-cp-c.db";
    create_replica(path);
    check("C exists+related", path, || {
        let exists = Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: participants_rel(
                "zsubq_participants",
                Some(simple("userId", "=", s("me"))),
                None,
            ),
            op: "EXISTS".to_string(),
            flip: Some(false),
            scalar: false,
            plan_id: None,
        });
        channels_related_ast(participants_rel("participants", None, None), Some(exists))
    });
}

// A: plain scan channel_participants.where(channelId=ch1) -> expect all 4
#[test]
fn repro_sql_a_plain_scan() {
    let path = "/tmp/rust-ivm-repro-cp-a.db";
    create_replica(path);
    check("A plain", path, || Ast {
        table: "channel_participants".to_string(),
        where_clause: Some(simple("channelId", "=", s("ch1"))),
        ..Default::default()
    });
}
