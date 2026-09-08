//! REAL-PATH heap profile — the prod advance path: Snapshotter + TableSource +
//! engine.advance_to_head_stream, driven by genuine `_zero.changeLog2` writes +
//! stateVersion bumps (exactly how the replication stream feeds prod). Bounded
//! live set, ever-new row keys. Any monotonic growth in dhat live bytes is the
//! prod per-advance leak — this is the path MemorySource tests can't reach.
//!
//! Run: cargo test -p rust-ivm --test advance_leak_realpath -- --ignored --nocapture

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use rusqlite::Connection;

use rust_ivm::builder::ast::{
    Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery, SimpleCondition, ValuePosition,
};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::snapshotter::Snapshotter;
use rust_ivm::snapshotter::spec::{ColumnSchema, LiteAndZqlSpec, TableSpec};
use rust_ivm::sqlite::table_source::TableSource;
use rust_ivm::streamer::RowChange;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const DB: &str = "/tmp/rust-ivm-leak-realpath.db";

fn ver(n: usize) -> String {
    format!("v{n:08}")
}

fn clean() {
    for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
        let _ = std::fs::remove_file(format!("{DB}{suffix}"));
    }
}

fn seed() {
    clean();
    let conn = Connection::open(DB).unwrap();
    let _ = conn.pragma_update(None, "journal_mode", "wal2");
    let _ = conn.pragma_update(None, "journal_mode", "wal");
    conn.execute_batch(&format!(
        r#"
        CREATE TABLE "_zero.replicationConfig" (lock TEXT PRIMARY KEY DEFAULT 'singleton',
            replicaVersion TEXT NOT NULL, publications TEXT NOT NULL);
        CREATE TABLE "_zero.replicationState" (lock TEXT PRIMARY KEY DEFAULT 'singleton',
            stateVersion TEXT NOT NULL);
        CREATE TABLE "_zero.changeLog2" ("stateVersion" TEXT NOT NULL, "table" TEXT NOT NULL,
            "rowKey" TEXT NOT NULL, "op" TEXT NOT NULL, "pos" INTEGER NOT NULL,
            PRIMARY KEY ("stateVersion","pos"));
        CREATE TABLE issues (id TEXT PRIMARY KEY, ownerId TEXT NOT NULL, _0_version TEXT NOT NULL);
        CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL, _0_version TEXT NOT NULL);
        INSERT INTO "_zero.replicationConfig" VALUES ('singleton','{v1}','[]');
        INSERT INTO "_zero.replicationState"  VALUES ('singleton','{v1}');
        INSERT INTO users  VALUES ('u1','Alice','{v1}');
        INSERT INTO issues VALUES ('i100','Alice','{v1}');
        INSERT INTO issues VALUES ('i101','Alice','{v1}');
        "#,
        v1 = ver(1),
    ))
    .unwrap();
}

fn issues_spec() -> LiteAndZqlSpec {
    let mut columns = HashMap::new();
    for c in ["id", "ownerId", "_0_version"] {
        columns.insert(
            c.to_string(),
            ColumnSchema {
                r#type: "TEXT".to_string(),
                optional: false,
            },
        );
    }
    LiteAndZqlSpec {
        table_spec: TableSpec {
            name: "issues".to_string(),
            columns: columns.clone(),
            unique_keys: vec![vec!["id".to_string()]],
            min_row_version: None,
        },
        zql_spec: columns,
    }
}

fn users_zqlspec() -> LiteAndZqlSpec {
    let mut columns = HashMap::new();
    for c in ["id", "name", "_0_version"] {
        columns.insert(
            c.to_string(),
            ColumnSchema {
                r#type: "TEXT".to_string(),
                optional: false,
            },
        );
    }
    LiteAndZqlSpec {
        table_spec: TableSpec {
            name: "users".to_string(),
            columns: columns.clone(),
            unique_keys: vec![vec!["id".to_string()]],
            min_row_version: None,
        },
        zql_spec: columns,
    }
}

/// issues WHERE EXISTS(users WHERE id='u1') correlated ownerId=users.name, flipped
/// — the prod flipped-join shape whose advance re-fetches parents (join.push_parents).
fn flip_exists_ast() -> Ast {
    let subquery = Ast {
        schema: None,
        table: "users".to_string(),
        alias: Some("users".to_string()),
        where_clause: Some(Condition::Simple(SimpleCondition {
            op: "=".to_string(),
            left: ValuePosition::Column {
                name: "id".to_string(),
            },
            right: ValuePosition::Literal {
                value: rust_ivm::ivm::data::Value::Str("u1".into()),
            },
        })),
        related: vec![],
        limit: None,
        order_by: None,
        start: None,
    };
    Ast {
        schema: None,
        table: "issues".to_string(),
        alias: None,
        where_clause: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: RelatedSubquery {
                subquery: Box::new(subquery),
                relationship_name: "users".to_string(),
                parent_key: vec!["ownerId".to_string()],
                child_key: vec!["name".to_string()],
                hidden: false,
                system: None,
            },
            op: "EXISTS".to_string(),
            flip: Some(true),
            scalar: false,
            plan_id: None,
        })),
        related: vec![],
        limit: Some(50),
        order_by: Some(vec![rust_ivm::builder::ast::OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    }
}

/// One replication step: add issue `iN`, remove `i{N-2}`, bump stateVersion.
fn write_step(w: &Connection, n: usize) {
    let v = ver(n);
    let id_new = format!("i{}", n + 100);
    w.execute(
        "INSERT INTO issues (id,ownerId,_0_version) VALUES (?,?,?)",
        rusqlite::params![id_new, "Alice", v],
    )
    .unwrap();
    w.execute(
        r#"INSERT INTO "_zero.changeLog2" ("stateVersion","table","rowKey","op","pos") VALUES (?,?,?,?,?)"#,
        rusqlite::params![v, "issues", format!(r#"{{"id":"{id_new}"}}"#), "s", 0i64],
    )
    .unwrap();
    if n >= 3 {
        let id_old = format!("i{}", n + 100 - 2);
        w.execute("DELETE FROM issues WHERE id=?", rusqlite::params![id_old])
            .unwrap();
        w.execute(
            r#"INSERT INTO "_zero.changeLog2" ("stateVersion","table","rowKey","op","pos") VALUES (?,?,?,?,?)"#,
            rusqlite::params![v, "issues", format!(r#"{{"id":"{id_old}"}}"#), "d", 1i64],
        )
        .unwrap();
    }
    w.execute(
        r#"UPDATE "_zero.replicationState" SET stateVersion=? WHERE lock='singleton'"#,
        rusqlite::params![v],
    )
    .unwrap();
}

#[test]
#[ignore = "profiling harness; run explicitly with --ignored --nocapture"]
fn advance_leak_realpath() {
    let _profiler = dhat::Profiler::builder().build();
    seed();

    let mut snap = Snapshotter::new(DB, "", None);
    snap.init().unwrap();
    let curr = snap.current_conn().unwrap();

    let icols: HashMap<String, ColumnType> = ["id", "ownerId", "_0_version"]
        .iter()
        .map(|n| (n.to_string(), ColumnType::String { optional: false }))
        .collect();
    let ucols: HashMap<String, ColumnType> = ["id", "name", "_0_version"]
        .iter()
        .map(|n| (n.to_string(), ColumnType::String { optional: false }))
        .collect();
    let its = TableSource::new(curr.clone(), "issues", icols, vec!["id".to_string()]);
    let uts = TableSource::new(curr.clone(), "users", ucols, vec!["id".to_string()]);
    let mut eng = Engine::new(HashMap::from([
        ("issues".to_string(), vec!["id".to_string()]),
        ("users".to_string(), vec!["id".to_string()]),
    ]));
    eng.register_source(Rc::new(RefCell::new(its)));
    eng.register_source(Rc::new(RefCell::new(uts)));
    eng.set_unique_keys("issues", vec![vec!["id".to_string()]]);
    eng.set_unique_keys("users", vec![vec!["id".to_string()]]);
    eng.add_queries_streaming(
        &[QuerySpec {
            query_id: "q".into(),
            ast: flip_exists_ast(),
        }],
        |_rc: &RowChange| {},
    );

    let syncable: HashMap<String, LiteAndZqlSpec> = HashMap::from([
        ("issues".to_string(), issues_spec()),
        ("users".to_string(), users_zqlspec()),
    ]);
    let all_tables: HashSet<String> = HashSet::from(["issues".to_string(), "users".to_string()]);

    let writer = Connection::open(DB).unwrap();
    let base = dhat::HeapStats::get();
    println!(
        "baseline after hydrate: curr_bytes={} curr_blocks={}",
        base.curr_bytes, base.curr_blocks
    );

    let iters = 8000usize;
    let mut prev = base.curr_bytes;
    for n in 2..(iters + 2) {
        write_step(&writer, n);
        let r = eng.advance_to_head_stream(&mut snap, &syncable, &all_tables, |_, _| {}, |_| {});
        if let Err(e) = r {
            println!("advance error at n={n}: {e:?}");
            break;
        }
        if (n - 1) % 1000 == 0 {
            let s = dhat::HeapStats::get();
            println!(
                "  adv {:6}: curr_bytes={:>10} curr_blocks={:>8}  Δ={:+}",
                n - 1,
                s.curr_bytes,
                s.curr_blocks,
                s.curr_bytes as i64 - prev as i64
            );
            prev = s.curr_bytes;
        }
    }
    let end = dhat::HeapStats::get();
    let grew = end.curr_bytes as i64 - base.curr_bytes as i64;
    println!(
        "END: curr_bytes={} curr_blocks={}  grew={:+} over {} advances ({:.1} bytes/advance)",
        end.curr_bytes,
        end.curr_blocks,
        grew,
        iters,
        grew as f64 / iters as f64
    );
    snap.destroy();
    clean();
}
