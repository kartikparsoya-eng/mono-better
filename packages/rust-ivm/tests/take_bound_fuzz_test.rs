//! take_bound_fuzz_test.rs — seeded fuzz of the Take/LIMIT operator over the
//! REAL advance path (Snapshotter + TableSource + advance_to_head_stream),
//! hunting the live-prod Take bound divergences:
//!   take.rs:545 "Take: boundNode must be found during fetch"
//!   take.rs:702 "Bound should be set"
//! observed on zero-02-rust (2026-08-12) and preprod hf2cg.
//!
//! Shape: one table with a NULLABLE REAL order-by column (NULLs + duplicate
//! values force the bound onto degenerate sort keys), LIMIT 1..=3, random
//! upstream insert/update/delete sequences applied as genuine replication
//! steps (changeLog2 + stateVersion bump), advanced through the engine.
//! Two spec variants per seed: the order-by column declared optional:true
//! (faithful spec) and optional:false (drifted spec — data still contains
//! NULLs), because the start-constraint SQL only uses NULL-aware operators
//! (IS vs =) when the column is declared optional (query_builder.rs
//! nullable_aware_equality / nullable_aware_range_comparison).
//!
//! Failure modes caught:
//!   1. Panic inside advance (the prod signature) — reported with seed + step
//!      + full op history for deterministic replay.
//!   2. Silent wrong rows: client materialization from emitted RowChanges is
//!      compared against a direct SQL top-K requery after every advance.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use rusqlite::Connection;

use rust_ivm::builder::ast::{Ast, OrderPart};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::change::ChangeType;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::snapshotter::Snapshotter;
use rust_ivm::snapshotter::spec::{ColumnSchema, LiteAndZqlSpec, TableSpec};
use rust_ivm::sqlite::table_source::TableSource;

fn ver(n: usize) -> String {
    format!("v{n:08}")
}

/// xorshift64* — deterministic, dependency-free.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.max(1))
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Clone, Debug)]
enum Op {
    Insert { id: String, val: Option<f64> },
    Update { id: String, val: Option<f64> },
    Delete { id: String },
}

fn db_path(tag: &str) -> String {
    format!("/tmp/rust-ivm-take-fuzz-{tag}-{}.db", std::process::id())
}

fn clean(path: &str) {
    for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
}

fn seed_db(path: &str) {
    clean(path);
    let conn = Connection::open(path).unwrap();
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
            PRIMARY KEY ("stateVersion","pos"),
            UNIQUE("table","rowKey"));
        CREATE TABLE items (id TEXT PRIMARY KEY, val REAL, _0_version TEXT NOT NULL);
        INSERT INTO "_zero.replicationConfig" VALUES ('singleton','{v1}','[]');
        INSERT INTO "_zero.replicationState"  VALUES ('singleton','{v1}');
        "#,
        v1 = ver(1),
    ))
    .unwrap();
}

fn items_spec(val_optional: bool) -> LiteAndZqlSpec {
    let mut columns = HashMap::new();
    columns.insert(
        "id".to_string(),
        ColumnSchema {
            r#type: "TEXT".to_string(),
            optional: false,
        },
    );
    columns.insert(
        "val".to_string(),
        ColumnSchema {
            r#type: "REAL".to_string(),
            optional: val_optional,
        },
    );
    columns.insert(
        "_0_version".to_string(),
        ColumnSchema {
            r#type: "TEXT".to_string(),
            optional: false,
        },
    );
    LiteAndZqlSpec {
        table_spec: TableSpec {
            name: "items".to_string(),
            columns: columns.clone(),
            unique_keys: vec![vec!["id".to_string()]],
            min_row_version: None,
        },
        zql_spec: columns,
    }
}

fn source_columns(val_optional: bool) -> HashMap<String, ColumnType> {
    let mut c = HashMap::new();
    c.insert("id".to_string(), ColumnType::String { optional: false });
    c.insert(
        "val".to_string(),
        ColumnType::Number {
            optional: val_optional,
        },
    );
    c.insert(
        "_0_version".to_string(),
        ColumnType::String { optional: false },
    );
    c
}

fn take_ast(limit: usize, desc: bool) -> Ast {
    Ast {
        schema: None,
        table: "items".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: Some(limit),
        order_by: Some(vec![
            OrderPart {
                column: "val".to_string(),
                direction: if desc { "desc" } else { "asc" }.to_string(),
            },
            OrderPart {
                column: "id".to_string(),
                direction: "asc".to_string(),
            },
        ]),
        start: None,
    }
}

/// Apply one op to the db as a genuine replication step.
fn apply_op(w: &Connection, op: &Op, version: &str, pos: i64) {
    match op {
        Op::Insert { id, val } | Op::Update { id, val } => {
            w.execute(
                "INSERT INTO items (id, val, _0_version) VALUES (?,?,?)
                 ON CONFLICT(id) DO UPDATE SET val=excluded.val, _0_version=excluded._0_version",
                rusqlite::params![id, val, version],
            )
            .unwrap();
            w.execute(
                r#"INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","table","rowKey","op","pos") VALUES (?,?,?,?,?)"#,
                rusqlite::params![version, "items", format!(r#"{{"id":"{id}"}}"#), "s", pos],
            )
            .unwrap();
        }
        Op::Delete { id } => {
            w.execute("DELETE FROM items WHERE id=?", rusqlite::params![id])
                .unwrap();
            w.execute(
                r#"INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","table","rowKey","op","pos") VALUES (?,?,?,?,?)"#,
                rusqlite::params![version, "items", format!(r#"{{"id":"{id}"}}"#), "d", pos],
            )
            .unwrap();
        }
    }
}

/// Direct SQL top-K oracle. SQLite ASC sorts NULLs first, matching zql
/// compare_values (Null orders lowest); DESC sorts NULLs last symmetric.
fn sql_top_k(r: &Connection, limit: usize, desc: bool) -> Vec<String> {
    let dir = if desc { "DESC" } else { "ASC" };
    let sql = format!("SELECT id FROM items ORDER BY val {dir}, id ASC LIMIT {limit}");
    let mut stmt = r.prepare(&sql).unwrap();
    stmt.query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .map(|x| x.unwrap())
        .collect()
}

fn gen_val(rng: &mut Rng, allow_null: bool) -> Option<f64> {
    match rng.below(10) {
        0..=2 if allow_null => None,              // 30% NULL
        0..=2 => Some(4.0),                       // consume the same rng draw
        3..=6 => Some(rng.below(4) as f64),       // heavy duplicates 0..3
        _ => Some(rng.below(1000) as f64 / 10.0), // spread
    }
}

struct FuzzFailure {
    seed: u64,
    variant: String,
    step: usize,
    history: Vec<Op>,
    kind: String,
}

fn run_seed(seed: u64, val_optional: bool, limit: usize, desc: bool) -> Result<(), FuzzFailure> {
    run_seed_verbose(seed, val_optional, limit, desc, false)
}

fn run_seed_verbose(
    seed: u64,
    val_optional: bool,
    limit: usize,
    desc: bool,
    verbose: bool,
) -> Result<(), FuzzFailure> {
    let variant = format!(
        "optional={val_optional} limit={limit} {}",
        if desc { "desc" } else { "asc" }
    );
    let path = db_path(&format!("s{seed}"));
    seed_db(&path);
    let mut rng = Rng::new(seed);

    // Seed initial rows (some before hydration, exercising hydrate-set bounds).
    let writer = Connection::open(&path).unwrap();
    let initial = 2 + rng.below(6) as usize;
    let mut next_id = 0usize;
    let mut live: BTreeSet<String> = BTreeSet::new();
    {
        let v = ver(1);
        for _ in 0..initial {
            let id = format!("r{next_id:04}");
            next_id += 1;
            let val = gen_val(&mut rng, val_optional);
            writer
                .execute(
                    "INSERT INTO items (id, val, _0_version) VALUES (?,?,?)",
                    rusqlite::params![id, val, v],
                )
                .unwrap();
            live.insert(id);
        }
    }

    let mut snap = Snapshotter::new(&path, "", None);
    snap.init().unwrap();
    let curr = snap.current_conn().unwrap();

    let ts = TableSource::new(
        curr.clone(),
        "items",
        source_columns(val_optional),
        vec!["id".to_string()],
    );
    let mut eng = Engine::new(HashMap::from([(
        "items".to_string(),
        vec!["id".to_string()],
    )]));
    eng.register_source(Rc::new(RefCell::new(ts)));
    eng.set_unique_keys("items", vec![vec!["id".to_string()]]);

    // Client materialization: id -> present.
    let client: Rc<RefCell<BTreeMap<String, bool>>> = Rc::new(RefCell::new(BTreeMap::new()));

    {
        let client = client.clone();
        eng.add_queries_streaming(
            &[QuerySpec {
                query_id: "q".into(),
                ast: take_ast(limit, desc),
            }],
            move |rc| {
                if rc.table != "items" {
                    return;
                }
                if let Some(rust_ivm::ivm::data::Value::Str(id)) = rc.row_key.get("id") {
                    client.borrow_mut().insert(id.to_string(), true);
                }
            },
        );
    }

    let syncable: HashMap<String, LiteAndZqlSpec> =
        HashMap::from([("items".to_string(), items_spec(val_optional))]);
    let all_tables: HashSet<String> = HashSet::from(["items".to_string()]);

    let reader = Connection::open(&path).unwrap();
    let mut history: Vec<Op> = Vec::new();

    // Verify hydration matches the SQL oracle before any advance.
    {
        let expect = sql_top_k(&reader, limit, desc);
        let got: Vec<String> = client.borrow().keys().cloned().collect();
        let mut expect_sorted = expect.clone();
        expect_sorted.sort();
        if got != expect_sorted {
            clean(&path);
            return Err(FuzzFailure {
                seed,
                variant,
                step: 0,
                history,
                kind: format!("hydrate mismatch: got {got:?} expect {expect_sorted:?}"),
            });
        }
    }

    let steps = 30 + rng.below(20) as usize;
    for step in 1..=steps {
        let v = ver(step + 1);
        // 1-3 ops per replication step.
        let nops = 1 + rng.below(3) as usize;
        for pos in 0..nops {
            let live_ids: Vec<String> = live.iter().cloned().collect();
            let op = match rng.below(10) {
                0..=3 => {
                    let id = format!("r{next_id:04}");
                    next_id += 1;
                    live.insert(id.clone());
                    Op::Insert {
                        id,
                        val: gen_val(&mut rng, val_optional),
                    }
                }
                4..=6 if !live_ids.is_empty() => {
                    let id = live_ids[rng.below(live_ids.len() as u64) as usize].clone();
                    Op::Update {
                        id,
                        val: gen_val(&mut rng, val_optional),
                    }
                }
                _ if !live_ids.is_empty() => {
                    let id = live_ids[rng.below(live_ids.len() as u64) as usize].clone();
                    live.remove(&id);
                    Op::Delete { id }
                }
                _ => {
                    let id = format!("r{next_id:04}");
                    next_id += 1;
                    live.insert(id.clone());
                    Op::Insert {
                        id,
                        val: gen_val(&mut rng, val_optional),
                    }
                }
            };
            if verbose {
                eprintln!("step {step} op[{pos}]: {op:?}");
            }
            apply_op(&writer, &op, &v, pos as i64);
            history.push(op);
        }
        writer
            .execute(
                r#"UPDATE "_zero.replicationState" SET stateVersion=? WHERE lock='singleton'"#,
                rusqlite::params![v],
            )
            .unwrap();

        // Advance through the REAL path; catch the prod panic.
        let client2 = client.clone();
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            eng.advance_to_head_stream(
                &mut snap,
                &syncable,
                &all_tables,
                |_, _| {},
                |rc| {
                    if rc.table != "items" {
                        return;
                    }
                    let id = match rc.row_key.get("id") {
                        Some(rust_ivm::ivm::data::Value::Str(s)) => s.to_string(),
                        _ => return,
                    };
                    if verbose {
                        eprintln!(
                            "  emit {:?} id={id} row={:?}",
                            rc.change_type,
                            rc.row.as_ref().map(|r| r.get("val").cloned())
                        );
                    }
                    match rc.change_type {
                        ChangeType::Add => {
                            client2.borrow_mut().insert(id, true);
                        }
                        ChangeType::Remove => {
                            client2.borrow_mut().remove(&id);
                        }
                        ChangeType::Edit => {
                            client2.borrow_mut().insert(id, true);
                        }
                        ChangeType::Child => {}
                    }
                },
            )
        }));

        match result {
            Err(p) => {
                let msg = p
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| p.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "<non-string panic>".to_string());
                clean(&path);
                return Err(FuzzFailure {
                    seed,
                    variant,
                    step,
                    history,
                    kind: format!("PANIC during advance: {msg}"),
                });
            }
            Ok(Err(e)) => {
                clean(&path);
                return Err(FuzzFailure {
                    seed,
                    variant,
                    step,
                    history,
                    kind: format!("advance error: {e:?}"),
                });
            }
            Ok(Ok(_)) => {}
        }

        // Compare client materialization vs the SQL top-K oracle.
        let expect = {
            let mut e = sql_top_k(&reader, limit, desc);
            e.sort();
            e
        };
        let got: Vec<String> = client.borrow().keys().cloned().collect();
        if verbose {
            eprintln!("  after step {step}: client={got:?} oracle={expect:?}");
        }
        if got != expect {
            clean(&path);
            return Err(FuzzFailure {
                seed,
                variant,
                step,
                history,
                kind: format!("row mismatch after advance: got {got:?} expect {expect:?}"),
            });
        }
    }

    snap.destroy();
    clean(&path);
    Ok(())
}

#[test]
fn take_bound_fuzz_debug_single() {
    let Ok(cfg) = std::env::var("TAKE_FUZZ_DEBUG") else {
        return;
    };
    let parts: Vec<&str> = cfg.split(',').collect();
    let seed: u64 = parts[0].parse().unwrap();
    let optional: bool = parts[1].parse().unwrap();
    let limit: usize = parts[2].parse().unwrap();
    let desc: bool = parts[3].parse().unwrap();
    if let Err(f) = run_seed_verbose(seed, optional, limit, desc, true) {
        panic!("debug repro failed at step {}: {}", f.step, f.kind);
    }
    eprintln!("debug repro PASSED");
}

#[test]
fn take_bound_fuzz() {
    let seeds: u64 = std::env::var("TAKE_FUZZ_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    let mut failures: Vec<FuzzFailure> = Vec::new();

    for seed in 1..=seeds {
        for &val_optional in &[true, false] {
            for &limit in &[1usize, 2, 3] {
                for &desc in &[false, true] {
                    if let Err(f) = run_seed(seed, val_optional, limit, desc) {
                        eprintln!(
                            "FUZZ FAILURE seed={} [{}] step={} kind={}\n  history ({} ops): {:?}",
                            f.seed,
                            f.variant,
                            f.step,
                            f.kind,
                            f.history.len(),
                            f.history
                        );
                        failures.push(f);
                    }
                }
            }
        }
    }

    assert!(
        failures.is_empty(),
        "take_bound_fuzz: {} failing configurations (see stderr for seeds + op histories)",
        failures.len()
    );
}
