//! take_bound_fuzz_test.rs — seeded fuzz of the Take/LIMIT and Skip/cursor
//! operators over the REAL advance path (Snapshotter + TableSource +
//! advance_to_head_stream), hunting the live-prod Take bound divergences:
//!   take.rs:545 "Take: boundNode must be found during fetch"
//!   take.rs:702 "Bound should be set"
//! observed on zero-02-rust (2026-08-12) and preprod hf2cg, plus the SILENT
//! sibling: wrong LIMIT/page rows without any panic.
//!
//! Shape: one table with a NULLABLE REAL order-by column (NULLs + duplicate
//! values force bounds/cursors onto degenerate sort keys), random upstream
//! insert/update/delete sequences applied as genuine replication steps
//! (changeLog2 + stateVersion bump), advanced through the engine.
//!
//! Dimensions per seed:
//!   - optionality CONTRACT variants (declared non-optional => data never
//!     contains NULL): optional:true with NULL-heavy data (the prod shape —
//!     requires computeZqlSpecs to populate SchemaValue.optional, the
//!     2026-08-12 root-cause fix in zero-cache/src/db/lite-tables.ts) and
//!     optional:false with NULL-free data.
//!   - LIMIT 1..=3 (Take) and no-limit (pure Skip).
//!   - asc/desc ordering.
//!   - client cursors (Skip): none / inclusive / exclusive, with cursor
//!     values covering NULL, a duplicate-heavy value, and a mid-range value;
//!     cursor id may refer to a row that gets deleted (persisted-cursor edge).
//!
//! Failure modes caught:
//!   1. Panic inside advance (the prod signature) — reported with seed +
//!      step + full op history for deterministic replay via
//!      TAKE_FUZZ_DEBUG="seed,optional,limit,desc,cursor" single-config
//!      verbose mode; TAKE_FUZZ_SEEDS scales depth.
//!   2. Silent wrong rows: client materialization from emitted RowChanges is
//!      compared after every advance against an in-test oracle implementing
//!      the zql TOTAL ORDER (NULL sorts lowest, id tiebreak) + cursor filter
//!      + limit truncation over a direct full-table SQL read.

use std::cell::RefCell;
use std::cmp::Ordering as CmpOrd;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::sync::Arc;

use rusqlite::Connection;
use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{Ast, Bound, OrderPart};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::change::ChangeType;
use rust_ivm::ivm::data::Value;
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

/// A Skip cursor: (order-by value, row id, exclusive).
#[derive(Clone, Copy, Debug, PartialEq)]
struct Cursor {
    val: Option<f64>,
    exclusive: bool,
}

const CURSOR_ID: &str = "r0001";

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

fn query_ast(limit: Option<usize>, desc: bool, cursor: Option<Cursor>) -> Ast {
    let start = cursor.map(|c| {
        let mut row = FxHashMap::default();
        row.insert(
            "val".to_string(),
            c.val.map(Value::F64).unwrap_or(Value::Null),
        );
        row.insert("id".to_string(), Value::Str(CURSOR_ID.into()));
        Bound {
            row: Arc::new(row),
            exclusive: c.exclusive,
        }
    });
    Ast {
        schema: None,
        table: "items".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit,
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
        start,
    }
}

/// Apply one op to the db as a genuine replication step. changeLog2 mirrors
/// prod semantics: UNIQUE("table","rowKey") + INSERT OR REPLACE — the log
/// tracks the LAST op per row (duplicate same-row entries are illegal input).
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

/// zql total order over (val, id): NULL sorts lowest; asc reverses to desc on
/// val only (id tiebreak stays asc, matching the query's order_by).
fn total_order(a: &(Option<f64>, String), b: &(Option<f64>, String), desc: bool) -> CmpOrd {
    let vcmp = match (a.0, b.0) {
        (None, None) => CmpOrd::Equal,
        (None, Some(_)) => CmpOrd::Less,
        (Some(_), None) => CmpOrd::Greater,
        (Some(x), Some(y)) => x.partial_cmp(&y).unwrap(),
    };
    let vcmp = if desc { vcmp.reverse() } else { vcmp };
    vcmp.then_with(|| a.1.cmp(&b.1))
}

/// In-test oracle: full-table read, zql total order, cursor filter, limit.
fn oracle_rows(
    r: &Connection,
    limit: Option<usize>,
    desc: bool,
    cursor: Option<Cursor>,
) -> Vec<String> {
    let mut stmt = r.prepare("SELECT id, val FROM items").unwrap();
    let mut rows: Vec<(Option<f64>, String)> = stmt
        .query_map([], |row| {
            Ok((row.get::<_, Option<f64>>(1)?, row.get::<_, String>(0)?))
        })
        .unwrap()
        .map(|x| x.unwrap())
        .collect();
    rows.sort_by(|a, b| total_order(a, b, desc));
    if let Some(c) = cursor {
        let cur = (c.val, CURSOR_ID.to_string());
        rows.retain(|row| match total_order(row, &cur, desc) {
            CmpOrd::Greater => true,
            CmpOrd::Equal => !c.exclusive,
            CmpOrd::Less => false,
        });
    }
    if let Some(k) = limit {
        rows.truncate(k);
    }
    let mut ids: Vec<String> = rows.into_iter().map(|(_, id)| id).collect();
    ids.sort();
    ids
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

fn run_seed(
    seed: u64,
    val_optional: bool,
    limit: Option<usize>,
    desc: bool,
    cursor: Option<Cursor>,
) -> Result<(), FuzzFailure> {
    run_seed_verbose(seed, val_optional, limit, desc, cursor, false)
}

fn run_seed_verbose(
    seed: u64,
    val_optional: bool,
    limit: Option<usize>,
    desc: bool,
    cursor: Option<Cursor>,
    verbose: bool,
) -> Result<(), FuzzFailure> {
    let variant = format!(
        "optional={val_optional} limit={limit:?} {} cursor={cursor:?}",
        if desc { "desc" } else { "asc" }
    );
    let path = db_path(&format!("s{seed}"));
    seed_db(&path);
    let mut rng = Rng::new(seed);

    // Seed initial rows (some before hydration, exercising hydrate-set
    // bounds; r0001 always exists initially so cursors reference a real row
    // that later ops may delete — the persisted-cursor edge).
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
                ast: query_ast(limit, desc, cursor),
            }],
            move |rc| {
                if rc.table != "items" {
                    return;
                }
                if let Some(Value::Str(id)) = rc.row_key.get("id") {
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

    // Verify hydration matches the oracle before any advance.
    {
        let expect = oracle_rows(&reader, limit, desc, cursor);
        let got: Vec<String> = client.borrow().keys().cloned().collect();
        if got != expect {
            clean(&path);
            return Err(FuzzFailure {
                seed,
                variant,
                step: 0,
                history,
                kind: format!("hydrate mismatch: got {got:?} expect {expect:?}"),
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
                        Some(Value::Str(s)) => s.to_string(),
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

        // Compare client materialization vs the oracle.
        let expect = oracle_rows(&reader, limit, desc, cursor);
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

/// Cursor variants exercised for each (seed, optionality, direction):
/// NULL-valued cursor (the Skip form of the prod bug), a duplicate-heavy
/// value, and a mid-range value; alternating inclusive/exclusive.
fn cursor_variants(seed: u64, val_optional: bool) -> Vec<Cursor> {
    let exclusive = seed.is_multiple_of(2);
    let mut out = vec![
        Cursor {
            val: Some(1.0),
            exclusive,
        },
        Cursor {
            val: Some(50.0),
            exclusive: !exclusive,
        },
    ];
    if val_optional {
        out.push(Cursor {
            val: None,
            exclusive,
        });
    }
    out
}

#[test]
fn take_bound_fuzz_debug_single() {
    let Ok(cfg) = std::env::var("TAKE_FUZZ_DEBUG") else {
        return;
    };
    // "seed,optional,limit(0=none),asc|desc[,cursorval|null,incl|excl]"
    let parts: Vec<&str> = cfg.split(',').collect();
    let seed: u64 = parts[0].parse().unwrap();
    let optional: bool = parts[1].parse().unwrap();
    let limit: Option<usize> = match parts[2].parse::<usize>().unwrap() {
        0 => None,
        n => Some(n),
    };
    let desc: bool = parts[3] == "desc";
    let cursor = if parts.len() > 5 {
        Some(Cursor {
            val: if parts[4] == "null" {
                None
            } else {
                Some(parts[4].parse().unwrap())
            },
            exclusive: parts[5] == "excl",
        })
    } else {
        None
    };
    if let Err(f) = run_seed_verbose(seed, optional, limit, desc, cursor, true) {
        panic!("debug repro failed at step {}: {}", f.step, f.kind);
    }
    eprintln!("debug repro PASSED");
}

#[test]
fn take_bound_fuzz() {
    let seeds: u64 = std::env::var("TAKE_FUZZ_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(80);
    let mut failures: Vec<FuzzFailure> = Vec::new();

    for seed in 1..=seeds {
        for &val_optional in &[true, false] {
            for &desc in &[false, true] {
                // Take-only configs (the original prod shape).
                for &limit in &[1usize, 2, 3] {
                    if let Err(f) = run_seed(seed, val_optional, Some(limit), desc, None) {
                        report(&f);
                        failures.push(f);
                    }
                }
                // Skip configs: pure cursor (no limit) + cursor-under-limit.
                for c in cursor_variants(seed, val_optional) {
                    for &limit in &[None, Some(2usize)] {
                        if let Err(f) = run_seed(seed, val_optional, limit, desc, Some(c)) {
                            report(&f);
                            failures.push(f);
                        }
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

fn report(f: &FuzzFailure) {
    eprintln!(
        "FUZZ FAILURE seed={} [{}] step={} kind={}\n  history ({} ops): {:?}",
        f.seed,
        f.variant,
        f.step,
        f.kind,
        f.history.len(),
        f.history
    );
}
