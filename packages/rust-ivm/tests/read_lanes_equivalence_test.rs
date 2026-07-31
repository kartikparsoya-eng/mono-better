//! Serial-vs-parallel equivalence fuzzer for the TableSource read pool.
//!
//! # Why this file exists
//!
//! `DESIGN-read-parallelism.md` gates default-ON on four criteria. Two were
//! already met (full suite green, microbench parallel < serial). The other two
//! were **not reachable with the existing tooling**:
//!
//! * **Criterion 2** — "differential fuzzer with the read-parallel path ON,
//!   byte-identical over ≥50k seeds". The differential fuzzer drives
//!   `src/bin/replay.rs`, which goes through `src/replay.rs` — and that builds
//!   **`MemorySource` only**. Read lanes are a `TableSource`/SQLite feature, so
//!   no number of seeds through that harness can ever exercise them. The
//!   `fuzz-parallel-equiv-loop.mjs` cursor at 221k seeds covers
//!   `RUST_IVM_PARALLEL_HYDRATE`, which is a *different* feature.
//! * **Criterion 4** — the no-leak soak had no test.
//!
//! # The oracle
//!
//! TypeScript is the wrong oracle here and is not needed. The property that
//! matters is **parallel output == serial output**, which is self-checking:
//! two configurations of the same engine over the same data. That is stronger
//! than a TS diff for this feature (it pins emission *order*, not just
//! content) and it has no reference implementation to drift against.
//!
//! Each seed randomizes parent count, per-parent child count (including zero,
//! so childless parents are covered), NULL foreign keys, and value shapes —
//! then asserts the two runs are byte-identical and that the pool returned
//! every connection.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rusqlite::Connection;

use rust_ivm::builder::ast::{Ast, RelatedSubquery};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::Source;
use rust_ivm::snapshotter::Snapshotter;
use rust_ivm::sqlite::table_source::TableSource;
use rust_ivm::streamer::RowChange;

/// Lanes used for the parallel arm. 2 is what the Dockerfile would plausibly
/// ship; 4 is exercised too, since lane count changes how work is chunked.
const LANES: [usize; 2] = [2, 4];

/// xorshift64* — deterministic, so a failing seed is a complete bug report.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407)
            | 1)
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
        if n == 0 { 0 } else { self.next() % n }
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi - lo + 1)
    }
}

fn canon(rc: &RowChange) -> String {
    fn row_str(r: &rust_ivm::ivm::data::Row) -> String {
        let mut kv: Vec<(String, String)> = r
            .iter()
            .map(|(k, v)| (k.clone(), format!("{v:?}")))
            .collect();
        kv.sort();
        format!("{kv:?}")
    }
    format!(
        "ct={:?} q={} t={} key={} row={} hidden={}",
        rc.change_type,
        rc.query_id,
        rc.table,
        row_str(&rc.row_key),
        rc.row.as_ref().map(row_str).unwrap_or_default(),
        rc.is_hidden,
    )
}

/// Build a randomized replica. Returns a description for failure messages.
fn create_replica(path: &str, rng: &mut Rng) -> String {
    let num_parents = rng.range(1, 40) as usize;
    let conn = Connection::open(path).unwrap();
    let _: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    conn.execute_batch(
        "CREATE TABLE \"_zero.replicationState\" (stateVersion TEXT PRIMARY KEY);
         INSERT INTO \"_zero.replicationState\" (stateVersion) VALUES ('v1');
         CREATE TABLE parents (id TEXT PRIMARY KEY, name TEXT);
         CREATE TABLE children (id TEXT PRIMARY KEY, parent_id TEXT, name TEXT);",
    )
    .unwrap();

    let mut total_children = 0usize;
    let mut orphans = 0usize;
    for i in 0..num_parents {
        let pid = format!("p{i}");
        conn.execute(
            "INSERT INTO parents (id, name) VALUES (?, ?)",
            [&pid, &format!("parent-{i}")],
        )
        .unwrap();
        // Zero children is a real and interesting case: the batched leaf fetch
        // must produce nothing for that parent in both arms.
        let kids = rng.below(6) as usize;
        for j in 0..kids {
            let cid = format!("c{i}_{j}");
            conn.execute(
                "INSERT INTO children (id, parent_id, name) VALUES (?, ?, ?)",
                [&cid, &pid, &format!("child-{i}-{j}")],
            )
            .unwrap();
            total_children += 1;
        }
    }
    // Orphan children with a NULL FK — NULL must join to nothing on both paths.
    let n_orphans = rng.below(4) as usize;
    for k in 0..n_orphans {
        conn.execute(
            "INSERT INTO children (id, parent_id, name) VALUES (?, NULL, ?)",
            [&format!("orphan{k}"), &format!("orphan-{k}")],
        )
        .unwrap();
        orphans += 1;
    }
    drop(conn);
    format!("{num_parents} parents, {total_children} children, {orphans} orphans")
}

fn build_join_ast() -> Ast {
    Ast {
        table: "parents".to_string(),
        related: vec![RelatedSubquery {
            subquery: Box::new(Ast {
                table: "children".to_string(),
                ..Default::default()
            }),
            relationship_name: "children".to_string(),
            parent_key: vec!["id".to_string()],
            child_key: vec!["parent_id".to_string()],
            hidden: false,
            system: Some(rust_ivm::ivm::schema::System::Client),
        }],
        ..Default::default()
    }
}

/// Hydrate once. Returns (canonical rows, free connections left in the pool).
///
/// `free_after` is gate criterion 4: every borrowed connection must be back in
/// the pool when the hydrate ends, or the WAL frame stays pinned.
fn hydrate_run(path: &str, pool_lanes: usize) -> (Vec<String>, usize) {
    let pks: HashMap<String, Vec<String>> = [
        ("parents".to_string(), vec!["id".to_string()]),
        ("children".to_string(), vec!["id".to_string()]),
    ]
    .into_iter()
    .collect();

    let mut snap = Snapshotter::with_read_pool(path, "equiv", None, pool_lanes, None);
    snap.init().unwrap();
    let curr_conn = snap.current_conn().unwrap();

    let parent_columns: HashMap<String, ColumnType> = [
        ("id".to_string(), ColumnType::String { optional: false }),
        ("name".to_string(), ColumnType::String { optional: false }),
    ]
    .into_iter()
    .collect();
    let child_columns: HashMap<String, ColumnType> = [
        ("id".to_string(), ColumnType::String { optional: false }),
        (
            "parent_id".to_string(),
            ColumnType::String { optional: true },
        ),
        ("name".to_string(), ColumnType::String { optional: false }),
    ]
    .into_iter()
    .collect();

    let mut parent_source = TableSource::new(
        curr_conn.clone(),
        "parents",
        parent_columns,
        vec!["id".to_string()],
    );
    let mut child_source = TableSource::new(
        curr_conn.clone(),
        "children",
        child_columns,
        vec!["id".to_string()],
    );
    if pool_lanes > 0 {
        parent_source.set_read_pool(snap.read_pool());
        child_source.set_read_pool(snap.read_pool());
    }

    let mut eng = Engine::new(pks);
    eng.register_source(Rc::new(RefCell::new(parent_source)));
    eng.register_source(Rc::new(RefCell::new(child_source)));

    let specs = vec![QuerySpec {
        query_id: "q1".to_string(),
        ast: build_join_ast(),
    }];

    let mut rows: Vec<String> = Vec::new();
    eng.add_queries_streaming(&specs, |rc| rows.push(canon(rc)));

    let free_after = snap.read_pool().free_count();

    // MUST destroy. The operator graph is an `Rc` cycle that is broken only by
    // `destroy()` clearing `Connection.output` (RUST-DRIFT-LEDGER R2) — dropping
    // the Engine is not enough. Without this the sources stay alive, so their
    // pooled SQLite connections are never closed, and a long soak dies at
    // `kern.maxfilesperproc` (~61440 fds ≈ 3000 seeds here) with the very
    // misleading "unable to open database file".
    eng.destroy();

    (rows, free_after)
}

fn tmp_db(tag: &str, seed: u64) -> String {
    let dir = std::env::temp_dir();
    dir.join(format!(
        "read-lanes-equiv-{tag}-{seed}-{}.db",
        std::process::id()
    ))
    .to_string_lossy()
    .to_string()
}

fn cleanup(path: &str) {
    for suffix in ["", "-wal", "-shm", "-wal2"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
}

/// One seed: serial vs every lane count, byte-identical, no leaked connections.
fn run_seed(seed: u64) {
    let mut rng = Rng::new(seed);
    let path = tmp_db("s", seed);
    cleanup(&path);
    let shape = create_replica(&path, &mut rng);

    let (serial, _) = hydrate_run(&path, 0);

    for lanes in LANES {
        let (parallel, free_after) = hydrate_run(&path, lanes);
        assert_eq!(
            serial.len(),
            parallel.len(),
            "seed {seed} ({shape}), lanes={lanes}: row COUNT differs \
             (serial {}, parallel {})",
            serial.len(),
            parallel.len()
        );
        for (i, (s, p)) in serial.iter().zip(parallel.iter()).enumerate() {
            assert_eq!(
                s, p,
                "seed {seed} ({shape}), lanes={lanes}: row {i} differs.\n  \
                 serial:   {s}\n  parallel: {p}"
            );
        }
        assert_eq!(
            free_after, lanes,
            "seed {seed} ({shape}), lanes={lanes}: pool leaked connections \
             — {free_after} free, expected {lanes}. A borrowed connection left \
             behind keeps the WAL frame pinned (gate criterion 4)."
        );
    }
    cleanup(&path);
}

/// Default run: enough seeds to be a useful PR gate without being slow.
/// For the ≥50k the design doc asks for, run the `soak` test below with
/// `READ_LANES_SEEDS=50000 cargo test --release --test read_lanes_equivalence_test -- --ignored --nocapture`.
#[test]
fn read_lanes_match_serial_over_seeds() {
    for seed in 0..120 {
        run_seed(seed);
    }
}

/// Regression pins for the shapes most likely to diverge, kept separate so a
/// failure names the shape rather than a seed number.
#[test]
fn read_lanes_match_serial_for_degenerate_shapes() {
    // Every parent childless — the batched leaf fetch returns nothing at all.
    let path = tmp_db("empty", 0);
    cleanup(&path);
    let conn = Connection::open(&path).unwrap();
    let _: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    conn.execute_batch(
        "CREATE TABLE \"_zero.replicationState\" (stateVersion TEXT PRIMARY KEY);
         INSERT INTO \"_zero.replicationState\" (stateVersion) VALUES ('v1');
         CREATE TABLE parents (id TEXT PRIMARY KEY, name TEXT);
         CREATE TABLE children (id TEXT PRIMARY KEY, parent_id TEXT, name TEXT);
         INSERT INTO parents VALUES ('p0','a'),('p1','b');",
    )
    .unwrap();
    drop(conn);
    let (serial, _) = hydrate_run(&path, 0);
    let (parallel, free_after) = hydrate_run(&path, 2);
    assert_eq!(serial, parallel, "childless parents diverged");
    assert_eq!(free_after, 2, "pool leaked on the empty-child path");
    cleanup(&path);

    // A single parent with many children — one oversized batch, one lane busy.
    let path = tmp_db("wide", 0);
    cleanup(&path);
    let conn = Connection::open(&path).unwrap();
    let _: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    conn.execute_batch(
        "CREATE TABLE \"_zero.replicationState\" (stateVersion TEXT PRIMARY KEY);
         INSERT INTO \"_zero.replicationState\" (stateVersion) VALUES ('v1');
         CREATE TABLE parents (id TEXT PRIMARY KEY, name TEXT);
         CREATE TABLE children (id TEXT PRIMARY KEY, parent_id TEXT, name TEXT);
         INSERT INTO parents VALUES ('p0','a');",
    )
    .unwrap();
    for j in 0..500 {
        conn.execute(
            "INSERT INTO children (id, parent_id, name) VALUES (?, 'p0', ?)",
            [&format!("c{j}"), &format!("n{j}")],
        )
        .unwrap();
    }
    drop(conn);
    let (serial, _) = hydrate_run(&path, 0);
    let (parallel, free_after) = hydrate_run(&path, 4);
    assert_eq!(serial, parallel, "wide fan-out diverged");
    assert_eq!(free_after, 4, "pool leaked on the wide fan-out path");
    cleanup(&path);
}

/// The long soak the design doc's criterion 2 actually asks for.
///
/// Ignored by default because 50k seeds is minutes, not seconds. Run it before
/// flipping `RUST_IVM_READ_LANES` on by default:
///
/// ```text
/// READ_LANES_SEEDS=50000 cargo test --release \
///   --test read_lanes_equivalence_test -- --ignored --nocapture
/// ```
#[test]
#[ignore]
fn read_lanes_soak() {
    let n: u64 = std::env::var("READ_LANES_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000);
    let start = std::time::Instant::now();
    for seed in 0..n {
        run_seed(seed);
        if seed > 0 && seed % 500 == 0 {
            eprintln!("  {seed}/{n} seeds, {:?} elapsed", start.elapsed());
        }
    }
    eprintln!("read-lanes soak: {n} seeds clean in {:?}", start.elapsed());
}

/// Scaled read-lane benchmark — "how much do lanes actually buy us?"
///
/// The existing `read_parallel_bench_test` runs 200x5 = 1200 rows. Production
/// tail latency lives on *whale* hydrates (the ART baseline's
/// `zero_sync_hydration_time` p99 is 5380 ms against a p50 of 20.8 ms — a 258x
/// spread, i.e. a handful of enormous queries). So the number that matters is
/// the speedup at whale scale, not at 1200 rows.
///
/// ```text
/// LANES_BENCH_PARENTS=2600 cargo test --release \
///   --test read_lanes_equivalence_test lanes_scaling -- --ignored --nocapture
/// ```
#[test]
#[ignore]
fn lanes_scaling_bench() {
    let parents: usize = std::env::var("LANES_BENCH_PARENTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2600);
    let kids: usize = std::env::var("LANES_BENCH_CHILDREN")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);

    let path = tmp_db("bench", 0);
    cleanup(&path);
    let conn = Connection::open(&path).unwrap();
    let _: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    conn.execute_batch(
        "CREATE TABLE \"_zero.replicationState\" (stateVersion TEXT PRIMARY KEY);
         INSERT INTO \"_zero.replicationState\" (stateVersion) VALUES ('v1');
         CREATE TABLE parents (id TEXT PRIMARY KEY, name TEXT);
         CREATE TABLE children (id TEXT PRIMARY KEY, parent_id TEXT, name TEXT);",
    )
    .unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    for i in 0..parents {
        tx.execute(
            "INSERT INTO parents (id, name) VALUES (?, ?)",
            [&format!("p{i}"), &format!("parent-{i}")],
        )
        .unwrap();
        for j in 0..kids {
            tx.execute(
                "INSERT INTO children (id, parent_id, name) VALUES (?, ?, ?)",
                [
                    &format!("c{i}_{j}"),
                    &format!("p{i}"),
                    &format!("child-{i}-{j}"),
                ],
            )
            .unwrap();
        }
    }
    tx.commit().unwrap();
    drop(conn);

    let total_rows = parents * (1 + kids);
    eprintln!("\n  {parents} parents x {kids} children = ~{total_rows} rows\n");

    // Warm the page cache so the first configuration is not penalised.
    let _ = hydrate_run(&path, 0);

    let mut baseline = std::time::Duration::ZERO;
    for lanes in [0usize, 2, 4, 8] {
        // Best of 3 — SQLite I/O is noisy, and we want the achievable number.
        let mut best = std::time::Duration::MAX;
        let mut rows = 0;
        for _ in 0..3 {
            let t = std::time::Instant::now();
            let (r, _) = hydrate_run(&path, lanes);
            let d = t.elapsed();
            if d < best {
                best = d;
            }
            rows = r.len();
        }
        if lanes == 0 {
            baseline = best;
            eprintln!("  lanes=0 (serial) : {best:?}   ({rows} rows)   baseline");
        } else {
            let pct =
                (baseline.as_secs_f64() - best.as_secs_f64()) / baseline.as_secs_f64() * 100.0;
            eprintln!("  lanes={lanes}          : {best:?}   ({rows} rows)   {pct:+.1}% vs serial");
        }
    }
    eprintln!();
    cleanup(&path);
}
