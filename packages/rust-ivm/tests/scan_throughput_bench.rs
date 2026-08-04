//! Isolates raw TableSource scan throughput: how long it takes to turn SQLite
//! rows into `Row` values, with no join/operator work on top.
//!
//! Run: cargo test --release --test scan_throughput_bench -- --nocapture --ignored
//! Env: SCAN_ROWS (default 200_000), SCAN_ITERS (default 12).

use rusqlite::Connection;
use rust_ivm::ivm::schema::ColumnType;

use rust_ivm::sqlite::table_source::TableSource;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Instant;

fn envn<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(d)
}

#[test]
#[ignore]
fn scan_throughput() {
    let rows: usize = envn("SCAN_ROWS", 200_000usize);
    let iters: usize = envn("SCAN_ITERS", 12usize);
    let path = "/tmp/rust-ivm-scan-bench.db";
    for suf in ["", "-wal", "-wal2", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suf));
    }

    let conn = Connection::open(path).unwrap();
    let _: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    conn.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY, a TEXT, b TEXT, c TEXT, d TEXT);")
        .unwrap();
    {
        let tx = conn.unchecked_transaction().unwrap();
        let mut stmt = tx
            .prepare("INSERT INTO t (id,a,b,c,d) VALUES (?,?,?,?,?)")
            .unwrap();
        for i in 0..rows {
            stmt.execute([
                &format!("id{i}"),
                &format!("a{i}"),
                &format!("b{i}"),
                &format!("c{i}"),
                &format!("d{i}"),
            ])
            .unwrap();
        }
        drop(stmt);
        tx.commit().unwrap();
    }
    drop(conn);

    let columns: HashMap<String, ColumnType> = ["id", "a", "b", "c", "d"]
        .into_iter()
        .map(|c| (c.to_string(), ColumnType::String { optional: false }))
        .collect();

    let conn = Rc::new(RefCell::new(Connection::open(path).unwrap()));
    let mut src = TableSource::new(conn, "t", columns, vec!["id".to_string()]);
    let input = src.connect(None, None, None, None);

    let mut best = std::time::Duration::MAX;
    let mut counted = 0usize;
    for _ in 0..iters {
        let start = Instant::now();
        let n = {
            let stream = input.borrow().fetch(&Default::default());
            rust_ivm::ivm::stream::skip_yields(stream).count()
        };
        let el = start.elapsed();
        if el < best {
            best = el;
        }
        counted = n;
    }
    assert_eq!(counted, rows, "scanned row count");
    let per_row = best.as_nanos() as f64 / rows as f64;

    // Floor A: SQLite stepping alone — no column extraction at all.
    let raw = Connection::open(path).unwrap();
    let mut step_best = std::time::Duration::MAX;
    for _ in 0..iters {
        let mut stmt = raw.prepare("SELECT id,a,b,c,d FROM t").unwrap();
        let start = Instant::now();
        let mut rs = stmt.query([]).unwrap();
        let mut n = 0usize;
        while rs.next().unwrap().is_some() {
            n += 1;
        }
        let el = start.elapsed();
        assert_eq!(n, rows);
        step_best = step_best.min(el);
    }

    // Floor B: stepping + pulling all 5 columns out as rusqlite Values
    // (this is where TEXT columns get their String allocation), but WITHOUT
    // building our `Arc<FxHashMap<String, Value>>` row.
    let mut get_best = std::time::Duration::MAX;
    for _ in 0..iters {
        let mut stmt = raw.prepare("SELECT id,a,b,c,d FROM t").unwrap();
        let start = Instant::now();
        let mut rs = stmt.query([]).unwrap();
        let mut n = 0usize;
        while let Some(r) = rs.next().unwrap() {
            for i in 0..5 {
                std::hint::black_box(r.get::<usize, rusqlite::types::Value>(i).unwrap());
            }
            n += 1;
        }
        let el = start.elapsed();
        assert_eq!(n, rows);
        get_best = get_best.min(el);
    }

    let step_ns = step_best.as_nanos() as f64 / rows as f64;
    let get_ns = get_best.as_nanos() as f64 / rows as f64;
    eprintln!("\n=== {rows} rows x 5 cols, best of {iters} ===");
    eprintln!("  A. sqlite step only            {step_ns:7.0} ns/row");
    eprintln!(
        "  B. step + get 5 Values         {get_ns:7.0} ns/row   (+{:.0})",
        get_ns - step_ns
    );
    eprintln!(
        "  C. full TableSource -> Row     {per_row:7.0} ns/row   (+{:.0} for Row repr)",
        per_row - get_ns
    );
    eprintln!(
        "  => Row representation is {:.0}% of scan cost",
        (per_row - get_ns) / per_row * 100.0
    );

    // ---- Cost of statement PREPARATION, the per-fetch fixed cost ----
    // A nested-loop join issues one fetch per outer row, so this is paid
    // once per outer row. Simulates the inner-side lookup.
    raw.execute_batch("CREATE INDEX IF NOT EXISTS t_a ON t(a);")
        .unwrap();
    let lookup = "SELECT id,a,b,c,d FROM t WHERE a = ? ORDER BY id";
    const N: usize = 20_000;

    let mut prep_best = std::time::Duration::MAX;
    for _ in 0..5 {
        let start = Instant::now();
        for i in 0..N {
            let mut s = raw.prepare(lookup).unwrap();
            let mut r = s.query([&format!("a{i}")]).unwrap();
            while r.next().unwrap().is_some() {}
        }
        prep_best = prep_best.min(start.elapsed());
    }

    let mut cached_best = std::time::Duration::MAX;
    for _ in 0..5 {
        let start = Instant::now();
        for i in 0..N {
            let mut s = raw.prepare_cached(lookup).unwrap();
            let mut r = s.query([&format!("a{i}")]).unwrap();
            while r.next().unwrap().is_some() {}
        }
        cached_best = cached_best.min(start.elapsed());
    }

    let p = prep_best.as_nanos() as f64 / N as f64;
    let c = cached_best.as_nanos() as f64 / N as f64;
    eprintln!("\n=== per-fetch statement cost ({N} indexed lookups, best of 5) ===");
    eprintln!("  prepare()          {p:7.0} ns/fetch");
    eprintln!("  prepare_cached()   {c:7.0} ns/fetch");
    eprintln!("  => caching saves {:.0} ns/fetch ({:.1}x)", p - c, p / c);
}
