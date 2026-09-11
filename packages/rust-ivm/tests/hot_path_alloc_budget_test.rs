//! Regression: the per-row `Row` map must be built with its capacity
//! reserved, so a wide table does not pay rehash-growth allocations on EVERY
//! fetched row.
//!
//! `TableSource`'s row materialisation (table_source.rs, the
//! `source.row_mat` scope) builds one `FxHashMap<String, Value>` per row — the
//! innermost loop of every hydrate. It started that map at ZERO capacity and
//! inserted column-by-column, so a 40-column row paid four table-growth
//! allocations (hashbrown steps 3 → 7 → 14 → 28 → 56) before it held its
//! columns — per row, on every scan. At the 800K-row scale of the data-differential oracle
//! that is millions of avoidable allocations.
//!
//! WHAT THIS DOES NOT FIX: `map.insert(col.clone(), value)` still heap-copies
//! the column NAME per column per row. Removing that needs `Row`'s key to
//! become `Arc<str>` (interned row keys) — a cross-crate
//! type change, deliberately not bundled here. This test measures exactly that
//! cost too, so it is the gate for that work when it lands: the budget drops
//! to ~0.1 and this assertion is what proves it.
//!
//! HOW IT MEASURES: a counting global allocator (this is its own test binary,
//! so the allocator is private to it) records allocation COUNT across a full
//! scan of the same rows at two column widths. The per-row cost at each width
//! is the slope; the DIFFERENCE between the two slopes isolates the per-column
//! cost from every fixed per-row cost (the `Arc`, the SQL step, the statement
//! cache). Differencing slopes rather than asserting an absolute count keeps
//! this stable against unrelated allocations elsewhere on the fetch path.
//!
//! Mutation test: restore `FxHashMap::default()` in place of
//! `with_capacity_and_hasher(column_names.len(), ...)` and the growth
//! allocations push the measured per-column cost over the budget. The
//! assertion message prints both slopes, so re-measuring is a single run.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rusqlite::Connection;

use rust_ivm::ivm::operator::FetchRequest;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::stream::StreamItem;
use rust_ivm::sqlite::table_source::TableSource;

// ---------------------------------------------------------------------------
// Counting allocator — allocation COUNT, not bytes. `alloc_balance_test.rs`
// counts live bytes to prove nothing LEAKS; this counts calls to prove the hot
// path does not CHURN.
// ---------------------------------------------------------------------------

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static COUNTING: AtomicUsize = AtomicUsize::new(0);

struct CountingAlloc;

// SAFETY: defers every operation to `System`; only a counter is added.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) == 1 {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) == 1 {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

/// Run `f` with the allocation counter on, returning the count and `f`'s value.
fn count_allocs<T>(f: impl FnOnce() -> T) -> (usize, T) {
    ALLOCS.store(0, Ordering::Relaxed);
    COUNTING.store(1, Ordering::Relaxed);
    let out = f();
    COUNTING.store(0, Ordering::Relaxed);
    (ALLOCS.load(Ordering::Relaxed), out)
}

const ROWS: i64 = 300;

fn col_names(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("c{i:03}")).collect()
}

/// Allocations per fetched row for a table with `n_cols` non-PK columns.
///
/// INTEGER columns (not TEXT) so the per-VALUE conversion allocates nothing:
/// what is left to count is the per-column KEY cost and the row map's own
/// growth, which is exactly what this test is about.
fn per_row_allocs(n_cols: usize) -> f64 {
    let names = col_names(n_cols);
    let conn = Connection::open_in_memory().unwrap();
    let ddl = names
        .iter()
        .map(|c| format!("\"{c}\" INTEGER NOT NULL"))
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute_batch(&format!(
        "CREATE TABLE wide (id INTEGER PRIMARY KEY, {ddl});"
    ))
    .unwrap();
    let quoted = names
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = vec!["?"; n_cols + 1].join(",");
    {
        let mut stmt = conn
            .prepare(&format!(
                "INSERT INTO wide (id, {quoted}) VALUES ({placeholders})"
            ))
            .unwrap();
        for r in 0..ROWS {
            let mut vals: Vec<i64> = Vec::with_capacity(n_cols + 1);
            vals.push(r);
            for c in 0..n_cols as i64 {
                vals.push(r * 31 + c);
            }
            stmt.execute(rusqlite::params_from_iter(vals.iter()))
                .unwrap();
        }
    }

    let mut columns: HashMap<String, ColumnType> =
        HashMap::from([("id".to_string(), ColumnType::Number { optional: false })]);
    for c in &names {
        columns.insert(c.clone(), ColumnType::Number { optional: false });
    }

    let mut src = TableSource::new(
        Rc::new(RefCell::new(conn)),
        "wide",
        columns,
        vec!["id".to_string()],
    );
    let input = src.connect(None, None, None, None, None);

    let req = FetchRequest {
        constraint: None,
        multi_constraints: Vec::new(),
        start: None,
        reverse: false,
        limit: None,
    };
    let scan = || -> i64 {
        let mut n = 0;
        for item in input.borrow().fetch(&req) {
            if let StreamItem::Data(node) = item {
                // Touch the row so nothing can be optimised away.
                n += node.row.len() as i64;
            }
        }
        n
    };

    // Warm the statement cache and every lazy static OUTSIDE the count: the
    // first scan compiles the SQL and seeds `stmts_cache`.
    let touched = scan();
    assert_eq!(
        touched,
        ROWS * (n_cols as i64 + 1),
        "the warm-up scan must see every row and every column"
    );

    let (allocs, touched) = count_allocs(scan);
    assert_eq!(touched, ROWS * (n_cols as i64 + 1));
    allocs as f64 / ROWS as f64
}

/// The per-column allocation cost of materialising a row, isolated from every
/// fixed per-row cost by differencing two column widths.
#[test]
fn row_materialisation_pays_no_growth_allocations_per_column() {
    let narrow = per_row_allocs(4);
    let wide = per_row_allocs(40);
    let per_column = (wide - narrow) / 36.0;

    // MEASURED on this tree (aarch64-apple-darwin, hashbrown as
    // vendored): 1.017 per column with the capacity reserved, 1.100 without.
    // The floor of ~1 is the `String` per column that the `String`-keyed `Row`
    // still pays (interning the row keys as `Arc<str>` takes it to ~0);
    // the 0.083 on top is the growth-rehash churn, which is what reserving
    // `column_names.len()` removes. In absolute terms the fix saves 1
    // allocation per row at 4 columns (8.15 → 7.15) and 4 at 40 columns
    // (47.76 → 43.76) — the hashbrown growth steps, exactly.
    //
    // 1.05 sits between the two with ~3% headroom over the measured value and
    // a clear margin under the pre-fix one.
    const BUDGET: f64 = 1.05;
    assert!(
        per_column <= BUDGET,
        "row materialisation costs {per_column:.3} allocations per column \
         (budget {BUDGET:.2}); narrow(4 cols)={narrow:.2}/row, \
         wide(40 cols)={wide:.2}/row. A per-column cost above ~1 means the row \
         map grows as it is filled instead of reserving `column_names.len()` \
         up front (table_source.rs, the `source.row_mat` scope)."
    );
}
