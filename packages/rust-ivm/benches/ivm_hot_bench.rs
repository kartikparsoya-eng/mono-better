//! Axis-8 microbenchmarks (PROFILING.md) for the two innermost IVM hot units.
//! "1:1 behavior" is gated by the test suite; criterion gates "1:1 performance"
//! at the unit level. Run `cargo bench`; pin a baseline with
//! `cargo bench -- --save-baseline <name>` and compare later runs against it.
//!
//! NOTE: rust-ivm statically links the WAL2 SQLite, so run with the same env the
//! tests use, e.g.:
//!   SQLITE3_LIB_DIR=$(scripts/build-wal2-static-lib.sh) SQLITE3_STATIC=1 \
//!   SQLITE3_INCLUDE_DIR=$SQLITE3_LIB_DIR cargo bench

use std::sync::Arc;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rustc_hash::FxHashMap;

use rust_ivm::engine::row_signature_unit;
use rust_ivm::ivm::data::{SortOrder, Value, make_comparator};

fn row(pairs: &[(&str, Value)]) -> Arc<FxHashMap<String, Value>> {
    Arc::new(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

// row_signature_unit — h64(rowIDString(id)) — runs once per row on every
// advance/emit, and its exact hash must match the TS process (the stage_e
// FxHasher-vs-h64 regression lived here). A 2-column key is the common shape.
fn bench_row_signature(c: &mut Criterion) {
    let key = row(&[
        ("id", Value::Str(Arc::from("73if0bb7s"))),
        ("orgID", Value::F64(42.0)),
    ]);
    c.bench_function("row_signature_unit/2col", |b| {
        b.iter(|| row_signature_unit(black_box("issue"), black_box(&key)))
    });
}

// make_comparator + a full-key compare — the innermost primitive of every
// binary_search / partition_point on a sorted view. Bench the worst case: two
// rows that agree on the first sort column and differ on the second, so the
// comparator iterates the whole key.
fn bench_comparator(c: &mut Criterion) {
    let order: SortOrder = Arc::new(vec![
        ["orgID".to_string(), "asc".to_string()],
        ["id".to_string(), "asc".to_string()],
    ]);
    let cmp = make_comparator(order.clone(), false);
    let a = row(&[
        ("orgID", Value::F64(1.0)),
        ("id", Value::Str(Arc::from("aaa"))),
    ]);
    let b = row(&[
        ("orgID", Value::F64(1.0)),
        ("id", Value::Str(Arc::from("bbb"))),
    ]);

    c.bench_function("make_comparator/build", |bench| {
        bench.iter(|| make_comparator(black_box(order.clone()), false))
    });
    c.bench_function("comparator/compare_2col", |bench| {
        bench.iter(|| cmp(black_box(&a), black_box(&b)))
    });
}

criterion_group!(benches, bench_row_signature, bench_comparator);
criterion_main!(benches);
