//! Hydrate / advance wall-time on the clone-density workload
//! (`tests/support/clone_density_fixture.rs`): a `related` join, a correlated
//! EXISTS and a `related` + `limit`, over MemorySource (operator-graph
//! overhead only) and TableSource (the SQLite fetch path). The companion
//! `tests/clone_density_test.rs` counts allocations for the same shapes.
//!
//! Run like the tests (rust-ivm links the static WAL2 SQLite):
//!   SQLITE3_LIB_DIR=$(scripts/build-wal2-static-lib.sh) SQLITE3_STATIC=1 \
//!   SQLITE3_INCLUDE_DIR=$SQLITE3_LIB_DIR cargo bench --bench hydrate_bench
//! Pin a baseline with `-- --save-baseline <name>` and compare later runs.

#[path = "../tests/support/clone_density_fixture.rs"]
mod fixture;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use fixture::{Fixture, SourceKind, exists_ast, related_ast, take_ast};

const N_ISSUES: usize = 1_000;

fn bench_kind(c: &mut Criterion, kind: SourceKind) {
    let tag = format!("{kind:?}");
    let mut fx = Fixture::build(kind, N_ISSUES);

    c.bench_function(&format!("hydrate/related/{tag}"), |b| {
        b.iter(|| {
            let rows = fx.hydrate("related", related_ast());
            fx.remove("related");
            rows
        })
    });
    c.bench_function(&format!("hydrate/exists/{tag}"), |b| {
        b.iter(|| {
            let rows = fx.hydrate("exists", exists_ast());
            fx.remove("exists");
            rows
        })
    });
    c.bench_function(&format!("hydrate/take100/{tag}"), |b| {
        b.iter(|| {
            let rows = fx.hydrate("take", take_ast(100));
            fx.remove("take");
            rows
        })
    });

    // Advance keeps a live `related` query and streams new issues through it.
    // Each iteration gets a fresh fixture so the working set stays fixed.
    c.bench_function(&format!("advance/related+20issues/{tag}"), |b| {
        b.iter_batched(
            || {
                let mut fx = Fixture::build(kind, N_ISSUES);
                fx.hydrate("related", related_ast());
                fx
            },
            |mut fx| fx.advance_new_issues(20),
            BatchSize::LargeInput,
        )
    });
}

fn bench_memory(c: &mut Criterion) {
    bench_kind(c, SourceKind::Memory);
}

fn bench_sqlite(c: &mut Criterion) {
    bench_kind(c, SourceKind::Sqlite);
}

criterion_group!(benches, bench_memory, bench_sqlite);
criterion_main!(benches);
