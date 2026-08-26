//! Axis-8 microbenchmark exemplar (PROFILING.md): the version-string codec —
//! on the hot path of every cookie parse, poke assembly, and CVR row write.
//! "1:1 behavior" is gated elsewhere; criterion gates "1:1 performance" at
//! the unit level (run `cargo bench` here; compare with `--save-baseline`).
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rust_cvr::schema::types::{CVRVersion, maybe_version_string, version_string};

fn bench_version_codec(c: &mut Criterion) {
    let v = CVRVersion {
        state_version: "73if0bb7s".to_string(),
        config_version: Some(42),
    };
    let encoded = version_string(&v);
    c.bench_function("version_string", |b| {
        b.iter(|| version_string(black_box(&v)))
    });
    c.bench_function("maybe_version_string", |b| {
        b.iter(|| maybe_version_string(black_box(&encoded)))
    });
}

criterion_group!(benches, bench_version_codec);
criterion_main!(benches);
