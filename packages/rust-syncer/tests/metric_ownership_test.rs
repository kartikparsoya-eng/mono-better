//! Metric ownership is 1:1 (AGENTS.md rule 9 applied to instruments): every
//! TS instrument has exactly ONE rust emitter, in the crate/file that ports
//! the TS class owning it. TS registers `cvr.load_attempts`,
//! `cvr.load_duration` and `cvr.flush_attempts` once, on `CVRStore`
//! (cvr-store.ts:207-217), and bumps them only in `#recordLoad`
//! (cvr-store.ts:308-311) and `flush` (cvr-store.ts:1254-1264). rust-cvr
//! ports both sites (otel_metrics.rs record_load / record_flush_attempt).
//!
//! Caught 2026-09-07 at the collector: rust reported 12 `cvr_load_attempts`
//! against 6 `cvr_load_duration` samples for the same window because
//! rust-syncer registered a SECOND `zero.sync.cvr.load_attempts` counter and
//! bumped it around the same `CVRStore::load` call — every load counted twice.
//! Pre-fix this test failed with `zero.sync.cvr.load_attempts` built in 2
//! places; proven by running it before the removal.

use std::fs;
use std::path::{Path, PathBuf};

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display())) {
        let p = entry.unwrap().path();
        if p.is_dir() {
            rust_sources(&p, out);
        } else if p.extension().is_some_and(|e| e == "rs") {
            out.push(p);
        }
    }
}

#[test]
fn every_cvr_store_instrument_has_exactly_one_rust_emitter() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .unwrap();
    let packages = manifest.parent().unwrap().to_path_buf();
    let mut files = Vec::new();
    rust_sources(&manifest.join("src"), &mut files);
    rust_sources(&manifest.join("../rust-cvr/src"), &mut files);
    assert!(
        files.len() > 20,
        "expected the rust-syncer + rust-cvr source trees"
    );

    // TS instrument name -> the single rust file allowed to build it
    // (the port of cvr-store.ts, where TS registers all three).
    let owned_by_cvr_store = [
        "zero.sync.cvr.load_attempts",
        "zero.sync.cvr.load_duration",
        "zero.sync.cvr.flush_attempts",
    ];
    for name in owned_by_cvr_store {
        let needle = format!("\"{name}\"");
        let builders: Vec<String> = files
            .iter()
            .filter(|f| fs::read_to_string(f).unwrap().contains(&needle))
            .map(|f| {
                f.canonicalize()
                    .unwrap()
                    .strip_prefix(&packages)
                    .unwrap()
                    .display()
                    .to_string()
            })
            .collect();
        assert_eq!(
            builders,
            vec!["rust-cvr/src/otel_metrics.rs".to_string()],
            "{name}: TS builds it once on CVRStore (cvr-store.ts:207-217); \
             rust must build it exactly once, in the cvr-store port"
        );
    }
}
