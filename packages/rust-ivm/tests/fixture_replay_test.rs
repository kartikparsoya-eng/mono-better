// Replays every agentic/fixtures/*.input.json through the Rust engine and
// compares against the TS-oracle-generated .expected.json byte-for-byte
// (after canonicalization). A mismatch = Rust-vs-TS divergence; never "fix"
// by editing the fixture or the expected file.
//
// fixtures/regressions/ is deliberately NOT scanned here: those are pending
// divergences owned by divergence-fix tasks; once fixed, the pair is promoted
// into fixtures/ and becomes a permanent regression test.

use rust_ivm::replay::{assert_matches, run_fixture_file};
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn fixture_replay() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("agentic/fixtures");
    if !dir.exists() {
        eprintln!("fixture_replay: no fixtures dir at {}", dir.display());
        return;
    }
    let mut inputs: Vec<PathBuf> = fs::read_dir(&dir)
        .expect("read fixtures dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .map(|n| n.to_string_lossy().ends_with(".input.json"))
                .unwrap_or(false)
        })
        .collect();
    inputs.sort();

    let mut failures: Vec<String> = Vec::new();
    let mut ran = 0usize;
    for input in &inputs {
        let expected_path = input
            .to_string_lossy()
            .replace(".input.json", ".expected.json");
        if !Path::new(&expected_path).exists() {
            eprintln!("SKIP (no expected yet): {}", input.display());
            continue;
        }
        let actual = run_fixture_file(input.to_str().expect("utf8 path"));
        let expected: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&expected_path).expect("read expected"))
                .expect("parse expected");
        ran += 1;
        if let Err(msg) = assert_matches(&actual, &expected) {
            failures.push(format!("{}\n{}", input.display(), msg));
        }
    }
    eprintln!(
        "fixture_replay: {ran} fixtures compared, {} diverged",
        failures.len()
    );
    assert!(
        failures.is_empty(),
        "Rust-vs-TS divergence in {} fixture(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
