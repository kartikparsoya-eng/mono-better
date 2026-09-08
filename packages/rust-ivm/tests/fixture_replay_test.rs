// Replays every agentic/fixtures/*.input.json through the Rust engine and
// compares against the TS-oracle-generated .expected.json byte-for-byte
// (after canonicalization). A mismatch = Rust-vs-TS divergence; never "fix"
// by editing the fixture or the expected file.
//
use rust_ivm::replay::{assert_matches, run_fixture_file};
use std::fs;
use std::path::{Path, PathBuf};

fn collect_inputs(dir: &Path, inputs: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read fixtures dir") {
        let path = entry.expect("read fixture entry").path();
        if path.is_dir() {
            // Advance fixtures use a driver-level schema and are covered by
            // advance_fixture_replay_test through the real NAPI addon.
            if path.file_name().is_some_and(|name| name == "advance") {
                continue;
            }
            collect_inputs(&path, inputs);
        } else if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().ends_with(".input.json"))
        {
            inputs.push(path);
        }
    }
}

#[test]
fn fixture_replay() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("agentic/fixtures");
    if !dir.exists() {
        eprintln!("fixture_replay: no fixtures dir at {}", dir.display());
        return;
    }
    let mut inputs = Vec::new();
    collect_inputs(&dir, &mut inputs);
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
