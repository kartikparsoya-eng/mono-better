// tests/version_split_test.rs — runs the `_0_version` split check through the
// real napi addon, so `cargo test` covers it with no special invocation.
//
// Same shape as advance_fixture_replay_test.rs: shell out to node and let the
// script do the work. This one exists because the advance fixtures CANNOT
// cover the split — they hand `_0_version` to the engine but their comparison
// deliberately ignores that column, so they prove the other columns are
// unaffected and nothing more.
//
// The engine lifts `_0_version` out of row contents and reports it separately
// (`NapiRowChange.version`) so the JS side never parses a row just to split
// one column off it. If that ever regresses, every put patch would ship a
// `_0_version` field to clients and arrive with no version at all.

use std::process::Command;

#[test]
fn version_is_split_out_of_row_contents() {
    let script = std::path::PathBuf::from(std::env!("CARGO_MANIFEST_DIR"))
        .join("agentic/oracle/version-split-check.mjs");

    let out = match Command::new("node").arg(&script).output() {
        Ok(out) => out,
        Err(e) => {
            // No node on PATH — nothing to assert here.
            eprintln!("SKIP version_is_split_out_of_row_contents: cannot run node ({e})");
            return;
        }
    };

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);

    match out.status.code() {
        Some(0) => eprintln!("{}", stdout.trim()),
        // Exit 2 is the script's "cannot run here" signal (addon not built).
        Some(2) => eprintln!(
            "SKIP version_is_split_out_of_row_contents: {}",
            stderr.lines().next().unwrap_or("napi addon unavailable")
        ),
        _ => panic!(
            "version-split-check failed (exit {:?}).\n--- stdout ---\n{}\n--- stderr ---\n{}",
            out.status.code(),
            stdout,
            stderr
        ),
    }
}
