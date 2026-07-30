// tests/advance_fixture_replay_test.rs — replay advance-path regression fixtures
// against the TS advance oracle using the real napi addon.
//
// Fixtures in agentic/fixtures/regressions/adv-seed-* are advance-only:
// their .expected.json is produced by agentic/oracle/ts-advance-runner.mjs
// (production-style net diff) and compared against
// agentic/oracle/napi-advance-runner.mjs output.

use std::process::Command;

#[test]
fn advance_fixture_replay() {
    let advance_dir =
        std::path::PathBuf::from(std::env!("CARGO_MANIFEST_DIR")).join("agentic/fixtures/advance");

    let mut inputs: Vec<_> = std::fs::read_dir(&advance_dir)
        .expect("read regressions dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let s = name.to_string_lossy();
            s.starts_with("adv-seed-") && s.ends_with(".input.json")
        })
        .map(|e| e.path())
        .collect();
    inputs.sort();

    let mut diverged = Vec::new();
    let mut skipped = 0;

    for input in &inputs {
        let expected = input.with_extension("").with_extension("expected.json");
        if !expected.exists() {
            skipped += 1;
            continue;
        }

        let actual =
            std::env::temp_dir().join(format!("adv-fixture-{}.actual.json", std::process::id()));

        let run = |cmd: &str| {
            let out = Command::new("node")
                .arg(cmd)
                .arg(input)
                .arg("--out")
                .arg(&actual)
                .output()
                .unwrap_or_else(|_| panic!("{} failed to run", cmd));
            if !out.status.success() {
                let stderr = String::from_utf8_lossy(&out.stderr);
                return Err(format!("{} failed: {}", cmd, stderr));
            }
            Ok(())
        };

        if let Err(e) = run("agentic/oracle/napi-advance-runner.mjs") {
            diverged.push((input.clone(), e));
            continue;
        }

        let diff = Command::new("node")
            .arg("agentic/oracle/napi-advance-diff.mjs")
            .arg(&expected)
            .arg(&actual)
            .output()
            .expect("diff failed to run");

        if !diff.status.success() {
            let stderr = String::from_utf8_lossy(&diff.stderr);
            diverged.push((input.clone(), stderr.to_string()));
        }
    }

    if !diverged.is_empty() {
        for (p, e) in &diverged {
            eprintln!(
                "DIVERGED {}: {}",
                p.display(),
                e.lines().next().unwrap_or("")
            );
        }
        panic!(
            "advance_fixture_replay: {} fixtures diverged ({} skipped)",
            diverged.len(),
            skipped
        );
    }

    println!(
        "advance_fixture_replay: {} fixtures compared, {} diverged, {} skipped",
        inputs.len(),
        diverged.len(),
        skipped
    );
}
