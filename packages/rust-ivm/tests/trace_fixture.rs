// Pipeline trace harness for debugging push routing on a single fixture.
//
// Usage:
//   FIX=seed-159916 IVM_TRACE=1 cargo test --test trace_fixture -- --nocapture
//
// Prints one `[ivm-trace] <op> recv <change>` line per push each routing
// operator receives (source/fan_out/fan_in/join/flipped_join/exists/union_*/
// catch), so the emit→recv chain reconstructs the flow. Zero cost when
// IVM_TRACE is unset. FIX defaults to seed-159916; point it at any fixture in
// agentic/fixtures/ or agentic/fixtures/regressions/.

use rust_ivm::replay::run_fixture_file;
use std::path::PathBuf;

#[test]
fn trace_fixture() {
    let fix = std::env::var("FIX").unwrap_or_else(|_| "seed-159916".to_string());
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        base.join(format!("agentic/fixtures/{fix}.input.json")),
        base.join(format!("agentic/fixtures/regressions/{fix}.input.json")),
    ];
    let path = candidates
        .iter()
        .find(|p| p.exists())
        .unwrap_or_else(|| panic!("fixture {fix} not found in fixtures/ or regressions/"));
    if !rust_ivm::ivm::trace::enabled() {
        eprintln!("(set IVM_TRACE=1 to see the trace; running {fix} silently)");
    }
    let _ = run_fixture_file(path.to_str().unwrap());
}
