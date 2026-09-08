// replay bin — prints the Rust engine's canonicalized output for one fixture.
// Usage: cargo run --bin replay -- <input.json>
// The fuzzer and orchestrator diff this against the TS oracle's expected JSON.

use rust_ivm::replay::{canonicalize, run_fixture_file};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: replay <input.json>");
        std::process::exit(2);
    }
    let out = canonicalize(&run_fixture_file(&args[1]));
    println!(
        "{}",
        serde_json::to_string_pretty(&out).expect("serialize output")
    );
}
