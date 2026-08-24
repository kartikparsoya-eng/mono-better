//! Rust replay driver for the CVR *sequence* differential (companion to
//! `agentic/parity/seq/run-ts.mjs`).
//!
//! Reads a program (gen.mjs format) on argv[1] (or stdin) and replays it against
//! the REAL Rust `CVRStore` + `CVRConfigDrivenUpdater` over `TEST_CVR_PG_URI`,
//! emitting a canonical trace as JSON on stdout, byte-compatible with the TS
//! driver so `diff.mjs` / `fuzz.mjs` can assert the two traces match. The replay
//! engine itself lives in `rust_cvr::seq_replay` (shared with the CI gate,
//! `tests/seq_diff_pg_test.rs`).
//!
//! Usage: TEST_CVR_PG_URI=... cargo run --bin cvr_seq_replay -- <program.json>

use rust_cvr::seq_replay::{Program, run};
use sqlx::postgres::PgPoolOptions;
use std::io::Read;

#[tokio::main]
async fn main() {
    let uri = std::env::var("TEST_CVR_PG_URI").expect("TEST_CVR_PG_URI unset");

    let prog_text = match std::env::args().nth(1) {
        Some(p) => std::fs::read_to_string(&p).expect("read program file"),
        None => {
            let mut s = String::new();
            std::io::stdin().read_to_string(&mut s).expect("read stdin");
            s
        }
    };
    let prog: Program = serde_json::from_str(&prog_text).expect("parse program");

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&uri)
        .await
        .expect("connect TEST_CVR_PG_URI");

    let trace = run(&pool, &prog).await;
    println!("{}", serde_json::to_string_pretty(&trace).unwrap());
}
