//! Live-Postgres CVR *sequence* differential (CI gate).
//!
//! Replays every checked-in program in `agentic/parity/seq/corpus/` (deterministic
//! sequences of config-driven CVR transactions — ensureClient / putDesiredQueries /
//! markDesiredInactive / deleteDesired / clearDesired / deleteClient across many
//! version transitions) through the REAL Rust `CVRStore` + `CVRConfigDrivenUpdater`
//! and asserts the resulting trace equals the frozen TS golden trace produced by
//! `run-ts.mjs` (which drives the real TS updaters). This pins the *stateful*
//! surface — version/configVersion progression, per-client desired-query sets,
//! TTL inactivation, `deleted` flags, internal-query ASTs, no-op-flush detection,
//! first-sight instance init — that the single-scenario fixtures never reach.
//!
//! The corpus caught four real port divergences on introduction: the `lmids`
//! internal-query AST `and`-wrapper, a missing first-sight instance write, an
//! inactivated-desire `deleted` flag, and nondeterministic patch ordering.
//!
//! Regenerate the corpus + goldens:
//!   TEST_CVR_PG_URI=... node agentic/parity/seq/gen.mjs --corpus 40
//!   TEST_CVR_PG_URI=... agentic/parity/seq/refresh-goldens.sh
//!
//! Gated on `TEST_CVR_PG_URI`; skips (passes) when unset.

use rust_cvr::seq_replay::{Program, canonicalize, run};
use serde_json::Value;
use std::path::PathBuf;

#[tokio::test]
async fn sequence_traces_match_ts_goldens() {
    let uri = match std::env::var("TEST_CVR_PG_URI") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!("SKIP sequence_traces_match_ts_goldens: TEST_CVR_PG_URI unset");
            return;
        }
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&uri)
        .await
        .expect("connect to TEST_CVR_PG_URI");

    let corpus = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("agentic/parity/seq/corpus");
    let mut programs: Vec<PathBuf> = std::fs::read_dir(&corpus)
        .unwrap_or_else(|e| panic!("read corpus dir {}: {e}", corpus.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.extension().and_then(|s| s.to_str()) == Some("json")
                && !p.to_string_lossy().ends_with(".trace.json")
        })
        .collect();
    programs.sort();
    assert!(
        !programs.is_empty(),
        "no corpus programs found in {}",
        corpus.display()
    );

    let mut checked = 0;
    for prog_path in &programs {
        let prog: Program =
            serde_json::from_str(&std::fs::read_to_string(prog_path).expect("read program"))
                .expect("parse program");

        let golden_path = prog_path.with_extension("trace.json");
        let golden: Value = serde_json::from_str(
            &std::fs::read_to_string(&golden_path)
                .unwrap_or_else(|e| panic!("read golden {}: {e}", golden_path.display())),
        )
        .expect("parse golden");

        let actual = run(&pool, &prog).await;

        let name = prog_path.file_name().unwrap().to_string_lossy();
        assert_eq!(
            canonicalize(&actual),
            canonicalize(&golden),
            "Rust sequence trace differs from the TS golden for {name}"
        );
        checked += 1;
    }
    eprintln!("sequence differential: {checked} corpus programs matched the TS golden");
}
