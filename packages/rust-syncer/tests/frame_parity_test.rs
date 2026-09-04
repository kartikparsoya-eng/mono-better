//! M13 — protocol frame differential: rust `parse_upstream` vs the real TS
//! `JSON.parse` + `valita.parse(upstreamSchema)`.
//!
//! The surrogate divergence this layer was built for — rust closed the socket
//! with `InvalidMessage` on a frame TS served — was invisible to every other
//! parity layer: the symbol ledger saw a clean `up.ts` -> `up.rs` mapping, the
//! body-diff saw matching branches, and the call-topology guard saw the call in
//! the right context. It only surfaces when both parsers are asked the SAME
//! question and their ANSWERS are compared.
//!
//! The corpus (`parity/frame_corpus.py`) and the TS golden
//! (`parity/ts_frame_oracle.mts`, which drives `upstreamSchema` itself rather
//! than restating it, so the golden cannot drift from the spec) are checked in.
//! Regenerate with:
//!
//!   python3 parity/frame_corpus.py
//!   node_modules/.bin/tsx parity/ts_frame_oracle.mts
//!
//! `KNOWN_DIVERGENCES` is a RATCHET, not an allow-list: an unlisted divergence
//! fails, and so does a listed one that no longer reproduces. Fixing a row means
//! deleting it in the same commit. Every row carries its root-cause code.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use rust_syncer::protocol::parse_upstream;

/// Root causes behind `KNOWN_DIVERGENCES`, each a separate burn-down item.
///
/// R1 `js-number-vs-i64` — TS types `pushVersion`/`timestamp` as `v.number()`,
///    i.e. an f64; rust types them `i64`. TS accepts `1E5`, `1.5`, `-0`,
///    `1e309` (-> Infinity) and `1e-330` (-> 0); rust rejects and DISCONNECTS.
///    Same client-visible harm as the surrogate bug — `1E5` is an ordinary way
///    to write a timestamp. (zero-protocol/src/push.ts)
///
/// R2 `pull-body-unvalidated` — rust's `"pull" => Upstream::Pull(body.clone())`
///    keeps the body as a raw `Value` and validates NOTHING; TS validates it
///    against `pullRequestBodySchema` (zero-protocol/src/pull.ts). Every wrong
///    type, missing field and null field sails through rust.
///
/// R3 `valita-objects-are-strict` — valita `v.object` REJECTS unknown keys;
///    serde ignores them by default. TS closes the connection on an unknown
///    field where rust serves it. Systematic across every object-bodied type,
///    and a live hazard during a client rollout that adds a field.
///
/// R4 `optional-is-not-nullable` — valita `.optional()` means absent-or-value,
///    NOT null, so TS rejects an explicit `null`; rust's `Option<T>` accepts it
///    via serde. This is the tri-state (null / undefined / absent) handling
///    AGENTS.md rule 1 calls out by name.
///
/// R5 `tuple-arity-and-body-shape` — TS `v.tuple([literal, body])` pins the
///    frame to EXACTLY two elements and validates the body (`pingBodySchema =
///    v.object({})` requires an object). Rust checks `arr.len() < 2` and, for
///    `ping`, ignores the body entirely.
const KNOWN_DIVERGENCES: &[(&str, &str)] = &[
    // R1 — 12 frames. TS ACCEPTS, rust REJECTS: a real client is disconnected.
    ("number/push.pushVersion/neg-zero", "R1"),
    ("number/push.pushVersion/fractional", "R1"),
    ("number/push.pushVersion/overflow-f64", "R1"),
    ("number/push.pushVersion/neg-overflow-f64", "R1"),
    ("number/push.pushVersion/tiny", "R1"),
    ("number/push.pushVersion/exp-upper", "R1"),
    ("number/push.timestamp/neg-zero", "R1"),
    ("number/push.timestamp/fractional", "R1"),
    ("number/push.timestamp/overflow-f64", "R1"),
    ("number/push.timestamp/neg-overflow-f64", "R1"),
    ("number/push.timestamp/tiny", "R1"),
    ("number/push.timestamp/exp-upper", "R1"),
    // R2 — 30 frames. The whole pull body is unchecked in rust.
    ("wrongtype/pull.clientGroupID/null", "R2"),
    ("wrongtype/pull.clientGroupID/true", "R2"),
    ("wrongtype/pull.clientGroupID/number", "R2"),
    ("wrongtype/pull.clientGroupID/array", "R2"),
    ("wrongtype/pull.clientGroupID/object", "R2"),
    ("wrongtype/pull.requestID/null", "R2"),
    ("wrongtype/pull.requestID/true", "R2"),
    ("wrongtype/pull.requestID/number", "R2"),
    ("wrongtype/pull.requestID/array", "R2"),
    ("wrongtype/pull.requestID/object", "R2"),
    ("wrongtype/pull.cookie/true", "R2"),
    ("wrongtype/pull.cookie/number", "R2"),
    ("wrongtype/pull.cookie/array", "R2"),
    ("wrongtype/pull.cookie/object", "R2"),
    ("number/pull.cookie/zero", "R2"),
    ("number/pull.cookie/neg-zero", "R2"),
    ("number/pull.cookie/neg-one", "R2"),
    ("number/pull.cookie/int32-max-plus", "R2"),
    ("number/pull.cookie/max-safe", "R2"),
    ("number/pull.cookie/max-safe-plus-1", "R2"),
    ("number/pull.cookie/max-safe-plus-2", "R2"),
    ("number/pull.cookie/min-safe", "R2"),
    ("number/pull.cookie/fractional", "R2"),
    ("number/pull.cookie/tiny", "R2"),
    ("number/pull.cookie/exp-upper", "R2"),
    ("missing/pull/clientGroupID", "R2"),
    ("missing/pull/cookie", "R2"),
    ("missing/pull/requestID", "R2"),
    ("nullfield/pull/clientGroupID", "R2"),
    ("nullfield/pull/requestID", "R2"),
    // R3 — 6 frames, one per object-bodied message type.
    ("extrafield/updateAuth", "R3"),
    ("extrafield/pull", "R3"),
    ("extrafield/changeDesiredQueries", "R3"),
    ("extrafield/initConnection", "R3"),
    ("extrafield/inspect", "R3"),
    ("extrafield/push", "R3"),
    // R4 — 4 frames. `.optional()` is absent-or-value, never null.
    ("wrongtype/initConnection.userPushURL/null", "R4"),
    ("wrongtype/initConnection.traceparent/null", "R4"),
    ("wrongtype/changeDesiredQueries.traceparent/null", "R4"),
    ("nullfield/push/schemaVersion", "R4"),
    // R5 — 4 frames. Frame arity and body shape are unchecked.
    ("structural/three-elements", "R5"),
    ("structural/body-null", "R5"),
    ("structural/body-string", "R5"),
    ("structural/body-array", "R5"),
];

fn parity_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../parity/frame-fixtures")
        .canonicalize()
        .expect("parity/frame-fixtures must exist — run parity/frame_corpus.py")
}

fn read_ndjson(name: &str) -> Vec<serde_json::Value> {
    let path = parity_dir().join(name);
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("corpus line must be JSON"))
        .collect()
}

/// TS `upstreamSchema` accepts a frame exactly when rust `parse_upstream` does,
/// except for the itemized `KNOWN_DIVERGENCES`.
///
/// Divergence in EITHER direction is a bug: rust rejecting what TS accepts
/// disconnects a client TS would have served (the surrogate bug, and R1);
/// rust accepting what TS rejects lets a malformed frame reach handling that TS
/// never runs (R2-R5).
#[test]
fn rust_frame_parse_matches_the_ts_upstream_schema() {
    let corpus = read_ndjson("frame-corpus.ndjson");
    let oracle = read_ndjson("frame-oracle-ts.ndjson");
    assert!(!corpus.is_empty(), "corpus is empty");
    assert_eq!(
        corpus.len(),
        oracle.len(),
        "corpus and TS golden are out of sync — regenerate both",
    );

    let ts: BTreeMap<String, &serde_json::Value> = oracle
        .iter()
        .map(|r| (r["id"].as_str().expect("oracle id").to_string(), r))
        .collect();
    let known: BTreeMap<&str, &str> = KNOWN_DIVERGENCES.iter().copied().collect();
    assert_eq!(
        known.len(),
        KNOWN_DIVERGENCES.len(),
        "duplicate id in KNOWN_DIVERGENCES",
    );

    let mut unexpected = Vec::new();
    let mut still_diverging = BTreeSet::new();

    for row in &corpus {
        let id = row["id"].as_str().expect("corpus id");
        let frame = row["frame"].as_str().expect("corpus frame");
        let expected = ts
            .get(id)
            .unwrap_or_else(|| panic!("no TS oracle row for {id}"));
        let ts_accepted = expected["accepted"].as_bool().expect("accepted flag");

        let diverged = match (parse_upstream(frame), ts_accepted) {
            (Ok(_), true) | (Err(_), false) => None,
            (Err(e), true) => Some(format!("{id}: TS accepted, rust REJECTED ({e})")),
            (Ok(_), false) => Some(format!(
                "{id}: TS rejected at {}, rust ACCEPTED",
                expected["stage"]
            )),
        };
        match (diverged, known.get(id)) {
            (Some(what), None) => unexpected.push(what),
            (Some(_), Some(_)) => {
                still_diverging.insert(id.to_string());
            }
            (None, _) => {}
        }
    }

    let fixed: Vec<&str> = known
        .keys()
        .copied()
        .filter(|id| !still_diverging.contains(*id))
        .collect();

    assert!(
        unexpected.is_empty(),
        "\nM13: {} NEW frame-parse divergence(s) against the TS upstreamSchema.\n{}\n\n\
         Fix rust to match TS, or — only with a TS citation justifying it — add \
         the id to KNOWN_DIVERGENCES with its root-cause code.",
        unexpected.len(),
        unexpected.join("\n"),
    );
    assert!(
        fixed.is_empty(),
        "\nM13 ratchet: {} KNOWN_DIVERGENCES row(s) no longer reproduce — rust now \
         matches TS here. Delete them from KNOWN_DIVERGENCES in the fixing commit:\n  {}",
        fixed.len(),
        fixed.join("\n  "),
    );
}
