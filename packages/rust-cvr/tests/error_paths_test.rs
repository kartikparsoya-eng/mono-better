//! Error-path parity tests for rust-cvr — the promoted bucket-3 targets from
//! the coverage triage: the `versionFromString` throw and the `trackRemoved`
//! unknown-query throw. Both mirror real TS throw branches (HARD RULE 7:
//! error semantics ARE behavior), so they are pinned to exact messages.

use std::collections::BTreeMap;

use rust_cvr::cvr::{CVR, CVRQueryDrivenUpdater};
use rust_cvr::schema::types::{
    CVRVersion, maybe_version_string, version_from_string, version_string,
};

// --- versionFromString / maybeVersionString ------------------------------- //

// Port of TS `versionFromString` throw (types.ts): a >2-part version string is
// malformed and must panic (TS throws). Pins the exact message.
#[test]
#[should_panic(expected = "more than one ':' separator")]
fn version_from_string_panics_on_too_many_parts() {
    let _ = version_from_string("a:b:c");
}

// The non-panicking Result twin (`maybeVersionString`) that untrusted input
// takes: the SAME malformed string returns Err, and a well-formed one parses.
#[test]
fn maybe_version_string_pins_both_branches() {
    assert!(
        maybe_version_string("a:b:c").is_err(),
        "3-part version string is malformed"
    );
    // A well-formed 2-part `stateVersion:configVersion` (lexi-encoded config)
    // round-trips through version_string.
    let v = CVRVersion {
        state_version: "1a9".to_string(),
        config_version: Some(3),
    };
    assert_eq!(
        maybe_version_string(&version_string(&v)).expect("round-tripped version parses"),
        v
    );
}

// --- trackRemoved unknown-query throw ------------------------------------- //

fn empty_cvr() -> CVR {
    CVR {
        id: "cg-err".to_string(),
        version: CVRVersion {
            state_version: "v0".to_string(),
            config_version: None,
        },
        last_active: 0,
        ttl_clock: 0,
        replica_version: Some("r1".to_string()),
        clients: BTreeMap::new(),
        queries: BTreeMap::new(),
        client_schema: None,
        profile_id: None,
    }
}

// Port of TS `trackRemoved` throw (cvr.ts): removing a query that is not in the
// CVR is an invariant violation. track_queries routes removed hashes through
// track_removed, which panics on an unknown query id.
#[test]
#[should_panic(expected = "Query nonexistent not found")]
fn track_removed_panics_on_unknown_query() {
    let mut updater =
        CVRQueryDrivenUpdater::new(empty_cvr(), "v1".to_string(), "r1".to_string(), None);
    // No queries in the CVR, so removing "nonexistent" must panic.
    updater.track_queries(&[], &["nonexistent"]);
}
