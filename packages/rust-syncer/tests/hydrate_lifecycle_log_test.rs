//! End-to-end check that the pipeline driver emits the per-query hydrate
//! lifecycle log — port of TS `PipelineDriver.#logQueryPipelineLifecycle`
//! (pipeline-driver.ts:470/608/784). This is the always-on analog of TS
//! `VENDED` (which is gated behind the `trackRowCountsVended` debug flag): it
//! makes a slow/heavy query identifiable from logs by `hydration_time_ms` +
//! `hydration_row_count`.
//!
//! Lives in its OWN integration-test binary on purpose. The test captures
//! `tracing` output, and `tracing`'s callsite-interest cache is PROCESS-global:
//! if this ran as a lib `#[cfg(test)]` test alongside others that install their
//! own subscribers, an unrelated test could cache one of the two `info!`
//! callsites (`-start` is a distinct callsite from `-finish`) as disabled,
//! making the assertion flaky. A dedicated integration binary gets a clean
//! process, so callsite interest is evaluated only against this subscriber.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rust_syncer::services::view_syncer::pipeline_driver::{
    HydrateQuery, IvmColumnSchema, IvmPipelines, IvmTableSpec, PipelineHydrationReason,
};

fn users_spec() -> IvmTableSpec {
    IvmTableSpec {
        table: "users".to_string(),
        columns: HashMap::from([(
            "id".to_string(),
            IvmColumnSchema {
                r#type: "string".to_string(),
                optional: false,
            },
        )]),
        column_order: vec!["id".to_string()],
        primary_key: vec!["id".to_string()],
        unique_keys: None,
        all_potential_primary_keys: vec![vec!["id".to_string()]],
        min_row_version: None,
    }
}

#[derive(Clone)]
struct BufWriter(Arc<Mutex<Vec<u8>>>);
struct BufGuard(Arc<Mutex<Vec<u8>>>);
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
    type Writer = BufGuard;
    fn make_writer(&'a self) -> BufGuard {
        BufGuard(self.0.clone())
    }
}
impl std::io::Write for BufGuard {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Non-vacuous: a real in-memory hydrate must emit the per-query hydrate
/// lifecycle log. Reverting the `Self::log_query_pipeline_lifecycle(...)` calls
/// in `hydrate` (or renaming an event) makes the captured buffer empty and this
/// fails. (Proven: neutering the log fn body → "lifecycle message emitted;
/// got:" empty.)
#[test]
fn hydrate_emits_query_pipeline_lifecycle_log() {
    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(BufWriter(buf.clone()))
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        let mut p = IvmPipelines::new();
        p.init(vec![users_spec()], None, "zero").unwrap();
        // Empty in-memory source → the query hydrates with zero rows, which is
        // exactly what we want: the `-finish` event must still fire and carry a
        // `hydration_row_count` field (of 0). The count's CORRECTNESS on a
        // non-empty source is pinned by the rust-ivm engine test
        // `hydration_row_count_tracks_rows_produced`.
        {
            let timer = std::rc::Rc::new(
                rust_syncer::services::view_syncer::view_syncer::TimeSliceTimer::new(),
            );
            timer.start_without_yielding();
            let mut changes = p
                .hydrate(
                    &[("q1".to_string(), r#"{"table":"users"}"#.to_string())],
                    timer,
                )
                .unwrap();
            for _ in changes.by_ref() {}
            changes.finish().unwrap();
        }
    });

    let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(
        logged.contains("query pipeline lifecycle"),
        "lifecycle message emitted; got: {logged}"
    );
    assert!(
        logged.contains("query-pipeline-hydrate-start"),
        "start event emitted; got: {logged}"
    );
    assert!(
        logged.contains("query-pipeline-hydrate-finish"),
        "finish event emitted; got: {logged}"
    );
    assert!(
        logged.contains("hydration_time_ms") && logged.contains("hydration_row_count"),
        "finish carries timing + row-count fields; got: {logged}"
    );
    assert!(logged.contains("q1"), "query hash present; got: {logged}");
}

/// Non-vacuous: tearing a hydrated pipeline down must emit `query-pipeline-stop`
/// (port of TS `#destroyPipeline`, pipeline-driver.ts:846) carrying `stopReason`,
/// `pipelineLifetimeMs`, and the pipeline's hydration stats. Reverting the log in
/// `destroy_pipeline` (or dropping a field) makes this fail.
#[test]
fn remove_query_emits_query_pipeline_stop_log() {
    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(BufWriter(buf.clone()))
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        let mut p = IvmPipelines::new();
        p.init(vec![users_spec()], None, "zero").unwrap();
        {
            let timer = std::rc::Rc::new(
                rust_syncer::services::view_syncer::view_syncer::TimeSliceTimer::new(),
            );
            timer.start_without_yielding();
            let mut changes = p
                .hydrate(
                    &[("q1".to_string(), r#"{"table":"users"}"#.to_string())],
                    timer,
                )
                .unwrap();
            for _ in changes.by_ref() {}
            changes.finish().unwrap();
        }
        // TS `removeQuery(queryID, stopReason)` → `#destroyPipeline`. TTL/errored
        // removals use the default stop reason `remove-query`.
        p.remove_query("q1", "remove-query");
    });

    let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(
        logged.contains("query-pipeline-stop"),
        "stop event emitted; got: {logged}"
    );
    assert!(
        logged.contains("stop_reason") && logged.contains("remove-query"),
        "stop carries the stop_reason; got: {logged}"
    );
    assert!(
        logged.contains("pipeline_lifetime_ms"),
        "stop carries pipeline_lifetime_ms; got: {logged}"
    );
    assert!(
        logged.contains("hydration_time_ms") && logged.contains("hydration_row_count"),
        "stop carries the pipeline's hydration stats; got: {logged}"
    );
    assert!(logged.contains("q1"), "query hash present; got: {logged}");
}

/// The `pipeline_run_id=<id>` value of every lifecycle line in `logged`, in
/// order (quotes stripped whatever formatter quoting applies).
fn pipeline_run_ids(logged: &str) -> Vec<String> {
    logged
        .lines()
        .filter(|l| l.contains("query pipeline lifecycle"))
        .map(|l| {
            let start = l
                .find("pipeline_run_id=")
                .unwrap_or_else(|| panic!("lifecycle line without pipeline_run_id: {l}"))
                + "pipeline_run_id=".len();
            l[start..]
                .split([' ', ','])
                .next()
                .unwrap()
                .trim_matches('"')
                .to_string()
        })
        .collect()
}

/// Non-vacuous (log parity, 2026-09-08): every TS lifecycle line carries the
/// pipeline's identity — `pipelineRunID` (a fresh `randomID()` per `addQuery`,
/// pipeline-driver.ts:607), `transformationHash`, `queryName` (when defined) and
/// `hydrationReason` (:470-505, :608-615, :784-792, :851-856) — which is what
/// lets an operator map a slow `queryHash` back to the named query and its
/// transform, and correlate `-start`/`-finish`/`-stop` of one hydrate. Rust
/// emitted only `zero_event` + `query_hash`. Dropping any of the four fields
/// from `log_query_pipeline_lifecycle` fails the matching assert; minting the
/// run id per LINE instead of per pipeline fails the "same id" assert.
#[test]
fn lifecycle_log_carries_the_ts_pipeline_identity_fields() {
    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(BufWriter(buf.clone()))
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        let mut p = IvmPipelines::new();
        p.init(vec![users_spec()], None, "zero").unwrap();
        {
            let timer = std::rc::Rc::new(
                rust_syncer::services::view_syncer::view_syncer::TimeSliceTimer::new(),
            );
            timer.start_without_yielding();
            let mut changes = p
                .hydrate(
                    &[HydrateQuery {
                        query_id: "q1".to_string(),
                        ast_json: r#"{"table":"users"}"#.to_string(),
                        transformation_hash: "th-1".to_string(),
                        query_name: Some("usersByName".to_string()),
                        hydration_reason: PipelineHydrationReason::UnchangedQueryRehydrate,
                    }],
                    timer,
                )
                .unwrap();
            for _ in changes.by_ref() {}
            changes.finish().unwrap();
        }
        p.remove_query("q1", "remove-query");
    });

    let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let lines: Vec<&str> = logged
        .lines()
        .filter(|l| l.contains("query pipeline lifecycle"))
        .collect();
    assert_eq!(
        lines.len(),
        3,
        "-start, -finish, -stop for one pipeline; got:\n{logged}"
    );
    for line in &lines {
        assert!(
            line.contains("th-1"),
            "TS `transformationHash` context on every line; got: {line}"
        );
        assert!(
            line.contains("usersByName"),
            "TS `queryName` context on every line; got: {line}"
        );
        assert!(
            line.contains("unchanged-query-rehydrate"),
            "TS `hydrationReason` context on every line; got: {line}"
        );
    }
    let ids = pipeline_run_ids(&logged);
    assert!(
        !ids[0].is_empty()
            && ids[0]
                .chars()
                .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase()),
        "TS `randomID()` = randInt(1, MAX_SAFE_INTEGER).toString(36); got {ids:?}"
    );
    assert!(
        ids.iter().all(|id| id == &ids[0]),
        "one pipelineRunID per hydrate, shared by -start/-finish/-stop; got {ids:?}"
    );
}

/// TS omits `queryName` when the query has none and defaults
/// `hydrationReason` to `'query-set-sync'` (pipeline-driver.ts:580, :488-491);
/// a bare `(query_id, ast_json)` hydrate must log exactly that shape.
#[test]
fn lifecycle_log_omits_query_name_when_undefined_and_defaults_the_reason() {
    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(BufWriter(buf.clone()))
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        let mut p = IvmPipelines::new();
        p.init(vec![users_spec()], None, "zero").unwrap();
        let timer = std::rc::Rc::new(
            rust_syncer::services::view_syncer::view_syncer::TimeSliceTimer::new(),
        );
        timer.start_without_yielding();
        let mut changes = p
            .hydrate(
                &[("q1".to_string(), r#"{"table":"users"}"#.to_string())],
                timer,
            )
            .unwrap();
        for _ in changes.by_ref() {}
        changes.finish().unwrap();
    });

    let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(
        logged.contains("query-pipeline-hydrate-finish"),
        "got:\n{logged}"
    );
    assert!(
        !logged.contains("query_name="),
        "no `queryName` context for a nameless query; got:\n{logged}"
    );
    assert!(
        logged.contains("query-set-sync"),
        "default `hydrationReason` = 'query-set-sync'; got:\n{logged}"
    );
    assert!(
        logged.contains("pipeline_run_id="),
        "`pipelineRunID` is always present; got:\n{logged}"
    );
}
