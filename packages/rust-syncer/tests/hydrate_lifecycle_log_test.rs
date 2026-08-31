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
    IvmColumnSchema, IvmPipelines, IvmTableSpec,
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
        primary_key: vec!["id".to_string()],
        unique_keys: None,
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
        p.hydrate(
            &[("q1".to_string(), r#"{"table":"users"}"#.to_string())],
            |_rc| {},
        )
        .unwrap();
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
