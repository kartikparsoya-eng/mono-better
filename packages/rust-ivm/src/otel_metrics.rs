//! OTLP instrument owned by the IVM layer — the Rust analog of the per-change
//! advance histogram the TS `PipelineDriver` maintains (`pipeline-driver.ts`
//! `#advanceTime`):
//!
//! | TS instrument                | kind      | unit | attributes |
//! |------------------------------|-----------|------|------------|
//! | `zero.sync.ivm.advance-time` | histogram | `s`  | `table`    |
//!
//! NOTE on attributes: TS declares `let type;` in `#advance` but never assigns
//! it before `recordMs(elapsed, {table, type})`, so `type` is always
//! `undefined` there — the histogram is effectively dimensioned by `table`
//! alone. We replicate the *effective* behavior and record only `table`.
//!
//! The instrument is created ONCE (lazily, via `OnceLock`) from the *global*
//! `zero` meter; the SDK + OTLP exporter are installed by the rust-syncer binary
//! (`rust_syncer::otel::init_metrics`). With no provider (unit tests, standalone
//! rust-ivm, OTLP disabled) `global::meter` is a no-op, so recording is cheap
//! and nothing is exported. This is distinct from rust-syncer's coarse
//! `zero.sync.advance-time` (whole-transaction advance = TS
//! `#transactionAdvanceTime`); this one is the granular per-change advance.

use std::sync::OnceLock;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};

/// Latency-histogram bucket boundaries in SECONDS — byte-identical to TS
/// `LATENCY_HISTOGRAM_BOUNDARIES_S` (observability/metrics.ts).
const LATENCY_BOUNDARIES_S: &[f64] = &[
    0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0,
];

fn advance_time() -> &'static Histogram<f64> {
    static ADVANCE_TIME: OnceLock<Histogram<f64>> = OnceLock::new();
    ADVANCE_TIME.get_or_init(|| {
        global::meter("zero")
            .f64_histogram("zero.sync.ivm.advance-time")
            .with_unit("s")
            .with_description(
                "Time to advance all queries for a given client group in response \
                 to a single change.",
            )
            .with_boundaries(LATENCY_BOUNDARIES_S.to_vec())
            .build()
    })
}

/// Record the PROCESS time of one change during an advance, tagged by `table`
/// — TS `#advanceTime.recordMs(elapsed, {table})` with `elapsed =
/// timer.totalElapsed() - start` (pipeline-driver.ts:1034-1038): a delta of the
/// view-syncer's TimeSliceTimer (yields excluded), which the engine reads
/// through the advance gate's clock — not wall-clock.
pub fn record_ivm_advance(table: &str, elapsed_ms: f64) {
    *LAST_IVM_ADVANCE_MS.lock().unwrap() = Some(elapsed_ms);
    advance_time().record(
        elapsed_ms / 1000.0,
        &[KeyValue::new("table", table.to_string())],
    );
}

/// TEST SEAM (rust-only): the last value handed to `zero.sync.ivm.advance-time`,
/// so an integration test can pin WHAT the engine records (the global meter is
/// a no-op under `cargo test`, and integration tests cannot see `cfg(test)`
/// items). Not part of the production contract.
#[doc(hidden)]
pub static LAST_IVM_ADVANCE_MS: std::sync::Mutex<Option<f64>> = std::sync::Mutex::new(None);

fn conflict_rows_deleted() -> &'static Counter<u64> {
    static CONFLICT_ROWS_DELETED: OnceLock<Counter<u64>> = OnceLock::new();
    CONFLICT_ROWS_DELETED.get_or_init(|| {
        global::meter("zero")
            .u64_counter("zero.sync.ivm.conflict-rows-deleted")
            .with_description("Number of rows deleted because they conflicted with added row")
            .build()
    })
}

/// Record a row removed because a different-PK unique conflict was displaced by
/// an added/edited row — TS `#conflictRowsDeleted.add(1)` (pipeline-driver.ts,
/// counted only when the change has a `nextValue`).
pub fn record_conflict_row_deleted() {
    conflict_rows_deleted().add(1, &[]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_is_noop_without_provider() {
        // No meter provider installed → global meter is a no-op. Must not panic.
        record_ivm_advance("issue", 4.0);
    }
}
