//! OTLP instruments owned by the CVR layer — the Rust analog of the OTel
//! instruments the TS `ClientHandler` (`client-handler.ts`) and CVR flush path
//! (`row-record-cache.ts`) maintain. Names/types/units mirror TS exactly:
//!
//! | TS instrument                    | kind      | unit | attributes    |
//! |----------------------------------|-----------|------|---------------|
//! | `zero.sync.cvr.flush-time`       | histogram | `s`  | `flush.type`  |
//! | `zero.sync.cvr.rows-flushed`     | counter   | —    | —             |
//! | `zero.sync.poke.time`            | histogram | `s`  | —             |
//! | `zero.sync.poke.transactions`    | counter   | —    | —             |
//! | `zero.sync.poke.rows`            | counter   | —    | —             |
//!
//! Instruments are created ONCE (lazily, via `OnceLock`) from the *global*
//! `zero` meter — the SDK + OTLP exporter are installed by the rust-syncer
//! binary (`rust_syncer::otel::init_metrics`). When no provider is installed
//! (unit tests, standalone rust-cvr, OTLP disabled) `global::meter` returns a
//! no-op meter, so every record here costs a couple of atomic loads and nothing
//! is exported. TS pushes these over OTLP; so do we.

use std::sync::OnceLock;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};

/// Latency-histogram bucket boundaries in SECONDS — byte-identical to TS
/// `LATENCY_HISTOGRAM_BOUNDARIES_S` (observability/metrics.ts).
const LATENCY_BOUNDARIES_S: &[f64] = &[
    0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0,
];

struct Instruments {
    cvr_flush_time: Histogram<f64>,
    cvr_rows_flushed: Counter<u64>,
    poke_time: Histogram<f64>,
    poke_transactions: Counter<u64>,
    poke_rows: Counter<u64>,
    row_set_signature_drifts: Counter<u64>,
    cvr_load_attempts: Counter<u64>,
    cvr_load_duration: Histogram<f64>,
    cvr_flush_attempts: Counter<u64>,
    crud_queries_processed: Counter<u64>,
    custom_queries_processed: Counter<u64>,
}

fn instruments() -> &'static Instruments {
    static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let m = global::meter("zero");
        let latency = |name: &'static str, desc: &'static str| {
            m.f64_histogram(name)
                .with_unit("s")
                .with_description(desc)
                .with_boundaries(LATENCY_BOUNDARIES_S.to_vec())
                .build()
        };
        Instruments {
            cvr_flush_time: latency(
                "zero.sync.cvr.flush-time",
                "Time to flush a CVR transaction. This includes both synchronous \
                 and asynchronous flushes, distinguished by the flush.type attribute.",
            ),
            cvr_rows_flushed: m
                .u64_counter("zero.sync.cvr.rows-flushed")
                .with_description("Number of (changed) rows flushed to a CVR")
                .build(),
            poke_time: latency(
                "zero.sync.poke.time",
                "Time elapsed for each poke transaction. Canceled / noop pokes are excluded.",
            ),
            poke_transactions: m
                .u64_counter("zero.sync.poke.transactions")
                .with_description("Count of poke transactions.")
                .build(),
            poke_rows: m
                .u64_counter("zero.sync.poke.rows")
                .with_description("Count of poked rows.")
                .build(),
            row_set_signature_drifts: m
                .u64_counter("zero.sync.query.row-set-signature-drifts")
                .with_description(
                    "Queries whose row-set signature changed for the SAME transformation \
                     hash — expected near-zero; non-zero indicates non-deterministic query \
                     execution (a silent-correctness canary).",
                )
                .build(),
            // Ports of TS CVRStore's own instruments (cvr-store.ts:207-221) —
            // F-RRC-4.
            cvr_load_attempts: m
                .u64_counter("zero.sync.cvr.load_attempts")
                .with_description("CVR load attempts, labeled by result.")
                .build(),
            cvr_load_duration: latency("zero.sync.cvr.load_duration", "Time to load a CVR."),
            cvr_flush_attempts: m
                .u64_counter("zero.sync.cvr.flush_attempts")
                .with_description("CVR flush attempts, labeled by result and flush.type.")
                .build(),
            // Ports of the TS anonymous-telemetry query counters
            // (anonymous-otel-start.ts:180-187), fed by `recordQuery`. TS keeps
            // these on a separate opt-out telemetry meter; the Rust process has
            // no anonymous-telemetry subsystem, so they live on the shared
            // meter under the same names.
            crud_queries_processed: m
                .u64_counter("zero.crud_queries_processed")
                .with_description("Number of CRUD (ZQL) queries processed")
                .build(),
            custom_queries_processed: m
                .u64_counter("zero.custom_queries_processed")
                .with_description("Number of custom (named) queries processed")
                .build(),
        }
    })
}

/// Port of TS `CVRStore.#recordLoad` (cvr-store.ts:308-311): one count on
/// `cvr.load_attempts` + the elapsed time on `cvr.load_duration`, labeled by
/// result ("success" | "error") and, on error, `error.kind`.
pub fn record_load(elapsed_ms: f64, result: &'static str, error_kind: Option<&str>) {
    let i = instruments();
    let mut attrs = vec![KeyValue::new("result", result)];
    if let Some(kind) = error_kind {
        attrs.push(KeyValue::new("error.kind", kind.to_string()));
    }
    i.cvr_load_attempts.add(1, &attrs);
    i.cvr_load_duration.record(elapsed_ms / 1000.0, &attrs);
}

/// Port of the TS `#cvrFlushes` counts (cvr-store.ts:1254-1264):
/// `cvr.flush_attempts` labeled by result ("success" | "error") and flush.type
/// ("sync" | "noop"), plus `error.kind` on errors.
pub fn record_flush_attempt(
    result: &'static str,
    flush_type: &'static str,
    error_kind: Option<&str>,
) {
    let mut attrs = vec![
        KeyValue::new("result", result),
        KeyValue::new("flush.type", flush_type),
    ];
    if let Some(kind) = error_kind {
        attrs.push(KeyValue::new("error.kind", kind.to_string()));
    }
    instruments().cvr_flush_attempts.add(1, &attrs);
}

/// Port of TS `recordQuery(type: 'crud' | 'custom')`
/// (anonymous-otel-start.ts:325-331). Fired by `putDesiredQueries` for NEW and
/// REACTIVATED desired queries (cvr.ts:349, 361) — F-CVR-3.
pub fn record_query(query_type: &str) {
    let i = instruments();
    match query_type {
        "crud" => i.crud_queries_processed.add(1, &[]),
        "custom" => i.custom_queries_processed.add(1, &[]),
        _ => {}
    }
}

/// Record a row-set-signature drift — TS view-syncer `query.row-set-signature-drifts`.
/// Fired when a query's persisted signature CHANGES (same hash, different row
/// set), never on first computation.
pub fn record_row_set_signature_drift() {
    instruments().row_set_signature_drifts.add(1, &[]);
}

/// Record a CVR flush — TS `recordSyncFlushStats` / `#recordAsyncFlushStats`
/// (`row-record-cache.ts`). `flush_type` is `"sync"` or `"async"`; `rows` is the
/// number of changed rows persisted (TS adds these to `cvr.rows-flushed`).
pub fn record_cvr_flush(elapsed_ms: f64, rows: u64, flush_type: &'static str) {
    let i = instruments();
    i.cvr_flush_time.record(
        elapsed_ms / 1000.0,
        &[KeyValue::new("flush.type", flush_type)],
    );
    if rows > 0 {
        i.cvr_rows_flushed.add(rows, &[]);
    }
}

/// Record a completed poke transaction — TS `#pokeTransactions` /
/// `#pokeTime.recordMs` in `ClientHandler` `end()`. Canceled/noop pokes never
/// reach this (matching TS, which records only after `pokeEnd` is pushed).
pub fn record_poke(elapsed_ms: f64) {
    let i = instruments();
    i.poke_transactions.add(1, &[]);
    i.poke_time.record(elapsed_ms / 1000.0, &[]);
}

/// Record a poked row — TS `#pokedRows.add(1)` per `type === 'row'` patch.
pub fn record_poked_row() {
    instruments().poke_rows.add(1, &[]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_paths_are_noops_without_provider() {
        // No meter provider installed → global meter is a no-op. These must not
        // panic and must be cheap.
        record_cvr_flush(12.0, 5, "sync");
        record_cvr_flush(3.0, 0, "async");
        record_poke(8.0);
        record_poked_row();
    }
}
