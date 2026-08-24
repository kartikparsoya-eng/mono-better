//! Heap-profiling churn of the per-advance CVR write path — the rust-cvr analog
//! of rust-ivm's `advance_leak_profile`. Each iteration models ONE advance: a
//! fresh `CVRQueryDrivenUpdater` receives a brand-new row and drops (exactly how
//! the view-syncer creates one updater per advance and discards it), against a
//! BOUNDED, ever-new-key `existing_rows` cache (a new row streams in, the one
//! from two iters ago ages out — ~2 rows live, monotonic keyspace).
//!
//! Any growth in dhat's live bytes across iterations is a genuine retained
//! allocation in the `received()` / `merge_ref_counts()` path or in something it
//! reaches (row-id interning, a static, a thread-local). The live set is flat, so
//! a real per-advance retention blows the budget within a few hundred iterations.
//!
//! Run: cargo test -p rust-cvr --test cvr_leak_profile -- --nocapture --ignored
//! dhat writes dhat-heap.json (view at https://nnethercote.github.io/dh_view/dh_view.html)
//!
//! NOTE: dhat sees only RUST allocations. The CVR Postgres store / row-record
//! cache flush path is not exercised here (it needs a live PG); this harness
//! isolates the pure in-memory updater, which is where a logic leak would live.

use std::collections::{BTreeMap, HashMap};

use rust_cvr::cvr::CVRQueryDrivenUpdater;
use rust_cvr::row_key::{RowID, row_id_string};
use rust_cvr::types::{CVR, RefCounts, RowRecord, RowUpdate};
use rust_cvr::version::CVRVersion;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// A fresh base CVR pinned at state `v0` — cloned-in per advance the way the
/// view-syncer hands the loaded CVR to each new updater.
fn base_cvr() -> CVR {
    CVR {
        id: "cg-leak".to_string(),
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

fn row_id(n: usize) -> RowID {
    RowID {
        schema: "s".to_string(),
        table: "t".to_string(),
        row_key: serde_json::json!({ "id": n }).as_object().unwrap().clone(),
    }
}

fn ref_counts(q: &str, n: i64) -> RefCounts {
    let mut m: RefCounts = BTreeMap::new();
    m.insert(q.to_string(), n);
    m
}

/// Model the row-record cache's stored form for a row already in the view.
fn row_record(n: usize) -> RowRecord {
    RowRecord {
        id: row_id(n),
        row_version: "rv".to_string(),
        patch_version: CVRVersion {
            state_version: "v1".to_string(),
            config_version: None,
        },
        ref_counts: Some(ref_counts("q", 1)),
    }
}

fn run(iters: usize) {
    let _profiler = dhat::Profiler::builder().build();

    // Persistent BOUNDED cache of rows currently in the view (~2 live).
    let mut existing_rows: HashMap<String, RowRecord> = HashMap::new();

    let baseline = dhat::HeapStats::get();
    println!(
        "baseline: curr_bytes={} curr_blocks={}",
        baseline.curr_bytes, baseline.curr_blocks
    );

    let mut prev_bytes = baseline.curr_bytes;
    for i in 0..iters {
        // ── one advance: a fresh updater receives one new row, then drops ──
        let mut updater =
            CVRQueryDrivenUpdater::new(base_cvr(), "v1".to_string(), "r1".to_string(), None);
        updater.track_queries(&[], &[]);

        let n_new = i + 100;
        let id = row_id(n_new);
        let id_str = row_id_string(&id);
        let update = RowUpdate {
            version: Some("rv".to_string()),
            contents: Some(std::sync::Arc::new(
                serde_json::json!({ "id": n_new, "name": "x" }),
            )),
            ref_counts: ref_counts("q", 1),
        };
        let mut rows: HashMap<String, (RowID, RowUpdate)> = HashMap::new();
        rows.insert(id_str.clone(), (id, update));

        let _patches = updater.received(&rows, &existing_rows);
        // updater drops here — its received_rows/last_patches maps are freed.

        // Advance the bounded cache: new row in, the one from 2 iters ago out.
        existing_rows.insert(id_str, row_record(n_new));
        if i >= 2 {
            let old = row_id_string(&row_id(i + 100 - 2));
            existing_rows.remove(&old);
        }

        if (i + 1) % 1000 == 0 {
            let s = dhat::HeapStats::get();
            let delta = s.curr_bytes as i64 - prev_bytes as i64;
            println!(
                "  iter {:6}: curr_bytes={:>10} curr_blocks={:>8}  live_rows={:>3}  Δ_since_last={:+}",
                i + 1,
                s.curr_bytes,
                s.curr_blocks,
                existing_rows.len(),
                delta
            );
            prev_bytes = s.curr_bytes;
        }
    }

    let end = dhat::HeapStats::get();
    let grew = end.curr_bytes as i64 - baseline.curr_bytes as i64;
    println!(
        "END: curr_bytes={} curr_blocks={}  grew_since_baseline={:+} over {} advances ({:.2} bytes/advance)",
        end.curr_bytes,
        end.curr_blocks,
        grew,
        iters,
        grew as f64 / iters as f64
    );

    // Live bytes must stay FLAT per advance (bounded live set). A generous jitter
    // budget; a genuine per-advance retention blows through it within hundreds.
    assert!(
        (grew as f64 / iters as f64) < 4.0,
        "per-advance CVR heap growth: {grew} bytes over {iters} advances"
    );
}

#[test]
#[ignore = "profiling harness; run explicitly with --ignored --nocapture"]
fn cvr_received_advance_leak() {
    run(20_000);
}
