//! D (VENDED runtime toggle): the `queryHydrationStats` config option flips the
//! process-global `runtimeDebugFlags.trackRowCountsVended` (TS zero-config.ts:1213),
//! which the pipeline driver's VENDED log gate reads.
//!
//! Lives in its OWN integration binary because `runtimeDebugFlags` is a
//! process-global `AtomicBool` and `apply_runtime_debug_flags` mutates it — a
//! co-located test could race parallel tests. A dedicated binary gets a clean
//! process.
//!
//! NON-VACUOUS: with `query_hydration_stats = true` the flag turns ON; reverting
//! `apply_runtime_debug_flags` to a no-op leaves it OFF and the assertion fails.

use rust_ivm::builder::debug_delegate::runtime_debug_flags;
use rust_syncer::config::zero_config::SyncerConfig;

#[test]
fn query_hydration_stats_toggles_track_row_counts_vended() {
    let flags = runtime_debug_flags();
    let prev = flags.track_row_counts_vended();
    flags.set_track_row_counts_vended(false);

    // `from_env` with ZERO_QUERY_HYDRATION_STATS unset ⇒ the option is off and
    // applying it is a no-op (the flag stays where we left it: off).
    let mut config = SyncerConfig::from_env();
    config.query_hydration_stats = false;
    config.apply_runtime_debug_flags();
    assert!(
        !flags.track_row_counts_vended(),
        "flag stays OFF when queryHydrationStats is disabled"
    );

    // Enabling the option turns the flag ON (the VENDED log becomes eligible).
    config.query_hydration_stats = true;
    config.apply_runtime_debug_flags();
    assert!(
        flags.track_row_counts_vended(),
        "queryHydrationStats=true enables trackRowCountsVended"
    );

    flags.set_track_row_counts_vended(prev);
}
