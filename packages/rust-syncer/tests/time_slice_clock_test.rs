//! `TimeSliceTimer` must measure EXECUTION time, not wall time.
//!
//! TS reads `performance.now()` (wall) but runs one event loop per sync-worker
//! PROCESS (`ZERO_NUM_SYNC_WORKERS`, 6 in the replay sandbox), so a running slice
//! is never preempted and its wall time IS its execution time. Rust's shard
//! model (INVENTIONS.md I-12) runs `ZERO_SYNCER_SHARDS` `current_thread`
//! executors as OS threads — 1,523 on a 20-core cpuset in the replay sandbox — so
//! wall time additionally counts OS preemption TS never experiences.
//!
//! That difference is not cosmetic: `MIN_ADVANCEMENT_TIME_LIMIT_MS` (50ms) is an
//! ABSOLUTE floor, and it is what stops TS's advance budget from firing on short
//! advances. Inflated wall time walks straight through it. Measured on the
//! same image and the same compressed 60m trace, ONLY `ZERO_SYNCER_SHARDS` changed:
//! 1500 shards/1523 threads -> 1,194 `advancement-timeout` resets per 10 min;
//! 40 shards/63 threads -> 3. Identical work, only the preemption differed.
//!
//! Mutation test: revert the lap clock to `Instant::now()`/`elapsed()` and the
//! blocked-thread assertion fails, because a sleeping thread accrues wall time.

use rust_syncer::services::view_syncer::view_syncer::TimeSliceTimer;

/// Burn real CPU so the process clock has something to count.
fn burn_cpu_ms(ms: u64) {
    let until = std::time::Instant::now() + std::time::Duration::from_millis(ms);
    let mut x = 0u64;
    while std::time::Instant::now() < until {
        for _ in 0..2048 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
        }
    }
    std::hint::black_box(x);
}

#[test]
fn a_descheduled_thread_does_not_accrue_advance_budget() {
    let timer = TimeSliceTimer::new();
    timer.start_without_yielding();

    // A thread that is not executing — exactly what OS preemption looks like to
    // the timer when 1,523 shard threads share 20 cores.
    std::thread::sleep(std::time::Duration::from_millis(300));
    let after_block = timer.total_elapsed();
    assert!(
        after_block < 50.0,
        "300ms of NOT executing must not be charged to the advance budget: TS's \
         event loop is never preempted, so its `performance.now()` delta would \
         be ~0 here. Got {after_block}ms, which would blow the absolute 50ms \
         MIN_ADVANCEMENT_TIME_LIMIT_MS floor without doing any work."
    );

    // Real work IS charged, so the budget still sheds when it should.
    burn_cpu_ms(120);
    let after_work = timer.total_elapsed();
    assert!(
        after_work - after_block >= 60.0,
        "CPU work must still count against the budget (TS charges it); the \
         timer moved only {}ms across ~120ms of burn",
        after_work - after_block
    );

    let total = timer.stop();
    assert!(
        total >= after_work - 1.0,
        "stop() must return the accumulated process time; got {total}"
    );
}

/// The lap clock feeds `elapsed_lap`, which is what the yield threshold reads
/// (`PipelineDriver#shouldYield` vs `yieldThresholdMs`, default 10ms). A
/// blocked thread must not look like a slice worth yielding, or a preempted
/// shard yields on every change and never makes progress.
#[test]
fn elapsed_lap_tracks_work_not_blocking() {
    let timer = TimeSliceTimer::new();
    timer.start_without_yielding();
    std::thread::sleep(std::time::Duration::from_millis(200));
    let blocked_lap = timer.elapsed_lap();
    assert!(
        blocked_lap < 10.0,
        "a blocked lap must stay under the 10ms yield threshold; got {blocked_lap}ms"
    );
    burn_cpu_ms(60);
    assert!(
        timer.elapsed_lap() > 10.0,
        "a lap that burned 60ms of CPU must exceed the yield threshold"
    );
}
