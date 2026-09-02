//! Non-vacuous guards for the IVM time-slicing port (D1): the `'yield'`
//! sentinel produced by the SQLite `TableSource` (zqlite `generateWithYields`)
//! reaches the view-syncer's consumer through the engine + driver as
//! `StreamItem::Yield`; `yieldProcess` actually hands the shard's event loop to
//! a co-scheduled task; and `TimeSliceTimer` measures process time (yielded
//! time excluded) — the number per-query hydration time and the advance budget
//! are built on (pipeline-driver.ts:703 / view-syncer.ts:2943-3010).
//!
//! Each test fails on the pre-port shape: the engine `skip_yields`'d every
//! sentinel and the source never produced one (0 yields at any threshold); a
//! callback-driven hydrate could not yield the thread at all; and a wall-clock
//! timer would count the other task's slice.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use rusqlite::Connection;
use rust_ivm::ivm::stream::StreamItem;
use rust_syncer::services::view_syncer::pipeline_driver::IvmPipelines;
use rust_syncer::services::view_syncer::view_syncer::TimeSliceTimer;

const ROWS: usize = 200;

fn pipelines_over_rows(threshold: Rc<dyn Fn() -> f64>) -> IvmPipelines {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE "item" ("id" "text|NOT_NULL", "n" "text", "_0_version" "text");
        CREATE UNIQUE INDEX "item_pk" ON "item" ("id");
        "#,
    )
    .unwrap();
    for i in 0..ROWS {
        conn.execute(
            r#"INSERT INTO "item" VALUES (?1, ?2, '01')"#,
            rusqlite::params![format!("id{i:04}"), format!("{i}")],
        )
        .unwrap();
    }
    let specs = rust_syncer::compute_zql_specs(&conn).unwrap();
    let mut pipelines = IvmPipelines::new();
    // TS ctor param `yieldThresholdMs` (pipeline-driver.ts:304); installed
    // BEFORE `init` so every TableSource gets `() => this.#shouldYield()`.
    pipelines.set_yield_threshold_ms(threshold);
    pipelines
        .init_from_connection(specs, Rc::new(RefCell::new(conn)))
        .unwrap();
    pipelines
}

/// Drain one hydrate of `item`, returning (data rows, yields).
fn hydrate_counts(pipelines: &mut IvmPipelines) -> (usize, usize) {
    let timer = Rc::new(TimeSliceTimer::new());
    timer.start_without_yielding();
    let mut changes = pipelines
        .hydrate(
            &[("q1".to_string(), r#"{"table":"item"}"#.to_string())],
            timer,
        )
        .unwrap();
    let (mut data, mut yields) = (0usize, 0usize);
    for item in changes.by_ref() {
        match item {
            StreamItem::Data(rc) => {
                assert_eq!(rc.table, "item");
                data += 1;
            }
            StreamItem::Yield => yields += 1,
        }
    }
    changes.finish();
    (data, yields)
}

/// The sentinel round-trips: with a 0ms threshold `shouldYield()` is true
/// after every row (zqlite table-source.ts:692-699 checks it per node), so the
/// consumer sees yields interleaved with the rows; with an infinite threshold
/// it sees none. The row set is identical either way — a yield is control
/// flow, never data.
#[test]
fn hydrate_surfaces_a_yield_per_row_when_the_slice_threshold_is_exceeded() {
    let (data, yields) = hydrate_counts(&mut pipelines_over_rows(Rc::new(|| 0.0)));
    assert_eq!(data, ROWS, "every row is delivered");
    assert!(
        yields >= ROWS,
        "a 0ms threshold must yield before every row (got {yields} yields for {data} rows)"
    );

    let (data, yields) = hydrate_counts(&mut pipelines_over_rows(Rc::new(|| f64::INFINITY)));
    assert_eq!(data, ROWS);
    assert_eq!(yields, 0, "an unreachable threshold must never yield");
}

/// `should_yield` is TS `elapsedLap() > yieldThresholdMs()`: the lap is what
/// `TimeSliceTimer::yield_process` resets, so a fresh lap yields nothing until
/// the threshold passes. With a generous threshold and a tiny table, zero
/// yields; the previous assertion (0ms → yields) proves the same wiring is
/// live, so this one pins the comparison direction.
#[test]
fn a_fresh_lap_under_the_threshold_does_not_yield() {
    let (data, yields) = hydrate_counts(&mut pipelines_over_rows(Rc::new(|| 10_000.0)));
    assert_eq!(data, ROWS);
    assert_eq!(yields, 0);
}

/// Port contract of TS `yieldProcess` (view-syncer.ts:2861): between two time
/// slices the event loop runs — a task queued behind the slice owner on the
/// same shard gets its turn BEFORE the owner's next slice. On a
/// `current_thread` runtime + `LocalSet` (the shard model) that is exactly what
/// `tokio::task::yield_now` after the slice queue's lock gives us. A no-op
/// `yield_process` runs task A to completion in one poll and B never sees the
/// flag flip before A finishes.
#[tokio::test]
async fn yield_process_lets_a_co_scheduled_task_run_before_the_slice_owner_finishes() {
    let local = tokio::task::LocalSet::new();
    let b_ran = Rc::new(Cell::new(false));
    let b_seen_by_a = Rc::new(Cell::new(false));
    local
        .run_until(async {
            let b_ran_a = Rc::clone(&b_ran);
            let seen = Rc::clone(&b_seen_by_a);
            let a = tokio::task::spawn_local(async move {
                let timer = TimeSliceTimer::new();
                timer.start_without_yielding();
                for _ in 0..5 {
                    // A slice of work, then hand the loop back.
                    let t = std::time::Instant::now();
                    while t.elapsed() < std::time::Duration::from_millis(1) {}
                    timer.yield_process().await;
                }
                timer.stop();
                seen.set(b_ran_a.get());
            });
            let b_ran_b = Rc::clone(&b_ran);
            let b = tokio::task::spawn_local(async move {
                b_ran_b.set(true);
            });
            a.await.unwrap();
            b.await.unwrap();
        })
        .await;
    assert!(b_ran.get());
    assert!(
        b_seen_by_a.get(),
        "task B (queued behind A) must run during one of A's yields, not after A finishes"
    );
}

/// `TimeSliceTimer.totalElapsed()` is process time: `yieldProcess` stops the
/// lap before yielding and starts a new one after (view-syncer.ts:2965-2969),
/// so a slow task that runs during the yield is NOT charged to the timer.
/// This is the number `hydration_time_ms` (and hence the advance economic
/// budget) is measured in, so a wall-clock timer here would inflate the
/// advance budget by whatever the neighbour did.
#[tokio::test]
async fn time_slice_timer_excludes_time_spent_yielded() {
    let local = tokio::task::LocalSet::new();
    let total = local
        .run_until(async {
            let a = tokio::task::spawn_local(async move {
                let timer = TimeSliceTimer::new();
                timer.start_without_yielding();
                let t = std::time::Instant::now();
                while t.elapsed() < std::time::Duration::from_millis(1) {}
                timer.yield_process().await;
                timer.stop()
            });
            // Queued behind A; runs during A's yield and blocks the thread.
            let b = tokio::task::spawn_local(async move {
                std::thread::sleep(std::time::Duration::from_millis(40));
            });
            let total = a.await.unwrap();
            b.await.unwrap();
            total
        })
        .await;
    assert!(
        total < 20.0,
        "total_elapsed must exclude the 40ms the neighbour ran during the yield (got {total} ms)"
    );
}
