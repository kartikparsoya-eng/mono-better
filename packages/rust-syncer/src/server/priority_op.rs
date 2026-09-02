//! Port of `zero-cache/src/server/priority-op.ts`.
//!
//! TS keeps `runningPriorityOpCounter` as a module-level `let` — one counter
//! per sync-worker PROCESS, i.e. per event loop. The rust twin is a thread-local
//! on the shard's executor thread: each shard is one `current_thread` runtime,
//! i.e. one event loop, so "a priority op is running on this event loop" means
//! the same thing on both sides. `PipelineDriver`'s `yieldThresholdMs` selector
//! reads it (server/syncer.ts:230-233) to shrink IVM time slices while one runs.

use std::cell::Cell;
use std::future::Future;

thread_local! {
    static PRIORITY_OP_COUNTER: Cell<u64> = const { Cell::new(0) };
    static RUNNING_PRIORITY_OP_COUNTER: Cell<u64> = const { Cell::new(0) };
}

/// Decrements the running counter on drop — the twin of TS's `finally {
/// runningPriorityOpCounter--; }`. Rust-only shape (rule 5): a future can be
/// dropped mid-await (task cancellation), which a JS `finally` never sees, so
/// the decrement is RAII rather than a trailing statement.
struct RunningPriorityOp;

impl Drop for RunningPriorityOp {
    fn drop(&mut self) {
        RUNNING_PRIORITY_OP_COUNTER.with(|c| c.set(c.get().saturating_sub(1)));
    }
}

/// Run an operation with priority, indicating that IVM should use smaller time
/// slices to allow this operation to proceed more quickly.
///
/// Port of TS `runPriorityOp` (priority-op.ts:10-33). TS's `lc` (LogContext)
/// is not carried; the `priorityOpID` context becomes a tracing field.
pub async fn run_priority_op<T, F: Future<Output = T>>(description: &str, op: F) -> T {
    let id = PRIORITY_OP_COUNTER.with(|c| {
        let id = c.get();
        c.set(id + 1);
        id
    });
    RUNNING_PRIORITY_OP_COUNTER.with(|c| c.set(c.get() + 1));
    let _running = RunningPriorityOp;
    let start = std::time::Instant::now();
    tracing::debug!(priority_op_id = id, "running priority op {description}");
    let result = op.await;
    tracing::debug!(
        priority_op_id = id,
        "finished priority op {description} in {} ms",
        start.elapsed().as_millis()
    );
    result
}

/// Port of TS `isPriorityOpRunning` (priority-op.ts:35-37).
pub fn is_priority_op_running() -> bool {
    RUNNING_PRIORITY_OP_COUNTER.with(|c| c.get() > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Non-vacuous: the counter is observable only DURING the op, and a
    /// dropped (cancelled) op releases it — the RAII twin of TS `finally`.
    #[tokio::test]
    async fn priority_op_is_running_only_while_the_op_is_in_flight() {
        assert!(!is_priority_op_running());
        let seen_inside = run_priority_op("t", async { is_priority_op_running() }).await;
        assert!(seen_inside);
        assert!(!is_priority_op_running());

        // Nested ops: running until the LAST one finishes.
        run_priority_op("outer", async {
            run_priority_op("inner", async {}).await;
            assert!(is_priority_op_running(), "outer still running after inner");
        })
        .await;
        assert!(!is_priority_op_running());

        // A cancelled (dropped) op must not leak a running count: poll it
        // once (it increments and parks), then drop it mid-await.
        let mut pinned = Box::pin(run_priority_op("dropped", std::future::pending::<()>()));
        std::future::poll_fn(|cx| {
            let _ = std::pin::Pin::new(&mut pinned).poll(cx);
            std::task::Poll::Ready(())
        })
        .await;
        assert!(is_priority_op_running(), "counted while pending");
        drop(pinned);
        assert!(!is_priority_op_running(), "released on drop");
    }
}
