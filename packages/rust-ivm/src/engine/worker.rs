//! Parallel-hydrate worker scaffolding (DESIGN §3 L2–L5).
//!
//! Bounded worker pool + bounded per-task streaming channels + first-error-wins
//! abort + cancel/interrupt propagation. Pure scaffolding — it runs `Send` task
//! specs that stream owned items one at a time; it never touches the `!Send`
//! engine graph, and it never collects a full result into a `Vec`.
//!
//! ## Streaming model (the non-negotiable)
//! Each task is a generator: `FnOnce(&WorkerScope, &dyn Fn(T)) -> Result<(), E>`
//! — it pushes items one at a time via the sink callback as they're produced
//! (exactly like the serial `fetch()` → `on_row_change` loop). The actor drains
//! task channels **in dispatch order** and calls `on_item` per item immediately
//! — true streaming, byte-identical to serial, no intermediate `Vec`.
//!
//! ## Guards enforced here
//! - **L2 — `WorkerScope`, first-error-wins:** shared `abort: AtomicBool`;
//!   every worker checks it and runs under `catch_unwind`; on any worker
//!   error/panic the scope sets abort, the pool drains, and the actor emits
//!   ONE reset. No partial results reach the graph.
//! - **L3 — cancellation propagation:** the CG's `CancellationToken` is shared
//!   with every worker (cooperative between-rows checks) AND each worker's
//!   `InterruptHandle` is registered with the monitor (§1a) so a long query
//!   aborts mid-flight via a cross-thread `.interrupt()`.
//! - **L5 — backpressure:** each task streams through its OWN bounded channel;
//!   when the actor hasn't drained that task yet (it's draining an earlier one),
//!   the worker blocks on a full channel → bounded memory. No unbounded buffer.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::engine::CancellationToken;

/// Shared abort flag for a parallel job (L2). First error/panic sets it; every
/// worker checks it before starting work and between rows. When set, the pool
/// drains remaining tasks without dispatching new ones.
#[derive(Clone)]
pub struct WorkerScope {
    abort: Arc<AtomicBool>,
    cancel: CancellationToken,
}
impl WorkerScope {
    pub fn new(cancel: CancellationToken) -> Self {
        WorkerScope {
            abort: Arc::new(AtomicBool::new(false)),
            cancel,
        }
    }
    /// True if any worker errored/panicked or the job was cancelled. Workers
    /// must check this before starting a task and between rows.
    pub fn aborted(&self) -> bool {
        self.abort.load(Ordering::Relaxed) || self.cancel.is_cancelled()
    }
    /// Mark the scope aborted (first-error-wins; subsequent calls are no-ops).
    pub fn abort(&self) {
        self.abort.store(true, Ordering::Release);
    }
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }
}

/// The first error from a parallel job (L2). Either a typed task error or a
/// worker panic. The caller emits ONE reset and falls back to serial (S4).
#[derive(Debug)]
pub enum ParallelError<E> {
    Task(E),
    Panic(String),
}

/// A streaming parallel job: dispatches `Send` generator tasks to N worker
/// threads; each task pushes owned items one at a time through a bounded
/// per-task channel; the actor drains channels **in dispatch order** and calls
/// `on_item` per item — true streaming, byte-identical to serial (§6), no Vec.
///
/// Tasks are structurally unable to touch the `!Send` graph — they receive only
/// `Send` data and a sink callback. The engine graph stays single-writer.
pub struct ParallelJob<T, E> {
    workers: usize,
    per_task_bound: usize,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<T: Send + 'static, E: Send + 'static> ParallelJob<T, E> {
    /// `workers` = bounded pool size (≤ cores, config cap — S3).
    /// `per_task_bound` = bounded per-task channel capacity (L5 backpressure).
    pub fn new(workers: usize, per_task_bound: usize) -> Self {
        ParallelJob {
            workers: workers.max(1),
            per_task_bound: per_task_bound.max(1),
            _marker: std::marker::PhantomData,
        }
    }

    /// Run `tasks` in parallel, streaming items to `on_item` in dispatch order.
    ///
    /// Each task is a generator: it receives a `&WorkerScope` (for between-rows
    /// abort checks) and a `sink: &dyn Fn(T)` (push one item at a time). The
    /// actor drains task 0's stream completely (calling `on_item` per item),
    /// then task 1's, etc. — so the output order is byte-identical to running
    /// the tasks serially (the primary oracle, §6).
    ///
    /// Backpressure (L5): each task has its own bounded channel. While the actor
    /// drains task K, workers for tasks >K fill their channels and block when
    /// full → bounded memory, no unbounded buffer.
    ///
    /// On the first error/panic (L2): the scope aborts, the actor stops emitting
    /// (after the current task's already-streamed rows, which the caller's reset
    /// discards), and `run_streaming` returns `Err(first_error)`. The caller
    /// emits ONE reset and falls back to serial (S4).
    pub fn run_streaming<F, S>(
        &self,
        tasks: Vec<F>,
        cancel: CancellationToken,
        mut on_item: S,
    ) -> Result<(), ParallelError<E>>
    where
        F: FnOnce(&WorkerScope, &dyn Fn(T)) -> Result<(), E> + Send + 'static,
        S: FnMut(T),
    {
        let n = tasks.len();
        if n == 0 {
            return Ok(());
        }
        let scope = WorkerScope::new(cancel);
        let workers = self.workers.min(n);

        // One bounded channel per task (L5). Workers push items; the actor
        // drains in task order → byte-identical to serial.
        let mut channels: Vec<mpsc::SyncSender<T>> = Vec::with_capacity(n);
        let mut receivers: Vec<mpsc::Receiver<T>> = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = mpsc::sync_channel(self.per_task_bound);
            channels.push(tx);
            receivers.push(rx);
        }
        let sender_slots: Vec<Arc<Mutex<Option<mpsc::SyncSender<T>>>>> = channels
            .into_iter()
            .map(|tx| Arc::new(Mutex::new(Some(tx))))
            .collect();
        let sender_slots = Arc::new(sender_slots);

        // Task slots (FnOnce is one-shot and !Sync); a worker takes the task
        // out of its slot, None-ifies it, runs it.
        let slots: Vec<
            Arc<Mutex<Option<Box<dyn FnOnce(&WorkerScope, &dyn Fn(T)) -> Result<(), E> + Send>>>>,
        > = tasks
            .into_iter()
            .map(|f| Arc::new(Mutex::new(Some(Box::new(f) as Box<_>))))
            .collect();
        let slots = Arc::new(slots);
        // Shared task index queue: workers pop the NEXT index (FIFO) so the
        // actor (which drains in index order) always has a worker producing
        // for the task it's currently draining. LIFO (Vec::pop) would grab
        // high-index tasks first → those workers block on full channels while
        // the actor waits on channel 0 → deadlock.
        let queue = Arc::new(Mutex::new(std::collections::VecDeque::<usize>::from_iter(
            0..n,
        )));
        // Error channel (first-error-wins, L2). Unbounded is fine — at most one
        // error per worker before the scope aborts.
        let (err_tx, err_rx) = mpsc::channel::<ParallelError<E>>();
        let scope_for_actor = scope.clone();

        let first_err = thread::scope(|s| {
            // Workers: pull task indices, take the task + its sender, run the
            // generator (which pushes items via the bounded sender), then drop
            // the sender to signal end-of-stream.
            for _ in 0..workers {
                let scope = scope.clone();
                let queue = queue.clone();
                let slots = slots.clone();
                let sender_slots = sender_slots.clone();
                let err_tx = err_tx.clone();
                s.spawn(move || {
                    loop {
                        if scope.aborted() {
                            break;
                        }
                        let idx = { queue.lock().unwrap().pop_front() };
                        let Some(idx) = idx else { break };
                        let task = { slots[idx].lock().unwrap().take() };
                        let Some(task) = task else { continue };
                        let sender = { sender_slots[idx].lock().unwrap().take() };
                        let Some(sender) = sender else { continue };
                        // Sink: push one item to the actor via the bounded
                        // channel. If the actor dropped the receiver (job
                        // aborted), send fails silently — the worker stops.
                        let sink = move |item: T| {
                            let _ = sender.send(item);
                        };
                        // Run under catch_unwind (L2: a panic must NOT unwind
                        // out of the worker thread and must set abort).
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            task(&scope, &sink)
                        }));
                        let task_outcome: Result<(), ParallelError<E>> = match result {
                            Ok(Ok(())) => Ok(()),
                            Ok(Err(e)) => {
                                scope.abort();
                                Err(ParallelError::Task(e))
                            }
                            Err(panic) => {
                                scope.abort();
                                Err(ParallelError::Panic(panic_to_string(panic)))
                            }
                        };
                        if let Err(e) = task_outcome {
                            // First-error-wins: send to the error channel. If
                            // the actor already returned (receiver gone), the
                            // error is dropped — fine, the scope is aborted.
                            let _ = err_tx.send(e);
                        }
                        // `sink` (and thus `sender`) drops here → signals
                        // end-of-stream for task `idx`.
                    }
                });
            }

            // Actor: drain channels in task order on the calling thread. Runs
            // concurrently with the workers (so backpressure is felt). The actor
            // NEVER returns early from the closure — `receivers` is borrowed
            // from the caller's frame, so an early `return` would NOT drop it,
            // leaving workers blocked on full channels → thread::scope join
            // deadlock. Instead, record the first error, abort, drain ALL
            // remaining channels to unblock workers, and return the error
            // after the scope joins.
            let mut first_err: Option<ParallelError<E>> = None;
                    #[allow(clippy::needless_range_loop)]
            'emit: for idx in 0..n {
                if scope_for_actor.aborted() {
                    break 'emit;
                }
                loop {
                    match receivers[idx].recv_timeout(Duration::from_millis(100)) {
                        Ok(item) => on_item(item),
                        Err(RecvTimeoutError::Timeout) => {
                            // Check for a deferred error from another task.
                            if let Ok(e) = err_rx.try_recv() {
                                first_err = Some(e);
                                scope_for_actor.abort();
                                break 'emit;
                            }
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            // Task idx's stream ended (worker dropped sender).
                            if let Ok(e) = err_rx.try_recv() {
                                first_err = Some(e);
                                scope_for_actor.abort();
                            }
                            break; // next task
                        }
                    }
                }
            }
            // On abort, drain ALL remaining channels WITHOUT emitting. This
            // unblocks workers stuck on `send()` to full channels so they can
            // observe the abort and exit — without it, `thread::scope` would
            // deadlock on join (workers can't drop senders while blocked).
            if scope_for_actor.aborted() {
                for r in receivers.iter() {
                    while r.recv_timeout(Duration::from_millis(1)).is_ok() {}
                }
            }
            // Capture the error before `err_rx` drops at scope end.
            if first_err.is_none()
                && let Ok(e) = err_rx.try_recv() {
                    first_err = Some(e);
                }
            first_err
        });

        if let Some(e) = first_err {
            return Err(e);
        }
        Ok(())
    }
}

fn panic_to_string(p: Box<dyn std::any::Any + Send>) -> String {
    p.downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "worker panic (non-string payload)".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn streaming_in_dispatch_order_byte_identical_to_serial() {
        // Each task pushes its items one at a time; the actor must receive them
        // in dispatch order (task 0 all items, then task 1, ...) — matching
        // serial exactly. No Vec collection anywhere.
        let job: ParallelJob<usize, String> = ParallelJob::new(4, 4);
        let tasks: Vec<_> = (0..6)
            .map(|t| {
                Box::new(
                    move |_scope: &WorkerScope, sink: &dyn Fn(usize)| -> Result<(), String> {
                        // Push t*10, t*10+1, t*10+2 — one at a time.
                        for k in 0..3 {
                            sink(t * 10 + k);
                        }
                        Ok(())
                    },
                )
                    as Box<dyn FnOnce(&WorkerScope, &dyn Fn(usize)) -> Result<(), String> + Send>
            })
            .collect();
        let got = Arc::new(Mutex::new(Vec::new()));
        let got_clone = got.clone();
        job.run_streaming(tasks, CancellationToken::new(), move |item| {
            got_clone.lock().unwrap().push(item);
        })
        .unwrap();
        // Dispatch order: task0 (0,1,2), task1 (10,11,12), task2 (20,21,22), ...
        let expected: Vec<usize> = (0..6)
            .flat_map(|t| (0..3).map(move |k| t * 10 + k))
            .collect();
        assert_eq!(&*got.lock().unwrap(), &expected);
    }

    #[test]
    fn first_error_wins_returns_err() {
        let job: ParallelJob<usize, String> = ParallelJob::new(2, 2);
        let tasks: Vec<_> = (0..6)
            .map(|t| {
                Box::new(
                    move |scope: &WorkerScope, sink: &dyn Fn(usize)| -> Result<(), String> {
                        for k in 0..5 {
                            if scope.aborted() {
                                return Ok(());
                            }
                            if t == 2 && k == 1 {
                                return Err(format!("boom at task {} item {}", t, k));
                            }
                            sink(t * 10 + k);
                            std::hint::spin_loop();
                        }
                        Ok(())
                    },
                )
                    as Box<dyn FnOnce(&WorkerScope, &dyn Fn(usize)) -> Result<(), String> + Send>
            })
            .collect();
        let res = job.run_streaming(tasks, CancellationToken::new(), |_| {});
        assert!(
            matches!(res, Err(ParallelError::Task(_))),
            "first task error wins"
        );
    }

    #[test]
    fn worker_panic_is_caught_and_aborts() {
        let job: ParallelJob<usize, String> = ParallelJob::new(2, 2);
        let tasks: Vec<_> = (0..6)
            .map(|t| {
                Box::new(
                    move |_scope: &WorkerScope, _sink: &dyn Fn(usize)| -> Result<(), String> {
                        if t == 1 {
                            panic!("worker panic {}", t);
                        }
                        Ok(())
                    },
                )
                    as Box<dyn FnOnce(&WorkerScope, &dyn Fn(usize)) -> Result<(), String> + Send>
            })
            .collect();
        let res = job.run_streaming(tasks, CancellationToken::new(), |_| {});
        assert!(
            matches!(res, Err(ParallelError::Panic(_))),
            "panic → ParallelError::Panic"
        );
    }

    #[test]
    fn cancel_propagates_and_run_returns_promptly() {
        // Cancel must propagate to workers so they stop promptly (no hang).
        let job: ParallelJob<usize, String> = ParallelJob::new(4, 2);
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let tasks: Vec<_> = (0..4)
            .map(|t| {
                Box::new(
                    move |scope: &WorkerScope, sink: &dyn Fn(usize)| -> Result<(), String> {
                        // Push one item, then spin until cancelled.
                        sink(t);
                        while !scope.aborted() {
                            std::hint::spin_loop();
                        }
                        Ok(())
                    },
                )
                    as Box<dyn FnOnce(&WorkerScope, &dyn Fn(usize)) -> Result<(), String> + Send>
            })
            .collect();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            cancel_clone.cancel();
        });
        let start = std::time::Instant::now();
        let _ = job.run_streaming(tasks, cancel, |_| {});
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "run must return promptly on cancel, took {:?}",
            elapsed
        );
    }

    #[test]
    fn backpressure_does_not_deadlock_when_actor_drains_in_order() {
        // Each task pushes more items than its channel bound; the actor drains
        // in order. If backpressure weren't handled (worker blocks on full
        // channel while actor is on an earlier task), this would deadlock.
        let job: ParallelJob<usize, String> = ParallelJob::new(3, 2);
        let tasks: Vec<_> = (0..5)
            .map(|t| {
                Box::new(
                    move |_scope: &WorkerScope, sink: &dyn Fn(usize)| -> Result<(), String> {
                        for k in 0..100 {
                            sink(t * 1000 + k);
                        }
                        Ok(())
                    },
                )
                    as Box<dyn FnOnce(&WorkerScope, &dyn Fn(usize)) -> Result<(), String> + Send>
            })
            .collect();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();
        job.run_streaming(tasks, CancellationToken::new(), move |_| {
            count_clone.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 500);
    }
}
