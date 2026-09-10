//! Env-gated perf-trace instrumentation (RUST_IVM_PERF_TRACE).
//!
//! Usage: `let _t = crate::perf_trace::scope("name");` at the top of the timed
//! region (RAII; drops at scope end). Nested scopes double-count into parents —
//! intentional (umbrella + decomposition).
//!
//! If RUST_IVM_PERF_TRACE's value starts with '/', report lines are ALSO
//! appended to that file (vitest can swallow addon stderr).
//!
//! The planner's per-attempt cost-model dump (`[rust-ivm][PLANDBG]`, engine
//! `plan_ast`) is NOT part of the perf trace: it has its own gate,
//! `RUST_IVM_PLAN_DEBUG` (see [`plan_debug_dump_enabled`]).

use rustc_hash::FxHashMap;
use std::cell::RefCell;
use std::time::Instant;

thread_local! {
    static STATS: RefCell<FxHashMap<&'static str, (u128 /*ns*/, u64 /*hits*/)>> =
        RefCell::new(FxHashMap::default());
}

fn env_value() -> Option<&'static str> {
    static VAL: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    VAL.get_or_init(|| std::env::var("RUST_IVM_PERF_TRACE").ok())
        .as_deref()
}

pub fn enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| env_value().is_some())
}

/// Whether `plan_ast` dumps every planner cost-model event
/// (`[rust-ivm][PLANDBG] {"type":"node-cost",...}`) for PRODUCTION plans.
/// Gated by `RUST_IVM_PLAN_DEBUG` ALONE — never by `RUST_IVM_PERF_TRACE`.
///
/// Until 2026-09-08 the dump rode on the perf-trace env: on the GKE sandbox
/// (`RUST_IVM_PERF_TRACE=1`) it was 54,355 of 55,608 log lines (19 MB in 11.5
/// min for ONE user), made kubelet rotate the container log every ~80 s, and
/// its synchronous `eprintln` per event ran inside `hydrate.build` (13,434
/// lines during one 1.8 s hydrate). TS has no such output at all; the same data
/// is `analyzeQuery --join-plans`. Read once, cached.
pub fn plan_debug_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        plan_debug_dump_enabled(
            env_value(),
            std::env::var("RUST_IVM_PLAN_DEBUG").ok().as_deref(),
        )
    })
}

/// Pure gate behind [`plan_debug_enabled`]: `(RUST_IVM_PERF_TRACE,
/// RUST_IVM_PLAN_DEBUG)` → dump? Only the dedicated env (non-empty, not `0`)
/// enables the dump; the perf-trace env is deliberately ignored.
pub fn plan_debug_dump_enabled(perf_trace: Option<&str>, plan_debug: Option<&str>) -> bool {
    let _ = perf_trace;
    plan_debug.is_some_and(|v| !v.is_empty() && v != "0")
}

pub struct Scope {
    name: &'static str,
    start: Instant,
}

#[inline]
pub fn scope(name: &'static str) -> Option<Scope> {
    enabled().then(|| Scope {
        name,
        start: Instant::now(),
    })
}

impl Drop for Scope {
    fn drop(&mut self) {
        let d = self.start.elapsed().as_nanos();
        STATS.with(|s| {
            let mut s = s.borrow_mut();
            let e = s.entry(self.name).or_insert((0, 0));
            e.0 += d;
            e.1 += 1;
        });
    }
}

pub fn reset() {
    if enabled() {
        STATS.with(|s| s.borrow_mut().clear());
    }
}

pub fn report(op: &str, total_ms: f64) {
    if !enabled() {
        return;
    }
    STATS.with(|s| {
        let mut v: Vec<_> = s.borrow().iter().map(|(k, &(ns, n))| (*k, ns, n)).collect();
        v.sort_by_key(|e| std::cmp::Reverse(e.1));
        let lines: Vec<String> = v
            .iter()
            .map(|(k, ns, n)| {
                let ms = *ns as f64 / 1e6;
                format!(
                    "{k}={ms:.1}ms({:.0}%)/{n}h/{:.1}us",
                    if total_ms > 0.0 {
                        ms / total_ms * 100.0
                    } else {
                        0.0
                    },
                    ms * 1000.0 / (*n).max(1) as f64
                )
            })
            .collect();
        let line = format!(
            "[rust-ivm][PERF] {op} total={total_ms:.1}ms  {}",
            lines.join("  ")
        );
        emit(&line);
        // Clear after printing so anything accumulated AFTER this report (e.g.
        // `deliver.drain`, which runs post-compute on this same client-group
        // thread) is exactly what `report_residual` picks up.
        s.borrow_mut().clear();
    });
}

/// Report ONLY the stats accumulated since the last `report()`/`reset()` on
/// this thread — the post-engine residual (e.g. `deliver.drain`, which runs
/// after the engine's own report). Prints nothing when empty; clears after
/// printing. MUST be called on the same thread that ran the scopes (the engine
/// actor thread).
pub fn report_residual(op: &str) {
    if !enabled() {
        return;
    }
    STATS.with(|s| {
        {
            let stats = s.borrow();
            if stats.is_empty() {
                return;
            }
            let mut v: Vec<_> = stats.iter().map(|(k, &(ns, n))| (*k, ns, n)).collect();
            v.sort_by_key(|e| std::cmp::Reverse(e.1));
            // Nested scopes double-count into parents; use the largest span as
            // the umbrella total (in practice this is `deliver.drain`).
            let total_ms = v.first().map(|e| e.1 as f64 / 1e6).unwrap_or(0.0);
            let lines: Vec<String> = v
                .iter()
                .map(|(k, ns, n)| {
                    let ms = *ns as f64 / 1e6;
                    format!(
                        "{k}={ms:.1}ms/{n}h/{:.1}us",
                        ms * 1000.0 / (*n).max(1) as f64
                    )
                })
                .collect();
            emit(&format!(
                "[rust-ivm][PERF] {op}-post total={total_ms:.1}ms  {}",
                lines.join("  ")
            ));
        }
        s.borrow_mut().clear();
    });
}

fn emit(line: &str) {
    eprintln!("{line}");
    if let Some(path) = env_value().filter(|v| v.starts_with('/')) {
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            let _ = writeln!(f, "{line}");
        }
    }
}

#[cfg(test)]
mod plan_debug_gate_tests {
    use super::plan_debug_dump_enabled;

    /// Non-vacuous: the pre-2026-09-08 gate was `RUST_IVM_PERF_TRACE` set (any
    /// value) — reverting `plan_debug_dump_enabled` to `perf_trace.is_some()`
    /// fails the first assertion (the sandbox flood) and the third (the
    /// dedicated env did not exist).
    #[test]
    fn perf_trace_alone_never_enables_the_planner_dump() {
        assert!(!plan_debug_dump_enabled(Some("1"), None));
        assert!(!plan_debug_dump_enabled(Some("/tmp/perf.log"), None));
        assert!(plan_debug_dump_enabled(None, Some("1")));
        assert!(plan_debug_dump_enabled(Some("1"), Some("1")));
        assert!(!plan_debug_dump_enabled(None, Some("")));
        assert!(!plan_debug_dump_enabled(None, Some("0")));
    }
}
