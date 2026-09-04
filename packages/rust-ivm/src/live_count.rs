//! Live-instance census for leak hunting. Each tracked type increments its
//! counter on construction and decrements in `Drop`; `snapshot()` renders the
//! process-wide totals. Rendered (env-gated) at snapshotter teardown to show which
//! operator structs survive `Engine::destroy` — a nonzero census after the
//! graph is destroyed means Rc cycles (or an external holder) are retaining
//! subtrees, which in turn pin source DB cells and defer SQLite closes.
//! Counters are process-global across engines; the interesting signal is the
//! DELTA logged across teardown stages, not the absolute value.
use std::sync::atomic::{AtomicI64, Ordering};

pub static TABLE_SOURCE: AtomicI64 = AtomicI64::new(0);
pub static TABLE_SOURCE_INPUT: AtomicI64 = AtomicI64::new(0);
pub static TABLE_CONNECTION: AtomicI64 = AtomicI64::new(0);
pub static UNION_FAN_OUT: AtomicI64 = AtomicI64::new(0);
pub static UNION_FAN_IN: AtomicI64 = AtomicI64::new(0);
pub static JOIN: AtomicI64 = AtomicI64::new(0);
pub static FLIPPED_JOIN: AtomicI64 = AtomicI64::new(0);
pub static EXISTS: AtomicI64 = AtomicI64::new(0);
/// Plan graphs (planner). Nonzero after a `plan_ast` returns = a graph
/// (or an escaped node subtree) is being retained — the planner leak class.
pub static PLANNER_GRAPH: AtomicI64 = AtomicI64::new(0);
/// Planner nodes (connection/join/fan-in/fan-out/terminus), aggregate.
pub static PLANNER_NODE: AtomicI64 = AtomicI64::new(0);

pub fn inc(c: &AtomicI64) {
    c.fetch_add(1, Ordering::Relaxed);
}

pub fn dec(c: &AtomicI64) {
    c.fetch_sub(1, Ordering::Relaxed);
}

pub fn snapshot() -> String {
    format!(
        "ts={} tsi={} conn={} ufo={} ufi={} join={} fjoin={} exists={} pgraph={} pnode={}",
        TABLE_SOURCE.load(Ordering::Relaxed),
        TABLE_SOURCE_INPUT.load(Ordering::Relaxed),
        TABLE_CONNECTION.load(Ordering::Relaxed),
        UNION_FAN_OUT.load(Ordering::Relaxed),
        UNION_FAN_IN.load(Ordering::Relaxed),
        JOIN.load(Ordering::Relaxed),
        FLIPPED_JOIN.load(Ordering::Relaxed),
        EXISTS.load(Ordering::Relaxed),
        PLANNER_GRAPH.load(Ordering::Relaxed),
        PLANNER_NODE.load(Ordering::Relaxed),
    )
}
