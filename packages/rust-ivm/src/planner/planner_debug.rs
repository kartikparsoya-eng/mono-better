//! Port of `packages/zql/src/planner/planner-debug.ts` — the structured
//! planner debug-event stream that feeds `analyzeQuery`'s `joinPlans`.
//!
//! Ports `AccumulatorDebugger` (collects events, stamps `attemptNumber` onto
//! node events) and `serializePlanDebugEvents` (drops the internal
//! `planSnapshot` from `plan-complete`). Events are built directly in the
//! `PlanDebugEventJSON` wire shape (serde_json `Value`) at emission time — rust
//! has no `undefined`, so TS's `convertConstraintUndefinedToNull` step is a
//! no-op here (absent constraint values are emitted as `null`).
//!
//! `formatPlannerEvents` (the human-readable CLI formatter, planner-debug.ts:474)
//! is NOT ported: it is not on the `joinPlans` wire path (only
//! `serializePlanDebugEvents` is called by `analyze.ts`).
//!
//! Rust-only adaptation (AGENTS rule 5): TS threads `planDebugger?` as an
//! explicit parameter through every planner node method (`estimateCost`,
//! `propagateConstraints`) and the graph's `plan`. The rust planner dispatches
//! node methods through the `PlannerNode` enum; threading a param through all
//! impls + the enum + recursion would be a large signature change with NO
//! behavioral difference. Since the analysis planner runs single-threaded (the
//! `!Send` analyze engine on one blocking thread), a thread-local sink is
//! equivalent and localized. The emitted events + their fields/order are 1:1
//! with TS; only the plumbing (thread-local vs. param) differs.

use std::cell::RefCell;
use std::rc::Rc;

use serde_json::{Value, json};

use crate::ivm::data::Value as IvmValue;
use crate::planner::planner_constraint::PlannerConstraint;
use crate::planner::planner_node::{CostEstimate, JoinType, NodeKind};

/// Port of `AccumulatorDebugger` (planner-debug.ts:144): collects every event,
/// tracking the current attempt so `node-cost` / `node-constraint` events (which
/// TS emits without an attempt number) get stamped with it.
#[derive(Default)]
pub struct AccumulatorDebugger {
    pub events: Vec<Value>,
    current_attempt: i64,
}

impl AccumulatorDebugger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Port of `AccumulatorDebugger.log` (planner-debug.ts:148).
    pub fn log(&mut self, mut event: Value) {
        match event.get("type").and_then(Value::as_str) {
            Some("attempt-start") => {
                if let Some(n) = event.get("attemptNumber").and_then(Value::as_i64) {
                    self.current_attempt = n;
                }
            }
            Some("node-cost") | Some("node-constraint") => {
                if let Value::Object(map) = &mut event {
                    map.insert("attemptNumber".to_string(), json!(self.current_attempt));
                }
            }
            _ => {}
        }
        self.events.push(event);
    }
}

thread_local! {
    /// The active debugger for the current thread's planning pass (see the
    /// module doc for why this is a thread-local rather than a threaded param).
    static PLAN_DEBUGGER: RefCell<Option<Rc<RefCell<AccumulatorDebugger>>>> =
        const { RefCell::new(None) };
}

/// Emit a planner debug event if a debugger is active on this thread. `build` is
/// invoked ONLY when a debugger is installed, so the production hot path (no
/// debugger) pays just one thread-local `Option` check per would-be event.
pub fn plan_debug_log(build: impl FnOnce() -> Value) {
    PLAN_DEBUGGER.with(|d| {
        if let Some(dbg) = d.borrow().as_ref() {
            dbg.borrow_mut().log(build());
        }
    });
}

/// Run `f` with `dbg` installed as the active debugger, restoring the previous
/// debugger afterward (RAII, panic-safe). Mirrors TS passing `planDebugger`
/// down a single `plan` call.
pub fn with_plan_debugger<R>(dbg: Rc<RefCell<AccumulatorDebugger>>, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<Rc<RefCell<AccumulatorDebugger>>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            PLAN_DEBUGGER.with(|d| *d.borrow_mut() = self.0.take());
        }
    }
    let prev = PLAN_DEBUGGER.with(|d| d.borrow_mut().replace(dbg));
    let _restore = Restore(prev);
    f()
}

/// Port of `omitFanout(cost)` (planner-node.ts:61) as JSON — the `CostEstimate`
/// without its (non-serializable) `fanout` closure.
pub fn omit_fanout(cost: &CostEstimate) -> Value {
    json!({
        "startupCost": cost.startup_cost,
        "scanEst": cost.scan_est,
        "cost": cost.cost,
        "returnedRows": cost.returned_rows,
        "selectivity": cost.selectivity,
        "limit": cost.limit,
    })
}

/// `"semi"` / `"flipped"` — the wire form of a join type.
pub fn join_type_str(t: JoinType) -> &'static str {
    match t {
        JoinType::Semi => "semi",
        JoinType::Flipped => "flipped",
    }
}

/// The wire form of a node kind (TS `PlannerNode['kind']`).
pub fn node_kind_str(k: NodeKind) -> &'static str {
    match k {
        NodeKind::Connection => "connection",
        NodeKind::Join => "join",
        NodeKind::FanOut => "fan-out",
        NodeKind::FanIn => "fan-in",
        NodeKind::Terminus => "terminus",
    }
}

/// Serialize a `PlannerConstraint` (or its absence) to the debug JSON form.
/// TS `PlannerConstraint` is a `Record<string, undefined>` whose keys are the
/// constrained columns (values unknown at plan time); TS converts the
/// `undefined` values to `null` for JSON. Rust's `Option<Value>` maps `None`
/// (unknown) to `null` directly.
pub fn constraint_to_json(c: Option<&PlannerConstraint>) -> Value {
    match c {
        None => Value::Null,
        Some(map) => {
            let mut obj = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                obj.insert(
                    k.clone(),
                    match v {
                        Some(val) => ivm_value_to_json(val),
                        None => Value::Null,
                    },
                );
            }
            Value::Object(obj)
        }
    }
}

/// Minimal IVM `Value` → JSON for constraint values (rare — usually `null`).
fn ivm_value_to_json(v: &IvmValue) -> Value {
    match v {
        IvmValue::Null => Value::Null,
        IvmValue::Bool(b) => Value::Bool(*b),
        IvmValue::F64(n) => serde_json::Number::from_f64(*n)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        IvmValue::Str(s) => Value::String(s.to_string()),
        IvmValue::Json(s) => {
            serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.to_string()))
        }
    }
}

/// Port of `serializePlanDebugEvents` (planner-debug.ts:453): drop the internal
/// `planSnapshot` from `plan-complete` events. (Rust builds `plan-complete`
/// without a `planSnapshot` field, so this is defensive; the
/// undefined→null constraint conversion is already done at emission time.)
pub fn serialize_plan_debug_events(events: &[Value]) -> Vec<Value> {
    events.iter().map(serialize_event).collect()
}

fn serialize_event(event: &Value) -> Value {
    if event.get("type").and_then(Value::as_str) == Some("plan-complete")
        && let Value::Object(map) = event
    {
        let mut m = map.clone();
        m.remove("planSnapshot");
        return Value::Object(m);
    }
    event.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulator_stamps_attempt_number_on_node_events() {
        let mut dbg = AccumulatorDebugger::new();
        dbg.log(json!({"type": "attempt-start", "attemptNumber": 3, "totalAttempts": 4}));
        dbg.log(json!({"type": "node-cost", "nodeType": "connection", "node": "issue"}));
        // The node-cost event is stamped with the current attempt (3).
        assert_eq!(dbg.events[1]["attemptNumber"], json!(3));
    }

    #[test]
    fn serialize_drops_plan_snapshot() {
        let events = vec![json!({
            "type": "plan-complete",
            "attemptNumber": 0,
            "planSnapshot": {"internal": true},
            "totalCost": 1.0,
        })];
        let out = serialize_plan_debug_events(&events);
        assert!(out[0].get("planSnapshot").is_none());
        assert_eq!(out[0]["totalCost"], json!(1.0));
    }
}
