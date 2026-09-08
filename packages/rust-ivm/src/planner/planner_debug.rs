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
//! Rust-only adaptation (AGENTS rule 5), scoped to the NODE methods only: TS
//! threads `planDebugger?` as an explicit parameter through `planQuery` ->
//! `planRecursively` -> `PlannerGraph.plan` -> every planner node method
//! (`estimateCost`, `propagateConstraints`). Rust threads the SAME parameter
//! through the first three (1:1 signatures — see `plan_query`,
//! `plan_recursively`, `PlannerGraph::plan`); only the last hop is different,
//! because the rust planner dispatches node methods through the `PlannerNode`
//! enum and threading a param through all impls + the enum + recursion would be
//! a large signature change with NO behavioral difference. `PlannerGraph::plan`
//! therefore INSTALLS the debugger it is handed as this thread's sink
//! (`install_plan_debugger`) for the duration of the call, and the node
//! emitters read it back via `plan_debug_log`. Planning is single-threaded (the
//! `!Send` engine runs on one blocking thread), so the sink is equivalent and
//! localized. The emitted events + their fields/order are 1:1 with TS.

use std::cell::RefCell;
use std::rc::Rc;

use serde_json::{Value, json};

use crate::ivm::data::Value as IvmValue;
use crate::planner::planner_constraint::PlannerConstraint;
use crate::planner::planner_node::{CostEstimate, JoinType, NodeKind};

/// Port of the `PlanDebugger` interface (planner-debug.ts:135): the sink every
/// planner event is handed to. TS's `log(event: PlanDebugEvent)` takes the
/// event object; rust builds events directly in their `PlanDebugEventJSON`
/// wire shape, so the parameter is a `serde_json::Value`.
pub trait PlanDebugger {
    fn log(&mut self, event: Value);
}

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
}

impl PlanDebugger for AccumulatorDebugger {
    /// Port of `AccumulatorDebugger.log` (planner-debug.ts:148).
    fn log(&mut self, mut event: Value) {
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

/// A `PlanDebugger` handle as the planner passes it around: TS's
/// `planDebugger?: PlanDebugger` parameter. Shared (`Rc`) because the same sink
/// is handed to the graph AND read back by the node emitters, and interior-
/// mutable (`RefCell`) because `log` takes `&mut self`.
pub type SharedPlanDebugger = Rc<RefCell<dyn PlanDebugger>>;

thread_local! {
    /// The debugger `PlannerGraph::plan` was handed, for the duration of that
    /// call — the last hop of TS's `planDebugger` parameter (see the module doc
    /// for why only the node hop uses a thread-local rather than a param).
    static PLAN_DEBUGGER: RefCell<Option<SharedPlanDebugger>> = const { RefCell::new(None) };
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

/// The debugger installed on this thread, if any. The rust-only engine layer
/// (`Engine::plan_ast`) reads it here so it can pass it EXPLICITLY to
/// `plan_query`, exactly as TS's `buildPipeline` forwards its `planDebugger`
/// option (builder.ts:141).
pub fn current_plan_debugger() -> Option<SharedPlanDebugger> {
    PLAN_DEBUGGER.with(|d| d.borrow().clone())
}

/// Installs `dbg` as this thread's active debugger until the returned guard is
/// dropped, restoring whatever was installed before (RAII, panic-safe).
#[must_use = "the debugger is uninstalled as soon as the guard is dropped"]
pub fn install_plan_debugger(dbg: SharedPlanDebugger) -> InstalledPlanDebugger {
    InstalledPlanDebugger(PLAN_DEBUGGER.with(|d| d.borrow_mut().replace(dbg)))
}

/// Guard returned by [`install_plan_debugger`]; restores the previous debugger.
pub struct InstalledPlanDebugger(Option<SharedPlanDebugger>);

impl Drop for InstalledPlanDebugger {
    fn drop(&mut self) {
        PLAN_DEBUGGER.with(|d| *d.borrow_mut() = self.0.take());
    }
}

/// Run `f` with `dbg` installed as the active debugger. Used by the callers that
/// sit ABOVE the ported planner chain (the analyze path in rust-syncer's
/// `run_ast`, whose TS twin passes `planDebugger` down through `runAst` ->
/// `buildPipeline` options) — everything from `plan_query` down takes the
/// debugger as a parameter like TS does.
pub fn with_plan_debugger<R>(dbg: SharedPlanDebugger, f: impl FnOnce() -> R) -> R {
    let _installed = install_plan_debugger(dbg);
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
