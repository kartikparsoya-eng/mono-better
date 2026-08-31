//! Planner graph — port of `planner-graph.ts`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::planner::planner_connection::{ConnectionCostModel, PlannerConnection};
use crate::planner::planner_constraint::PlannerConstraint;
use crate::planner::planner_fan_in::PlannerFanIn;
use crate::planner::planner_fan_out::PlannerFanOut;
use crate::planner::planner_join::PlannerJoin;
use crate::planner::planner_node::{FanInType, FanOutType, JoinType, PlannerNode};
use crate::planner::planner_source::PlannerSource;
use crate::planner::planner_terminus::PlannerTerminus;

const MAX_FLIPPABLE_JOINS: usize = 9;

#[derive(Clone)]
pub struct PlanState {
    connections: Vec<Option<usize>>,
    joins: Vec<JoinType>,
    fan_outs: Vec<FanOutType>,
    fan_ins: Vec<FanInType>,
    connection_constraints: Vec<HashMap<String, Option<PlannerConstraint>>>,
}

pub struct PlannerGraph {
    sources: HashMap<String, PlannerSource>,
    terminus: Option<Rc<RefCell<PlannerTerminus>>>,
    pub joins: Vec<Rc<RefCell<PlannerJoin>>>,
    pub fan_outs: Vec<Rc<RefCell<PlannerFanOut>>>,
    pub fan_ins: Vec<Rc<RefCell<PlannerFanIn>>>,
    pub connections: Vec<Rc<RefCell<PlannerConnection>>>,
}

impl Default for PlannerGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl PlannerGraph {
    pub fn new() -> Self {
        crate::live_count::inc(&crate::live_count::PLANNER_GRAPH);
        PlannerGraph {
            sources: HashMap::new(),
            terminus: None,
            joins: Vec::new(),
            fan_outs: Vec::new(),
            fan_ins: Vec::new(),
            connections: Vec::new(),
        }
    }

    pub fn has_source(&self, name: &str) -> bool {
        self.sources.contains_key(name)
    }

    pub fn add_source(&mut self, name: &str, model: ConnectionCostModel) {
        self.sources
            .insert(name.to_string(), PlannerSource::new(name, model));
    }

    pub fn connect_source(
        &mut self,
        name: &str,
        sort: Vec<(String, String)>,
        filters: Option<crate::builder::ast::Condition>,
        is_root: bool,
        base_constraints: Option<PlannerConstraint>,
        limit: Option<usize>,
    ) -> Rc<RefCell<PlannerConnection>> {
        let source = self.sources.get(name).unwrap();
        let conn = source.connect(sort, filters, is_root, base_constraints, limit);
        Rc::new(RefCell::new(conn))
    }

    pub fn set_terminus(&mut self, terminus: Rc<RefCell<PlannerTerminus>>) {
        self.terminus = Some(terminus);
    }

    pub fn reset_planning_state(&mut self) {
        for j in &self.joins {
            j.borrow_mut().reset();
        }
        for fo in &self.fan_outs {
            fo.borrow_mut().reset();
        }
        for fi in &self.fan_ins {
            fi.borrow_mut().reset();
        }
        for c in &self.connections {
            c.borrow_mut().reset();
        }
    }

    pub fn propagate_constraints(&self) {
        if let Some(ref t) = self.terminus {
            t.borrow().propagate_constraints();
        }
    }

    pub fn get_total_cost(&self) -> f64 {
        let est = self.terminus.as_ref().unwrap().borrow().estimate_cost();
        est.cost + est.startup_cost
    }

    pub fn capture_planning_snapshot(&self) -> PlanState {
        PlanState {
            connections: self.connections.iter().map(|c| c.borrow().limit).collect(),
            joins: self.joins.iter().map(|j| j.borrow().join_type()).collect(),
            fan_outs: self
                .fan_outs
                .iter()
                .map(|fo| fo.borrow().node_type())
                .collect(),
            fan_ins: self
                .fan_ins
                .iter()
                .map(|fi| fi.borrow().node_type())
                .collect(),
            connection_constraints: self
                .connections
                .iter()
                .map(|c| c.borrow().capture_constraints())
                .collect(),
        }
    }

    pub fn restore_planning_snapshot(&mut self, state: &PlanState) {
        for (i, c) in self.connections.iter_mut().enumerate() {
            c.borrow_mut().limit = state.connections[i];
            c.borrow_mut()
                .restore_constraints(state.connection_constraints[i].clone());
        }
        for (i, j) in self.joins.iter_mut().enumerate() {
            j.borrow_mut().reset();
            if state.joins[i] == JoinType::Flipped && j.borrow().join_type() != JoinType::Flipped {
                j.borrow_mut().flip();
            }
        }
        for (i, fo) in self.fan_outs.iter_mut().enumerate() {
            if state.fan_outs[i] == FanOutType::UFO && fo.borrow().node_type() == FanOutType::FO {
                fo.borrow_mut().convert_to_ufo();
            }
        }
        for (i, fi) in self.fan_ins.iter_mut().enumerate() {
            if state.fan_ins[i] == FanInType::UFI && fi.borrow().node_type() == FanInType::FI {
                fi.borrow_mut().convert_to_ufi();
            }
        }
    }

    pub fn plan(&mut self) {
        let flippable_indices: Vec<usize> = self
            .joins
            .iter()
            .enumerate()
            .filter(|(_, j)| j.borrow().is_flippable())
            .map(|(i, _)| i)
            .collect();

        if flippable_indices.len() > MAX_FLIPPABLE_JOINS {
            return;
        }

        let n = flippable_indices.len();
        let num_patterns = if n == 0 { 0 } else { 1usize << n };

        let mut best_cost = f64::INFINITY;
        let mut best_plan: Option<PlanState> = None;
        // Tracked only for the `best-plan-selected` debug event (TS
        // `bestAttemptNumber`); costs nothing when no debugger is active.
        let mut best_attempt_number: i64 = -1;

        // Build the FO→FI cache ONCE before the pattern loop (TS
        // planner-graph.ts:270 "to avoid redundant BFS traversals in each
        // iteration"); graph topology is immutable during planning, so the
        // cache is pattern-invariant (F-PLANNER-6).
        let fofi_cache = build_fofi_cache(self);

        for pattern in 0..num_patterns {
            self.reset_planning_state();

            // Port of the `attempt-start` emission (planner-graph.ts:283).
            crate::planner::planner_debug::plan_debug_log(|| {
                serde_json::json!({
                    "type": "attempt-start",
                    "attemptNumber": pattern,
                    "totalAttempts": num_patterns,
                })
            });

            for (bit, &join_idx) in flippable_indices.iter().enumerate() {
                if pattern & (1 << bit) != 0 {
                    self.joins[join_idx].borrow_mut().flip();
                }
            }

            check_and_convert_fofi(self, &fofi_cache);
            propagate_unlimit(self);
            self.propagate_constraints();

            // Port of the `constraints-propagated` emission (planner-graph.ts:308).
            crate::planner::planner_debug::plan_debug_log(|| {
                let connection_constraints: Vec<serde_json::Value> = self
                    .connections
                    .iter()
                    .map(|c| {
                        let c = c.borrow();
                        serde_json::json!({
                            "connection": c.name,
                            "constraints": c.get_constraints_for_debug(),
                            "constraintCosts": c.get_constraint_costs_for_debug(),
                        })
                    })
                    .collect();
                serde_json::json!({
                    "type": "constraints-propagated",
                    "attemptNumber": pattern,
                    "connectionConstraints": connection_constraints,
                })
            });

            let total_cost = self.get_total_cost();

            // Port of the `plan-complete` emission (planner-graph.ts:333). The
            // internal `planSnapshot` is NOT included (TS `serializeEvent` strips
            // it; rust omits it at the source).
            crate::planner::planner_debug::plan_debug_log(|| {
                serde_json::json!({
                    "type": "plan-complete",
                    "attemptNumber": pattern,
                    "totalCost": total_cost,
                    "flipPattern": pattern,
                    "joinStates": join_states_json(self),
                })
            });

            if total_cost < best_cost {
                best_cost = total_cost;
                best_plan = Some(self.capture_planning_snapshot());
                best_attempt_number = pattern as i64;
            }
        }

        if let Some(ref plan) = best_plan {
            self.restore_planning_snapshot(plan);
            self.propagate_constraints();

            // Port of the `best-plan-selected` emission (planner-graph.ts:364).
            crate::planner::planner_debug::plan_debug_log(|| {
                serde_json::json!({
                    "type": "best-plan-selected",
                    "bestAttemptNumber": best_attempt_number,
                    "totalCost": best_cost,
                    "flipPattern": best_attempt_number,
                    "joinStates": join_states_json(self),
                })
            });
        } else {
            // TS planner-graph.ts:377-380: reaching here with flippable joins
            // means every candidate cost was NaN — fail loudly instead of
            // silently leaving the query unplanned.
            assert!(
                num_patterns == 0,
                "no plan was found but flippable joins did exist!"
            );
        }
    }
}

impl Drop for PlannerGraph {
    /// Census only. The graph needs NO cycle-breaking here: every upward
    /// back-edge (`output`/`outputs`) is `Weak` (see `PlannerNodeWeak`), so the
    /// strong topology is a DAG — dropping the owning `Vec`s cascades every
    /// node to zero unconditionally. (An earlier revision stored strong
    /// back-edges like TS and cleared them in this Drop; that freed correctly
    /// only for nodes registered in the graph's `Vec`s, leaving the cycle
    /// CLASS alive for any node a future edit forgets to register. Weak
    /// back-edges remove the class.)
    fn drop(&mut self) {
        crate::live_count::dec(&crate::live_count::PLANNER_GRAPH);
    }
}

/// The per-join `{join, type}` list for the `plan-complete` /
/// `best-plan-selected` debug events (TS `joinStates`).
fn join_states_json(graph: &PlannerGraph) -> Vec<serde_json::Value> {
    graph
        .joins
        .iter()
        .map(|j| {
            let j = j.borrow();
            serde_json::json!({
                "join": j.get_name(),
                "type": crate::planner::planner_debug::join_type_str(j.join_type()),
            })
        })
        .collect()
}

struct FofiInfo {
    fi_index: Option<usize>,
    join_indices: Vec<usize>,
}

fn build_fofi_cache(graph: &PlannerGraph) -> HashMap<usize, FofiInfo> {
    let mut cache = HashMap::new();
    for (fo_idx, fo) in graph.fan_outs.iter().enumerate() {
        let info = find_fi_and_joins(graph, &fo.borrow());
        cache.insert(fo_idx, info);
    }
    cache
}

fn find_fi_and_joins(graph: &PlannerGraph, fo: &PlannerFanOut) -> FofiInfo {
    let mut join_indices = Vec::new();
    let mut fi_index = None;

    // BFS through FO outputs
    let mut queue: Vec<PlannerNode> = fo.outputs();
    let mut visited_rcs: Vec<*const ()> = Vec::new();

    while !queue.is_empty() {
        let node = queue.remove(0);
        let ptr = match &node {
            PlannerNode::Join(j) => Rc::as_ptr(j) as *const (),
            PlannerNode::FanOut(fo) => Rc::as_ptr(fo) as *const (),
            PlannerNode::FanIn(fi) => Rc::as_ptr(fi) as *const (),
            PlannerNode::Connection(c) => Rc::as_ptr(c) as *const (),
            PlannerNode::Terminus(t) => Rc::as_ptr(t) as *const (),
        };
        if visited_rcs.contains(&ptr) {
            continue;
        }
        visited_rcs.push(ptr);

        match &node {
            PlannerNode::Join(j) => {
                for (i, gj) in graph.joins.iter().enumerate() {
                    if Rc::ptr_eq(j, gj) {
                        join_indices.push(i);
                        break;
                    }
                }
                // Traverse to join's output (TS: queue.push(node.output))
                if let Some(out) = j.borrow().get_output() {
                    queue.push(out);
                }
            }
            PlannerNode::FanOut(inner_fo) => {
                queue.extend(inner_fo.borrow().outputs());
            }
            PlannerNode::FanIn(fi) => {
                for (i, gfi) in graph.fan_ins.iter().enumerate() {
                    if Rc::ptr_eq(fi, gfi) {
                        fi_index = Some(i);
                        break;
                    }
                }
            }
            _ => {}
        }
    }

    FofiInfo {
        fi_index,
        join_indices,
    }
}

fn check_and_convert_fofi(graph: &mut PlannerGraph, cache: &HashMap<usize, FofiInfo>) {
    for (fo_idx, info) in cache {
        let has_flipped = info
            .join_indices
            .iter()
            .any(|&j_idx| graph.joins[j_idx].borrow().join_type() == JoinType::Flipped);
        if has_flipped && let Some(fi_idx) = info.fi_index {
            graph.fan_outs[*fo_idx].borrow_mut().convert_to_ufo();
            graph.fan_ins[fi_idx].borrow_mut().convert_to_ufi();
        }
    }
}

fn propagate_unlimit(graph: &mut PlannerGraph) {
    for j in &graph.joins {
        if j.borrow().join_type() == JoinType::Flipped {
            j.borrow_mut().propagate_unlimit();
        }
    }
}
