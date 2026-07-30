//! Planner node types — port of `planner-node.ts`.
//!
//! Design: PlannerNode is a tagged reference to an Rc<RefCell<inner>>.
//! The graph owns the Rc<RefCell> allocations; PlannerNode is a cheap clone
//! of the Rc pointer. This mirrors TS's shared mutable class instances.

use std::cell::RefCell;
use std::rc::Rc;

#[derive(Clone)]
pub struct FanoutEst {
    pub fanout: f64,
    pub confidence: Confidence,
}

pub type FanoutCostModel = Rc<dyn Fn(&[String]) -> FanoutEst>;

#[derive(Clone)]
pub struct CostEstimate {
    pub startup_cost: f64,
    pub scan_est: f64,
    pub cost: f64,
    pub returned_rows: f64,
    pub selectivity: f64,
    pub limit: Option<f64>,
    pub fanout: FanoutCostModel,
}

impl Default for CostEstimate {
    fn default() -> Self {
        CostEstimate {
            startup_cost: 0.0,
            scan_est: 0.0,
            cost: 0.0,
            returned_rows: 0.0,
            selectivity: 0.0,
            limit: None,
            fanout: Rc::new(|_cols: &[String]| FanoutEst {
                fanout: 1.0,
                confidence: Confidence::None,
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Confidence {
    High,
    Med,
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeKind {
    Connection,
    Join,
    FanOut,
    FanIn,
    Terminus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinType {
    Semi,
    Flipped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FanOutType {
    FO,
    UFO,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FanInType {
    FI,
    UFI,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinOrConnection {
    Join,
    Connection,
}

/// A tagged reference into the plan graph. Cloning is cheap (Rc bump).
#[derive(Clone)]
pub enum PlannerNode {
    Connection(Rc<RefCell<crate::planner::connection::PlannerConnection>>),
    Join(Rc<RefCell<crate::planner::join::PlannerJoin>>),
    FanOut(Rc<RefCell<crate::planner::fan_out::PlannerFanOut>>),
    FanIn(Rc<RefCell<crate::planner::fan_in::PlannerFanIn>>),
    Terminus(Rc<RefCell<crate::planner::terminus::PlannerTerminus>>),
}

impl PlannerNode {
    pub fn kind(&self) -> NodeKind {
        match self {
            PlannerNode::Connection(_) => NodeKind::Connection,
            PlannerNode::Join(_) => NodeKind::Join,
            PlannerNode::FanOut(_) => NodeKind::FanOut,
            PlannerNode::FanIn(_) => NodeKind::FanIn,
            PlannerNode::Terminus(_) => NodeKind::Terminus,
        }
    }

    pub fn set_output(&self, node: PlannerNode) {
        match self {
            PlannerNode::Connection(c) => c.borrow_mut().set_output(node),
            PlannerNode::Join(j) => j.borrow_mut().set_output(node),
            PlannerNode::FanIn(fi) => fi.borrow_mut().set_output(node),
            PlannerNode::FanOut(fo) => fo.borrow_mut().add_output(node),
            PlannerNode::Terminus(_) => panic!("Terminus cannot have outputs"),
        }
    }

    pub fn propagate_constraints(
        &self,
        branch_pattern: &[usize],
        constraint: Option<&crate::planner::constraint::PlannerConstraint>,
        from: Option<&PlannerNode>,
    ) {
        match self {
            PlannerNode::Connection(c) => {
                c.borrow_mut()
                    .propagate_constraints(branch_pattern, constraint, from)
            }
            PlannerNode::Join(j) => {
                j.borrow_mut()
                    .propagate_constraints(branch_pattern, constraint, from)
            }
            PlannerNode::FanOut(fo) => {
                fo.borrow()
                    .propagate_constraints(branch_pattern, constraint, from)
            }
            PlannerNode::FanIn(fi) => {
                fi.borrow_mut()
                    .propagate_constraints(branch_pattern, constraint, from)
            }
            PlannerNode::Terminus(_) => {}
        }
    }

    pub fn estimate_cost(
        &self,
        downstream_child_selectivity: f64,
        branch_pattern: &[usize],
    ) -> CostEstimate {
        match self {
            PlannerNode::Connection(c) => c
                .borrow()
                .estimate_cost(downstream_child_selectivity, branch_pattern),
            PlannerNode::Join(j) => j
                .borrow()
                .estimate_cost(downstream_child_selectivity, branch_pattern),
            PlannerNode::FanOut(fo) => fo
                .borrow()
                .estimate_cost(downstream_child_selectivity, branch_pattern),
            PlannerNode::FanIn(fi) => fi
                .borrow()
                .estimate_cost(downstream_child_selectivity, branch_pattern),
            PlannerNode::Terminus(t) => t.borrow().estimate_cost(),
        }
    }

    pub fn propagate_unlimit_from_flipped_join(&self) {
        match self {
            PlannerNode::Connection(c) => c.borrow_mut().propagate_unlimit_from_flipped_join(),
            PlannerNode::Join(j) => j.borrow_mut().propagate_unlimit_from_flipped_join(),
            PlannerNode::FanOut(fo) => fo.borrow().propagate_unlimit_from_flipped_join(),
            PlannerNode::FanIn(fi) => fi.borrow().propagate_unlimit_from_flipped_join(),
            PlannerNode::Terminus(_) => {}
        }
    }

    pub fn name(&self) -> String {
        match self {
            PlannerNode::Connection(c) => c.borrow().name.clone(),
            PlannerNode::Join(j) => j.borrow().get_name(),
            PlannerNode::FanOut(_) => "FO".to_string(),
            PlannerNode::FanIn(fi) => format!("{:?}", fi.borrow().node_type()),
            PlannerNode::Terminus(_) => "terminus".to_string(),
        }
    }

    pub fn closest_join_or_source(&self) -> JoinOrConnection {
        match self {
            PlannerNode::Connection(c) => c.borrow().closest_join_or_source(),
            PlannerNode::Join(j) => j.borrow().closest_join_or_source(),
            PlannerNode::FanOut(fo) => fo.borrow().closest_join_or_source(),
            PlannerNode::FanIn(fi) => fi.borrow().closest_join_or_source(),
            PlannerNode::Terminus(t) => t.borrow().closest_join_or_source(),
        }
    }
}
