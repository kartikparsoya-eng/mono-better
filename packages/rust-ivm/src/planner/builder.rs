//! Planner builder — port of `planner-builder.ts`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::builder::ast::{Ast, Condition, CorrelatedSubqueryCondition};
use crate::planner::connection::{ConnectionCostModel, PlannerConnection};
use crate::planner::constraint::PlannerConstraint;
use crate::planner::fan_in::PlannerFanIn;
use crate::planner::fan_out::PlannerFanOut;
use crate::planner::graph::PlannerGraph;
use crate::planner::join::PlannerJoin;
use crate::planner::node::{JoinType, PlannerNode};
use crate::planner::terminus::PlannerTerminus;

pub struct Plans {
    pub plan: PlannerGraph,
    pub sub_plans: HashMap<String, Plans>,
}

fn extract_constraint(fields: &[String]) -> PlannerConstraint {
    fields.iter().map(|f| (f.clone(), None)).collect()
}

fn has_correlated_subquery(condition: &Condition) -> bool {
    match condition {
        Condition::CorrelatedSubquery(_) => true,
        Condition::And(conds) | Condition::Or(conds) => {
            conds.iter().any(has_correlated_subquery)
        }
        Condition::Simple(_) => false,
    }
}

fn wire_output(from: &PlannerNode, to: PlannerNode) {
    from.set_output(to);
}

fn order_to_tuples(order: &Option<Vec<crate::builder::ast::OrderPart>>) -> Vec<(String, String)> {
    order.as_ref().map(|v| v.iter().map(|p| (p.column.clone(), p.direction.clone())).collect()).unwrap_or_default()
}

fn process_condition(
    condition: &Condition,
    input: PlannerNode,
    graph: &mut PlannerGraph,
    model: &ConnectionCostModel,
    parent_table: &str,
    plan_id_counter: &mut usize,
) -> PlannerNode {
    match condition {
        Condition::Simple(_) => input,
        Condition::And(conds) => {
            let mut end = input;
            for sub in conds {
                end = process_condition(sub, end, graph, model, parent_table, plan_id_counter);
            }
            end
        }
        Condition::Or(conds) => {
            let subquery_conds: Vec<&Condition> = conds.iter()
                .filter(|c| matches!(c, Condition::CorrelatedSubquery(_)) || has_correlated_subquery(c))
                .collect();

            if subquery_conds.is_empty() {
                return input;
            }

            let fo = Rc::new(RefCell::new(PlannerFanOut::new(input.clone())));
            graph.fan_outs.push(fo.clone());
            let fo_node = PlannerNode::FanOut(fo);
            wire_output(&input, fo_node.clone());

            let mut branches = Vec::new();
            for sub in &subquery_conds {
                let branch = process_condition(sub, fo_node.clone(), graph, model, parent_table, plan_id_counter);
                branches.push(branch);
            }

            let fi = Rc::new(RefCell::new(PlannerFanIn::new(branches.clone())));
            graph.fan_ins.push(fi.clone());
            let fi_node = PlannerNode::FanIn(fi);
            for branch in &branches {
                wire_output(branch, fi_node.clone());
            }

            fi_node
        }
        Condition::CorrelatedSubquery(csq) => {
            process_correlated_subquery(csq, input, graph, model, parent_table, plan_id_counter)
        }
    }
}

fn process_correlated_subquery(
    condition: &CorrelatedSubqueryCondition,
    input: PlannerNode,
    graph: &mut PlannerGraph,
    model: &ConnectionCostModel,
    _parent_table: &str,
    plan_id_counter: &mut usize,
) -> PlannerNode {
    let related = &condition.related;
    let child_table = &related.subquery.table;

    // Create child source if needed
    if !graph.has_source(child_table) {
        graph.add_source(child_table, model.clone());
    }

    let child_conn = graph.connect_source(
        child_table,
        order_to_tuples(&related.subquery.order_by),
        related.subquery.where_clause.clone(),
        false,
        None,
        if condition.op == "EXISTS" { Some(1) } else { None },
    );
    graph.connections.push(child_conn.clone());
    let mut child_end = PlannerNode::Connection(child_conn);

    if let Some(ref where_clause) = related.subquery.where_clause {
        child_end = process_condition(where_clause, child_end, graph, model, child_table, plan_id_counter);
    }

    let parent_constraint = extract_constraint(&related.parent_key);
    let child_constraint = extract_constraint(&related.child_key);

    let plan_id = *plan_id_counter;
    *plan_id_counter += 1;

    let is_not_exists = condition.op == "NOT EXISTS";

    let (flippable, initial_type) = if is_not_exists {
        (false, JoinType::Semi)
    } else if condition.flip {
        (false, JoinType::Flipped)
    } else {
        // flip is false: planner can decide
        (true, JoinType::Semi)
    };

    let join = Rc::new(RefCell::new(PlannerJoin::new(
        input.clone(), child_end.clone(), parent_constraint, child_constraint,
        flippable, plan_id, initial_type,
    )));
    graph.joins.push(join.clone());
    let join_node = PlannerNode::Join(join);

    wire_output(&input, join_node.clone());
    wire_output(&child_end, join_node.clone());

    join_node
}

pub fn build_plan_graph(
    ast: &Ast,
    model: ConnectionCostModel,
    is_root: bool,
    base_constraints: Option<PlannerConstraint>,
) -> Plans {
    let mut graph = PlannerGraph::new();
    let mut plan_id_counter = 0;

    graph.add_source(&ast.table, model.clone());
    let conn = graph.connect_source(
        &ast.table,
        order_to_tuples(&ast.order_by),
        ast.where_clause.clone(),
        is_root,
        base_constraints,
        ast.limit,
    );
    graph.connections.push(conn.clone());
    let mut end = PlannerNode::Connection(conn);

    if let Some(ref where_clause) = ast.where_clause {
        end = process_condition(where_clause, end, &mut graph, &model, &ast.table, &mut plan_id_counter);
    }

    let terminus = Rc::new(RefCell::new(PlannerTerminus::new(end.clone())));
    graph.set_terminus(terminus);

    let mut sub_plans = HashMap::new();
    if let related = &ast.related {
        for csq in related {
            if let Some(ref alias) = csq.subquery.alias {
                let child_constraints = extract_constraint(&csq.child_key);
                sub_plans.insert(alias.clone(), build_plan_graph(
                    &csq.subquery, model.clone(), true, Some(child_constraints),
                ));
            }
        }
    }

    Plans { plan: graph, sub_plans }
}

fn plan_recursively(plans: &mut Plans) {
    for sub in plans.sub_plans.values_mut() {
        plan_recursively(sub);
    }
    plans.plan.plan();
}

pub fn plan_query(ast: &Ast, model: ConnectionCostModel) -> Ast {
    let mut plans = build_plan_graph(ast, model, true, None);
    plan_recursively(&mut plans);
    apply_plans_to_ast(ast, &plans)
}

pub fn apply_plans_to_ast(ast: &Ast, plans: &Plans) -> Ast {
    let mut flipped_ids = std::collections::HashSet::new();
    for join in &plans.plan.joins {
        if join.borrow().join_type() == JoinType::Flipped {
            flipped_ids.insert(join.borrow().plan_id);
        }
    }

    let mut result = ast.clone();
    if let Some(ref where_clause) = ast.where_clause {
        result.where_clause = Some(apply_to_condition(where_clause, &flipped_ids));
    }
    if !ast.related.is_empty() {
        result.related = ast.related.iter().map(|csq| {
            let mut csq = csq.clone();
            if let Some(ref alias) = csq.subquery.alias {
                if let Some(sub_plan) = plans.sub_plans.get(alias) {
                    csq.subquery = Box::new(apply_plans_to_ast(&csq.subquery, sub_plan));
                }
            }
            csq
        }).collect();
    }
    result
}

fn apply_to_condition(condition: &Condition, flipped_ids: &std::collections::HashSet<usize>) -> Condition {
    match condition {
        Condition::Simple(_) => condition.clone(),
        Condition::CorrelatedSubquery(csq) => {
            // The plan_id is stored on the join, not on the condition in our port.
            // We match by position — the nth CorrelatedSubquery in the condition tree
            // corresponds to the nth join with that plan_id.
            // For now, use the scalar field as plan_id (matches TS planIdSymbol).
            let should_flip = flipped_ids.contains(&(csq.scalar as usize));
            let mut result = csq.clone();
            result.flip = should_flip;
            if let Some(ref sub_where) = csq.related.subquery.where_clause {
                result.related.subquery = Box::new({
                    let mut sub = (*csq.related.subquery).clone();
                    sub.where_clause = Some(apply_to_condition(sub_where, flipped_ids));
                    sub
                });
            }
            Condition::CorrelatedSubquery(result)
        }
        Condition::And(conds) => {
            Condition::And(conds.iter().map(|c| apply_to_condition(c, flipped_ids)).collect())
        }
        Condition::Or(conds) => {
            Condition::Or(conds.iter().map(|c| apply_to_condition(c, flipped_ids)).collect())
        }
    }
}
