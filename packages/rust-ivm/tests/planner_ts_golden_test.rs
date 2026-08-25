//! TS-golden PLAN differential: replay the fixture ASTs through the REAL Rust
//! planner (`plan_query`) with the same deterministic mock cost model the TS
//! generator used, and assert the planned `flip` annotations match the TS
//! `planQuery` output byte-for-byte.
//!
//! Why: plan-CHOICE divergences are invisible to the row-output oracle — a
//! semi and a flipped plan return the same rows. The 2026-08 sweep found
//! three Rust planner bugs only a plan comparison can pin:
//!   NEW-1  chunk-size 500 shadowing the runtime's 256,
//!   NEW-2  BTreeMap re-sorting breaking multi-column constraint pairing,
//!   NEW-3  the connection cost cache never being written.
//! The `chunk-boundary-sensitive`, `multi-col-correlation-pairing`, and
//! `or-two-exists-fanin` scenarios are tuned so a regression of each flips at
//! least one plan (proven by temp-revert).
//!
//! Regenerate the golden with:
//!   npx tsx agentic/parity/generate-planner-fixture.mjs > agentic/parity/planner-fixture.json

use std::rc::Rc;

use serde_json::Value as Json;

use rust_ivm::builder::ast::{Ast, Condition};
use rust_ivm::planner::plan_query;
use rust_ivm::planner::planner_connection::{ConnectionCostModel, CostModelCost};
use rust_ivm::planner::planner_node::{Confidence, FanoutEst};
use rust_ivm::replay::json_to_ast;

/// Mirror of `makeModel` in generate-planner-fixture.mjs — MUST stay
/// semantically identical (see the comment there).
fn mock_model(tables: Json) -> ConnectionCostModel {
    Rc::new(move |table, _sort, filters, constraint| {
        let cfg = tables.get(table).cloned().unwrap_or(Json::Null);
        let num = |v: &Json, key: &str| v.get(key).and_then(Json::as_f64);
        let rows = if let Some(c) = constraint {
            // NATURAL iteration order — the TS mock uses Object.keys (Record
            // insertion order). PlannerConstraint must therefore be
            // insertion-ordered (NEW-2); a re-sorting map type changes this
            // key and diverges from the TS golden.
            let cols: Vec<&str> = c.keys().map(String::as_str).collect();
            let key = cols.join(",");
            cfg.get("constrained")
                .and_then(|m| m.get(&key))
                .and_then(Json::as_f64)
                .or_else(|| num(&cfg, "constrainedDefault"))
                .unwrap_or(1.0)
        } else if filters.is_some() {
            num(&cfg, "filtered")
                .or_else(|| num(&cfg, "rows"))
                .unwrap_or(100.0)
        } else {
            num(&cfg, "rows").unwrap_or(100.0)
        };
        let fanout = num(&cfg, "fanout").unwrap_or(1.0);
        CostModelCost {
            startup_cost: num(&cfg, "startup").unwrap_or(1.0),
            rows,
            fanout: Rc::new(move |_cols| FanoutEst {
                fanout,
                confidence: Confidence::None,
            }),
        }
    })
}

/// Flip map from the TS planned AST (zero-protocol JSON). Path scheme shared
/// with `flips_from_ast` so entries compare positionally AND by name.
fn flips_from_json(v: &Json, path: &str, out: &mut Vec<(String, String, bool)>) {
    fn walk_cond(c: &Json, p: &str, out: &mut Vec<(String, String, bool)>) {
        match c.get("type").and_then(Json::as_str) {
            Some("correlatedSubquery") => {
                let sub = &c["related"]["subquery"];
                let alias = sub.get("alias").and_then(Json::as_str).unwrap_or("?");
                let path = format!("{p}/{alias}");
                out.push((
                    path.clone(),
                    c.get("op").and_then(Json::as_str).unwrap_or("").to_string(),
                    c.get("flip").and_then(Json::as_bool).unwrap_or(false),
                ));
                if let Some(w) = sub.get("where") {
                    walk_cond(w, &path, out);
                }
            }
            Some(t @ ("and" | "or")) => {
                for (i, cc) in c["conditions"].as_array().unwrap().iter().enumerate() {
                    walk_cond(cc, &format!("{p}/{t}{i}"), out);
                }
            }
            _ => {}
        }
    }
    if let Some(w) = v.get("where") {
        walk_cond(w, path, out);
    }
    if let Some(related) = v.get("related").and_then(Json::as_array) {
        for r in related {
            let sq = &r["subquery"];
            let alias = sq.get("alias").and_then(Json::as_str).unwrap_or("?");
            flips_from_json(sq, &format!("{path}/rel:{alias}"), out);
        }
    }
}

/// Flip map from the Rust planned `Ast` — identical path scheme.
fn flips_from_ast(ast: &Ast, path: &str, out: &mut Vec<(String, String, bool)>) {
    fn walk_cond(c: &Condition, p: &str, out: &mut Vec<(String, String, bool)>) {
        match c {
            Condition::CorrelatedSubquery(csq) => {
                let alias = csq.related.subquery.alias.as_deref().unwrap_or("?");
                let path = format!("{p}/{alias}");
                out.push((path.clone(), csq.op.clone(), csq.flip.unwrap_or(false)));
                if let Some(ref w) = csq.related.subquery.where_clause {
                    walk_cond(w, &path, out);
                }
            }
            Condition::And(conds) => {
                for (i, cc) in conds.iter().enumerate() {
                    walk_cond(cc, &format!("{p}/and{i}"), out);
                }
            }
            Condition::Or(conds) => {
                for (i, cc) in conds.iter().enumerate() {
                    walk_cond(cc, &format!("{p}/or{i}"), out);
                }
            }
            Condition::Simple(_) => {}
        }
    }
    if let Some(ref w) = ast.where_clause {
        walk_cond(w, path, out);
    }
    for r in &ast.related {
        let alias = r.subquery.alias.as_deref().unwrap_or("?");
        flips_from_ast(&r.subquery, &format!("{path}/rel:{alias}"), out);
    }
}

#[test]
fn plans_match_ts_golden() {
    let golden: Json = serde_json::from_str(include_str!("../agentic/parity/planner-fixture.json"))
        .expect("planner-fixture.json");
    let scenarios = golden["scenarios"].as_array().expect("scenarios");
    assert!(!scenarios.is_empty(), "empty planner fixture");

    for sc in scenarios {
        let name = sc["name"].as_str().unwrap_or("?");
        let ast = json_to_ast(&sc["ast"]);
        let planned = plan_query(&ast, mock_model(sc["tables"].clone()));

        let mut expected = Vec::new();
        flips_from_json(&sc["plannedAst"], "", &mut expected);
        let mut actual = Vec::new();
        flips_from_ast(&planned, "", &mut actual);

        assert!(
            !expected.is_empty(),
            "[{name}] golden has no correlated subqueries — broken scenario"
        );
        assert_eq!(
            actual, expected,
            "[{name}] Rust plan_query flip decisions diverge from TS planQuery"
        );
    }
}
