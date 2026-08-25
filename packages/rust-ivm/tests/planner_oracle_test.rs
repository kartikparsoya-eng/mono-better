//! Differential planner test against the TS oracle.
//!
//! Feeds the SAME corpus of ASTs (agentic/oracle/planner-corpus.json) to both
//! the Rust `plan_query` and TS `planQuery` (run offline by
//! agentic/oracle/planner-ts-runner.mjs, which writes planner-expected.json),
//! and asserts the flip annotations are identical. Expected flips are produced
//! ONLY by TS — never hand-written — so a green run proves parity with zero 1.7.
//!
//! Regenerate the expected file after changing the corpus:
//!   node --experimental-strip-types agentic/oracle/planner-ts-runner.mjs \
//!     agentic/oracle/planner-corpus.json --out agentic/oracle/planner-expected.json

use std::collections::HashMap;
use std::rc::Rc;

use serde_json::Value as JsonValue;

use rust_ivm::builder::ast::{Ast, Condition};
use rust_ivm::planner::{Confidence, ConnectionCostModel, CostModelCost, FanoutEst, plan_query};
use rust_ivm::replay::json_to_ast;

/// Constraint-aware mock — MUST match the JS mock in planner-ts-runner.mjs:
/// a constrained read is an indexed key seek (~1 row); unconstrained is a full
/// scan of the table's row count.
fn mock_from_costs(costs: HashMap<String, f64>) -> ConnectionCostModel {
    Rc::new(
        move |table: &str,
              _sort: &[(String, String)],
              _filters: Option<&Condition>,
              constraint: Option<&rust_ivm::planner::planner_constraint::PlannerConstraint>| {
            let rows = if constraint.is_some() {
                1.0
            } else {
                *costs.get(table).unwrap_or(&100.0)
            };
            CostModelCost {
                startup_cost: 1.0,
                rows,
                fanout: Rc::new(|_cols: &[String]| FanoutEst {
                    fanout: 1.0,
                    confidence: Confidence::None,
                }),
            }
        },
    )
}

/// Ordered flip extraction — MUST match extractFlips in planner-ts-runner.mjs:
/// WHERE conditions (pre-order, recursing into each subquery's where), then the
/// `related` subqueries in order.
fn extract_flips(ast: &Ast) -> Vec<Option<bool>> {
    let mut flips = Vec::new();
    if let Some(ref where_clause) = ast.where_clause {
        extract_flips_from_condition(where_clause, &mut flips);
    }
    for csq in &ast.related {
        flips.append(&mut extract_flips(&csq.subquery));
    }
    flips
}

fn extract_flips_from_condition(condition: &Condition, flips: &mut Vec<Option<bool>>) {
    match condition {
        Condition::Simple(_) => {}
        Condition::CorrelatedSubquery(csq) => {
            flips.push(csq.flip);
            if let Some(ref sub_where) = csq.related.subquery.where_clause {
                extract_flips_from_condition(sub_where, flips);
            }
        }
        Condition::And(conds) | Condition::Or(conds) => {
            for c in conds {
                extract_flips_from_condition(c, flips);
            }
        }
    }
}

fn read_json(rel: &str) -> JsonValue {
    let path = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), rel);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {path}: {e}"))
}

fn json_flips(v: &JsonValue) -> Vec<Option<bool>> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|f| {
            if f.is_null() {
                None
            } else {
                Some(f.as_bool().unwrap())
            }
        })
        .collect()
}

#[test]
fn planner_matches_ts_oracle() {
    let corpus = read_json("agentic/oracle/planner-corpus.json");
    let expected = read_json("agentic/oracle/planner-expected.json");

    let expected_by_name: HashMap<String, Vec<Option<bool>>> = expected
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["name"].as_str().unwrap().to_string(),
                json_flips(&c["flips"]),
            )
        })
        .collect();

    let cases = corpus.as_array().expect("corpus is an array");
    assert!(!cases.is_empty(), "corpus is empty");
    assert_eq!(
        cases.len(),
        expected_by_name.len(),
        "corpus/expected case count mismatch — regenerate planner-expected.json",
    );

    for case in cases {
        let name = case["name"].as_str().unwrap();
        let ast = json_to_ast(&case["ast"]);
        let costs: HashMap<String, f64> = case["tableCosts"]
            .as_object()
            .map(|o| {
                o.iter()
                    .map(|(k, v)| (k.clone(), v.as_f64().unwrap()))
                    .collect()
            })
            .unwrap_or_default();

        let planned = plan_query(&ast, mock_from_costs(costs));
        let rust_flips = extract_flips(&planned);
        let ts_flips = expected_by_name
            .get(name)
            .unwrap_or_else(|| panic!("no TS expected for case '{name}'"));

        assert_eq!(
            &rust_flips, ts_flips,
            "case '{name}': Rust plan_query flips != TS planQuery oracle",
        );
    }
}
