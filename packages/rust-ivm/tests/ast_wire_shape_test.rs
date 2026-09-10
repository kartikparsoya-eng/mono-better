//! The typed `Ast`'s serde shape is a RUST-INTERNAL representation, not the TS
//! wire — and this test exists so nobody concludes otherwise.
//!
//! Reading the TS first turned finding F-39 around. The finding said rust
//! emitted `"scalar":false` where TS omits an unset `scalar` (TS declares it
//! `scalar: v.boolean().optional()`, zero-protocol/src/ast.ts:126, and only
//! ever sets it to `true`). That is true about TS and irrelevant to rust,
//! because nothing serializes a typed `Ast` in production:
//!
//! * `analyze_query` — the one place that serializes an AST at all —
//!   serializes the WIRE-derived value it was handed (`resolve_analyze_ast`
//!   returns `Result<(serde_json::Value, bool)>`, inspect_handler.rs:206-210,
//!   consumed at :135-136).
//! * `hash_of_ast` takes an untyped `Value` (read_authorizer.rs:330), from the
//!   client's AST (:47) or the query-API response's `ast` property
//!   (transform_query.rs:275).
//!
//! So `#[serde(skip_serializing_if)]` on `scalar` was reverted: it would have
//! made an internal representation marginally more TS-like while a dozen other
//! fields still differ, which reads as a wire-compatibility guarantee that does
//! not exist. What IS worth pinning is the divergence itself, in executable
//! form, plus the two properties that do matter — a lossless round trip, and
//! `plan_id` never reaching any output.
//!
//! NON-VACUOUS: rename `where_clause` to `where` (serde attribute), make
//! `Condition` internally tagged, or drop `#[serde(skip)]` from `plan_id`, and
//! the corresponding assertion fails.

use rust_ivm::builder::ast::{Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery};

fn exists_condition(scalar: bool) -> Condition {
    Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: RelatedSubquery {
            subquery: Box::new(Ast {
                table: "comment".to_string(),
                ..Default::default()
            }),
            relationship_name: "comments".to_string(),
            parent_key: vec!["id".to_string()],
            child_key: vec!["issueId".to_string()],
            // `RelatedSubquery` has no `Default` (it is a ported struct, and
            // deriving one just for a test would be a Rust-only addition with
            // no TS origin), so both remaining fields are explicit.
            hidden: false,
            system: None,
        },
        op: "EXISTS".to_string(),
        flip: None,
        scalar,
        plan_id: None,
    })
}

fn serialize(scalar: bool) -> serde_json::Value {
    let ast = Ast {
        table: "issue".to_string(),
        where_clause: Some(exists_condition(scalar)),
        ..Default::default()
    };
    serde_json::to_value(&ast).expect("the AST must serialize")
}

/// The derived shape is internal. Every assertion here is a DIVERGENCE from the
/// TS wire, asserted so it stays visible: if someone ever needs a wire-shaped
/// AST, they must build it, not serialize this struct.
#[test]
fn the_typed_ast_serializes_to_the_internal_shape_not_the_ts_wire() {
    let v = serialize(true);
    let obj = v.as_object().expect("an AST serializes to an object");

    assert!(
        obj.contains_key("where_clause") && !obj.contains_key("where"),
        "the field is the rust name `where_clause`; TS's wire key is `where` \
         (zero-protocol/src/ast.ts). Got keys: {:?}",
        obj.keys().collect::<Vec<_>>()
    );

    let cond = obj
        .get("where_clause")
        .and_then(|w| w.as_object())
        .expect("the condition serializes to an object");
    assert!(
        cond.contains_key("CorrelatedSubquery") && !cond.contains_key("type"),
        "`Condition` is an EXTERNALLY tagged enum here — `{{\"CorrelatedSubquery\": …}}` \
         — whereas TS tags internally with `{{\"type\": \"correlatedSubquery\", …}}`. \
         Got: {cond:?}"
    );

    let csq = cond
        .get("CorrelatedSubquery")
        .and_then(|c| c.as_object())
        .expect("the correlated-subquery payload is an object");
    let related = csq
        .get("related")
        .and_then(|r| r.as_object())
        .expect("`related` is an object");
    assert!(
        related.contains_key("relationship_name") && !related.contains_key("relationshipName"),
        "field names stay snake_case; the TS wire is camelCase. Got: {:?}",
        related.keys().collect::<Vec<_>>()
    );

    // Absent optionals are explicit nulls, not omitted properties.
    assert_eq!(
        obj.get("alias"),
        Some(&serde_json::Value::Null),
        "an absent optional serializes as an explicit `null` here; TS omits the \
         property entirely"
    );
}

/// `plan_id` is TS's `planIdSymbol`, which `JSON.stringify` ignores because a
/// symbol key is not enumerable. `#[serde(skip)]` is the rust twin, and it is
/// the one field whose omission genuinely matters: it is planner-internal state.
#[test]
fn plan_id_never_reaches_any_serialized_output() {
    for scalar in [false, true] {
        let v = serialize(scalar);
        let text = serde_json::to_string(&v).expect("serializes");
        assert!(
            !text.contains("plan_id") && !text.contains("planId"),
            "`plan_id` is planner-internal (TS `planIdSymbol`, invisible to \
             JSON.stringify) and must never appear in output. Got: {text}"
        );
    }
}

/// The round trip must be lossless in both directions, so the representation
/// cannot quietly change how a stored AST is read back.
#[test]
fn scalar_round_trips_through_the_internal_shape_in_both_states() {
    for scalar in [false, true] {
        let v = serialize(scalar);
        let back: Ast = serde_json::from_value(v.clone())
            .unwrap_or_else(|e| panic!("re-parse failed for scalar={scalar}: {e} in {v}"));
        let Some(Condition::CorrelatedSubquery(csq)) = back.where_clause else {
            panic!("expected a correlated-subquery condition back, got {back:?}");
        };
        assert_eq!(
            csq.scalar, scalar,
            "`scalar` must survive the round trip in both states"
        );
        assert_eq!(
            csq.plan_id, None,
            "a skipped field comes back at its `Default`, which is what the \
             planner expects to fill in"
        );
    }
}

/// An absent `scalar` on INPUT must still deserialize — `#[serde(default)]` is
/// what makes a TS-wire AST (which omits an unset `scalar`) readable here.
#[test]
fn an_absent_scalar_on_input_deserializes_to_false() {
    let wire = serde_json::json!({
        "table": "issue",
        "where_clause": {
            "CorrelatedSubquery": {
                "related": {
                    "subquery": {"table": "comment"},
                    "relationship_name": "comments",
                    "parent_key": ["id"],
                    "child_key": ["issueId"],
                    "hidden": false,
                    "system": null
                },
                "op": "EXISTS"
            }
        }
    });
    let ast: Ast = serde_json::from_value(wire).expect("an AST with no `scalar` must parse");
    let Some(Condition::CorrelatedSubquery(csq)) = ast.where_clause else {
        panic!("expected a correlated-subquery condition");
    };
    assert!(
        !csq.scalar,
        "an omitted `scalar` is `false` — this is the direction that matters, \
         because TS omits it when unset"
    );
}
