//! Tests for the query-construction entry points — ports of
//! `newRunnableQuery` (runnable-query-impl.ts:23), `newStaticQuery`
//! (static-query.ts:11), and `newExpressionBuilder` (static-query.ts:22). These
//! public constructors were untested (triage #23).

use std::collections::HashMap;

use rust_ivm::query::query_impl::RelationshipSpec;
use rust_ivm::query::runnable_query_impl::{
    new_expression_builder, new_runnable_query, new_static_query,
};

type Rels = HashMap<String, HashMap<String, RelationshipSpec>>;

// Port of TS `newRunnableQuery`: builds a Query on `table` with a bare AST
// (no where/limit/order) rooted at that table.
#[test]
fn new_runnable_query_roots_ast_at_table() {
    let q = new_runnable_query("issue", Rels::new());
    assert_eq!(q.ast().table, "issue");
    assert!(q.ast().where_clause.is_none());
    assert!(q.ast().limit.is_none());
    assert!(q.ast().order_by.is_none());
}

// Port of TS `newStaticQuery`: same shape as runnable (permissions-system
// context is applied elsewhere via the system field).
#[test]
fn new_static_query_roots_ast_at_table() {
    let q = new_static_query("perm", Rels::new());
    assert_eq!(q.ast().table, "perm");
    assert!(q.ast().where_clause.is_none());
}

// Port of TS `newExpressionBuilder`: delegates to newStaticQuery.
#[test]
fn new_expression_builder_delegates_to_static_query() {
    let q = new_expression_builder("expr", Rels::new());
    assert_eq!(q.ast().table, "expr");
    assert!(q.ast().where_clause.is_none());
}
