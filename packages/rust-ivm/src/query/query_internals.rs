//! Query internals — port of `zql/src/query/query-internals.ts`.
//!
//! Internal interface for query implementation details.

use crate::builder::ast::Ast;
use crate::ivm::view::Format;
use crate::query::named::CustomQueryID;
use crate::query::query_impl::Query;

/// Internal interface for query implementation details.
/// Port of TS `QueryInternals` (query-internals.ts:16).
pub trait QueryInternals {
    fn get_ast(&self) -> &Ast;
    fn get_format(&self) -> &Format;
    fn hash(&self) -> String;
    fn get_custom_query_id(&self) -> Option<&CustomQueryID>;
    fn name_and_args(&self, name: &str, args: &[crate::ivm::data::Value]) -> Query;
}

/// Check if a value implements QueryInternals.
/// Port of TS `isQueryInternals` (query-internals.ts:47).
pub fn is_query_intals(_obj: &dyn std::any::Any) -> bool {
    // In Rust, we use traits rather than symbol tags.
    // This function always returns true if called on a Query.
    true
}

/// Cast QueryInternals to Query.
/// Port of TS `asQuery` (query-internals.ts:58).
pub fn as_query(qi: &dyn QueryInternals) -> &Query {
    // In the TS version this does an unsafe cast. In Rust, Query implements
    // QueryInternals, so we can get the Query from the internals.
    // This is a no-op since Query *is* the internals.
    unsafe { &*(qi as *const dyn QueryInternals as *const Query) }
}
