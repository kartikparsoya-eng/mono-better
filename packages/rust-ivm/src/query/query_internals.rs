//! Query internals — port of `zql/src/query/query-internals.ts`.
//!
//! Internal interface for query implementation details.

use std::any::Any;

use crate::builder::ast::Ast;
use crate::ivm::view::Format;
use crate::query::named::CustomQueryID;
use crate::query::query_impl::Query;

/// Internal interface for query implementation details.
/// Port of TS `QueryInternals` (query-internals.ts:16).
///
/// `Any` is a supertrait so `as_query` can perform TS's tag check as a real
/// runtime type test instead of an unchecked pointer cast — see `as_query`.
pub trait QueryInternals: Any {
    fn get_ast(&self) -> &Ast;
    fn get_format(&self) -> &Format;
    fn hash(&self) -> String;
    fn get_custom_query_id(&self) -> Option<&CustomQueryID>;
    fn name_and_args(&self, name: &str, args: &[crate::ivm::data::Value]) -> Query;
}

/// Check if a value implements QueryInternals.
/// Port of TS `isQueryInternals` (query-internals.ts:94):
/// `typeof obj === 'object' && obj !== null && queryInternalsTag in obj`.
///
/// TS's `queryInternalsTag in obj` is a real per-object test that answers
/// FALSE for anything lacking the tag. The rust equivalent of "carries the
/// tag" is "is actually a `Query`", so this downcasts. It used to
/// `return true` unconditionally, which is not the ported predicate: every
/// value answered yes, including ones `as_query` then reinterpreted as a
/// `Query` it was not.
pub fn is_query_internals(obj: &dyn Any) -> bool {
    obj.is::<Query>()
}

/// Cast QueryInternals to Query.
/// Port of TS `asQuery` (query-internals.ts:102).
///
/// TS is `assert(queryInternalsTag in queryInternals, 'Expected query
/// internals tag')` followed by a compile-time-only cast, so a value without
/// the tag THROWS rather than being reinterpreted. Rust reproduces the assert
/// with a checked downcast and panics with TS's message; the panic is the port
/// of TS's throw (`assert` raises), and it is caught the same way every other
/// ported throw on this path is.
///
/// This replaced `unsafe { &*(qi as *const dyn QueryInternals as *const Query) }`,
/// which discarded the vtable and reinterpreted the data pointer with no check
/// at all: type confusion, and undefined behaviour, for any implementor that
/// was not `Query` — while its guard (`is_query_internals`) answered `true` for
/// everything. No implementor other than `Query` exists today, so nothing
/// observable changes; what changes is that it can no longer become UB when one
/// is added.
pub fn as_query(qi: &dyn QueryInternals) -> &Query {
    (qi as &dyn Any)
        .downcast_ref::<Query>()
        .expect("Expected query internals tag")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TS `isQueryInternals` answers per-object: a plain value has no
    /// `queryInternalsTag`, so it is FALSE. Rust's predicate used to
    /// `return true` for every input, which is what made the unchecked
    /// `as_query` cast reachable with a non-`Query`.
    ///
    /// Mutation test: restore `pub fn is_query_internals(_obj: &dyn Any) -> bool
    /// { true }` and this fails on the first assertion.
    #[test]
    fn is_query_internals_answers_false_for_a_value_without_the_tag() {
        assert!(
            !is_query_internals(&42i64 as &dyn Any),
            "a value that is not a Query must not claim to carry the tag"
        );
        assert!(
            !is_query_internals(&"not a query".to_string() as &dyn Any),
            "a value that is not a Query must not claim to carry the tag"
        );
    }
}
