//! Named queries — port of `zql/src/query/named.ts`.
//!
//! CustomQueryID, syncedQuery, withValidation.
//! Named queries are queries identified by name + args that can be
//! resolved on the server.

use std::sync::Arc;

use crate::ivm::data::Value;

/// A custom query ID: name + arguments.
/// Port of TS `CustomQueryID` (named.ts:145).
#[derive(Clone, Debug)]
pub struct CustomQueryID {
    pub name: String,
    pub args: Vec<Value>,
}

/// A parse function: parses raw args into typed values.
/// Port of TS `ParseFn<T>` (named.ts:113).
pub type ParseFn = Box<dyn Fn(&[Value]) -> Result<Vec<Value>, String>>;

/// A synced query: a named query function with optional parse/validation.
/// Port of TS `SyncedQuery` (named.ts:24).
pub struct SyncedQuery {
    pub query_name: String,
    pub parse: Option<ParseFn>,
    pub takes_context: bool,
    /// The function that produces a query from args.
    pub fn_impl: Box<dyn Fn(Option<&Value>, &[Value]) -> crate::builder::query::Query>,
}

impl SyncedQuery {
    /// Create a synced query without context.
    /// Port of TS `syncedQuery` (named.ts:42).
    pub fn new(
        name: &str,
        parse: Option<ParseFn>,
        fn_impl: Box<dyn Fn(&[Value]) -> crate::builder::query::Query>,
    ) -> Self {
        let fn_owned = Arc::new(fn_impl);
        let fn_clone = fn_owned.clone();
        let wrapped: Box<dyn Fn(Option<&Value>, &[Value]) -> crate::builder::query::Query> =
            Box::new(move |_ctx, args| fn_clone(args));
        SyncedQuery {
            query_name: name.to_string(),
            parse,
            takes_context: false,
            fn_impl: wrapped,
        }
    }

    /// Create a synced query that takes context.
    /// Port of TS `syncedQueryWithContext` (named.ts:56).
    pub fn with_context(
        name: &str,
        parse: Option<ParseFn>,
        fn_impl: Box<dyn Fn(&Value, &[Value]) -> crate::builder::query::Query>,
    ) -> Self {
        let fn_owned = Arc::new(fn_impl);
        let fn_clone = fn_owned.clone();
        let wrapped: Box<dyn Fn(Option<&Value>, &[Value]) -> crate::builder::query::Query> =
            Box::new(move |ctx, args| {
                let ctx_val = ctx.cloned().unwrap_or(Value::Null);
                fn_clone(&ctx_val, args)
            });
        SyncedQuery {
            query_name: name.to_string(),
            parse,
            takes_context: true,
            fn_impl: wrapped,
        }
    }

    /// Call the query with args, applying validation if a parse function exists.
    /// Port of TS `withValidation` (named.ts:98).
    pub fn call(&self, context: Option<&Value>, args: &[Value]) -> Result<crate::builder::query::Query, crate::builder::error::QueryParseError> {
        let parsed_args = match &self.parse {
            Some(parse_fn) => match parse_fn(args) {
                Ok(parsed) => parsed,
                Err(msg) => return Err(crate::builder::error::QueryParseError::new(Some(
                    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, msg))
                ))),
            },
            None => args.to_vec(),
        };

        let q = (self.fn_impl)(context, &parsed_args);

        // Attach the custom query ID to the query via nameAndArgs
        // In the TS version this calls asQueryInternals(q).nameAndArgs(name, args)
        // In Rust, we return the query — the caller can attach the ID externally.
        Ok(q)
    }
}
