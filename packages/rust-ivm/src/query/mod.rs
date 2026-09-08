//! Query — port of `zql/src/query/`.
//!
//! The query fluent-API surface: `QueryImpl` (`query_impl.rs`), its delegates
//! (`query_delegate_base.rs`), the query registry / internals, expression &
//! ordering helpers, TTL, typed views, and input validation — mirroring the TS
//! `zql/src/query/` directory 1:1 by filename. A few TS files have no runtime to
//! port (`query.ts` type machinery, `create-builder.ts` client factory) and are
//! omitted; `runnable-query-impl.ts` also absorbs `static-query.ts`, and
//! `query-delegate-base.rs` absorbs the small `query-delegate.ts` residue whose
//! `newQuery` runtime lives in `sqlite/query_delegate.rs`.

pub mod complete_ordering;
pub mod error;
pub mod escape_like;
pub mod expression;
pub mod measure_push_operator;
pub mod metrics_delegate;
pub mod named;
pub mod query_delegate_base;
pub mod query_impl;
pub mod query_internals;
pub mod query_registry;
pub mod runnable_query_impl;
pub mod schema_query;
pub mod ttl;
pub mod typed_view;
pub mod validate_input;

pub use complete_ordering::*;
pub use error::*;
pub use escape_like::*;
pub use expression::*;
pub use measure_push_operator::*;
pub use metrics_delegate::*;
pub use named::*;
pub use query_delegate_base::*;
pub use query_impl::*;
pub use query_internals::*;
pub use query_registry::*;
pub use runnable_query_impl::*;
pub use schema_query::*;
pub use ttl::*;
pub use typed_view::*;
pub use validate_input::*;
