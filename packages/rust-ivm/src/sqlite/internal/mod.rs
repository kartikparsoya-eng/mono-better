//! Port of `zqlite/src/internal/`.

pub mod statement_cache;

pub use statement_cache::{DEFAULT_MAX_CACHED_STATEMENTS, StatementCache};
