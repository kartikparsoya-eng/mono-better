//! Escape LIKE pattern — port of `zql/src/query/escape-like.ts`.
//!
//! Escapes `%` and `_` in a string so it can be used as a literal
//! in a LIKE pattern.

/// Escape `%` and `_` with backslash so they match literally in LIKE.
/// Port of TS `escapeLike` (escape-like.ts:1).
pub fn escape_like(val: &str) -> String {
    val.replace('%', "\\%").replace('_', "\\_")
}
