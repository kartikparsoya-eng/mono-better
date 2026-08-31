//! Builder — port of `zql/src/builder/`.
//!
//! Mirrors the TS `builder/` directory: `builder.ts`, `filter.ts`, `like.ts`.
//! (`ast.rs` holds the AST node types the builder emits — TS keeps these in
//! `zero-protocol/src/ast.ts`; here they live beside the builder that produces
//! them.) The query fluent-API + delegates live in the sibling [`crate::query`]
//! module, mirroring TS's separate `zql/src/query/` directory.

pub mod ast;
#[allow(clippy::module_inception)]
pub mod builder;
pub mod debug_delegate;
pub mod filter;
pub mod like;

pub use ast::*;
pub use builder::*;
pub use filter::*;
pub use like::*;
// NOTE: `debug_delegate` is intentionally NOT glob-re-exported. Its `Debug`
// struct (1:1 with TS `class Debug`) would otherwise shadow `std::fmt::Debug`
// in every file that does `use crate::builder::*`. Consumers reference it via
// the full path `crate::builder::debug_delegate::{Debug, DebugDelegate, ...}`.
