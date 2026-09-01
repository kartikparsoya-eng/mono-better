//! Rust-IVM: a faithful port of the TypeScript ZQL IVM engine.
//!
//! Core design:
//! - `Stream<T>` (TS `Iterable<T>`) → Rust `Iterator<Item = T>`
//! - The `'yield'` cooperative-scheduling token is dropped entirely
//! - Operators implement `Input`/`Output` traits, exactly like TS
//! - Each engine graph is single-threaded, matching the TypeScript pipeline
//!
//! ## File structure
//! `builder/`, `ivm/`, `planner/`, `query/` mirror TS `zql/src/{builder,ivm,
//! planner,query}` 1:1 by filename (`query/query_impl.rs` ⟵ `query-impl.ts`,
//! `planner/planner_builder.rs` ⟵ `planner-builder.ts`, …). Deliberate
//! exceptions are documented in `parity/COVERAGE-ivm.md`: symbol-level fusions
//! where one Rust struct fuses several coupled TS files (`ivm/view.rs`,
//! `ivm/source.rs`), the `memory-source.ts` → `sqlite/` architectural rewrite
//! (in-memory overlay replaced by a SQLite-backed source), and the Rust-only
//! engine host (`engine/`, `streamer/`, `snapshotter/`, `sqlite/`, `bin/`,
//! `advance_gate.rs`, `planner/runtime.rs`, …) with no TS origin.

// The engine graph uses Rc<RefCell> (matching TS's mutable class instances).
// This is by design — the graph is single-threaded (actor model).
#![allow(clippy::arc_with_non_send_sync)]
#![allow(clippy::type_complexity)]
#![allow(ambiguous_glob_reexports)]

pub mod advance_gate;
pub mod builder;
pub mod engine;
pub mod ivm;
pub mod live_count;
pub mod otel_metrics;
pub mod perf_trace;
pub mod planner;
pub mod query;
pub mod replay;
pub mod snapshotter;
pub mod sqlite;
pub mod streamer;

pub use builder::*;
pub use engine::*;
pub use ivm::*;
pub use query::*;
pub use snapshotter::*;
pub use streamer::*;
