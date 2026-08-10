//! Rust-IVM: a faithful port of the TypeScript ZQL IVM engine.
//!
//! Core design:
//! - `Stream<T>` (TS `Iterable<T>`) → Rust `Iterator<Item = T>`
//! - The `'yield'` cooperative-scheduling token is dropped entirely
//! - Operators implement `Input`/`Output` traits, exactly like TS
//! - Each engine graph is single-threaded, matching the TypeScript pipeline

// The engine graph uses Rc<RefCell> (matching TS's mutable class instances).
// This is by design — the graph is single-threaded (actor model).
#![allow(clippy::arc_with_non_send_sync)]
#![allow(clippy::type_complexity)]
#![allow(ambiguous_glob_reexports)]

pub mod advance_gate;
pub mod builder;
pub mod credit;
pub mod engine;
pub mod ivm;
pub mod live_count;
pub mod perf_trace;
pub mod planner;
pub mod replay;
pub mod snapshotter;
pub mod sqlite;
pub mod streamer;

pub use builder::*;
pub use engine::*;
pub use ivm::*;
pub use snapshotter::*;
pub use streamer::*;
