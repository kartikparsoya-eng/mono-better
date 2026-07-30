//! Rust-IVM: a faithful port of the TypeScript ZQL IVM engine.
//!
//! Core design:
//! - `Stream<T>` (TS `Iterable<T>`) → Rust `Iterator<Item = T>`
//! - The `'yield'` cooperative-scheduling token is dropped entirely
//! - Operators implement `Input`/`Output` traits, exactly like TS
//! - Parallelism: `rayon` for hydrate, `crossbeam::scope` for inter-CG push

// The engine graph uses Rc<RefCell> (matching TS's mutable class instances).
// This is by design — the graph is single-threaded (actor model).
#![allow(clippy::arc_with_non_send_sync)]
#![allow(clippy::type_complexity)]
#![allow(ambiguous_glob_reexports)]

pub mod builder;
pub mod engine;
pub mod ivm;
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
