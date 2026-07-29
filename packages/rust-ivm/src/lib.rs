//! Rust-IVM: a faithful port of the TypeScript ZQL IVM engine.
//!
//! Core design:
//! - `Stream<T>` (TS `Iterable<T>`) → Rust `Iterator<Item = T>`
//! - The `'yield'` cooperative-scheduling token is dropped entirely
//! - Operators implement `Input`/`Output` traits, exactly like TS
//! - Parallelism: `rayon` for hydrate, `crossbeam::scope` for inter-CG push

pub mod ivm;
pub mod builder;
pub mod engine;
pub mod streamer;
pub mod sqlite;
pub mod snapshotter;
pub mod replay;

pub use ivm::*;
pub use builder::*;
pub use engine::*;
pub use streamer::*;
pub use snapshotter::*;
