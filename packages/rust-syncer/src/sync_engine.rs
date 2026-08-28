//! TRANSITIONAL SHIM (L9 Stage 3c-iii) — `SyncEngine` dissolved into
//! `services/view_syncer/view_syncer.rs`: TS `ViewSyncerService` owns
//! `#pipelines` / `#cvrStore` / `#clients` directly, so the rust engine
//! struct's fields and methods merged into `ViewSyncerService`. Every former
//! `crate::sync_engine::*` path is re-exported here until the Stage-4 shim
//! sweep deletes this file. `SyncEngine::new(pipelines)` (the storeless
//! engine-surface constructor) remains for the engine-level harness tests.

pub use crate::services::view_syncer::view_syncer::ViewSyncerService as SyncEngine;
pub use crate::services::view_syncer::view_syncer::{LoadCvrError, SyncResult, empty_cvr};
