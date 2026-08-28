//! TRANSITIONAL SHIM (L9 Stage 3b) — `router.rs` dissolved into the 1:1 tree:
//! the `Syncer` connection seat lives in `workers/syncer.rs`, the rust-only CG
//! executor substrate in `workers/cg_executor.rs`, and the per-CG serving core
//! (`CgState`/`ViewSyncerService`, the event loop, dispatch) in
//! `services/view_syncer/view_syncer.rs`. Every former `crate::router::*` path
//! is re-exported here until the Stage-4 shim sweep deletes this file.

// ─── moved to workers/syncer.rs (L9 Stage 2a) ────────────────────────────────
// The TS `Syncer` connection-management seat (ConnectionRouter + the live
// connection map + ConnectionSinks + group auth pinning) lives in the
// workers/syncer.ts-mirrored file. Re-exported here so existing paths hold
// until the Stage-4 shim removal.
/// Transitional alias (L9 Stage 2b): `ConnectionRouter` was the pre-1:1 name
/// of the TS `Syncer` twin. Removed with the Stage-4 shim sweep.
pub use crate::workers::syncer::Syncer as ConnectionRouter;
pub use crate::workers::syncer::{ConnectionSinks, GroupAuthState, Syncer};

// ─── moved to workers/cg_executor.rs (L9 Stage 3a) ───────────────────────────
// The rust-only CG scheduling substrate (channel message type, per-CG handle,
// executor threads, inbound forwarder). Re-exported here so existing paths
// hold until the Stage-4 shim removal.
pub use crate::workers::cg_executor::{CGHandle, CGMessage};
pub(crate) use crate::workers::cg_executor::{
    Executor, ExecutorCommand, default_num_shards, forward_inbound, run_executor,
};

pub use crate::services::view_syncer::view_syncer::{
    AuthValidator, CGServicesFactory, CvrPgConfig, SyncEngineConfig,
};
pub(crate) use crate::services::view_syncer::view_syncer::{
    cg_event_loop, decrement_nonzero, lock_unpoisoned, now_ms, shard_for,
};
