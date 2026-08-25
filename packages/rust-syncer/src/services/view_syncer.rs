//! `services/view-syncer/` — the view-syncer service's separable ports. The
//! `ViewSyncerService` serving loop + `ConnectionContextManager` are fused into
//! the per-CG actor core (`crate::router::CgState`), a documented exception; the
//! rest map 1:1 to their TS files here.
pub mod connection_context_manager;
pub mod drain_coordinator;
pub mod e2e_serving_lag;
pub mod pipeline_driver;
pub mod query_covering;
