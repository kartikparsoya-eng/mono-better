//! `services/view-syncer/` — the view-syncer service's separable ports. The
//! `ViewSyncerService` serving loop is fused into the per-CG actor core
//! (`crate::router::CgState`) — a documented exception scheduled for the L9
//! Stage-3 reconstruction (parity/ZERO-DIVERGENCE-PLAN.md Part 4). The
//! `ConnectionContextManager` is NOT fused: it lives in its 1:1 module below
//! and has been the live single owner of connection/auth state since task
//! #155 (I-8). The rest map 1:1 to their TS files here.
pub mod connection_context_manager;
pub mod drain_coordinator;
pub mod e2e_serving_lag;
pub mod inspect_handler;
pub mod pipeline_driver;
pub mod query_covering;
