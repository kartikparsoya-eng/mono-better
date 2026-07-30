//! Snapshotter — port of `zero-cache/src/services/view-syncer/snapshotter.ts`.
//!
//! A `Snapshotter` manages the progression of database snapshots for a
//! ViewSyncer. It holds two BEGIN CONCURRENT snapshots on the same replica
//! file and "leapfrogs" them to replay the timeline of changes in isolation
//! from the Replicator and other ViewSyncers.
//!
//! ```text
//! Replicator:  t1 --------------> t2 --------------> t3 --------------->
//! ViewSyncer:       [snapshot_a] ----> [snapshot_b] ----> [snapshot_c]
//!                     (conn_1)           (conn_2)           (conn_1)  ← reused
//! ```
//!
//! The diff between two snapshots is derived from the version-stamped,
//! append-only `_zero.changeLog2` table. Because the log is version-addressable,
//! "changes in (prev, curr]" is deterministic regardless of who reads or when.

pub mod diff;
pub mod read_pool;
#[allow(clippy::module_inception)]
pub mod snapshotter;
pub mod spec;

pub use diff::*;
pub use snapshotter::*;
pub use spec::*;

/// Change-log operation constants — port of `change-log.ts:43-46`.
pub const SET_OP: &str = "s";
pub const DEL_OP: &str = "d";
pub const TRUNCATE_OP: &str = "t";
pub const RESET_OP: &str = "r";

/// The `_0_version` column name — port of `constants.ts`.
pub const ZERO_VERSION_COLUMN_NAME: &str = "_0_version";
