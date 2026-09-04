//! `services/replicator/` — the replicator's schema-metadata READ side that the
//! view-syncer read path consumes (`schema/column_metadata.rs`). The replicator
//! process itself (change streaming, DDL application) is not ported.
pub mod schema;
