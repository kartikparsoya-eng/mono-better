//! Source trait + SourceChange — port of `zql/src/ivm/source.ts`.
//!
//! The `Source` trait abstracts over MemorySource (in-memory / test) and
//! TableSource (SQLite-backed / production), matching TS `Source` interface.
//! The concrete `MemorySource` + overlay/merge machinery is the
//! `memory-source.ts` twin and lives in `memory_source.rs`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::builder::ast::Condition;
use crate::ivm::change::{Change, ChangeType};
use crate::ivm::data::{Row, SortOrder, Value};
use crate::ivm::operator::{Input, Shared};

/// Source-level change — port of TS `SourceChange` (source.ts:4).
///
/// TS: `SourceChangeAdd = [ChangeType.ADD, row: Row, extra: null]`
/// TS: `SourceChangeEdit = [ChangeType.EDIT, row: Row, oldRow: Row]`
#[derive(Clone, Debug)]
pub enum SourceChange {
    Add {
        row: crate::ivm::data::Row,
    },
    Remove {
        row: crate::ivm::data::Row,
    },
    Edit {
        row: crate::ivm::data::Row,
        old_row: crate::ivm::data::Row,
    },
}

impl SourceChange {
    #[inline]
    pub fn change_type(&self) -> ChangeType {
        match self {
            SourceChange::Add { .. } => ChangeType::Add,
            SourceChange::Remove { .. } => ChangeType::Remove,
            SourceChange::Edit { .. } => ChangeType::Edit,
        }
    }
}

/// Port of TS `makeSourceChangeAdd` (source.ts:22).
pub fn make_source_change_add(row: crate::ivm::data::Row) -> SourceChange {
    SourceChange::Add { row }
}

/// Port of TS `makeSourceChangeRemove` (source.ts:26).
pub fn make_source_change_remove(row: crate::ivm::data::Row) -> SourceChange {
    SourceChange::Remove { row }
}

/// Port of TS `makeSourceChangeEdit` (source.ts:30).
pub fn make_source_change_edit(
    row: crate::ivm::data::Row,
    old_row: crate::ivm::data::Row,
) -> SourceChange {
    SourceChange::Edit { row, old_row }
}

// ---------------------------------------------------------------------------
// Source trait — port of TS `Source` interface (source.ts:42).
// ---------------------------------------------------------------------------

/// A source is the root data source of the pipeline. Abstracts over
/// MemorySource (in-memory/test) and TableSource (SQLite/production).
pub trait Source {
    fn table_name(&self) -> &str;
    fn primary_key(&self) -> &[String];

    /// Whether this source currently feeds at least one live pipeline. The TS
    /// driver creates TableSources lazily, so changes for unqueried tables are
    /// skipped entirely. Rust pre-registers schemas; this preserves the same
    /// observable behavior without conflating schema presence with a live source.
    fn has_active_connections(&self) -> bool;

    /// Checkpoint/rollback support for failure-atomic pipeline construction.
    fn connection_count(&self) -> usize;
    fn truncate_connections(&mut self, count: usize);

    /// Connect a new downstream consumer.
    ///
    /// `debug` is the optional [`DebugDelegate`](crate::builder::debug_delegate::DebugDelegate)
    /// threaded from the builder (port of TS `Source.connect(..., debug?)`,
    /// source.ts:72). The production `TableSource` records vended rows through
    /// it; `MemorySource` holds it without vending (matching memory-source.ts:87).
    fn connect(
        &mut self,
        sort: Option<SortOrder>,
        filter_condition: Option<Condition>,
        filter_predicate: Option<Arc<dyn Fn(&Row) -> bool>>,
        split_edit_keys: Option<Vec<String>>,
        debug: Option<crate::builder::debug_delegate::SharedDebug>,
    ) -> Shared<dyn Input>;

    /// Push a source change through all connections.
    fn push(&mut self, change: SourceChange) -> Vec<Change>;

    /// Gen push — yields per-connection results.
    fn gen_push(&mut self, change: SourceChange) -> Vec<Change>;

    /// Get a row by primary key.
    fn get_row(&self, pk: &[(String, Value)]) -> Option<Row>;

    /// Set the SQLite database path for SQLite-backed fetch.
    fn set_db_path(&mut self, _path: &str) {}

    /// Point this source at a specific pinned snapshot connection. Used during
    /// advance to read PREV while changes are processed and CURR afterwards
    /// (matches TS `pipeline-driver.ts` `table.setDB(...)`). MemorySource has no
    /// SQLite connection, so this is a no-op there.
    fn set_snapshot_db(&mut self, _db: std::rc::Rc<std::cell::RefCell<rusqlite::Connection>>) {}

    /// Drop per-advance bookkeeping (same-advance removed-PK set /
    /// applied-changes map). The snapshotter-driven advance clears these via
    /// its `set_snapshot_db` calls at the PREV/CURR boundaries; the plain
    /// `Engine::advance` path (tests, replay harnesses) has no snapshot swap,
    /// so it calls this at each advance start instead — without it, the
    /// per-advance sets accumulate one entry per removed row forever
    /// (dhat-measured: +1 block/advance across 20k advances).
    fn clear_advance_state(&mut self) {}

    /// Column types for this table, so the advance path can coerce raw SQLite
    /// values (Integer/Real → Bool for boolean cols, Text → Json) identically to
    /// the fetch path. Default empty (untyped → pass-through).
    fn column_types(&self) -> HashMap<String, crate::ivm::schema::ColumnType> {
        HashMap::new()
    }

    /// Re-key this source to the client-declared primary key. TS builds the
    /// `TableSource` with the client PK (`#getSource`); rust builds sources at
    /// `init()` — before the client schema is known — so the key is installed
    /// here once the schema arrives, always BEFORE the first fetch (and
    /// idempotent thereafter). Recomputes any derived ordering. Default no-op.
    fn set_primary_key(&mut self, _primary_key: Vec<String>) {}
}
