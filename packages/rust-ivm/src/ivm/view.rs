//! View types — port of `zql/src/ivm/view.ts` (+ `default-format.ts` Format).
//!
//! `View`/`Entry` are the client-facing output types of the IVM pipeline. The
//! `applyChange` machinery that mutates the tree is the `view-apply-change.ts`
//! twin and lives in `view_apply_change.rs`.

use std::cmp::Ordering as CmpOrdering;
use std::rc::Rc;

use rustc_hash::FxHashMap;

use crate::ivm::data::{Comparator, Value};

// ---------------------------------------------------------------------------
// Format — port of `zero-types/src/format.ts`
// ---------------------------------------------------------------------------

/// Format descriptor for query results.
/// Describes whether a result should be singular or a list,
/// and what the format of nested relationships should be.
#[derive(Clone, Debug, Default)]
pub struct Format {
    pub singular: bool,
    pub relationships: FxHashMap<String, Format>,
}

/// The default format: plural, no nested relationships.
pub fn default_format() -> Format {
    Format {
        singular: false,
        relationships: FxHashMap::default(),
    }
}

// ---------------------------------------------------------------------------
// View / Entry — port of `zql/src/ivm/view.ts`
// ---------------------------------------------------------------------------

/// A view: either a list of entries, a single entry, or absent.
/// Port of TS `View = EntryList | Entry | undefined`.
#[derive(Clone, Debug)]
pub enum View {
    None,
    Single(Rc<Entry>),
    List(Vec<Rc<Entry>>),
}

/// A view entry: a row plus metadata (refCount, optional id) and nested
/// relationship views. Port of TS `MetaEntry`.
#[derive(Clone, Debug)]
pub struct Entry {
    /// Column values — the row data (string keys → Value).
    pub row: FxHashMap<String, Value>,
    /// Reference count: how many edges reach this entry within its relationship.
    pub ref_count: usize,
    /// Optional stable identity (JSON-stringified PK).
    pub id: Option<String>,
    /// Nested relationship views.
    pub relationships: FxHashMap<String, View>,
}

impl Entry {
    /// Create a new entry from a row with refCount=1.
    pub fn new(row: FxHashMap<String, Value>, ref_count: usize) -> Self {
        Entry {
            row,
            ref_count,
            id: None,
            relationships: FxHashMap::default(),
        }
    }

    /// Compare two entries by their row data using a comparator.
    pub fn compare(&self, other: &Entry, cmp: &Comparator) -> CmpOrdering {
        // `Entry.row` is already an owned `FxHashMap`; the comparator borrows it
        // directly — no `Arc::new(row.clone())` per comparison.
        cmp(&self.row, &other.row)
    }
}
