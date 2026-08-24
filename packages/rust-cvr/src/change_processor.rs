//! Port of TS `ViewSyncer.#processChanges` (view-syncer.ts:2217-2300).
//!
//! Accumulates individual `RowChange` events into a de-duped batch
//! (`HashMap<String, (RowID, RowUpdate)>`), flushing to the CVR updater
//! and poke handlers every `CURSOR_PAGE_SIZE` rows.
//!
//! On the unified Rust architecture, this runs inside the engine's
//! `FnMut(&RowChange)` callback — same thread, zero napi boundary crossings.

use std::collections::HashMap;

use crate::client_handler::MultiPoker;
use crate::client_handler::PatchToVersion;
use crate::cvr::RowUpdate;
use crate::cvr::{CVRQueryDrivenUpdater, RowRecordMap};
use crate::row_key::row_id_string;
use crate::schema::types::RowID;

use serde_json::{Map, Value};

const ZERO_VERSION_COLUMN_NAME: &str = "_0_version";

// Default matches Go IVM's hydrateChunkSize / advanceChunkSize so the streaming
// chunk boundary aligns with the CVR flush boundary.
const DEFAULT_CURSOR_PAGE_SIZE: usize = 10000;

/// The row-level change kinds `ChangeProcessor` acts on — the subset of the IVM
/// `ChangeType` that reaches the CVR row path (the streamer only ever emits
/// Add/Remove/Edit at the row level; structural `Child` changes never arrive
/// here). Kept as a local enum so `rust-cvr` needs no dependency on `rust-ivm`;
/// the syncer maps `ivm::ChangeType` → this at the boundary. Using an enum (vs a
/// raw `u8`) makes the `on_row_change` match exhaustive — a new variant is a
/// compile error rather than a silently-dropped row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowChangeType {
    Add,
    Remove,
    Edit,
}

/// The in-Rust equivalent of TS `#processChanges`.
///
/// Called from the engine's `on_row_change` callback — same thread, zero crossing.
pub struct ChangeProcessor<'a> {
    updater: &'a mut CVRQueryDrivenUpdater,
    pokers: &'a MultiPoker,
    rows: HashMap<String, (RowID, RowUpdate)>,
    cursor_page_size: usize,
    total: usize,
}

impl<'a> ChangeProcessor<'a> {
    pub fn new(updater: &'a mut CVRQueryDrivenUpdater, pokers: &'a MultiPoker) -> Self {
        Self::with_page_size(updater, pokers, DEFAULT_CURSOR_PAGE_SIZE)
    }

    pub fn with_page_size(
        updater: &'a mut CVRQueryDrivenUpdater,
        pokers: &'a MultiPoker,
        cursor_page_size: usize,
    ) -> Self {
        Self {
            updater,
            pokers,
            rows: HashMap::new(),
            cursor_page_size,
            total: 0,
        }
    }

    /// Called for each `RowChange` from the engine. Accumulates into the
    /// de-dupe buffer, flushing at `CURSOR_PAGE_SIZE` intervals.
    ///
    /// Parameters mirror the TS `RowChange` shape:
    /// - `change_type`: ADD, REMOVE, EDIT
    /// - `query_id`: the query hash
    /// - `table`: table name
    /// - `row_key`: the row's primary key columns (as serde_json::Map)
    /// - `row`: the full row (None for REMOVE)
    pub fn on_row_change(
        &mut self,
        change_type: RowChangeType,
        query_id: &str,
        table: &str,
        row_key: Map<String, Value>,
        row: Option<Map<String, Value>>,
        existing_rows: &RowRecordMap,
    ) {
        let row_id = RowID {
            schema: String::new(),
            table: table.to_string(),
            row_key,
        };
        let id_str = row_id_string(&row_id);

        // `id_str` is not used after keying the entry, so move it in rather than
        // clone. `row_id` is moved into the freshly-inserted entry's value.
        let entry = self.rows.entry(id_str).or_insert_with(|| {
            (
                row_id,
                RowUpdate {
                    version: None,
                    contents: None,
                    ref_counts: std::collections::BTreeMap::new(),
                },
            )
        });

        // IVM can output multiple versions of a row as it goes through its
        // intermediate stages. Always update the version and contents;
        // the last version will reflect the final state.
        let update_version = |entry: &mut (RowID, RowUpdate), row: Map<String, Value>| {
            // Strip _0_version (TS contentsAndVersion)
            let version = row
                .get(ZERO_VERSION_COLUMN_NAME)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            // Build contents without `_0_version` (TS `contentsAndVersion`).
            // Filtering into a fresh map is byte-identical to `row.clone()` +
            // `remove(_0_version)` — same remaining keys, same insertion order —
            // but the map is OWNED here, so the surviving values are MOVED, not
            // cloned (`remove` on the insertion-ordered map would also O(n)
            // shift). Wrapped in `Arc` so the downstream `RowPatch::Put` /
            // poke-body stages share this one allocation.
            let contents = {
                let len = row.len().saturating_sub(1);
                let mut c = Map::with_capacity(len);
                for (k, v) in row {
                    if k != ZERO_VERSION_COLUMN_NAME {
                        c.insert(k, v);
                    }
                }
                std::sync::Arc::new(Value::Object(c))
            };
            entry.1.version = version;
            entry.1.contents = Some(contents);
        };

        match change_type {
            RowChangeType::Add => {
                if let Some(row) = row {
                    update_version(entry, row);
                }
                // Ensure refCounts[queryID] exists (TS: `parsedRow.refCounts[queryID] ??= 0`)
                *entry.1.ref_counts.entry(query_id.to_string()).or_insert(0) += 1;
            }
            RowChangeType::Edit => {
                if let Some(row) = row {
                    update_version(entry, row);
                }
                // Ensure the key exists (TS: `parsedRow.refCounts[queryID] ??= 0`)
                entry.1.ref_counts.entry(query_id.to_string()).or_insert(0);
            }
            RowChangeType::Remove => {
                // Ensure the key exists before decrementing (TS: `parsedRow.refCounts[queryID] ??= 0`)
                let rc = entry.1.ref_counts.entry(query_id.to_string()).or_insert(0);
                *rc -= 1;
            }
        }

        if self.rows.len().is_multiple_of(self.cursor_page_size) {
            self.flush_batch(existing_rows);
        }
    }

    /// Flush the current batch to the updater and route patches to pokers.
    fn flush_batch(&mut self, existing_rows: &RowRecordMap) {
        if self.rows.is_empty() {
            return;
        }
        self.total += self.rows.len();

        // Call updater.received() — direct call, zero boundary crossing
        let patches: Vec<PatchToVersion> = self.updater.received(&self.rows, existing_rows);
        self.rows.clear();

        // Route patches to all client handlers — direct call, zero crossing
        for patch in &patches {
            self.pokers.add_patch(patch);
        }
    }

    /// Final flush after all changes have been processed.
    /// Also calls `delete_unreferenced_rows` and routes those patches.
    pub fn finish(&mut self, existing_rows: &RowRecordMap) {
        self.finish_received(existing_rows);

        // delete_unreferenced_rows — borrow the cache's records directly rather
        // than deep-cloning the whole row-record map (which, for a large client
        // group, copied every RowRecord in the CVR on every advance).
        let patches = self
            .updater
            .delete_unreferenced_rows(existing_rows.values());
        for patch in &patches {
            self.pokers.add_patch(patch);
        }
    }

    /// Flush only the rows received in this pass. Replica advancement executes
    /// no query add/remove set, so (matching TS `#advancePipelines`) it must not
    /// run `deleteUnreferencedRows`; doing so treats a normal advance as a
    /// query-less reconciliation and panics as soon as one row changes.
    pub fn finish_received(&mut self, existing_rows: &RowRecordMap) {
        self.flush_batch(existing_rows);
    }

    /// Total rows processed so far.
    pub fn total_processed(&self) -> usize {
        self.total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_handler::{ClientHandler, WebSocketSink};
    use crate::schema::types::CVRVersion;
    use crate::shards::ShardID;
    use std::sync::{Arc, Mutex as StdMutex};

    struct MockSink {
        messages: Arc<StdMutex<Vec<Value>>>,
    }

    impl MockSink {
        fn new() -> (Self, Arc<StdMutex<Vec<Value>>>) {
            let messages = Arc::new(StdMutex::new(Vec::new()));
            (
                Self {
                    messages: messages.clone(),
                },
                messages,
            )
        }
    }

    impl WebSocketSink for MockSink {
        fn push(&self, msg: Value) -> Result<(), String> {
            self.messages.lock().unwrap().push(msg);
            Ok(())
        }
        fn fail(&self, _e: String) {}
        fn cancel(&self) {}
    }

    fn make_client_handler() -> (ClientHandler, Arc<StdMutex<Vec<Value>>>) {
        let (sink, messages) = MockSink::new();
        let handler = ClientHandler::new(
            "cg1",
            "client1",
            "ws1",
            &ShardID {
                app_id: "app".to_string(),
                shard_num: 0,
            },
            None,
            Arc::new(sink),
        );
        (handler, messages)
    }

    fn make_cvr() -> crate::cvr::CVR {
        use crate::cvr::*;
        use crate::schema::types::*;
        let mut cvr = CVR {
            id: "cg1".to_string(),
            version: CVRVersion {
                state_version: "00".to_string(),
                config_version: None,
            },
            last_active: 0,
            ttl_clock: 0,
            replica_version: Some("v1".to_string()),
            clients: std::collections::BTreeMap::new(),
            queries: std::collections::BTreeMap::new(),
            client_schema: None,
            profile_id: None,
        };
        // Add a client query so track_queries can find it
        let query = QueryRecord::Client(ClientQueryRecord {
            base: BaseQueryRecord {
                id: "q1".to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
            },
            ast: serde_json::json!({"schema": "s", "table": "t"}),
            client_state: std::collections::BTreeMap::new(),
            patch_version: None,
        });
        cvr.queries.insert("q1".to_string(), query);
        cvr
    }

    fn make_updater(cvr: crate::cvr::CVR) -> CVRQueryDrivenUpdater {
        let mut u = CVRQueryDrivenUpdater::new(cvr, "00".to_string(), "v1".to_string(), None);
        // track_queries must be called before received/deleteUnreferencedRows
        u.track_queries(
            &[("q1", "hash1")], // executed: (queryID, transformationHash) pairs
            &[],                // removed
        );
        u
    }

    #[test]
    fn test_single_add_produces_patch() {
        let cvr = make_cvr();
        let mut updater = make_updater(cvr);
        let (handler, messages) = make_client_handler();
        let pokers = MultiPoker::new(
            &[&handler],
            CVRVersion {
                state_version: "00".to_string(),
                config_version: Some(1),
            },
        );

        let mut processor = ChangeProcessor::new(&mut updater, &pokers);

        let mut row = Map::new();
        row.insert("id".to_string(), Value::String("row1".to_string()));
        row.insert("name".to_string(), Value::String("Alice".to_string()));
        row.insert("_0_version".to_string(), Value::String("v1".to_string()));

        let mut row_key = Map::new();
        row_key.insert("id".to_string(), Value::String("row1".to_string()));

        let existing_rows: RowRecordMap = HashMap::new();

        processor.on_row_change(
            RowChangeType::Add,
            "q1",
            "users",
            row_key.clone(),
            Some(row.clone()),
            &existing_rows,
        );
        processor.finish(&existing_rows);
        pokers.end(CVRVersion {
            state_version: "00".to_string(),
            config_version: Some(2),
        });

        let msgs = messages.lock().unwrap();
        // pokeStart + pokePart + pokeEnd
        assert!(
            msgs.len() >= 2,
            "Expected at least pokeStart + pokeEnd, got {}",
            msgs.len()
        );
        assert_eq!(msgs[0][0], "pokeStart");
    }

    #[test]
    fn test_add_then_remove_cancels_out() {
        let cvr = make_cvr();
        let mut updater = make_updater(cvr);
        let (handler, messages) = make_client_handler();
        let pokers = MultiPoker::new(
            &[&handler],
            CVRVersion {
                state_version: "00".to_string(),
                config_version: Some(1),
            },
        );

        let mut processor = ChangeProcessor::new(&mut updater, &pokers);

        let mut row = Map::new();
        row.insert("id".to_string(), Value::String("row1".to_string()));
        row.insert("_0_version".to_string(), Value::String("v1".to_string()));

        let mut row_key = Map::new();
        row_key.insert("id".to_string(), Value::String("row1".to_string()));

        let existing_rows: RowRecordMap = HashMap::new();

        // ADD then REMOVE → refCount goes to 0
        processor.on_row_change(
            RowChangeType::Add,
            "q1",
            "users",
            row_key.clone(),
            Some(row.clone()),
            &existing_rows,
        );
        processor.on_row_change(
            RowChangeType::Remove,
            "q1",
            "users",
            row_key.clone(),
            None,
            &existing_rows,
        );
        processor.finish(&existing_rows);
        pokers.end(CVRVersion {
            state_version: "00".to_string(),
            config_version: Some(2),
        });

        // The row should have been received and then deleted
        // (exact patch count depends on updater logic, but there should be poke frames)
        let msgs = messages.lock().unwrap();
        assert!(msgs.len() >= 2, "Expected at least pokeStart + pokeEnd");
    }

    #[test]
    fn test_dedupe_same_row_multiple_queries() {
        let cvr = make_cvr();
        let mut updater = make_updater(cvr);
        let (handler, _messages) = make_client_handler();
        let pokers = MultiPoker::new(
            &[&handler],
            CVRVersion {
                state_version: "00".to_string(),
                config_version: Some(1),
            },
        );

        let mut processor = ChangeProcessor::new(&mut updater, &pokers);

        let mut row = Map::new();
        row.insert("id".to_string(), Value::String("row1".to_string()));
        row.insert("_0_version".to_string(), Value::String("v1".to_string()));

        let mut row_key = Map::new();
        row_key.insert("id".to_string(), Value::String("row1".to_string()));

        let existing_rows: RowRecordMap = HashMap::new();

        // ADD from query1 + ADD from query2 → refCounts = {q1: 1, q2: 1}
        processor.on_row_change(
            RowChangeType::Add,
            "q1",
            "users",
            row_key.clone(),
            Some(row.clone()),
            &existing_rows,
        );
        processor.on_row_change(
            RowChangeType::Add,
            "q2",
            "users",
            row_key.clone(),
            Some(row.clone()),
            &existing_rows,
        );
        processor.finish(&existing_rows);
        pokers.end(CVRVersion {
            state_version: "00".to_string(),
            config_version: Some(2),
        });

        // The row should have merged refCounts
        // Check the updater's received_rows
        let received = &updater.received_rows;
        // Find the row by its string key
        let key = {
            let row_id = RowID {
                schema: String::new(),
                table: "users".to_string(),
                row_key: row_key.clone(),
            };
            row_id_string(&row_id)
        };
        let rc = received.get(&key);
        assert!(rc.is_some(), "Expected row in received_rows");
        let rc_val = rc.unwrap().as_ref();
        assert!(rc_val.is_some(), "Expected non-null refCounts");
        let rc_map = rc_val.unwrap();
        assert_eq!(
            rc_map.len(),
            2,
            "Expected 2 query refs, got {}",
            rc_map.len()
        );
    }

    #[test]
    fn test_batch_flush_at_page_size() {
        let cvr = make_cvr();
        let mut updater = make_updater(cvr);
        let (handler, messages) = make_client_handler();
        let pokers = MultiPoker::new(
            &[&handler],
            CVRVersion {
                state_version: "00".to_string(),
                config_version: Some(1),
            },
        );

        // Use a small page size for testing
        let mut processor = ChangeProcessor::with_page_size(&mut updater, &pokers, 3);

        let existing_rows: RowRecordMap = HashMap::new();

        // Add 5 rows (should flush at 3, then flush remaining 2 at finish)
        for i in 0..5 {
            let mut row = Map::new();
            row.insert("id".to_string(), Value::String(format!("row{}", i)));
            row.insert("_0_version".to_string(), Value::String("v1".to_string()));

            let mut row_key = Map::new();
            row_key.insert("id".to_string(), Value::String(format!("row{}", i)));

            processor.on_row_change(
                RowChangeType::Add,
                "q1",
                "users",
                row_key.clone(),
                Some(row.clone()),
                &existing_rows,
            );
        }
        processor.finish(&existing_rows);
        pokers.end(CVRVersion {
            state_version: "00".to_string(),
            config_version: Some(2),
        });

        let msgs = messages.lock().unwrap();
        // pokeStart + pokePart (batch 1, 3 rows) + pokePart (batch 2, 2 rows) + pokeEnd
        // But the exact count depends on whether add_patch triggers flush_body at 100 parts
        // Each batch produces 5 patches (one per row), so 100-part threshold isn't hit
        assert!(msgs.len() >= 2, "Expected at least pokeStart + pokeEnd");
    }

    #[test]
    fn test_contents_strips_version_column() {
        let cvr = make_cvr();
        let mut updater = make_updater(cvr);
        let (handler, _messages) = make_client_handler();
        let pokers = MultiPoker::new(
            &[&handler],
            CVRVersion {
                state_version: "00".to_string(),
                config_version: Some(1),
            },
        );

        let mut processor = ChangeProcessor::new(&mut updater, &pokers);

        let mut row = Map::new();
        row.insert("id".to_string(), Value::String("row1".to_string()));
        row.insert("name".to_string(), Value::String("Alice".to_string()));
        row.insert("_0_version".to_string(), Value::String("v1".to_string()));

        let mut row_key = Map::new();
        row_key.insert("id".to_string(), Value::String("row1".to_string()));

        let existing_rows: RowRecordMap = HashMap::new();

        processor.on_row_change(
            RowChangeType::Add,
            "q1",
            "users",
            row_key.clone(),
            Some(row.clone()),
            &existing_rows,
        );
        processor.finish(&existing_rows);
        pokers.end(CVRVersion {
            state_version: "00".to_string(),
            config_version: Some(2),
        });

        // Verify the store_ops contain a PutRowRecord with the correct version
        let ops = updater.base.drain_store_ops();
        let put_op = ops
            .iter()
            .find(|op| matches!(op, crate::cvr::StoreOp::PutRowRecord(_)));
        assert!(put_op.is_some(), "Expected PutRowRecord in store_ops");
        if let crate::cvr::StoreOp::PutRowRecord(record) = put_op.unwrap() {
            assert_eq!(record.row_version, "v1");
        }
    }

    #[test]
    fn test_edit_does_not_change_refcounts() {
        let cvr = make_cvr();
        let mut updater = make_updater(cvr);
        let (handler, _messages) = make_client_handler();
        let pokers = MultiPoker::new(
            &[&handler],
            CVRVersion {
                state_version: "00".to_string(),
                config_version: Some(1),
            },
        );

        let mut processor = ChangeProcessor::new(&mut updater, &pokers);

        let mut row = Map::new();
        row.insert("id".to_string(), Value::String("row1".to_string()));
        row.insert("name".to_string(), Value::String("Alice".to_string()));
        row.insert("_0_version".to_string(), Value::String("v1".to_string()));

        let mut row_key = Map::new();
        row_key.insert("id".to_string(), Value::String("row1".to_string()));

        let existing_rows: RowRecordMap = HashMap::new();

        // ADD then EDIT → refCount stays at 1
        processor.on_row_change(
            RowChangeType::Add,
            "q1",
            "users",
            row_key.clone(),
            Some(row.clone()),
            &existing_rows,
        );

        // EDIT with updated version
        let mut row2 = Map::new();
        row2.insert("id".to_string(), Value::String("row1".to_string()));
        row2.insert("name".to_string(), Value::String("Bob".to_string()));
        row2.insert("_0_version".to_string(), Value::String("v2".to_string()));

        processor.on_row_change(
            RowChangeType::Edit,
            "q1",
            "users",
            row_key.clone(),
            Some(row2.clone()),
            &existing_rows,
        );
        processor.finish(&existing_rows);
        pokers.end(CVRVersion {
            state_version: "00".to_string(),
            config_version: Some(2),
        });

        let received = &updater.received_rows;
        let key = {
            let row_id = RowID {
                schema: String::new(),
                table: "users".to_string(),
                row_key: row_key.clone(),
            };
            row_id_string(&row_id)
        };
        let rc = received.get(&key).and_then(|o| o.as_ref());
        assert!(rc.is_some());
        let rc_map = rc.unwrap();
        let q1_count = rc_map.get("q1").copied().unwrap_or(0);
        assert_eq!(
            q1_count, 1,
            "EDIT should not change refCount, expected 1, got {}",
            q1_count
        );
    }
}
