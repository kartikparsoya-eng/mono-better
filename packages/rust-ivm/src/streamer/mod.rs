//! Streamer — port of `pipeline-driver.ts` Streamer class (main branch).
//!
//! Accumulates changes during push, then drains them as RowChanges.
//! This is the terminal sink that collects output from the pipeline.
//!
//! Key behaviors ported from TS:
//! - Recursive `streamNodes`: walks node relationships, emitting child rows
//! - `minRowVersion` bumping: bumps `_0_version` up to spec.minRowVersion
//! - Permissions filtering: skips rows where `schema.system == Permissions`
//! - REMOVE carries no `row` (row is omitted for removes)
//! - EDIT emits node with empty relationships (no recursion)

use std::collections::HashMap;

use rustc_hash::FxHashMap;

use crate::ivm::change::{Change, ChangeType};
use crate::ivm::data::{Node, Row, Value};
use crate::ivm::schema::{SourceSchema, System};
use crate::snapshotter::ZERO_VERSION_COLUMN_NAME;

/// Reserved relationship-alias prefix for correlated subqueries (TS `SUBQ_PREFIX`
/// in zero-protocol/src/ast.ts). EXISTS/NOT-EXISTS WHERE conditions materialise a
/// relationship named `zsubq_<rel>`; two-hop junction intermediates use
/// `zsubq_zhidden_<rel>`.
const SUBQ_PREFIX: &str = "zsubq_";
const SUBQ_JUNCTION_PREFIX: &str = "zsubq_zhidden_";

/// A relationship is an EXISTS-condition subquery (client-invisible, driven by
/// WHERE not by the query `related`/format) iff its alias carries the reserved
/// `zsubq_` prefix but is NOT a `zsubq_zhidden_` junction intermediate (those are
/// fully materialised + ordered, hence deterministic and client-visible).
fn is_exists_condition_rel(rel_name: &str) -> bool {
    rel_name.starts_with(SUBQ_PREFIX) && !rel_name.starts_with(SUBQ_JUNCTION_PREFIX)
}

/// A row-level change — port of TS `RowChange`.
/// For REMOVE, `row` is None (TS: `row: undefined`).
#[derive(Clone, Debug)]
pub struct RowChange {
    pub change_type: ChangeType,
    pub query_id: String,
    pub table: String,
    pub row_key: Row,
    pub row: Option<Row>,
    /// True when this row was reached through an EXISTS/NOT-EXISTS WHERE-condition
    /// subquery relationship (`zsubq_` alias; see `is_exists_condition_rel`).
    /// Production streams these faithfully (matching TS `streamNodes`, which
    /// recurses into all relationships); the client discards them because they
    /// are not in the query format. The flag lets the differential test harness
    /// exclude them, since their identity is source-order-dependent by design
    /// (the Cap optimization leaves EXISTS children unordered) and thus not
    /// comparable across MemorySource/TableSource.
    pub is_hidden: bool,
}

/// Table spec info needed by the Streamer for minRowVersion bumping.
/// Port of the subset of `LiteTableSpecWithKeysAndVersion` the Streamer reads.
#[derive(Clone, Debug)]
pub struct TableSpecInfo {
    pub min_row_version: Option<String>,
}

/// The Streamer — accumulates changes, drains them as RowChanges.
/// Port of TS `Streamer` class (pipeline-driver.ts:882).
pub struct Streamer {
    primary_keys: HashMap<String, Vec<String>>,
    table_specs: HashMap<String, TableSpecInfo>,
    accumulated: Vec<RowChange>,
}

impl Streamer {
    pub fn new(
        primary_keys: HashMap<String, Vec<String>>,
        table_specs: HashMap<String, TableSpecInfo>,
    ) -> Self {
        Streamer {
            primary_keys,
            table_specs,
            accumulated: Vec::new(),
        }
    }

    /// Accumulate changes from a pipeline.
    /// Port of TS `Streamer.accumulate()`.
    pub fn accumulate(&mut self, query_id: &str, schema: &SourceSchema, changes: &[Change]) {
        self.stream_changes(query_id, schema, changes, false);
    }

    /// Drain all accumulated changes.
    /// Port of TS `Streamer.stream()`.
    pub fn stream(&mut self) -> Vec<RowChange> {
        std::mem::take(&mut self.accumulated)
    }

    // -- Internal: streamChanges --

    fn stream_changes(
        &mut self,
        query_id: &str,
        schema: &SourceSchema,
        changes: &[Change],
        hidden: bool,
    ) {
        // We do not sync rows gathered by the permissions system to the client.
        if schema.system == System::Permissions {
            return;
        }

        for change in changes {
            match change {
                Change::Add(node) => {
                    self.stream_nodes(query_id, schema, ChangeType::Add, std::iter::once(node), hidden);
                }
                Change::Remove(node) => {
                    self.stream_nodes(query_id, schema, ChangeType::Remove, std::iter::once(node), hidden);
                }
                Change::Edit { node, .. } => {
                    // EDIT: emit node with empty relationships (no recursion).
                    let edit_node = Node {
                        row: node.row.clone(),
                        relationships: HashMap::new(),
                        rel_order: Vec::new(),
                    };
                    self.stream_nodes(query_id, schema, ChangeType::Edit, std::iter::once(&edit_node), hidden);
                }
                Change::Child { node, child } => {
                    // CHILD: recurse into the child schema with the child change.
                    if let Some(child_schema) = schema.relationships.get(&child.relationship_name) {
                        // The child change is a single change — wrap in a vec.
                        let child_change = vec![child.change.as_ref().clone()];
                        let child_hidden =
                            hidden || is_exists_condition_rel(&child.relationship_name);
                        self.stream_changes(query_id, child_schema, &child_change, child_hidden);
                    }
                }
            }
        }
    }

    // -- Internal: streamNodes --

    fn stream_nodes<'a, I: Iterator<Item = &'a Node>>(
        &mut self,
        query_id: &str,
        schema: &SourceSchema,
        op: ChangeType,
        nodes: I,
        hidden: bool,
    ) {
        let table = &schema.table_name;

        // We do not sync rows gathered by the permissions system.
        if schema.system == System::Permissions {
            return;
        }

        let pk = self
            .primary_keys
            .get(table)
            .cloned()
            .unwrap_or_else(|| schema.primary_key.clone());

        let spec = self.table_specs.get(table).cloned();

        for node in nodes {
            let row_key = get_row_key(&pk, &node.row);

            // minRowVersion bumping: for non-REMOVE, bump _0_version up to spec.minRowVersion.
            let row = if op != ChangeType::Remove {
                bump_row_version(&node.row, spec.as_ref())
            } else {
                node.row.clone()
            };

            // REMOVE carries no row (TS: row: undefined).
            let row_opt = if op == ChangeType::Remove {
                None
            } else {
                Some(row.clone())
            };

            self.accumulated.push(RowChange {
                change_type: op,
                query_id: query_id.to_string(),
                table: table.clone(),
                row_key,
                row: row_opt,
                is_hidden: hidden,
            });

            // Recurse into relationships — emit child rows.
            // Use rel_order for deterministic iteration order.
            for rel_name in &node.rel_order {
                if let Some(rel_fn) = node.relationships.get(rel_name) {
                    if let Some(child_schema) = schema.relationships.get(rel_name) {
                        let stream = rel_fn();
                        let child_hidden = hidden || is_exists_condition_rel(rel_name);
                        // Stream children one at a time rather than collecting
                        // the whole relationship first (TS streamNodes yields
                        // per row). Depth-first order is unchanged.
                        for child in crate::ivm::stream::skip_yields(stream) {
                            self.stream_nodes(
                                query_id,
                                child_schema,
                                op,
                                std::iter::once(&child),
                                child_hidden,
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Extract the row key (PK columns) from a row.
/// Port of TS `getRowKey()`.
pub(crate) fn get_row_key(cols: &[String], row: &Row) -> Row {
    let mut key: FxHashMap<String, Value> = FxHashMap::default();
    for col in cols {
        let val = row.get(col).cloned().unwrap_or(Value::Null);
        key.insert(col.clone(), val);
    }
    Arc::new(key)
}

/// Bump `_0_version` up to `min_row_version` if the row's version is lower.
/// Port of TS `streamNodes` minRowVersion bumping.
fn bump_row_version(row: &Row, spec: Option<&TableSpecInfo>) -> Row {
    let min_version = match spec.and_then(|s| s.min_row_version.as_deref()) {
        Some(mv) => mv,
        None => return row.clone(),
    };

    let row_version = match row.get(ZERO_VERSION_COLUMN_NAME) {
        Some(Value::Str(s)) => &**s,
        _ => return row.clone(),
    };

    // Only bump if the row's version is below minRowVersion.
    if row_version < min_version {
        let mut new_row: FxHashMap<String, Value> = row.as_ref().clone();
        new_row.insert(
            ZERO_VERSION_COLUMN_NAME.to_string(),
            Value::Str(Arc::from(min_version)),
        );
        Arc::new(new_row)
    } else {
        row.clone()
    }
}

use std::sync::Arc;

// ---------------------------------------------------------------------------
// StreamSink — transport-agnostic streaming output (Phase 1).
// ---------------------------------------------------------------------------

/// A streaming frame — one chunk of the wire output.
/// Both HTTP and napi transports consume these.
#[derive(Clone, Debug)]
pub enum StreamFrame {
    /// A partial chunk of row changes for a query.
    /// `chunk_index` is monotonic across all frames in the stream.
    Partial {
        chunk_index: usize,
        query_id: String,
        changes: Vec<RowChange>,
    },
    /// Final frame for a query — no more rows for this query ID.
    Final {
        chunk_index: usize,
        query_id: String,
    },
    /// Terminal frame — the entire operation is complete.
    Done {
        chunk_index: usize,
    },
    /// Error frame — the operation failed.
    Error {
        chunk_index: usize,
        message: String,
    },
}

/// A sink that receives streaming frames.
/// Implementations: HTTP chunked response, napi ThreadsafeFunction queue.
pub trait StreamSink {
    fn send(&mut self, frame: StreamFrame);
}

/// A no-op sink for tests that don't care about streaming.
pub struct NullSink;
impl StreamSink for NullSink {
    fn send(&mut self, _frame: StreamFrame) {}
}

/// A collecting sink for tests — accumulates all frames.
pub struct CollectSink {
    pub frames: Vec<StreamFrame>,
}
impl CollectSink {
    pub fn new() -> Self {
        CollectSink { frames: Vec::new() }
    }
}
impl StreamSink for CollectSink {
    fn send(&mut self, frame: StreamFrame) {
        self.frames.push(frame);
    }
}

/// A chunker that wraps a StreamSink and batches RowChanges into bounded frames.
/// 
/// Usage: call `push_row_change` for each row, then `flush_query` when a query's
/// stream is done, then `done` when the entire operation is complete.
/// The chunker emits `Partial` frames when the batch reaches `chunk_size`,
/// and `Final` when `flush_query` is called.
pub struct Chunker<S: StreamSink> {
    sink: S,
    chunk_size: usize,
    chunk_index: usize,
    current_query_id: Option<String>,
    current_batch: Vec<RowChange>,
}

impl<S: StreamSink> Chunker<S> {
    pub fn new(sink: S, chunk_size: usize) -> Self {
        Chunker {
            sink,
            chunk_size,
            chunk_index: 0,
            current_query_id: None,
            current_batch: Vec::new(),
        }
    }

    /// Push a row change into the current batch. Flushes when batch is full.
    pub fn push_row_change(&mut self, query_id: &str, rc: RowChange) {
        // If query_id changed, flush the previous query
        let need_flush = self.current_query_id.as_ref().map_or(false, |cur| cur != query_id);
        if need_flush {
            let prev_qid = self.current_query_id.clone().unwrap();
            self.flush();
            self.sink.send(StreamFrame::Final {
                chunk_index: self.chunk_index,
                query_id: prev_qid,
            });
            self.chunk_index += 1;
        }
        self.current_query_id = Some(query_id.to_string());
        self.current_batch.push(rc);
        if self.current_batch.len() >= self.chunk_size {
            self.flush();
        }
    }

    /// Flush the current batch as a Partial frame.
    fn flush(&mut self) {
        if self.current_batch.is_empty() {
            return;
        }
        let qid = self.current_query_id.clone().unwrap_or_default();
        self.sink.send(StreamFrame::Partial {
            chunk_index: self.chunk_index,
            query_id: qid,
            changes: std::mem::take(&mut self.current_batch),
        });
        self.chunk_index += 1;
    }

    /// Mark a query as done (flush remaining + emit Final).
    pub fn flush_query(&mut self, query_id: &str) {
        if let Some(ref cur) = self.current_query_id {
            if cur == query_id {
                self.flush();
                self.sink.send(StreamFrame::Final {
                    chunk_index: self.chunk_index,
                    query_id: query_id.to_string(),
                });
                self.chunk_index += 1;
                self.current_query_id = None;
            }
        }
    }

    /// Emit the terminal Done frame.
    pub fn done(&mut self) {
        self.flush();
        if let Some(ref qid) = self.current_query_id {
            self.sink.send(StreamFrame::Final {
                chunk_index: self.chunk_index,
                query_id: qid.clone(),
            });
            self.chunk_index += 1;
            self.current_query_id = None;
        }
        self.sink.send(StreamFrame::Done {
            chunk_index: self.chunk_index,
        });
    }

    /// Emit an error frame.
    pub fn error(&mut self, message: String) {
        self.flush();
        self.sink.send(StreamFrame::Error {
            chunk_index: self.chunk_index,
            message,
        });
    }

    /// Consume the underlying sink.
    pub fn into_sink(self) -> S {
        self.sink
    }
}
