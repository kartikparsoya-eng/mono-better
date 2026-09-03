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
use std::rc::Rc;

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

/// Port of TS `Streamer` (pipeline-driver.ts:1268-1400): `accumulate` records
/// `(queryID, schema, changes)` triples and `stream()` walks them lazily as
/// the TS generator does — `#streamChanges` → `#streamNodes`, recursing into
/// every node's relationships and forwarding the `'yield'`s those child
/// streams produce (:1361-1364). The rust-only `is_hidden` flag (EXISTS
/// relationship rows, see `is_exists_condition_rel`) rides along.
///
/// Schemas are shared, never cloned per node: a frame holds the query's root
/// `Rc<SourceSchema>` plus the relationship path down to its own schema
/// (`schema_at`), because `SourceSchema.relationships` nests child schemas
/// inline and a deep clone per node cost more than the fetch itself.
pub struct Streamer {
    primary_keys: Rc<HashMap<String, Vec<String>>>,
    table_specs: Rc<HashMap<String, TableSpecInfo>>,
    /// TS `#changes: [queryID, schema, changes][]`.
    changes: Vec<(Rc<str>, Rc<SourceSchema>, Vec<Change>)>,
}

impl Streamer {
    pub fn new(
        primary_keys: HashMap<String, Vec<String>>,
        table_specs: HashMap<String, TableSpecInfo>,
    ) -> Self {
        Self::new_shared(Rc::new(primary_keys), Rc::new(table_specs))
    }

    /// `new` over already-shared maps (the per-push collector path creates a
    /// `Streamer` per change).
    pub fn new_shared(
        primary_keys: Rc<HashMap<String, Vec<String>>>,
        table_specs: Rc<HashMap<String, TableSpecInfo>>,
    ) -> Self {
        Streamer {
            primary_keys,
            table_specs,
            changes: Vec::new(),
        }
    }

    /// Port of TS `Streamer.accumulate()` (:1276-1283).
    pub fn accumulate(&mut self, query_id: &str, schema: &SourceSchema, changes: &[Change]) {
        self.accumulate_shared(
            Rc::from(query_id),
            Rc::new(schema.clone()),
            changes.to_vec(),
        );
    }

    /// `accumulate` without copying: the hydrate path calls it once per node
    /// with the query's shared id / schema and the node's own change.
    pub fn accumulate_shared(
        &mut self,
        query_id: Rc<str>,
        schema: Rc<SourceSchema>,
        changes: Vec<Change>,
    ) {
        self.changes.push((query_id, schema, changes));
    }

    /// Port of TS `Streamer.stream()` (:1285-1294): a lazy walk of everything
    /// accumulated so far. Takes the accumulated triples, so a `Streamer`
    /// reused across nodes (the hydrate path) streams each node's changes once.
    pub fn stream(&mut self) -> StreamerStream {
        let mut stack = Vec::new();
        // Frames are pushed in reverse so the first accumulated triple is on
        // top of the stack.
        for (query_id, root, changes) in std::mem::take(&mut self.changes).into_iter().rev() {
            stack.push(Frame::Changes {
                query_id,
                root,
                path: Rc::from(Vec::new()),
                changes: changes.into_iter(),
                hidden: false,
            });
        }
        StreamerStream {
            primary_keys: self.primary_keys.clone(),
            table_specs: self.table_specs.clone(),
            stack,
        }
    }

    /// `stream()` for the eager push path (`yield*` inside a push generator
    /// in TS): the child-stream yields are drained — the intra-push yield is
    /// the documented I-12 gap.
    pub fn stream_rows(&mut self) -> Vec<RowChange> {
        self.stream()
            .filter_map(|item| match item {
                crate::ivm::stream::StreamItem::Data(rc) => Some(rc),
                crate::ivm::stream::StreamItem::Yield => None,
            })
            .collect()
    }
}

/// The schema `path` relationships down from `root` (each step is a
/// relationship name the walk found in the parent's `relationships`).
fn schema_at<'a>(root: &'a SourceSchema, path: &[String]) -> &'a SourceSchema {
    path.iter().fold(root, |schema, rel| {
        schema
            .relationships
            .get(rel)
            .expect("streamer path was built from existing relationships")
    })
}

fn extend_path(path: &Rc<[String]>, rel: &str) -> Rc<[String]> {
    let mut next = Vec::with_capacity(path.len() + 1);
    next.extend_from_slice(path);
    next.push(rel.to_string());
    Rc::from(next)
}

/// One generator frame of the TS recursion.
enum Frame {
    /// `#streamChanges(queryID, schema, changes)` (:1297-1337).
    Changes {
        query_id: Rc<str>,
        root: Rc<SourceSchema>,
        path: Rc<[String]>,
        changes: std::vec::IntoIter<Change>,
        hidden: bool,
    },
    /// `#streamNodes(queryID, schema, op, nodes)` (:1341-1385): the node
    /// stream being walked.
    Nodes {
        query_id: Rc<str>,
        root: Rc<SourceSchema>,
        path: Rc<[String]>,
        op: ChangeType,
        nodes: crate::ivm::stream::NodeStream,
        hidden: bool,
    },
    /// The `for (const [relationship, children] of Object.entries(relationships))`
    /// loop (:1380-1383) of an emitted node.
    Relationships {
        query_id: Rc<str>,
        root: Rc<SourceSchema>,
        path: Rc<[String]>,
        op: ChangeType,
        node: Node,
        rel_idx: usize,
        hidden: bool,
    },
}

/// The lazy `Streamer.stream()` generator.
pub struct StreamerStream {
    primary_keys: Rc<HashMap<String, Vec<String>>>,
    table_specs: Rc<HashMap<String, TableSpecInfo>>,
    stack: Vec<Frame>,
}

impl Iterator for StreamerStream {
    type Item = crate::ivm::stream::StreamItem<RowChange>;

    fn next(&mut self) -> Option<Self::Item> {
        use crate::ivm::stream::StreamItem;
        loop {
            let frame = self.stack.last_mut()?;
            match frame {
                Frame::Changes {
                    query_id,
                    root,
                    path,
                    changes,
                    hidden,
                } => {
                    let schema = schema_at(root, path);
                    // We do not sync rows gathered by the permissions system to
                    // the client (:1302-1306).
                    if schema.system == System::Permissions {
                        self.stack.pop();
                        continue;
                    }
                    let Some(change) = changes.next() else {
                        self.stack.pop();
                        continue;
                    };
                    let (query_id, root, path, hidden) =
                        (query_id.clone(), root.clone(), path.clone(), *hidden);
                    let next = match change {
                        Change::Add(node) => Frame::Nodes {
                            query_id,
                            root,
                            path,
                            op: ChangeType::Add,
                            nodes: crate::ivm::stream::from_vec(vec![node]),
                            hidden,
                        },
                        Change::Remove(node) => Frame::Nodes {
                            query_id,
                            root,
                            path,
                            op: ChangeType::Remove,
                            nodes: crate::ivm::stream::from_vec(vec![node]),
                            hidden,
                        },
                        // EDIT: `{row: change[NODE].row, relationships: {}}`
                        // (:1329-1331) — no recursion.
                        Change::Edit { node, .. } => Frame::Nodes {
                            query_id,
                            root,
                            path,
                            op: ChangeType::Edit,
                            nodes: crate::ivm::stream::from_vec(vec![Node {
                                row: node.row.clone(),
                                relationships: HashMap::new(),
                                rel_order: Vec::new(),
                            }]),
                            hidden,
                        },
                        // CHILD: recurse into the child schema with the child
                        // change (:1319-1327).
                        Change::Child { node: _, child } => {
                            if !schema.relationships.contains_key(&child.relationship_name) {
                                continue;
                            }
                            let child_hidden =
                                hidden || is_exists_condition_rel(&child.relationship_name);
                            Frame::Changes {
                                query_id,
                                root,
                                path: extend_path(&path, &child.relationship_name),
                                changes: vec![child.change.as_ref().clone()].into_iter(),
                                hidden: child_hidden,
                            }
                        }
                    };
                    self.stack.push(next);
                }
                Frame::Nodes {
                    query_id,
                    root,
                    path,
                    op,
                    nodes,
                    hidden,
                } => {
                    let schema = schema_at(root, path);
                    // We do not sync rows gathered by the permissions system
                    // (:1352-1356).
                    if schema.system == System::Permissions {
                        self.stack.pop();
                        continue;
                    }
                    match nodes.next() {
                        None => {
                            self.stack.pop();
                            continue;
                        }
                        Some(StreamItem::Yield) => return Some(StreamItem::Yield),
                        Some(StreamItem::Data(node)) => {
                            let op = *op;
                            let hidden = *hidden;
                            let table = &schema.table_name;
                            let row_key = match self.primary_keys.get(table) {
                                Some(pk) => get_row_key(pk, &node.row),
                                None => get_row_key(&schema.primary_key, &node.row),
                            };
                            let spec = self.table_specs.get(table);
                            // minRowVersion bumping: for non-REMOVE, bump
                            // _0_version up to spec.minRowVersion (:1361-1371).
                            // REMOVE carries no row (TS: row: undefined).
                            let row_opt = if op == ChangeType::Remove {
                                None
                            } else {
                                Some(bump_row_version(&node.row, spec))
                            };
                            let rc = RowChange {
                                change_type: op,
                                query_id: query_id.to_string(),
                                table: table.clone(),
                                row_key,
                                row: row_opt,
                                is_hidden: hidden,
                            };
                            // Recurse into relationships after the row
                            // (:1380-1383), in `rel_order` for determinism.
                            let rels = Frame::Relationships {
                                query_id: query_id.clone(),
                                root: root.clone(),
                                path: path.clone(),
                                op,
                                node,
                                rel_idx: 0,
                                hidden,
                            };
                            self.stack.push(rels);
                            return Some(StreamItem::Data(rc));
                        }
                    }
                }
                Frame::Relationships {
                    query_id,
                    root,
                    path,
                    op,
                    node,
                    rel_idx,
                    hidden,
                } => {
                    let Some(rel_name) = node.rel_order.get(*rel_idx) else {
                        self.stack.pop();
                        continue;
                    };
                    *rel_idx += 1;
                    let schema = schema_at(root, path);
                    if let Some(rel_fn) = node.relationships.get(rel_name)
                        && schema.relationships.contains_key(rel_name)
                    {
                        let stream = rel_fn();
                        let child_hidden = *hidden || is_exists_condition_rel(rel_name);
                        let next = Frame::Nodes {
                            query_id: query_id.clone(),
                            root: root.clone(),
                            path: extend_path(path, rel_name),
                            op: *op,
                            nodes: stream,
                            hidden: child_hidden,
                        };
                        self.stack.push(next);
                    }
                }
            }
        }
    }
}

/// Extract the row key (PK columns) from a row.
/// Port of TS `getRowKey()`.
///
/// Invariant: a primary key is REQUIRED and is never legitimately absent or
/// null. An empty `cols` slice would silently produce an empty `{}` row key,
/// and a missing/null PK column would produce a `null` key value — both of
/// which reach the client as `rowKey:"{}"` / `rowKey:{"id":null}` and crash
/// `toPrimaryKeyString` ("Expected string, number or boolean. Got
/// undefined/null"). This is the common choke point for both the "undefined"
/// (empty key) and the torn-read "Got null" variants, so we hard-fail here
/// rather than emit a key that cannot round-trip. Matches TS, where a PK
/// column is always present and non-null.
pub(crate) fn get_row_key(cols: &[String], row: &Row) -> Row {
    assert!(
        !cols.is_empty(),
        "get_row_key called with an empty primary-key column list — a row key \
         must contain at least one PK column (empty key would emit rowKey:\"{{}}\" \
         and crash the client at toPrimaryKeyString)",
    );
    let mut key: FxHashMap<String, Value> = FxHashMap::default();
    for col in cols {
        let val = match row.get(col) {
            Some(Value::Null) | None => panic!(
                "get_row_key: primary-key column {col:?} is {} in the row — a \
                 primary key is never legitimately null/absent (would emit a \
                 null/undefined rowKey and crash the client)",
                if row.contains_key(col) {
                    "null"
                } else {
                    "absent"
                },
            ),
            Some(v) => v.clone(),
        };
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
    Done { chunk_index: usize },
    /// Error frame — the operation failed.
    Error { chunk_index: usize, message: String },
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
impl Default for CollectSink {
    fn default() -> Self {
        Self::new()
    }
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
        let need_flush = self
            .current_query_id
            .as_ref()
            .is_some_and(|cur| cur != query_id);
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
        if let Some(ref cur) = self.current_query_id
            && cur == query_id
        {
            self.flush();
            self.sink.send(StreamFrame::Final {
                chunk_index: self.chunk_index,
                query_id: query_id.to_string(),
            });
            self.chunk_index += 1;
            self.current_query_id = None;
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

#[cfg(test)]
mod get_row_key_tests {
    use super::get_row_key;
    use crate::ivm::data::{Row, Value};
    use rustc_hash::FxHashMap;
    use std::sync::Arc;

    fn row(pairs: &[(&str, Value)]) -> Row {
        let mut m: FxHashMap<String, Value> = FxHashMap::default();
        for (k, v) in pairs {
            m.insert((*k).to_string(), v.clone());
        }
        Arc::new(m)
    }

    /// BUG 3 repro: an empty PK column list (what `unwrap_or_default()` yields
    /// when a companion/scalar table is not registered in `primary_keys`) must
    /// NOT silently produce an empty `{}` row key. An empty key emits
    /// `rowKey:"{}"` on the wire and crashes the client at `toPrimaryKeyString`
    /// with "Got undefined". Locks in the invariant that a PK is never absent.
    #[test]
    #[should_panic(expected = "empty primary-key column list")]
    fn empty_pk_list_does_not_yield_empty_key() {
        let r = row(&[("id", Value::Str(Arc::from("abc")))]);
        // Empty cols == the unwrap_or_default() branch. Current (buggy) code
        // returns Arc::new({}) — an empty, undefined-producing key.
        let _ = get_row_key(&[], &r);
    }

    /// A well-formed PK column list over a row that actually carries the PK
    /// value must produce a non-empty key containing that column.
    #[test]
    fn present_pk_yields_non_empty_key_with_column() {
        let r = row(&[("id", Value::Str(Arc::from("abc")))]);
        let key = get_row_key(&["id".to_string()], &r);
        assert!(
            key.contains_key("id"),
            "row key must contain the PK column, got {key:?}",
        );
        assert_eq!(key.get("id"), Some(&Value::Str(Arc::from("abc"))));
    }

    /// A PK column that is present-but-null must hard-fail rather than emit a
    /// null key value (the torn-read "Got null" variant of the same crash).
    #[test]
    #[should_panic(expected = "is null")]
    fn null_pk_value_does_not_yield_null_key() {
        let r = row(&[("id", Value::Null)]);
        let _ = get_row_key(&["id".to_string()], &r);
    }

    /// A PK column entirely absent from the row must hard-fail (would emit an
    /// undefined key value otherwise).
    #[test]
    #[should_panic(expected = "is absent")]
    fn absent_pk_column_does_not_yield_missing_key() {
        let r = row(&[("other", Value::Str(Arc::from("x")))]);
        let _ = get_row_key(&["id".to_string()], &r);
    }
}
