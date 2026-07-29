//! View types and applyChange — port of `zql/src/ivm/view.ts` and `view-apply-change.ts`.
//!
//! The view tree is the client-facing output of the IVM pipeline. Each `applyChange`
//! call updates the tree immutably: path-copies the spine from root to the changed
//! node, keeping sibling references stable. Unchanged entries preserve identity,
//! enabling shallow-compare optimizations in UI frameworks.

use std::cmp::Ordering as CmpOrdering;
use std::rc::Rc;

use rustc_hash::FxHashMap;

use crate::ivm::change::ChangeType;
use crate::ivm::data::{compare_values, Comparator, Node, Row, Value};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::NodeStream;

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
        let a: Row = Arc::new(self.row.clone());
        let b: Row = Arc::new(other.row.clone());
        cmp(&a, &b)
    }
}

// ---------------------------------------------------------------------------
// ExpandedNode — port of TS `ExpandedNode` from view-apply-change.ts
// ---------------------------------------------------------------------------

/// A node with eagerly-expanded relationships (arrays instead of generators).
/// Used when batching changes to capture source state at push time.
#[derive(Clone, Debug)]
pub struct ExpandedNode {
    pub row: Row,
    pub relationships: FxHashMap<String, Vec<ExpandedNode>>,
}

/// A node for view changes — can be a lazy Node or an ExpandedNode.
/// Port of TS `ViewNode = Node | ExpandedNode`.
#[derive(Clone, Debug)]
pub enum ViewNode {
    Lazy(Node),
    Expanded(ExpandedNode),
}

impl ViewNode {
    /// Get the row reference.
    pub fn row(&self) -> &Row {
        match self {
            ViewNode::Lazy(n) => &n.row,
            ViewNode::Expanded(n) => &n.row,
        }
    }

    /// Get child nodes from a relationship, handling both lazy and expanded.
    pub fn children(&self, relationship: &str) -> Vec<ViewNode> {
        match self {
            ViewNode::Lazy(node) => {
                if let Some(rel_fn) = node.relationships.get(relationship) {
                    let stream: NodeStream = rel_fn();
                    crate::ivm::stream::skip_yields(stream).map(ViewNode::Lazy).collect()
                } else {
                    Vec::new()
                }
            }
            ViewNode::Expanded(node) => {
                node.relationships
                    .get(relationship)
                    .map(|children| children.iter().cloned().map(ViewNode::Expanded).collect())
                    .unwrap_or_default()
            }
        }
    }

    /// Get all relationship names present on this node.
    pub fn relationship_names(&self) -> Vec<String> {
        match self {
            ViewNode::Lazy(node) => node.rel_order.clone(),
            ViewNode::Expanded(node) => node.relationships.keys().cloned().collect(),
        }
    }
}

// ---------------------------------------------------------------------------
// ViewChange — port of TS `ViewChange`
// ---------------------------------------------------------------------------

/// A change to apply to the view tree. Port of TS `ViewChange`.
#[derive(Clone, Debug)]
pub enum ViewChange {
    Add { node: ViewNode },
    Remove { node: ViewNode },
    Child {
        node: RowOnlyNode,
        child: ChildViewChange,
    },
    Edit { node: RowOnlyNode, old_node: RowOnlyNode },
}

/// A node with only its row (relationships are not consumed for edit/child nodes).
/// Port of TS `RowOnlyNode = {row: Row}`.
#[derive(Clone, Debug)]
pub struct RowOnlyNode {
    pub row: Row,
}

/// Child view change data.
#[derive(Clone, Debug)]
pub struct ChildViewChange {
    pub relationship_name: String,
    pub change: Box<ViewChange>,
}

// ---------------------------------------------------------------------------
// applyChange — port of TS `applyChange` / `applyChangeInternal`
// ---------------------------------------------------------------------------

/// Mutate mode: fully immutable (false) or mutate-in-place (true).
/// The TS WeakSet-based copy-on-write is a JS GC optimization not needed in Rust.
pub type Mutate = bool;

/// Immutable view update. Returns a new Entry on change, same Entry if unchanged.
/// Port of TS `applyChange` (view-apply-change.ts:185).
pub fn apply_change(
    parent_entry: &Entry,
    change: &ViewChange,
    schema: &SourceSchema,
    relationship: &str,
    format: &Format,
    with_ids: bool,
    mutate: Mutate,
) -> Entry {
    apply_change_internal(parent_entry, change, schema, relationship, format, with_ids, mutate)
}

/// Batch apply multiple changes.
/// Port of TS `applyChanges`.
pub fn apply_changes(
    parent_entry: &Entry,
    changes: &[ViewChange],
    schema: &SourceSchema,
    relationship: &str,
    format: &Format,
    with_ids: bool,
    mutate: Mutate,
) -> Entry {
    let mut result = parent_entry.clone();
    for change in changes {
        result = apply_change(&result, change, schema, relationship, format, with_ids, mutate);
    }
    result
}

/// Internal recursive implementation.
fn apply_change_internal(
    parent_entry: &Entry,
    change: &ViewChange,
    schema: &SourceSchema,
    relationship: &str,
    format: &Format,
    with_ids: bool,
    mutate: Mutate,
) -> Entry {
    if schema.is_hidden {
        return apply_change_hidden(parent_entry, change, schema, format, with_ids, mutate);
    }

    let singular = format.singular;
    let child_formats = &format.relationships;

    match change {
        ViewChange::Add { node } => {
            if singular {
                apply_add_singular(parent_entry, node, schema, relationship, child_formats, with_ids, mutate)
            } else {
                apply_add_plural(parent_entry, node, schema, relationship, child_formats, with_ids, mutate)
            }
        }
        ViewChange::Remove { node } => {
            if singular {
                apply_remove_singular(parent_entry, node, schema, relationship, mutate)
            } else {
                apply_remove_plural(parent_entry, node, schema, relationship, mutate)
            }
        }
        ViewChange::Child { node, child } => {
            apply_child(parent_entry, node, child, schema, relationship, format, with_ids, mutate)
        }
        ViewChange::Edit { node, old_node } => {
            if singular {
                apply_edit_singular(parent_entry, node, old_node, schema, relationship, with_ids, mutate)
            } else {
                apply_edit_plural(parent_entry, node, old_node, schema, relationship, with_ids, mutate)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Hidden schema handling
// ---------------------------------------------------------------------------

fn apply_change_hidden(
    parent_entry: &Entry,
    change: &ViewChange,
    schema: &SourceSchema,
    format: &Format,
    with_ids: bool,
    mutate: Mutate,
) -> Entry {
    match change {
        ViewChange::Add { node } | ViewChange::Remove { node } => {
            let mut current = parent_entry.clone();
            let rel_names = node.relationship_names();
            for rel_name in &rel_names {
                if let Some(child_schema) = schema.relationships.get(rel_name) {
                    let child_format = format.relationships.get(rel_name).cloned().unwrap_or_default();
                    for child_node in node.children(rel_name) {
                        let child_change = match change {
                            ViewChange::Add { .. } => ViewChange::Add { node: child_node },
                            ViewChange::Remove { .. } => ViewChange::Remove { node: child_node },
                            _ => unreachable!(),
                        };
                        current = apply_change_internal(
                            &current,
                            &child_change,
                            child_schema,
                            rel_name,
                            &child_format,
                            with_ids,
                            mutate,
                        );
                    }
                }
            }
            current
        }
        ViewChange::Edit { .. } => {
            // Hidden row changed — if the row was changed in a way that would
            // change relationships, the edit would have been split into remove+add.
            parent_entry.clone()
        }
        ViewChange::Child { node: _, child } => {
            let child_schema = schema
                .relationships
                .get(&child.relationship_name)
                .expect("child schema not found");
            let child_format = format
                .relationships
                .get(&child.relationship_name)
                .cloned()
                .unwrap_or_default();
            apply_change_internal(
                parent_entry,
                &child.change,
                child_schema,
                &child.relationship_name,
                &child_format,
                with_ids,
                mutate,
            )
        }
    }
}

// ---------------------------------------------------------------------------
// ADD
// ---------------------------------------------------------------------------

fn apply_add_singular(
    parent_entry: &Entry,
    node: &ViewNode,
    schema: &SourceSchema,
    relationship: &str,
    child_formats: &FxHashMap<String, Format>,
    with_ids: bool,
    mutate: Mutate,
) -> Entry {
    let existing = get_optional_singular_entry(parent_entry, relationship);
    match existing {
        Some(old_entry) => {
            // Duplicate add: increment refCount
            let old_row: Row = Arc::new(old_entry.row.clone());
            let node_row = node.row().clone();
            assert_eq!(
                (schema.compare_rows)(&old_row, &node_row),
                CmpOrdering::Equal,
                "Singular relationship should not have multiple rows"
            );
            let new_entry = inc_ref_count(old_entry, mutate);
            set_relation(parent_entry, relationship, View::Single(new_entry), mutate)
        }
        None => {
            // New row: create with rc=1, initialize nested relationships
            let mut new_entry = make_new_meta_entry(node.row(), schema, with_ids, 1);
            let entry_mut = Rc::get_mut(&mut new_entry).expect("new entry has refcount 1");
            initialize_relationships_for_new_entry(
                entry_mut,
                node,
                schema,
                child_formats,
                with_ids,
            );
            set_relation(parent_entry, relationship, View::Single(new_entry), mutate)
        }
    }
}

fn apply_add_plural(
    parent_entry: &Entry,
    node: &ViewNode,
    schema: &SourceSchema,
    relationship: &str,
    child_formats: &FxHashMap<String, Format>,
    with_ids: bool,
    mutate: Mutate,
) -> Entry {
    let view = get_child_entry_list(parent_entry, relationship);
    let result = add_to_list(node.row(), &view, schema, with_ids, mutate);
    let mut new_view = result.view;

    if let Some((pos, new_entry)) = result.new_entry {
        let mut new_entry = new_entry;
        let entry_mut = Rc::make_mut(&mut new_entry);
        initialize_relationships_for_new_entry(
            entry_mut,
            node,
            schema,
            child_formats,
            with_ids,
        );
        // Replace the cloned entry with the initialized one.
        new_view[pos] = new_entry;
    }
    set_relation(parent_entry, relationship, View::List(new_view), mutate)
}

/// Result of add_to_list: the new view and optionally the position + entry to initialize.
struct AddResult {
    new_entry: Option<(usize, Rc<Entry>)>,
    view: Vec<Rc<Entry>>,
}

/// Insert into a sorted list, or increment refCount if duplicate.
/// Returns the new view and optionally the position + entry to initialize.
fn add_to_list(
    row: &Row,
    view: &[Rc<Entry>],
    schema: &SourceSchema,
    with_ids: bool,
    _mutate: Mutate,
) -> AddResult {
    let raw_pos = binary_search(view, row, &schema.compare_rows);

    if raw_pos >= 0 {
        // Found: increment refCount via Rc::make_mut (COW)
        let pos = raw_pos as usize;
        let mut new_view = view.to_vec();
        let entry = Rc::make_mut(&mut new_view[pos]);
        entry.ref_count += 1;
        AddResult { new_entry: None, view: new_view }
    } else {
        // Not found: insert at ~rawPos
        let pos = (!raw_pos) as usize;
        let new_entry = make_new_meta_entry(row, schema, with_ids, 1);
        let mut new_view = view.to_vec();
        new_view.insert(pos, Rc::clone(&new_entry));
        AddResult { new_entry: Some((pos, new_entry)), view: new_view }
    }
}

// ---------------------------------------------------------------------------
// REMOVE
// ---------------------------------------------------------------------------

fn apply_remove_singular(
    parent_entry: &Entry,
    _node: &ViewNode,
    _schema: &SourceSchema,
    relationship: &str,
    mutate: Mutate,
) -> Entry {
    let existing = get_singular_entry(parent_entry, relationship);
    if existing.ref_count == 1 {
        set_relation(parent_entry, relationship, View::None, mutate)
    } else {
        let new_entry = dec_ref_count(existing, mutate);
        set_relation(parent_entry, relationship, View::Single(new_entry), mutate)
    }
}

fn apply_remove_plural(
    parent_entry: &Entry,
    node: &ViewNode,
    schema: &SourceSchema,
    relationship: &str,
    mutate: Mutate,
) -> Entry {
    let view = get_child_entry_list(parent_entry, relationship);
    let new_view = remove_and_update_ref_count(&view, node.row(), &schema.compare_rows, mutate);
    set_relation(parent_entry, relationship, View::List(new_view), mutate)
}

fn remove_and_update_ref_count(
    view: &[Rc<Entry>],
    row: &Row,
    compare_rows: &Comparator,
    _mutate: Mutate,
) -> Vec<Rc<Entry>> {
    let pos = binary_search(view, row, compare_rows);
    assert!(pos >= 0, "node does not exist");
    let pos = pos as usize;
    let old_entry = &view[pos];
    if old_entry.ref_count == 1 {
        let mut new_view = view.to_vec();
        new_view.remove(pos);
        new_view
    } else {
        let mut new_view = view.to_vec();
        let entry = Rc::make_mut(&mut new_view[pos]);
        entry.ref_count -= 1;
        new_view
    }
}

// ---------------------------------------------------------------------------
// CHILD — propagate nested change
// ---------------------------------------------------------------------------

fn apply_child(
    parent_entry: &Entry,
    node: &RowOnlyNode,
    child: &ChildViewChange,
    schema: &SourceSchema,
    relationship: &str,
    format: &Format,
    with_ids: bool,
    mutate: Mutate,
) -> Entry {
    let child_schema = schema
        .relationships
        .get(&child.relationship_name)
        .expect("child schema not found");
    let child_format = match format.relationships.get(&child.relationship_name) {
        Some(f) => f.clone(),
        None => return parent_entry.clone(), // Relationship not in view format
    };

    let singular = format.singular;

    if singular {
        let existing = get_singular_entry(parent_entry, relationship);
        let new_existing = apply_change_internal(
            existing,
            &child.change,
            child_schema,
            &child.relationship_name,
            &child_format,
            with_ids,
            mutate,
        );
        // Preserve identity if child didn't change
        let new_existing_rc = Rc::new(new_existing);
        if entries_equal(existing, &new_existing_rc) {
            return parent_entry.clone();
        }
        set_relation(parent_entry, relationship, View::Single(new_existing_rc), mutate)
    } else {
        let view = get_child_entry_list(parent_entry, relationship);
        let pos = binary_search(&view, &node.row, &schema.compare_rows);
        assert!(pos >= 0, "node does not exist");
        let pos = pos as usize;
        let existing = &view[pos];
        let new_existing = apply_change_internal(
            &**existing,
            &child.change,
            child_schema,
            &child.relationship_name,
            &child_format,
            with_ids,
            mutate,
        );
        let new_existing_rc = Rc::new(new_existing);
        if entries_equal(existing, &new_existing_rc) {
            return parent_entry.clone();
        }
        let mut new_view = view.to_vec();
        new_view[pos] = new_existing_rc;
        set_relation(parent_entry, relationship, View::List(new_view), mutate)
    }
}

// ---------------------------------------------------------------------------
// EDIT — update row fields, may move position
// ---------------------------------------------------------------------------

fn apply_edit_singular(
    parent_entry: &Entry,
    node: &RowOnlyNode,
    old_node: &RowOnlyNode,
    schema: &SourceSchema,
    relationship: &str,
    with_ids: bool,
    mutate: Mutate,
) -> Entry {
    let existing = get_singular_entry(parent_entry, relationship);
    let new_entry = apply_edit(existing, node, old_node, schema, with_ids, mutate);
    set_relation(parent_entry, relationship, View::Single(Rc::new(new_entry)), mutate)
}

fn apply_edit_plural(
    parent_entry: &Entry,
    node: &RowOnlyNode,
    old_node: &RowOnlyNode,
    schema: &SourceSchema,
    relationship: &str,
    with_ids: bool,
    mutate: Mutate,
) -> Entry {
    let view = get_child_entry_list(parent_entry, relationship);
    let compare_rows = &schema.compare_rows;

    // Check if sort key changed
    if compare_rows(&old_node.row, &node.row) != CmpOrdering::Equal {
        // Sort key changed: row may need to move
        let old_pos = binary_search(&view, &old_node.row, compare_rows);
        assert!(old_pos >= 0, "old node does not exist");
        let old_pos = old_pos as usize;
        let old_entry = &view[old_pos];

        let raw_pos = binary_search(&view, &node.row, compare_rows);
        let found = raw_pos >= 0;
        let pos = if found { raw_pos as usize } else { (!raw_pos) as usize };

        // Special case: rc=1 and new pos is same as or directly after old pos
        if old_entry.ref_count == 1 && (pos == old_pos || pos - 1 == old_pos) {
            let new_entry = apply_edit(&**old_entry, node, old_node, schema, with_ids, mutate);
            let mut new_view = view.to_vec();
            new_view[old_pos] = Rc::new(new_entry);
            return set_relation(parent_entry, relationship, View::List(new_view), mutate);
        }

        // Move the row
        let new_ref_count = old_entry.ref_count - 1;
        let (mut new_view, adjusted_pos) = if new_ref_count == 0 {
            let mut nv = view.to_vec();
            nv.remove(old_pos);
            let adj = if old_pos < pos { pos - 1 } else { pos };
            (nv, adj)
        } else {
            let mut nv = view.to_vec();
            Rc::make_mut(&mut nv[old_pos]).ref_count = new_ref_count;
            (nv, pos)
        };

        if found {
            // Merge with existing at new pos
            let existing_entry = &new_view[adjusted_pos];
            let mut edited = apply_edit(&**existing_entry, node, old_node, schema, with_ids, mutate);
            edited.ref_count = existing_entry.ref_count + 1;
            new_view[adjusted_pos] = Rc::new(edited);
        } else {
            // Insert at new pos with rc=1
            let mut edited = apply_edit(old_entry, node, old_node, schema, with_ids, mutate);
            edited.ref_count = 1;
            new_view.insert(adjusted_pos, Rc::new(edited));
        }
        set_relation(parent_entry, relationship, View::List(new_view), mutate)
    } else {
        // Sort key unchanged: edit in place
        let pos = binary_search(&view, &old_node.row, compare_rows);
        assert!(pos >= 0, "node does not exist");
        let pos = pos as usize;
        let new_entry = apply_edit(&view[pos], node, old_node, schema, with_ids, mutate);
        let mut new_view = view.to_vec();
        new_view[pos] = Rc::new(new_entry);
        set_relation(parent_entry, relationship, View::List(new_view), mutate)
    }
}

/// Apply an edit to an existing entry: update row fields.
fn apply_edit(
    existing: &Entry,
    node: &RowOnlyNode,
    _old_node: &RowOnlyNode,
    _schema: &SourceSchema,
    with_ids: bool,
    _mutate: Mutate,
) -> Entry {
    let mut new_entry = existing.clone();
    // Merge the new row fields into the entry
    for (k, v) in node.row.iter() {
        new_entry.row.insert(k.clone(), v.clone());
    }
    if with_ids {
        new_entry.id = make_id(&new_entry.row, _schema);
    }
    new_entry
}

// ---------------------------------------------------------------------------
// Initialize relationships for new entries
// ---------------------------------------------------------------------------

fn initialize_relationships_for_new_entry(
    entry: &mut Entry,
    node: &ViewNode,
    schema: &SourceSchema,
    child_formats: &FxHashMap<String, Format>,
    with_ids: bool,
) {
    let rel_names = node.relationship_names();
    for rel_name in &rel_names {
        let child_schema = match schema.relationships.get(rel_name) {
            Some(s) => s,
            None => continue,
        };
        let child_format = match child_formats.get(rel_name) {
            Some(f) => f.clone(),
            None => continue,
        };

        if child_schema.is_hidden || child_format.singular {
            // Hidden/singular: use applyChange to handle properly
            let initial_view = if child_format.singular {
                View::None
            } else {
                View::List(Vec::new())
            };
            entry.relationships.insert(rel_name.clone(), initial_view);

            for child_node in node.children(rel_name) {
                let change = ViewChange::Add { node: child_node };
                let updated = apply_change_internal(
                    entry,
                    &change,
                    child_schema,
                    rel_name,
                    &child_format,
                    with_ids,
                    true, // mutate — new entry, safe to build in place
                );
                *entry = updated;
            }
        } else {
            // Plural non-hidden: build array directly
            let mut child_array: Vec<Rc<Entry>> = Vec::new();

            for child_node in node.children(rel_name) {
                let new_entry = make_new_meta_entry(child_node.row(), child_schema, with_ids, 1);
                let raw_pos = binary_search(&child_array, child_node.row(), &child_schema.compare_rows);

                if raw_pos >= 0 {
                    Rc::make_mut(&mut child_array[raw_pos as usize]).ref_count += 1;
                } else {
                    let pos = (!raw_pos) as usize;
                    let mut entry_with_rels = (*new_entry).clone();
                    initialize_relationships_for_new_entry(
                        &mut entry_with_rels,
                        &child_node,
                        child_schema,
                        &child_format.relationships,
                        with_ids,
                    );
                    child_array.insert(pos, Rc::new(entry_with_rels));
                }
            }

            entry.relationships.insert(rel_name.clone(), View::List(child_array));
        }
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Create a new MetaEntry from a row with the given refCount.
fn make_new_meta_entry(row: &Row, schema: &SourceSchema, with_ids: bool, rc: usize) -> Rc<Entry> {
    let mut entry = Entry::new(row.as_ref().clone(), rc);
    if with_ids {
        entry.id = make_id(&entry.row, schema);
    }
    Rc::new(entry)
}

/// Generate a stable ID from a row's primary key.
fn make_id(row: &FxHashMap<String, Value>, schema: &SourceSchema) -> Option<String> {
    if schema.primary_key.len() == 1 {
        let pk = &schema.primary_key[0];
        let val = row.get(pk).cloned().unwrap_or(Value::Null);
        Some(value_to_json_string(&val))
    } else {
        let parts: Vec<String> = schema
            .primary_key
            .iter()
            .map(|k| value_to_json_string(&row.get(k).cloned().unwrap_or(Value::Null)))
            .collect();
        Some(format!("[{}]", parts.join(",")))
    }
}

fn value_to_json_string(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::F64(n) => n.to_string(),
        Value::Str(s) => format!("\"{}\"", s),
        Value::Json(s) => s.to_string(),
    }
}

/// Binary search returning a number.
/// - If found at index `i`: returns `i` (>= 0).
/// - If not found, insertion point is `low`: returns `~low` (< 0).
fn binary_search(view: &[Rc<Entry>], target: &Row, comparator: &Comparator) -> i64 {
    let mut low: i64 = 0;
    let mut high: i64 = view.len() as i64 - 1;
    while low <= high {
        let mid = (low + high) >> 1;
        let mid_entry_row: Row = Arc::new(view[mid as usize].row.clone());
        let cmp = comparator(&mid_entry_row, target);
        if cmp == CmpOrdering::Less {
            low = mid + 1;
        } else if cmp == CmpOrdering::Greater {
            high = mid - 1;
        } else {
            return mid;
        }
    }
    !low
}

/// Get singular entry, throws if missing.
fn get_singular_entry<'a>(parent_entry: &'a Entry, relationship: &str) -> &'a Rc<Entry> {
    match parent_entry.relationships.get(relationship) {
        Some(View::Single(e)) => e,
        _ => panic!("node does not exist"),
    }
}

/// Get singular entry or None if not set.
fn get_optional_singular_entry<'a>(parent_entry: &'a Entry, relationship: &str) -> Option<&'a Rc<Entry>> {
    match parent_entry.relationships.get(relationship) {
        Some(View::Single(e)) => Some(e),
        _ => None,
    }
}

/// Get child entry list as a slice.
fn get_child_entry_list<'a>(parent_entry: &'a Entry, relationship: &str) -> Vec<Rc<Entry>> {
    match parent_entry.relationships.get(relationship) {
        Some(View::List(v)) => v.clone(),
        Some(View::Single(_)) | None | Some(View::None) => Vec::new(),
    }
}

/// Increment refCount on an entry.
fn inc_ref_count(entry: &Rc<Entry>, _mutate: Mutate) -> Rc<Entry> {
    let mut new_entry = (**entry).clone();
    new_entry.ref_count += 1;
    Rc::new(new_entry)
}

/// Decrement refCount on an entry.
fn dec_ref_count(entry: &Rc<Entry>, _mutate: Mutate) -> Rc<Entry> {
    let mut new_entry = (**entry).clone();
    new_entry.ref_count -= 1;
    Rc::new(new_entry)
}

/// Set a relationship on a parent entry, returning a new entry.
fn set_relation(parent_entry: &Entry, relationship: &str, value: View, _mutate: Mutate) -> Entry {
    let mut new_entry = parent_entry.clone();
    new_entry.relationships.insert(relationship.to_string(), value);
    new_entry
}

/// Check if two entries are equal (identity check + ref_count + row).
fn entries_equal(a: &Rc<Entry>, b: &Rc<Entry>) -> bool {
    if Rc::ptr_eq(a, b) {
        return true;
    }
    a.ref_count == b.ref_count
        && a.id == b.id
        && a.row == b.row
        && views_equal(&a.relationships, &b.relationships)
}

/// Deep-compare two relationship view maps for structural equality.
fn views_equal(
    a: &FxHashMap<String, View>,
    b: &FxHashMap<String, View>,
) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for (key, va) in a {
        match b.get(key) {
            None => return false,
            Some(vb) => if !view_equal(va, vb) {
                return false;
            }
        }
    }
    true
}

/// Deep-compare two Views for structural equality.
fn view_equal(a: &View, b: &View) -> bool {
    match (a, b) {
        (View::None, View::None) => true,
        (View::Single(ea), View::Single(eb)) => entries_equal(ea, eb),
        (View::List(la), View::List(lb)) => {
            la.len() == lb.len()
                && la.iter().zip(lb.iter()).all(|(a, b)| entries_equal(a, b))
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Conversion: Change → ViewChange
// ---------------------------------------------------------------------------

/// Convert a pipeline `Change` to a `ViewChange` for view-tree application.
/// Recursively expands Node relationships to ExpandedNode for add/remove.
pub fn change_to_view_change(change: &crate::ivm::change::Change) -> ViewChange {
    use crate::ivm::change::Change as C;
    match change {
        C::Add(node) => ViewChange::Add {
            node: ViewNode::Lazy(node.clone()),
        },
        C::Remove(node) => ViewChange::Remove {
            node: ViewNode::Lazy(node.clone()),
        },
        C::Edit { node, old_node } => ViewChange::Edit {
            node: RowOnlyNode { row: node.row.clone() },
            old_node: RowOnlyNode { row: old_node.row.clone() },
        },
        C::Child { node, child } => ViewChange::Child {
            node: RowOnlyNode { row: node.row.clone() },
            child: ChildViewChange {
                relationship_name: child.relationship_name.clone(),
                change: Box::new(change_to_view_change(&child.change)),
            },
        },
    }
}

/// Create an empty root entry (the top of the view tree).
pub fn empty_root_entry() -> Entry {
    Entry::new(FxHashMap::default(), 0)
}

use std::sync::Arc;
