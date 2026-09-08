//! Exists operator — port of `zql/src/ivm/exists.ts`.
//!
//! Filters parent nodes based on whether their relationship has any child
//! rows (EXISTS) or none (NOT EXISTS). A `FilterOperator`: `filter(node)`
//! instead of `fetch`, with a per-filter-loop size cache scoped by
//! `begin_filter`/`end_filter`.
//!
//! During push, add/remove child changes for the watched relationship can
//! change the size and thus the filter result. The operator handles the
//! transition from 0→1 (add) and 1→0 (remove) by emitting add/remove changes
//! for the parent node. Other changes are forwarded if the filter passes.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use crate::ivm::change::{Change, ChangeType, make_add_change, make_remove_change};
use crate::ivm::data::{Node, Value};
use crate::ivm::filter_operators::{
    FilterChainPusher, FilterInput, FilterInputHandle, FilterOutput, FilterOutputHandle,
    FilterResult, filter_and, filter_result, map_filter_result,
};
use crate::ivm::operator::{InputBase, Shared};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::{NodeStream, StreamItem};

/// Build a cache key from a node's parent join key values.
/// Port of TS Exists.#getCacheKey (exists.ts:224).
fn get_cache_key(node: &Node, parent_join_key: &[String]) -> String {
    let values: Vec<String> = parent_join_key
        .iter()
        .map(|k| crate::ivm::data::js_stringify_value(node.row.get(k).unwrap_or(&Value::Null)))
        .collect();
    values.join("\x00")
}

/// The Exists operator — port of TS `Exists` (exists.ts:21).
pub struct Exists {
    input: FilterInputHandle,
    relationship_name: String,
    not: bool,
    parent_join_key: Vec<String>,
    /// If the parentJoinKey is the primary key, no sense in trying to reuse.
    no_size_reuse: bool,
    schema: SourceSchema,
    output: Rc<RefCell<Option<FilterOutputHandle>>>,
    /// Per-filter-loop cache: cache_key -> exists. Cleared in `end_filter`
    /// (TS exists.ts:76).
    /// Shared with the lazy `filter` generator, which records the fetched
    /// size once the child stream is exhausted (TS `this.#cache.set(key,
    /// exists)` after `yield* this.#fetchExists(node)`, exists.ts:86-87).
    cache: Rc<RefCell<HashMap<String, bool>>>,
    /// True while a push is in flight (TS `#inPush`, exists.ts:39): during a
    /// push, relationships can be inconsistent (changes arrive one node at a
    /// time), so cached-size reuse across rows is disabled.
    in_push: Cell<bool>,
}

/// RAII: clears `in_push` when a push completes or unwinds (TS `finally`).
struct InPushReset<'a>(&'a Cell<bool>);
impl Drop for InPushReset<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

impl Exists {
    pub fn new(
        input: FilterInputHandle,
        relationship_name: String,
        parent_join_key: Vec<String>,
        not: bool,
    ) -> Shared<Exists> {
        crate::live_count::inc(&crate::live_count::EXISTS);
        let schema = input.borrow().get_schema();
        assert!(
            schema.relationships.contains_key(&relationship_name),
            "Input schema missing {relationship_name}"
        );
        let no_size_reuse = parent_join_key == schema.primary_key;
        let exists = Rc::new(RefCell::new(Exists {
            input: input.clone(),
            relationship_name,
            not,
            parent_join_key,
            no_size_reuse,
            schema,
            output: Rc::new(RefCell::new(None)),
            cache: Rc::new(RefCell::new(HashMap::new())),
            in_push: Cell::new(false),
        }));
        // TS: `input.setFilterOutput(this)`.
        let as_output: FilterOutputHandle = exists.clone();
        input.borrow().set_filter_output(as_output);
        exists
    }

    /// Port of TS `#fetchSize` (exists.ts:248).
    /// Port of TS `#fetchSize` (exists.ts:246-262) as the generator it is:
    /// the child relationship's `'yield'`s are forwarded and the count is the
    /// generator's return value.
    fn fetch_size_stream(&self, node: &Node) -> Box<dyn Iterator<Item = StreamItem<usize>>> {
        let rel_fn = node
            .relationships
            .get(&self.relationship_name)
            .unwrap_or_else(|| {
                panic!(
                    "Exists: relationship \"{}\" not found on node",
                    self.relationship_name
                )
            })
            .clone();
        Box::new(FetchSize {
            stream: Some(rel_fn()),
            size: 0,
        })
    }

    /// `#fetchSize` on the (eager) push path: drain the yields.
    fn fetch_size(&self, node: &Node) -> usize {
        let _t = crate::perf_trace::scope("exists.size");
        let mut stream = self.fetch_size_stream(node);
        loop {
            match stream.next() {
                Some(StreamItem::Data(size)) => return size,
                Some(StreamItem::Yield) => continue,
                None => panic!("fetchSize generator ended without a result"),
            }
        }
    }

    /// Port of TS `#fetchExists` (exists.ts:241) as a generator.
    fn fetch_exists_stream(&self, node: &Node) -> FilterResult {
        map_filter_result(self.fetch_size_stream(node), |size| size > 0)
    }

    /// Port of TS `#fetchExists` (exists.ts:241). (Cannot fetch just 1 node:
    /// Take does not support early return during initial fetch.)
    fn fetch_exists(&self, node: &Node) -> bool {
        self.fetch_size(node) > 0
    }

    /// Port of TS private `#filter(node, exists?)` (exists.ts:219).
    #[cfg_attr(feature = "profiling", inline(never))]
    fn filter_inner(&self, node: &Node, exists: Option<bool>) -> bool {
        let exists = exists.unwrap_or_else(|| self.fetch_exists(node));
        if self.not { !exists } else { exists }
    }

    /// Port of TS `#pushWithFilter` (exists.ts:235).
    fn push_with_filter(&self, change: Change, exists: Option<bool>) {
        if self.filter_inner(change.node(), exists) {
            self.push_to_output(change);
        }
    }

    fn push_to_output(&self, change: Change) {
        let output = self.output.borrow().clone();
        if let Some(output) = output {
            let pusher = FilterChainPusher {
                schema: self.schema.clone(),
            };
            output.borrow().push(change, &pusher);
        }
    }
}

impl InputBase for Exists {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        self.input.borrow_mut().destroy();
        // Rust-only: break the Rc back-edge cycle on teardown.
        *self.output.borrow_mut() = None;
    }
}

impl FilterInput for Exists {
    fn set_filter_output(&self, output: FilterOutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }
}

impl FilterOutput for Exists {
    fn begin_filter(&self) {
        if let Some(output) = self.output.borrow().clone() {
            output.borrow().begin_filter();
        }
    }

    /// TS clears the size cache at the end of each filter loop (exists.ts:75).
    fn end_filter(&self) {
        self.cache.borrow_mut().clear();
        if let Some(output) = self.output.borrow().clone() {
            output.borrow().end_filter();
        }
    }

    /// Port of TS `filter` (exists.ts:80): consult/populate the per-loop
    /// cache (disabled when keyed by primary key, or mid-push), then AND with
    /// the downstream chain.
    /// Port of TS `filter` (exists.ts:80-97): resolve `exists` through the
    /// per-filter-loop cache, then `(yield* this.#filter(node, exists)) &&
    /// (yield* this.#output.filter(node))` — a lazy generator so the child
    /// fetch's yields reach the caller.
    #[cfg_attr(feature = "profiling", inline(never))]
    fn filter(&self, node: &Node) -> FilterResult {
        let exists_stream: FilterResult = if !self.no_size_reuse && !self.in_push.get() {
            let key = get_cache_key(node, &self.parent_join_key);
            let cached = self.cache.borrow().get(&key).copied();
            match cached {
                Some(v) => filter_result(v),
                None => {
                    let cache = self.cache.clone();
                    map_filter_result(self.fetch_exists_stream(node), move |v| {
                        cache.borrow_mut().insert(key.clone(), v);
                        v
                    })
                }
            }
        } else {
            // `exists` stays undefined → `#filter(node, exists)` fetches it
            // (exists.ts:220).
            self.fetch_exists_stream(node)
        };
        let not = self.not;
        let output = self
            .output
            .borrow()
            .clone()
            .expect("Exists: output not set");
        let node = node.clone();
        filter_and(
            map_filter_result(
                exists_stream,
                move |exists| if not { !exists } else { exists },
            ),
            Box::new(move || output.borrow().filter(&node)),
        )
    }

    /// Port of TS `push` (exists.ts:109).
    fn push(&self, change: Change, _pusher: &dyn InputBase) {
        crate::ivm::trace::recv("exists#1", &change);
        // TS `assert(!this.#inPush, 'Unexpected re-entrancy')` (exists.ts:110):
        // a re-entrant push means relationships are inconsistent mid-batch —
        // reset the pipeline rather than silently drop. The panic is contained
        // at the engine boundary and surfaces as a pipeline reset.
        assert!(!self.in_push.get(), "Exists: unexpected re-entrant push");
        self.in_push.set(true);
        let _reset = InPushReset(&self.in_push);

        match change.change_type() {
            // add/remove/edit cannot change the size of the watched
            // relationship: simply pushWithFilter.
            ChangeType::Add | ChangeType::Edit | ChangeType::Remove => {
                self.push_with_filter(change, None);
            }
            ChangeType::Child => {
                let (node, child) = match &change {
                    Change::Child { node, child } => (node.clone(), child.clone()),
                    _ => unreachable!(),
                };
                // Only add/remove child changes for the watched relationship
                // can change its size; everything else pushes through the
                // filter (exists.ts:127-137).
                if child.relationship_name != self.relationship_name
                    || matches!(
                        child.change.change_type(),
                        ChangeType::Edit | ChangeType::Child
                    )
                {
                    self.push_with_filter(change, None);
                    return;
                }
                match child.change.change_type() {
                    ChangeType::Add => {
                        let size = self.fetch_size(&node);
                        if size == 1 {
                            if self.not {
                                // The add child change is not pushed to output,
                                // so the added child must be EXCLUDED from the
                                // remove being pushed (exists.ts:142-156).
                                let removed_node = node.set_relationship(
                                    &self.relationship_name,
                                    crate::ivm::stream::empty_rel(),
                                );
                                self.push_to_output(make_remove_change(removed_node));
                            } else {
                                self.push_to_output(make_add_change(node));
                            }
                        } else {
                            self.push_with_filter(change, Some(size > 0));
                        }
                    }
                    ChangeType::Remove => {
                        let size = self.fetch_size(&node);
                        if size == 0 {
                            if self.not {
                                self.push_to_output(make_add_change(node));
                            } else {
                                // The remove child change is not pushed to
                                // output, so the removed child must be ADDED to
                                // the remove being pushed (exists.ts:177-194).
                                let removed_child_node = match child.change.as_ref() {
                                    Change::Add(n) | Change::Remove(n) => n.clone(),
                                    _ => unreachable!(),
                                };
                                let rel =
                                    crate::ivm::stream::rel_from_vec(vec![removed_child_node]);
                                let removed_node =
                                    node.set_relationship(&self.relationship_name, rel);
                                self.push_to_output(make_remove_change(removed_node));
                            }
                        } else {
                            self.push_with_filter(change, Some(size > 0));
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}

impl Drop for Exists {
    fn drop(&mut self) {
        crate::live_count::dec(&crate::live_count::EXISTS);
    }
}

/// The generator state of TS `#fetchSize` (exists.ts:246-262).
struct FetchSize {
    stream: Option<NodeStream>,
    size: usize,
}

impl Iterator for FetchSize {
    type Item = StreamItem<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        let stream = self.stream.as_mut()?;
        loop {
            match stream.next() {
                Some(StreamItem::Yield) => return Some(StreamItem::Yield),
                Some(StreamItem::Data(_)) => self.size += 1,
                None => {
                    self.stream = None;
                    return Some(StreamItem::Data(self.size));
                }
            }
        }
    }
}
