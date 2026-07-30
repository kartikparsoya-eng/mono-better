//! Exists operator — port of `zql/src/ivm/exists.ts`.
//!
//! Filters parent nodes based on whether their relationship has any child
//! rows (EXISTS) or has none (NOT EXISTS). Caches sizes per node.
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
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output, OutputHandle, Shared};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::NodeStream;

/// Build a cache key from a node's parent join key values.
/// Port of TS Exists.#getCacheKey (exists.ts:224).
fn get_cache_key(node: &Node, parent_join_key: &[String]) -> String {
    let values: Vec<String> = parent_join_key
        .iter()
        .map(|k| format!("{:?}", node.row.get(k).cloned().unwrap_or(Value::Null)))
        .collect();
    values.join("\x00")
}

/// The Exists operator — port of TS `Exists` (exists.ts).
pub struct Exists {
    input: Shared<dyn Input>,
    relationship_name: String,
    not: bool,
    parent_join_key: Vec<String>,
    no_size_reuse: bool,
    schema: SourceSchema,
    output: Rc<RefCell<Option<OutputHandle>>>,
    /// Cached exists results: cache_key -> bool (exists or not)
    exists_cache: Rc<RefCell<HashMap<String, bool>>>,
    /// True while a push is in flight. Port of TS `#inPush` (exists.ts:39):
    /// a re-entrant push is an invariant violation and must reset the pipeline
    /// (TS asserts). Interior-mutable so it can be set through the shared
    /// (immutably-borrowed) handle held by `ExistsOutput`.
    in_push: Cell<bool>,
}

/// RAII: clears `Exists::in_push` when a push completes or unwinds, so a
/// panic (the re-entrancy assert) doesn't leave the flag stuck on.
struct InPushGuard(Shared<Exists>);
impl Drop for InPushGuard {
    fn drop(&mut self) {
        if let Ok(e) = self.0.try_borrow() {
            e.in_push.set(false);
        }
    }
}

impl Exists {
    pub fn new(
        input: Shared<dyn Input>,
        relationship_name: String,
        parent_join_key: Vec<String>,
        not: bool,
    ) -> Shared<Exists> {
        let schema = input.borrow().get_schema();

        // If the parentJoinKey is the primary key, no sense in trying to reuse.
        let no_size_reuse = parent_join_key == schema.primary_key;

        let exists = Rc::new(RefCell::new(Exists {
            input: input.clone(),
            relationship_name,
            not,
            parent_join_key,
            no_size_reuse,
            schema,
            output: Rc::new(RefCell::new(None)),
            exists_cache: Rc::new(RefCell::new(HashMap::new())),
            in_push: Cell::new(false),
        }));

        let exists_clone = exists.clone();
        input
            .borrow()
            .set_output(Rc::new(RefCell::new(ExistsOutput {
                exists: exists_clone,
            })));

        exists
    }

    fn fetch_size(&self, node: &Node) -> usize {
        if let Some(rel_fn) = node.relationships.get(&self.relationship_name) {
            rel_fn()
                .filter(|i| matches!(i, crate::ivm::stream::StreamItem::Data(_)))
                .count()
        } else {
            0
        }
    }

    /// Check if the node passes the EXISTS/NOT EXISTS filter.
    /// `exists_size` is optional — if None, the size is fetched.
    #[allow(dead_code)]
    fn filter(&self, node: &Node, exists_size: Option<usize>) -> bool {
        let exists = exists_size.unwrap_or_else(|| self.fetch_size(node)) > 0;
        if self.not { !exists } else { exists }
    }

    /// Push a change through the filter (forwarding if it passes).
    #[allow(dead_code)]
    fn push_with_filter(
        &self,
        change: &Change,
        exists_size: Option<usize>,
        pusher: &dyn InputBase,
    ) {
        if self.filter(change.node(), exists_size) {
            let output = self.output.borrow().clone();
            if let Some(output) = output {
                output.borrow_mut().push(change.clone(), pusher);
            }
        }
    }
}

impl InputBase for Exists {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        self.input.borrow_mut().destroy();
    }
}

impl Input for Exists {
    fn set_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        // The size cache is only valid within a single fetch pass (TS clears it
        // in endFilter). Clear it here so a re-fetch after a size-changing push
        // recomputes EXISTS rather than reusing a stale hit (e.g. a parent that
        // had 0 children at hydrate but gained one via a push). Use try_borrow_mut:
        // a nested/re-entrant fetch (the cache is mid-use) must not reset the
        // outer pass's cache, and clearing it then would panic on the live borrow.
        if let Ok(mut cache) = self.exists_cache.try_borrow_mut() {
            cache.clear();
        }
        let input = self.input.borrow();
        let rel_name = self.relationship_name.clone();
        let not = self.not;
        let parent_join_key = self.parent_join_key.clone();
        let no_size_reuse = self.no_size_reuse;
        let cache = self.exists_cache.clone();
        Box::new(input.fetch(req).filter_map(move |item| {
            // Pass Yield sentinels through untouched; only filter Data nodes.
            let n = match &item {
                crate::ivm::stream::StreamItem::Data(n) => n,
                crate::ivm::stream::StreamItem::Yield => {
                    return Some(crate::ivm::stream::StreamItem::Yield);
                }
            };
            // Cache lookup: if we've seen this parent join key before, reuse.
            // Port of TS Exists.#filter (exists.ts:80-94).
            let exists_result = if !no_size_reuse {
                let key = get_cache_key(n, &parent_join_key);
                if let Some(&cached) = cache.borrow().get(&key) {
                    cached
                } else {
                    let size = if let Some(rel_fn) = n.relationships.get(&rel_name) {
                        rel_fn()
                            .filter(|i| matches!(i, crate::ivm::stream::StreamItem::Data(_)))
                            .count()
                    } else {
                        0
                    };
                    let exists = size > 0;
                    cache.borrow_mut().insert(key, exists);
                    exists
                }
            } else {
                let size = if let Some(rel_fn) = n.relationships.get(&rel_name) {
                    rel_fn()
                        .filter(|i| matches!(i, crate::ivm::stream::StreamItem::Data(_)))
                        .count()
                } else {
                    0
                };
                size > 0
            };
            let keep = if not { !exists_result } else { exists_result };
            if keep { Some(item) } else { None }
        }))
    }
}

impl Output for Exists {
    fn push(&mut self, _change: Change, _pusher: &dyn InputBase) {
        // Pushes arrive via ExistsOutput adapter
    }
}

/// Output adapter that receives pushes from the input and applies the EXISTS filter.
struct ExistsOutput {
    exists: Shared<Exists>,
}

impl Output for ExistsOutput {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        crate::ivm::trace::recv("exists#1", &change);
        // Port of TS `assert(!this.#inPush, 'Unexpected re-entrancy')`
        // (exists.ts:110): a re-entrant push means relationships are
        // inconsistent mid-batch and the result would be wrong — reset rather
        // than silently drop the change. The panic is contained at the napi
        // boundary and surfaces as a pipeline reset.
        {
            let e = self.exists.borrow();
            assert!(!e.in_push.get(), "Exists: unexpected re-entrant push");
            e.in_push.set(true);
        }
        let _in_push_guard = InPushGuard(self.exists.clone());
        let exists = self.exists.borrow();

        // We need to release the borrow on `exists` during the push to avoid
        // borrow conflicts. Clone the necessary data first.
        let rel_name = exists.relationship_name.clone();
        let not = exists.not;
        let output = exists.output.borrow().clone();

        match change.change_type() {
            ChangeType::Add | ChangeType::Edit | ChangeType::Remove => {
                // These don't change the size of the watched relationship.
                // Just forward through the filter.
                let node = change.node().clone();
                let size = exists.fetch_size(&node);
                let passes = if not { size == 0 } else { size > 0 };
                if passes {
                    drop(exists);
                    if let Some(output) = output {
                        output.borrow_mut().push(change, pusher);
                    }
                } else {
                    drop(exists);
                }
            }
            ChangeType::Child => {
                let (node, child) = match &change {
                    Change::Child { node, child } => (node.clone(), child.clone()),
                    _ => unreachable!(),
                };

                // Only add/remove child changes for the watched relationship
                // can change the size. Other child changes (different relationship
                // or edit/child child changes) just pass through the filter.
                if child.relationship_name != rel_name
                    || matches!(
                        child.change.change_type(),
                        ChangeType::Edit | ChangeType::Child
                    )
                {
                    let size = exists.fetch_size(&node);
                    let passes = if not { size == 0 } else { size > 0 };
                    if passes {
                        drop(exists);
                        if let Some(output) = output {
                            output.borrow_mut().push(change, pusher);
                        }
                    } else {
                        drop(exists);
                    }
                    return;
                }

                match child.change.change_type() {
                    ChangeType::Add => {
                        let size = exists.fetch_size(&node);
                        if size == 1 {
                            // Transition from 0→1: the filter result flips.
                            if not {
                                // NOT EXISTS: was passing (size=0), now fails.
                                // Push a remove with the relationship emptied.
                                let mut removed_node = node.clone();
                                removed_node = removed_node
                                    .set_relationship(&rel_name, crate::ivm::stream::empty_rel());
                                drop(exists);
                                if let Some(output) = output {
                                    output
                                        .borrow_mut()
                                        .push(make_remove_change(removed_node), pusher);
                                }
                            } else {
                                // EXISTS: was failing (size=0), now passes.
                                // Push an add with the node as-is.
                                drop(exists);
                                if let Some(output) = output {
                                    output.borrow_mut().push(make_add_change(node), pusher);
                                }
                            }
                        } else {
                            // Size > 1: filter result unchanged, forward if passing.
                            let passes = if not { size == 0 } else { size > 0 };
                            drop(exists);
                            if passes && let Some(output) = output {
                                output.borrow_mut().push(change, pusher);
                            }
                        }
                    }
                    ChangeType::Remove => {
                        let size = exists.fetch_size(&node);
                        if size == 0 {
                            // Transition from 1→0: the filter result flips.
                            if not {
                                // NOT EXISTS: was failing, now passes.
                                drop(exists);
                                if let Some(output) = output {
                                    output.borrow_mut().push(make_add_change(node), pusher);
                                }
                            } else {
                                // EXISTS: was passing, now fails.
                                // Push a remove that includes the removed child
                                // (since the child change is not forwarded).
                                let removed_child_node = match child.change.as_ref() {
                                    Change::Add(n) | Change::Remove(n) => n.clone(),
                                    _ => unreachable!(),
                                };
                                let mut removed_node = node.clone();
                                let rel =
                                    crate::ivm::stream::rel_from_vec(vec![removed_child_node]);
                                removed_node = removed_node.set_relationship(&rel_name, rel);
                                drop(exists);
                                if let Some(output) = output {
                                    output
                                        .borrow_mut()
                                        .push(make_remove_change(removed_node), pusher);
                                }
                            }
                        } else {
                            // Size > 0: filter result unchanged, forward if passing.
                            let passes = if not { size == 0 } else { size > 0 };
                            drop(exists);
                            if passes && let Some(output) = output {
                                output.borrow_mut().push(change, pusher);
                            }
                        }
                    }
                    _ => {
                        // Edit or Child child changes: forward through filter.
                        let size = exists.fetch_size(&node);
                        let passes = if not { size == 0 } else { size > 0 };
                        drop(exists);
                        if passes && let Some(output) = output {
                            output.borrow_mut().push(change, pusher);
                        }
                    }
                }
            }
        }

        // `_in_push_guard` clears `in_push` here as it drops.
    }
}
