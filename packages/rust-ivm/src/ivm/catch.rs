//! Catch operator — port of `zql/src/ivm/catch.ts`.
//!
//! Catch is an Output that collects all incoming stream data into arrays.
//! Mainly useful for testing.

use std::cell::RefCell;
use std::rc::Rc;

use crate::ivm::change::Change;
use crate::ivm::data::{Node, Row};
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output, Shared};

/// A caught node — eagerly expanded (relationships as arrays, not generators).
#[derive(Clone, Debug)]
pub struct CaughtNode {
    pub row: Row,
    pub relationships: Vec<(String, Vec<CaughtNode>)>,
}

/// A caught change — the expanded form of a Change for testing.
#[derive(Clone, Debug)]
pub enum CaughtChange {
    Add {
        node: CaughtNode,
    },
    Remove {
        node: CaughtNode,
    },
    Edit {
        old_row: Row,
        row: Row,
    },
    Child {
        row: Row,
        child: Box<(String, CaughtChange)>,
    },
}

/// Catch — collects all pushes for testing. Optionally fetches current state
/// on each push.
pub struct Catch {
    input: Shared<dyn Input>,
    fetch_on_push: bool,
    pub pushes: Vec<CaughtChange>,
    pub pushes_with_fetch: Vec<(CaughtChange, Vec<CaughtNode>)>,
}

impl Catch {
    pub fn new(input: Shared<dyn Input>, fetch_on_push: bool) -> Rc<RefCell<Catch>> {
        let catch = Rc::new(RefCell::new(Catch {
            input: input.clone(),
            fetch_on_push,
            pushes: Vec::new(),
            pushes_with_fetch: Vec::new(),
        }));

        let catch_clone = catch.clone();
        input
            .borrow()
            .set_output(Rc::new(RefCell::new(CatchOutput { catch: catch_clone })));

        catch
    }

    pub fn fetch(&self, req: &FetchRequest) -> Vec<CaughtNode> {
        let input = self.input.borrow();
        crate::ivm::stream::skip_yields(input.fetch(req))
            .map(|n| expand_node(&n))
            .collect()
    }

    pub fn reset(&mut self) {
        self.pushes.clear();
        self.pushes_with_fetch.clear();
    }

    pub fn destroy(&self) {
        self.input.borrow_mut().destroy();
    }
}

struct CatchOutput {
    catch: Rc<RefCell<Catch>>,
}

impl Output for CatchOutput {
    fn push(&mut self, change: Change, _pusher: &dyn InputBase) {
        crate::ivm::trace::recv("catch#1", &change);
        // Expand BEFORE borrowing Catch — expand_node calls rel_fn() which
        // triggers nested fetches that must not collide with Catch's RefCell.
        let expanded = expand_change(&change);

        let fetch = if self.catch.borrow().fetch_on_push {
            // borrow() not borrow_mut() — fetch takes &self.
            Some(
                crate::ivm::stream::skip_yields(
                    self.catch
                        .borrow()
                        .input
                        .borrow()
                        .fetch(&FetchRequest::default()),
                )
                .map(|n| expand_node(&n))
                .collect(),
            )
        } else {
            None
        };

        let mut catch = self.catch.borrow_mut();
        if let Some(fetch) = fetch {
            catch.pushes_with_fetch.push((expanded.clone(), fetch));
        }
        catch.pushes.push(expanded);
    }
}

/// Expand a Change into a CaughtChange.
pub fn expand_change(change: &Change) -> CaughtChange {
    match change {
        Change::Add(node) => CaughtChange::Add {
            node: expand_node(node),
        },
        Change::Remove(node) => CaughtChange::Remove {
            node: expand_node(node),
        },
        Change::Edit { node, old_node } => CaughtChange::Edit {
            old_row: old_node.row.clone(),
            row: node.row.clone(),
        },
        Change::Child { node, child } => CaughtChange::Child {
            row: node.row.clone(),
            child: Box::new((
                child.relationship_name.clone(),
                expand_change(&child.change),
            )),
        },
    }
}

/// Expand a Node into a CaughtNode (eagerly evaluate all relationships).
pub fn expand_node(node: &Node) -> CaughtNode {
    let mut relationships = Vec::new();
    for name in &node.rel_order {
        if let Some(rel_fn) = node.relationships.get(name) {
            let stream = rel_fn();
            let children: Vec<CaughtNode> = crate::ivm::stream::skip_yields(stream)
                .map(|n| expand_node(&n))
                .collect();
            relationships.push((name.clone(), children));
        }
    }
    CaughtNode {
        row: node.row.clone(),
        relationships,
    }
}
