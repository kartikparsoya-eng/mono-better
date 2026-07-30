//! Skip operator — port of `zql/src/ivm/skip.ts`.
//!
//! Skips rows before a start position (pagination). Uses partial-bound
//! comparison so a partial cursor (e.g. {createdAt}) compares correctly
//! against a full sort key ([createdAt, id]).

use std::cell::RefCell;
use std::cmp::Ordering as CmpOrdering;
use std::rc::Rc;

use crate::ivm::change::{Change, ChangeType};
use crate::ivm::data::{Row, make_partial_bound_comparator};
use crate::ivm::operator::{Basis, FetchRequest, Input, InputBase, Output, OutputHandle, Shared};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::NodeStream;

/// The Skip operator — port of TS `Skip` (skip.ts).
pub struct Skip {
    input: Shared<dyn Input>,
    start_row: Row,
    exclusive: bool,
    compare: crate::ivm::data::Comparator,
    schema: SourceSchema,
    output: Rc<RefCell<Option<OutputHandle>>>,
}

impl Skip {
    pub fn new(input: Shared<dyn Input>, start: crate::builder::ast::Bound) -> Shared<Skip> {
        let schema = input.borrow().get_schema();
        let sort = schema.sort.clone().expect("Skip requires sorted input");
        let compare = make_partial_bound_comparator(sort, false);

        let start_row = start.row.clone();
        let exclusive = start.exclusive;

        let skip = Rc::new(RefCell::new(Skip {
            input: input.clone(),
            start_row,
            exclusive,
            compare,
            schema,
            output: Rc::new(RefCell::new(None)),
        }));

        let skip_clone = skip.clone();
        input
            .borrow()
            .set_output(Rc::new(RefCell::new(SkipOutput { skip: skip_clone })));

        skip
    }

    fn should_be_present(&self, row: &Row) -> bool {
        let cmp = (self.compare)(row, &self.start_row);
        if self.exclusive {
            cmp == CmpOrdering::Greater
        } else {
            cmp != CmpOrdering::Less
        }
    }
}

impl InputBase for Skip {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        self.input.borrow_mut().destroy();
    }
}

impl Input for Skip {
    fn set_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        let input = self.input.borrow();
        let start_row = self.start_row.clone();
        let exclusive = self.exclusive;
        let compare = self.compare.clone();

        // Compute the start to propagate to the upstream, matching TS
        // `Skip.#getStart` (skip.ts). TS passes `{...req, start}` into
        // `this.#input.fetch`, which lets the source run `overlaysForStartAt`
        // (dropping an overlay row that sorts before `start` in INDEX order).
        // Rust previously filtered post-hoc only, so the source never saw
        // `req.start` and the overlay-start check never ran — causing a
        // removed row to be suppressed during a re-entrant cascade fetch even
        // though TS returns it.
        let bound_basis = if exclusive { Basis::After } else { Basis::At };
        let bound_start = crate::ivm::operator::Start {
            row: start_row.clone(),
            basis: bound_basis,
        };
        let effective_start: Option<crate::ivm::operator::Start> = match &req.start {
            None => {
                if req.reverse {
                    None
                } else {
                    Some(bound_start.clone())
                }
            }
            Some(req_start) => {
                let cmp = compare(&start_row, &req_start.row);
                if !req.reverse {
                    if cmp == CmpOrdering::Greater {
                        Some(bound_start.clone())
                    } else if cmp == CmpOrdering::Equal {
                        if exclusive || req_start.basis == Basis::After {
                            Some(crate::ivm::operator::Start {
                                row: start_row.clone(),
                                basis: Basis::After,
                            })
                        } else {
                            Some(bound_start.clone())
                        }
                    } else {
                        Some(req_start.clone())
                    }
                } else {
                    // reverse: 'empty' → return nothing
                    if cmp == CmpOrdering::Greater {
                        return Box::new(std::iter::empty());
                    }
                    if cmp == CmpOrdering::Equal {
                        if !exclusive && req_start.basis == Basis::At {
                            Some(bound_start.clone())
                        } else {
                            return Box::new(std::iter::empty());
                        }
                    } else {
                        Some(req_start.clone())
                    }
                }
            }
        };

        let mut upstream_req = req.clone();
        upstream_req.start = effective_start;
        let stream = input.fetch(&upstream_req);

        // Post-hoc filter by the Skip's own bound — redundant when the upstream
        // honors `req.start` (the source does), but kept for operators that do
        // not, and to mirror TS's reverse-path `#shouldBePresent` re-check.
        Box::new(
            crate::ivm::stream::skip_yields(stream)
                .filter(move |n| {
                    let cmp = compare(&n.row, &start_row);
                    if exclusive {
                        cmp == CmpOrdering::Greater
                    } else {
                        cmp != CmpOrdering::Less
                    }
                })
                .map(crate::ivm::stream::StreamItem::Data),
        )
    }
}

impl Output for Skip {
    fn push(&mut self, _change: Change, _pusher: &dyn InputBase) {}
}

struct SkipOutput {
    skip: Shared<Skip>,
}

impl Output for SkipOutput {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        let skip = self.skip.borrow();
        let output = skip.output.borrow().clone();
        let Some(output) = output else { return };

        match change.change_type() {
            ChangeType::Edit => {
                let (node, old_node) = match &change {
                    Change::Edit { node, old_node } => (node.clone(), old_node.clone()),
                    _ => unreachable!(),
                };
                let old_was_present = skip.should_be_present(&old_node.row);
                let new_is_present = skip.should_be_present(&node.row);
                if old_was_present && new_is_present {
                    output.borrow_mut().push(change, pusher);
                } else if old_was_present && !new_is_present {
                    output
                        .borrow_mut()
                        .push(crate::ivm::change::make_remove_change(old_node), pusher);
                } else if !old_was_present && new_is_present {
                    output
                        .borrow_mut()
                        .push(crate::ivm::change::make_add_change(node), pusher);
                }
            }
            ChangeType::Add | ChangeType::Remove | ChangeType::Child => {
                if skip.should_be_present(&change.node().row) {
                    output.borrow_mut().push(change, pusher);
                }
            }
        }
    }
}
