//! Snitch operator — port of `zql/src/ivm/snitch.ts`.
//!
//! An Operator that records all messages it receives. Useful for debugging.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use crate::ivm::change::{Change, ChangeType};
use crate::ivm::data::{Node, Row};
use crate::ivm::operator::{
    FetchRequest, Input, InputBase, Output, OutputHandle, Shared,
};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::NodeStream;

/// Log types for Snitch filtering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogType {
    Fetch,
    Push,
    FetchCount,
}

/// A record of a change for logging.
#[derive(Clone, Debug)]
pub enum ChangeRecord {
    Add { row: Row },
    Remove { row: Row },
    Edit { row: Row, old_row: Row },
    Child { row: Row, child: Box<ChangeRecord> },
}

/// A snitch log message.
#[derive(Clone, Debug)]
pub enum SnitchMessage {
    Fetch { name: String, req: FetchRequest },
    FetchCount { name: String, req: FetchRequest, count: usize },
    Push { name: String, change: ChangeRecord },
}

/// Snitch — records all messages for debugging.
pub struct Snitch {
    input: Shared<dyn Input>,
    name: String,
    log_types: Vec<LogType>,
    pub log: Rc<RefCell<Vec<SnitchMessage>>>,
    output: Rc<RefCell<Option<OutputHandle>>>,
    schema: SourceSchema,
}

impl Snitch {
    pub fn new(
        input: Shared<dyn Input>,
        name: String,
        log: Vec<SnitchMessage>,
        log_types: Vec<LogType>,
    ) -> Shared<Snitch> {
        let schema = input.borrow().get_schema();

        let snitch = Rc::new(RefCell::new(Snitch {
            input: input.clone(),
            name,
            log_types,
            log: Rc::new(RefCell::new(log)),
            output: Rc::new(RefCell::new(None)),
            schema,
        }));

        let snitch_clone = snitch.clone();
        input.borrow().set_output(Rc::new(RefCell::new(SnitchOutput {
            snitch: snitch_clone,
        })));

        snitch
    }

    fn log_message(&self, msg: SnitchMessage) {
        let should_log = match &msg {
            SnitchMessage::Fetch { .. } => self.log_types.contains(&LogType::Fetch),
            SnitchMessage::FetchCount { .. } => self.log_types.contains(&LogType::FetchCount),
            SnitchMessage::Push { .. } => self.log_types.contains(&LogType::Push),
        };
        if should_log {
            self.log.borrow_mut().push(msg);
        }
    }
}

impl InputBase for Snitch {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        self.input.borrow_mut().destroy();
    }
}

impl Input for Snitch {
    fn set_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        // Log fetch
        {
            let mut s = self.clone_ref();
            s.log_message(SnitchMessage::Fetch {
                name: self.name.clone(),
                req: req.clone(),
            });
        }

        let input = self.input.borrow();
        let stream = input.fetch(req);
        // Snitch needs the count for logging, so we must consume the stream.
        // Use a tee-like approach: collect, log, re-emit.
        let nodes: Vec<Node> = crate::ivm::stream::skip_yields(stream).collect();

        // Log fetch count
        {
            let mut s = self.clone_ref();
            s.log_message(SnitchMessage::FetchCount {
                name: self.name.clone(),
                req: req.clone(),
                count: nodes.len(),
            });
        }

        crate::ivm::stream::from_vec(nodes)
    }
}

impl Output for Snitch {
    fn push(&mut self, _change: Change, _pusher: &dyn InputBase) {
        // Pushes arrive via SnitchOutput adapter
    }
}

impl Snitch {
    fn clone_ref(&self) -> Snitch {
        Snitch {
            input: self.input.clone(),
            name: self.name.clone(),
            log_types: self.log_types.clone(),
            log: self.log.clone(), // share the log via Rc
            output: self.output.clone(),
            schema: self.schema.clone(),
        }
    }
}

struct SnitchOutput {
    snitch: Shared<Snitch>,
}

impl Output for SnitchOutput {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        let snitch = self.snitch.borrow();
        if snitch.log_types.contains(&LogType::Push) {
            let record = to_change_record(&change);
            snitch.log.borrow_mut().push(SnitchMessage::Push {
                name: snitch.name.clone(),
                change: record,
            });
        }

        let output = snitch.output.borrow().clone();
        drop(snitch);
        if let Some(output) = output {
            output.borrow_mut().push(change, pusher);
        }
    }
}

/// Convert a Change to a ChangeRecord for logging.
pub fn to_change_record(change: &Change) -> ChangeRecord {
    match change {
        Change::Add(node) => ChangeRecord::Add { row: node.row.clone() },
        Change::Remove(node) => ChangeRecord::Remove { row: node.row.clone() },
        Change::Edit { node, old_node } => ChangeRecord::Edit {
            row: node.row.clone(),
            old_row: old_node.row.clone(),
        },
        Change::Child { node, child } => ChangeRecord::Child {
            row: node.row.clone(),
            child: Box::new(to_change_record(&child.change)),
        },
    }
}
