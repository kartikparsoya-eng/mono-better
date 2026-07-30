//! Stream types — port of `zql/src/ivm/stream.ts`.
//! TS `Stream<T> = Iterable<T>` → Rust `Iterator<Item = StreamItem<T>>`.
//!
//! The `'yield'` sentinel from TS is modeled as `StreamItem::Yield` — an
//! in-band cooperative-scheduling signal that lets the pipeline-driver pump
//! the event loop during long hydrations.

use std::rc::Rc;

use crate::ivm::data::Node;

/// A stream item — either a data Node or a cooperative yield signal.
/// Port of TS `Node | 'yield'` (data.ts:14, stream.ts).
#[derive(Clone, Debug)]
pub enum StreamItem<T> {
    Data(T),
    /// Cooperative pause: let the caller pump the event loop.
    /// Emitted by operators (Cap, Take, FlippedJoin, MemorySource, Exists)
    /// to yield control during long iterations.
    Yield,
}

/// A stream of nodes (with yield support).
pub type NodeStream = Box<dyn Iterator<Item = StreamItem<Node>>>;

/// A lazy relationship stream factory.
/// Wrapped in `Rc` so it can be cheaply cloned (mirrors JS closure sharing).
pub type RelStream = Rc<dyn Fn() -> NodeStream>;

pub fn node_stream<I: Iterator<Item = StreamItem<Node>> + 'static>(iter: I) -> NodeStream {
    Box::new(iter)
}

pub fn empty_stream() -> NodeStream {
    Box::new(std::iter::empty())
}

pub fn single_node(node: Node) -> NodeStream {
    Box::new(std::iter::once(StreamItem::Data(node)))
}

pub fn from_vec(nodes: Vec<Node>) -> NodeStream {
    Box::new(nodes.into_iter().map(StreamItem::Data))
}

pub fn rel_from_vec(nodes: Vec<Node>) -> RelStream {
    Rc::new(move || from_vec(nodes.clone()))
}

pub fn empty_rel() -> RelStream {
    Rc::new(|| empty_stream())
}

/// Filter out `StreamItem::Yield` values, returning only `StreamItem::Data`.
/// Port of TS `skipYields` (skip-yields.ts).
pub fn skip_yields(stream: NodeStream) -> Box<dyn Iterator<Item = Node>> {
    Box::new(stream.filter_map(|item| match item {
        StreamItem::Data(n) => Some(n),
        StreamItem::Yield => None,
    }))
}

/// Take up to `limit` data items from a stream, passing through Yield items.
/// Port of TS `take` (stream.ts:10).
pub fn take(stream: NodeStream, limit: usize) -> NodeStream {
    if limit == 0 {
        return empty_stream();
    }
    Box::new(TakeStream {
        inner: stream,
        count: 0,
        limit,
    })
}

struct TakeStream {
    inner: NodeStream,
    count: usize,
    limit: usize,
}

impl Iterator for TakeStream {
    type Item = StreamItem<Node>;

    fn next(&mut self) -> Option<StreamItem<Node>> {
        if self.count >= self.limit {
            return None;
        }
        match self.inner.next() {
            Some(StreamItem::Data(n)) => {
                self.count += 1;
                Some(StreamItem::Data(n))
            }
            Some(StreamItem::Yield) => Some(StreamItem::Yield),
            None => None,
        }
    }
}

/// Get the first data item from a stream, consuming and dropping the rest.
/// Port of TS `first` (stream.ts:21).
pub fn first(stream: NodeStream) -> Option<Node> {
    for item in stream {
        match item {
            StreamItem::Data(n) => return Some(n),
            StreamItem::Yield => continue,
        }
    }
    None
}

/// Count the number of data items in a stream (skipping yields).
pub fn count_data(stream: NodeStream) -> usize {
    skip_yields(stream).count()
}
