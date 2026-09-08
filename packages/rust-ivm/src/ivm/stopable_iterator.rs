//! StoppableIterator — port of `zql/src/ivm/stopable-iterator.ts`.
//!
//! An iterator that can be stopped. Useful if the data backing the iterator
//! changes and you don't want clients to be able to keep iterating.

use std::cell::Cell;

/// An iterator wrapper that can be stopped mid-iteration.
/// Once stopped, further `next()` calls panic.
pub struct StoppableIterator<I: Iterator> {
    iter: I,
    stopped: Cell<bool>,
}

impl<I: Iterator> StoppableIterator<I> {
    pub fn new(iter: I) -> Self {
        StoppableIterator {
            iter,
            stopped: Cell::new(false),
        }
    }

    pub fn stop(&self) {
        self.stopped.set(true);
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.get()
    }
}

impl<I: Iterator> Iterator for StoppableIterator<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped.get() {
            panic!("Iterator has been stopped");
        }
        self.iter.next()
    }
}
