//! Memory storage — port of `zql/src/ivm/memory-storage.ts`.
//!
//! In-memory key-value storage for Take/Cap operator state.
//! Uses a `BTreeMap` so `scan` returns entries in sorted key order,
//! matching the TS `BTreeSet`-backed implementation.

use std::collections::BTreeMap;

use crate::ivm::data::Value;
use crate::ivm::operator::Storage;

/// In-memory storage — stores values by string key.
pub struct MemoryStorage {
    data: BTreeMap<String, Value>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        MemoryStorage {
            data: BTreeMap::new(),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    fn get(&self, key: &str) -> Option<Value> {
        self.data.get(key).cloned()
    }

    fn set(&mut self, key: String, value: Value) {
        self.data.insert(key, value);
    }

    fn del(&mut self, key: &str) {
        self.data.remove(key);
    }

    fn scan(&self, prefix: Option<&str>) -> Vec<(String, Value)> {
        match prefix {
            None => self
                .data
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            Some(p) => self
                .data
                .range(p.to_string()..)
                .take_while(|(k, _)| k.starts_with(p))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }
}
