//! ZQLite options — port of `zqlite/src/options.ts`.
//!
//! Configuration for ZQLiteZero.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::sqlite::db::Database;

/// Configuration for ZQLiteZero.
/// Port of TS `ZQLiteZeroOptions<S>` (options.ts:7).
pub struct ZQLiteZeroOptions {
    pub db: Rc<RefCell<Database>>,
    pub table_names: Vec<String>,
    pub primary_keys: HashMap<String, Vec<String>>,
}
