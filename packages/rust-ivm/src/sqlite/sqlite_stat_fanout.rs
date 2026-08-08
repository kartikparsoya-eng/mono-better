//! SQLite stat fanout — faithful port of `zqlite/src/sqlite-stat-fanout.ts`.
//!
//! Computes join fanout factors (average child rows per distinct join-key
//! value) from SQLite statistics tables, exactly as TS `SQLiteStatFanout`:
//!
//! 1. `sqlite_stat4` histogram (most accurate): median fanout of the
//!    NON-NULL samples at the constraint's index depth. NULL samples are
//!    excluded because NULL never matches a join.
//! 2. `sqlite_stat1` average (includes NULLs, may overestimate): the stat
//!    value at the constraint's index depth.
//! 3. Default constant (3) when no statistics are available.
//!
//! Index resolution mirrors TS `#findIndexForColumns`: an index matches when
//! ALL constraint columns appear (order-independent, case-insensitive) in its
//! first N positions; `depth = columns.len()`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

/// Result of fanout calculation from SQLite statistics.
/// Port of TS `FanoutResult` (sqlite-stat-fanout.ts:6).
#[derive(Clone, Debug)]
pub struct FanoutResult {
    pub fanout: f64,
    pub confidence: Confidence,
    pub source: FanoutSource,
}

/// Confidence level of the fanout estimate (TS `'high' | 'med' | 'none'`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Confidence {
    High,
    Med,
    None,
}

/// Source of the fanout calculation (TS `'stat4' | 'stat1' | 'default'`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FanoutSource {
    Stat4,
    Stat1,
    Default,
}

/// Default fanout when statistics are unavailable — TS ctor default `3`
/// ("moderate, recommended, safe middle ground").
pub const DEFAULT_FANOUT: f64 = 3.0;

/// Computes join fanout factors from SQLite statistics tables.
/// Port of TS `SQLiteStatFanout` (sqlite-stat-fanout.ts:89).
pub struct SQLiteStatFanout {
    conn: Rc<RefCell<rusqlite::Connection>>,
    default_fanout: f64,
    /// Cache of fanout results by `"table:col1,col2"` (columns sorted) —
    /// TS `#cache`.
    cache: RefCell<HashMap<String, FanoutResult>>,
}

/// One decoded stat4 sample: fanout at the requested depth + NULL-ness of the
/// first sampled column.
struct DecodedSample {
    fanout: f64,
    is_null: bool,
}

impl SQLiteStatFanout {
    pub fn new(conn: Rc<RefCell<rusqlite::Connection>>) -> Self {
        Self::with_default_fanout(conn, DEFAULT_FANOUT)
    }

    pub fn with_default_fanout(
        conn: Rc<RefCell<rusqlite::Connection>>,
        default_fanout: f64,
    ) -> Self {
        SQLiteStatFanout {
            conn,
            default_fanout,
            cache: RefCell::new(HashMap::new()),
        }
    }

    /// Gets the fanout factor for join column(s).
    /// Port of TS `getFanout` (sqlite-stat-fanout.ts:172).
    pub fn get_fanout(&self, table_name: &str, columns: &[String]) -> FanoutResult {
        let mut sorted = columns.to_vec();
        sorted.sort();
        let cache_key = format!("{}:{}", table_name, sorted.join(","));
        if let Some(cached) = self.cache.borrow().get(&cache_key) {
            return cached.clone();
        }

        // Strategy 1: stat4 (most accurate; excludes NULLs).
        if let Some(result) = self.fanout_from_stat4(table_name, columns) {
            self.cache.borrow_mut().insert(cache_key, result.clone());
            return result;
        }

        // Strategy 2: stat1 (includes NULLs).
        if let Some(result) = self.fanout_from_stat1(table_name, columns) {
            self.cache.borrow_mut().insert(cache_key, result.clone());
            return result;
        }

        // Strategy 3: default.
        let result = FanoutResult {
            fanout: self.default_fanout,
            confidence: Confidence::None,
            source: FanoutSource::Default,
        };
        self.cache.borrow_mut().insert(cache_key, result.clone());
        result
    }

    /// Clears the fanout cache (call after ANALYZE) — TS `clearCache`.
    pub fn clear_cache(&self) {
        self.cache.borrow_mut().clear();
    }

    /// Port of TS `#getFanoutFromStat4` (sqlite-stat-fanout.ts:225).
    fn fanout_from_stat4(&self, table_name: &str, columns: &[String]) -> Option<FanoutResult> {
        let index_info = self.find_index_for_columns(table_name, columns)?;

        let conn = self.conn.borrow();
        let mut stmt = conn
            .prepare_cached(
                "SELECT neq, nlt, ndlt, sample
                 FROM sqlite_stat4
                 WHERE tbl = ? AND idx = ?
                 ORDER BY nlt",
            )
            .ok()?;

        // depth is 1-based; the neq list index is 0-based (TS `neqIndex`).
        let neq_index = index_info.depth - 1;
        let samples: Vec<DecodedSample> = stmt
            .query_map(
                rusqlite::params![table_name, index_info.index_name],
                |row| {
                    let neq: String = row.get(0)?;
                    // Column 3 is the binary-encoded sample. It is usually a
                    // BLOB but tolerate any type by falling back to empty
                    // (empty ⇒ treated as NULL, like TS's Buffer handling).
                    let sample: Vec<u8> = row.get::<_, Vec<u8>>(3).unwrap_or_default();
                    let neq_parts: Vec<&str> = neq.split(' ').collect();
                    let part = neq_parts
                        .get(neq_index)
                        .or_else(|| neq_parts.first())
                        .copied()
                        .unwrap_or("");
                    Ok(DecodedSample {
                        fanout: parse_int_js(part),
                        is_null: decode_sample_is_null(&sample),
                    })
                },
            )
            .ok()?
            .filter_map(|r| r.ok())
            .collect();

        if samples.is_empty() {
            return None;
        }

        let non_null: Vec<f64> = samples
            .iter()
            .filter(|s| !s.is_null)
            .map(|s| s.fanout)
            .collect();

        if non_null.is_empty() {
            // All samples NULL — fanout 0 (NULLs don't match in joins).
            return Some(FanoutResult {
                fanout: 0.0,
                confidence: Confidence::High,
                source: FanoutSource::Stat4,
            });
        }

        // Median of non-NULL fanouts (TS: even → floor of the mean of the two
        // middle values; odd → middle value).
        let mut fanouts = non_null;
        fanouts.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = fanouts.len();
        let median = if n % 2 == 0 {
            ((fanouts[n / 2 - 1] + fanouts[n / 2]) / 2.0).floor()
        } else {
            fanouts[n / 2]
        };

        Some(FanoutResult {
            fanout: median,
            confidence: Confidence::High,
            source: FanoutSource::Stat4,
        })
    }

    /// Port of TS `#getFanoutFromStat1` (sqlite-stat-fanout.ts:300).
    fn fanout_from_stat1(&self, table_name: &str, columns: &[String]) -> Option<FanoutResult> {
        let index_info = self.find_index_for_columns(table_name, columns)?;

        let conn = self.conn.borrow();
        let mut stmt = conn
            .prepare_cached("SELECT stat FROM sqlite_stat1 WHERE tbl = ? AND idx = ?")
            .ok()?;
        let stat: String = stmt
            .query_row(rusqlite::params![table_name, index_info.index_name], |r| {
                r.get(0)
            })
            .ok()?;

        let parts: Vec<&str> = stat.split(' ').collect();
        // TS: `parts.length < depth + 1 → undefined`.
        if parts.len() < index_info.depth + 1 {
            return None;
        }
        let fanout = parse_int_js(parts[index_info.depth]);
        if fanout.is_nan() {
            return None;
        }

        Some(FanoutResult {
            fanout,
            confidence: Confidence::Med,
            source: FanoutSource::Stat1,
        })
    }

    /// Port of TS `#findIndexForColumns` (sqlite-stat-fanout.ts:363).
    /// Finds an index whose FIRST `columns.len()` positions contain ALL the
    /// requested columns (order-independent, case-insensitive).
    fn find_index_for_columns(&self, table_name: &str, columns: &[String]) -> Option<IndexInfo> {
        let conn = self.conn.borrow();
        let mut stmt = conn
            .prepare_cached(
                "SELECT il.name as index_name, ii.seqno, ii.name as column_name
                 FROM pragma_index_list(?) il
                 JOIN pragma_index_info(il.name) ii
                 ORDER BY il.seq, ii.seqno",
            )
            .ok()?;

        let rows: Vec<(String, String)> = stmt
            .query_map(rusqlite::params![table_name], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(2)?))
            })
            .ok()?
            .filter_map(|r| r.ok())
            .collect();

        // Group columns per index, preserving first-seen index order (TS Map
        // preserves insertion order).
        let mut index_order: Vec<String> = Vec::new();
        let mut index_map: HashMap<String, Vec<String>> = HashMap::new();
        for (index_name, column_name) in rows {
            let entry = index_map.entry(index_name.clone()).or_insert_with(|| {
                index_order.push(index_name.clone());
                Vec::new()
            });
            entry.push(column_name);
        }

        for index_name in &index_order {
            let index_columns = &index_map[index_name];
            if is_prefix_match(columns, index_columns) {
                return Some(IndexInfo {
                    index_name: index_name.clone(),
                    depth: columns.len(),
                });
            }
        }
        None
    }
}

struct IndexInfo {
    index_name: String,
    depth: usize,
}

/// Port of TS `#isPrefixMatch` (sqlite-stat-fanout.ts:415): all query columns
/// exist in the first N index positions, order-independent, case-insensitive.
fn is_prefix_match(query_columns: &[String], index_columns: &[String]) -> bool {
    if query_columns.len() > index_columns.len() {
        return false;
    }
    let prefix: std::collections::HashSet<String> = index_columns[..query_columns.len()]
        .iter()
        .map(|c| c.to_lowercase())
        .collect();
    query_columns
        .iter()
        .all(|c| prefix.contains(&c.to_lowercase()))
}

/// Port of TS `#decodeSampleIsNull` (sqlite-stat-fanout.ts:450): decode a
/// sqlite_stat4 record header and check whether the FIRST column's serial
/// type is 0 (NULL).
fn decode_sample_is_null(sample: &[u8]) -> bool {
    if sample.is_empty() {
        return true;
    }
    // Header size varint — simplified single-byte read, same as TS.
    let header_size = sample[0] as usize;
    if header_size == 0 || header_size >= sample.len() {
        return true;
    }
    // First serial type at position 1; 0 = NULL.
    sample[1] == 0
}

/// JS `parseInt(s, 10)` semantics: parse the leading optional-signed digit
/// run; no digits → NaN.
fn parse_int_js(s: &str) -> f64 {
    let t = s.trim_start();
    let (sign, rest) = match t.as_bytes().first() {
        Some(b'-') => (-1.0, &t[1..]),
        Some(b'+') => (1.0, &t[1..]),
        _ => (1.0, t),
    };
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return f64::NAN;
    }
    digits.parse::<f64>().map(|v| sign * v).unwrap_or(f64::NAN)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_int_js_semantics() {
        assert_eq!(parse_int_js("42"), 42.0);
        assert_eq!(parse_int_js("-7"), -7.0);
        assert_eq!(parse_int_js("12abc"), 12.0);
        assert!(parse_int_js("abc").is_nan());
        assert!(parse_int_js("").is_nan());
    }

    #[test]
    fn prefix_match_semantics() {
        let cols = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        // (customerId, storeId, date) at depth 2 — both orders match.
        assert!(is_prefix_match(
            &cols(&["customerId", "storeId"]),
            &cols(&["customerId", "storeId", "date"])
        ));
        assert!(is_prefix_match(
            &cols(&["customerId", "storeId"]),
            &cols(&["storeId", "customerId", "date"])
        ));
        // Gaps not allowed.
        assert!(!is_prefix_match(
            &cols(&["customerId", "date"]),
            &cols(&["customerId", "storeId", "date"])
        ));
        // Case-insensitive.
        assert!(is_prefix_match(&cols(&["USERID"]), &cols(&["userId", "x"])));
        // Longer than index — no match.
        assert!(!is_prefix_match(&cols(&["a", "b"]), &cols(&["a"])));
    }

    #[test]
    fn decode_sample_null_detection() {
        assert!(decode_sample_is_null(&[]));
        assert!(decode_sample_is_null(&[0]));
        // header size 2, first serial type 0 → NULL
        assert!(decode_sample_is_null(&[2, 0]));
        // header size 2, first serial type 1 (8-bit int) → not NULL
        assert!(!decode_sample_is_null(&[2, 1, 5]));
    }
}
