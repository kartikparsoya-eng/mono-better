//! InspectorDelegate — port of `zero-cache/src/server/inspector-delegate.ts`
//! (the metrics + query-AST portion consumed by the inspector `metrics` and
//! `queries` ops). The authentication half of the TS class (`isAuthenticated` /
//! `setAuthenticated` / `clearAuthenticated`, backed by a module-global
//! `authenticatedClientGroupIDs` set) and `transformCustomQuery` are already
//! sited on `ViewSyncerService` (`inspector_authenticated` flag) and the
//! `analyze-query` path (`transform_custom_queries`) respectively, so this
//! module carries only the server-metrics store + the queryID→AST map.
//!
//! Rust-only scope note (AGENTS rule 5): TS constructs ONE `InspectorDelegate`
//! per Syncer worker (server/syncer.ts:207) and shares it across every
//! `ViewSyncerService` in that worker, so `getMetricsJSON()` returns a
//! worker-global aggregate. Rust runs each client group on its own `!Send` CG
//! thread with no shared mutable worker object, so this delegate is per-CG: the
//! `metrics` op returns THIS client group's aggregate (the caller's own
//! queries), not a cross-CG one. This is a direct consequence of the registered
//! CG-thread invention (I-2); the per-query rows the client actually inspects
//! (`queries` op) are unaffected, since those are keyed by the caller's own
//! queryIDs. See parity/INVENTIONS.md.

use std::collections::HashMap;

use rust_ivm::query::metrics_delegate::Metric;
use serde_json::{Value, json};

use crate::tdigest::TDigest;

/// Port of `ServerMetrics` (inspector-delegate.ts:24): the two global aggregate
/// histograms.
struct ServerMetrics {
    /// `query-materialization-server`.
    materialization: TDigest,
    /// `query-update-server`.
    update: TDigest,
}

impl ServerMetrics {
    /// Port of `newMetrics` (inspector-delegate.ts:166).
    fn new() -> Self {
        Self {
            materialization: TDigest::default(),
            update: TDigest::default(),
        }
    }
}

/// Port of `InspectorDelegate` (inspector-delegate.ts:37) — the metrics + AST
/// store. `implements MetricsDelegate` in TS; rust records through direct
/// `&mut` methods (the materialization metric is fed from the engine's existing
/// per-query hydration timing — see `query-update-server` note below).
pub struct InspectorDelegate {
    global_metrics: ServerMetrics,
    per_query_hydrate_ms: HashMap<String, f64>,
    per_query_update_metrics: HashMap<String, TDigest>,
    query_id_to_ast: HashMap<String, Value>,
}

impl Default for InspectorDelegate {
    fn default() -> Self {
        Self::new()
    }
}

impl InspectorDelegate {
    pub fn new() -> Self {
        Self {
            global_metrics: ServerMetrics::new(),
            per_query_hydrate_ms: HashMap::new(),
            per_query_update_metrics: HashMap::new(),
            query_id_to_ast: HashMap::new(),
        }
    }

    /// Port of `addMetric` (inspector-delegate.ts:48). Only server metrics reach
    /// here (TS `assert(isServerMetric(metric))`); `query-materialization-server`
    /// overwrites the query's single hydration ms, `query-update-server` appends
    /// to the query's update digest. Both feed the matching global aggregate.
    pub fn add_metric(&mut self, metric: Metric, value: f64, query_id: &str) {
        assert!(
            metric.is_server_metric(),
            "Invalid server metric: {}",
            metric.name()
        );
        if metric == Metric::QueryMaterializationServer {
            self.per_query_hydrate_ms
                .insert(query_id.to_string(), value);
            self.global_metrics.materialization.add(value, 1.0);
        } else {
            // query-update-server
            self.per_query_update_metrics
                .entry(query_id.to_string())
                .or_default()
                .add(value, 1.0);
            self.global_metrics.update.add(value, 1.0);
        }
    }

    /// Port of `getMetricsJSONForQuery` (inspector-delegate.ts:68): the per-query
    /// `QueryServerMetrics` wire shape, or `None` when the query has neither a
    /// hydration time nor any updates. `query-hydration-server-ms` is a plain
    /// number and is OMITTED when undefined (TS `v.number().optional()` +
    /// `JSON.stringify` drops an `undefined` value); `query-update-server` is the
    /// query's update digest, or an empty digest when it has had no updates
    /// (TS `updateMetrics ?? new TDigest()`).
    pub fn get_metrics_json_for_query(&mut self, query_id: &str) -> Option<Value> {
        let hydrate_ms = self.per_query_hydrate_ms.get(query_id).copied();
        let has_update = self.per_query_update_metrics.contains_key(query_id);
        if hydrate_ms.is_none() && !has_update {
            return None;
        }
        let update_json = match self.per_query_update_metrics.get_mut(query_id) {
            Some(d) => d.to_json_value(),
            None => TDigest::default().to_json_value(),
        };
        let mut obj = serde_json::Map::new();
        if let Some(ms) = hydrate_ms {
            obj.insert("query-hydration-server-ms".to_string(), number_to_value(ms));
        }
        obj.insert("query-update-server".to_string(), update_json);
        Some(Value::Object(obj))
    }

    /// Port of `getMetricsJSON` (inspector-delegate.ts:80): the two global
    /// aggregate digests (`mapValues(globalMetrics, v => v.toJSON())`).
    pub fn get_metrics_json(&mut self) -> Value {
        json!({
            "query-materialization-server": self.global_metrics.materialization.to_json_value(),
            "query-update-server": self.global_metrics.update.to_json_value(),
        })
    }

    /// Port of `getASTForQuery` (inspector-delegate.ts:84).
    pub fn get_ast_for_query(&self, query_id: &str) -> Option<&Value> {
        self.query_id_to_ast.get(query_id)
    }

    /// Port of `removeQuery` (inspector-delegate.ts:88).
    pub fn remove_query(&mut self, query_id: &str) {
        self.per_query_hydrate_ms.remove(query_id);
        self.per_query_update_metrics.remove(query_id);
        self.query_id_to_ast.remove(query_id);
    }

    /// Port of `addQuery` (inspector-delegate.ts:94).
    pub fn add_query(&mut self, query_id: &str, ast: Value) {
        self.query_id_to_ast.insert(query_id.to_string(), ast);
    }
}

/// Emit an integer-valued `f64` (a whole-millisecond hydration time) as a JSON
/// integer, matching JS `JSON.stringify` of a `number`.
fn number_to_value(n: f64) -> Value {
    if n.is_finite() && n.fract() == 0.0 && n.abs() < 9.007_199_254_740_992e15 {
        Value::Number((n as i64).into())
    } else {
        serde_json::Number::from_f64(n).map_or(Value::Null, Value::Number)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_metrics_json_is_two_digests_and_reflects_adds() {
        let mut d = InspectorDelegate::new();
        // Empty → both digests are `[1000]`.
        assert_eq!(
            d.get_metrics_json(),
            json!({"query-materialization-server": [1000], "query-update-server": [1000]})
        );
        // A materialization + two updates land in the global aggregates.
        d.add_metric(Metric::QueryMaterializationServer, 12.0, "q1");
        d.add_metric(Metric::QueryUpdateServer, 1.0, "q1");
        d.add_metric(Metric::QueryUpdateServer, 3.0, "q1");
        let g = d.get_metrics_json();
        assert_eq!(
            g["query-materialization-server"],
            json!([1000, 12, 1]),
            "one materialization point"
        );
        // Two update points 1 and 3 (sorted, unmerged under compression 1000).
        assert_eq!(g["query-update-server"], json!([1000, 1, 1, 3, 1]));
    }

    #[test]
    fn per_query_metrics_none_until_recorded_then_shape_matches_ts() {
        let mut d = InspectorDelegate::new();
        assert_eq!(d.get_metrics_json_for_query("q1"), None);

        // Only an update → hydration key OMITTED, update digest present.
        d.add_metric(Metric::QueryUpdateServer, 2.5, "q1");
        assert_eq!(
            d.get_metrics_json_for_query("q1"),
            Some(json!({"query-update-server": [1000, 2.5, 1]})),
            "no hydration key when hydration ms is undefined"
        );

        // Add a materialization → hydration key appears as a plain number.
        d.add_metric(Metric::QueryMaterializationServer, 7.0, "q1");
        assert_eq!(
            d.get_metrics_json_for_query("q1"),
            Some(json!({
                "query-hydration-server-ms": 7,
                "query-update-server": [1000, 2.5, 1],
            }))
        );

        // A query with ONLY a hydration time → empty update digest.
        d.add_metric(Metric::QueryMaterializationServer, 4.0, "q2");
        assert_eq!(
            d.get_metrics_json_for_query("q2"),
            Some(json!({"query-hydration-server-ms": 4, "query-update-server": [1000]}))
        );
    }

    #[test]
    fn add_and_remove_query_track_the_ast() {
        let mut d = InspectorDelegate::new();
        assert_eq!(d.get_ast_for_query("q1"), None);
        let ast = json!({"table": "issue"});
        d.add_query("q1", ast.clone());
        assert_eq!(d.get_ast_for_query("q1"), Some(&ast));
        d.remove_query("q1");
        assert_eq!(d.get_ast_for_query("q1"), None);
    }
}
