//! Metrics delegate — port of `zql/src/query/metrics-delegate.ts`.
//!
//! Collects timing metrics for query materialization and updates.

use crate::builder::ast::Ast;

/// All metric names, client and server.
/// Port of TS `MetricMap` (metrics-delegate.ts:16).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Metric {
    QueryMaterializationClient,
    QueryMaterializationEndToEnd,
    QueryMaterializationServer,
    QueryUpdateClient,
    QueryUpdateServer,
}

impl Metric {
    pub fn name(&self) -> &'static str {
        match self {
            Metric::QueryMaterializationClient => "query-materialization-client",
            Metric::QueryMaterializationEndToEnd => "query-materialization-end-to-end",
            Metric::QueryMaterializationServer => "query-materialization-server",
            Metric::QueryUpdateClient => "query-update-client",
            Metric::QueryUpdateServer => "query-update-server",
        }
    }

    pub fn is_client_metric(&self) -> bool {
        matches!(
            self,
            Metric::QueryMaterializationClient | Metric::QueryMaterializationEndToEnd | Metric::QueryUpdateClient
        )
    }

    pub fn is_server_metric(&self) -> bool {
        matches!(
            self,
            Metric::QueryMaterializationServer | Metric::QueryUpdateServer
        )
    }
}

/// Delegate for collecting metrics.
/// Port of TS `MetricsDelegate` (metrics-delegate.ts:33).
pub trait MetricsDelegate {
    fn add_metric(&self, metric: Metric, value: f64, query_id: &str, ast: Option<&Ast>);
}

/// A no-op metrics delegate for when metrics are not needed.
pub struct NullMetricsDelegate;
impl MetricsDelegate for NullMetricsDelegate {
    fn add_metric(&self, _metric: Metric, _value: f64, _query_id: &str, _ast: Option<&Ast>) {}
}
