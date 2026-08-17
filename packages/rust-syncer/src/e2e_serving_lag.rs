//! End-to-end serving-lag tracker — port of `view-syncer/e2e-serving-lag.ts`
//! (zero/v1.9.0 #6157/#6312).
//!
//! Pairs the upstream commit timestamps carried by `version-ready`
//! notifications with the moment the corresponding change is poked to clients,
//! yielding the end-to-end serving lag. This measures COMPLETION, not backlog:
//! an observation is produced only when work actually reaches clients, so the
//! resulting histogram is a latency distribution rather than a periodic snapshot
//! of how far behind things are. A ViewSyncer that is stuck contributes nothing
//! until it recovers, instead of re-reporting its age on every sample tick.

/// The upstream commit currently awaiting delivery to clients.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingUpstreamCommit {
    pub watermark: String,
    pub commit_time_ms: f64,
}

/// A recorded serving-lag observation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Observation {
    /// End-to-end lag in milliseconds, clamped to be non-negative.
    pub lag_ms: f64,
    /// Whether the clamp was applied, i.e. the raw measurement was negative.
    pub clamped: bool,
}

#[derive(Debug, Default)]
pub struct E2EServingLagTracker {
    pending: Option<PendingUpstreamCommit>,
}

impl E2EServingLagTracker {
    pub fn new() -> Self {
        Self { pending: None }
    }

    pub fn pending(&self) -> Option<&PendingUpstreamCommit> {
        self.pending.as_ref()
    }

    /// Records the upstream commit behind a `version-ready` notification.
    ///
    /// Notifications coalesce when the ViewSyncer is busy, so one state may
    /// stand in for several commits. The *oldest* commit time is kept, since it
    /// bounds the lag of everything the notification subsumed, while the
    /// watermark advances to the newest — that is the one that must be served
    /// for all of the subsumed commits to have been delivered. Both fields are
    /// required; a notification missing either is ignored.
    pub fn on_version_ready(
        &mut self,
        watermark: Option<&str>,
        upstream_commit_time_ms: Option<f64>,
    ) {
        let (watermark, upstream_commit_time_ms) = match (watermark, upstream_commit_time_ms) {
            (Some(w), Some(t)) => (w, t),
            _ => return,
        };
        let commit_time_ms = match &self.pending {
            None => upstream_commit_time_ms,
            Some(pending) => pending.commit_time_ms.min(upstream_commit_time_ms),
        };
        self.pending = Some(PendingUpstreamCommit {
            watermark: watermark.to_string(),
            commit_time_ms,
        });
    }

    /// Called once a version has been poked to clients. Returns the observation
    /// to record, or `None` if the served version does not yet cover an
    /// outstanding upstream commit.
    pub fn on_version_served(&mut self, served_version: &str, now_ms: f64) -> Option<Observation> {
        let pending = self.pending.as_ref()?;
        // LexiVersion strings compare lexicographically == numerically, matching
        // the TS `servedVersion < pending.watermark`.
        if served_version < pending.watermark.as_str() {
            return None;
        }
        let commit_time_ms = pending.commit_time_ms;
        self.pending = None;

        let lag_ms = now_ms - commit_time_ms;
        if lag_ms >= 0.0 {
            Some(Observation {
                lag_ms,
                clamped: false,
            })
        } else {
            // The commit time is on the upstream database's clock while `now_ms`
            // is local, so a negative duration means upstream's clock is running
            // ahead of ours by more than the entire pipeline latency. Clamp,
            // because a negative value would corrupt the histogram's sum — but
            // report the clamp, since it is proof of gross clock skew, and skew
            // in this direction biases the metric *low* (reads as healthy).
            Some(Observation {
                lag_ms: 0.0,
                clamped: true,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignores_notifications_missing_fields() {
        let mut t = E2EServingLagTracker::new();
        t.on_version_ready(None, Some(1.0));
        t.on_version_ready(Some("05"), None);
        assert!(t.pending().is_none());
    }

    #[test]
    fn keeps_oldest_commit_time_and_newest_watermark() {
        let mut t = E2EServingLagTracker::new();
        t.on_version_ready(Some("05"), Some(100.0));
        t.on_version_ready(Some("09"), Some(80.0));
        t.on_version_ready(Some("0a"), Some(200.0));
        let p = t.pending().unwrap();
        assert_eq!(p.watermark, "0a"); // newest watermark
        assert_eq!(p.commit_time_ms, 80.0); // oldest commit time
    }

    #[test]
    fn no_observation_until_watermark_is_served() {
        let mut t = E2EServingLagTracker::new();
        t.on_version_ready(Some("0a"), Some(100.0));
        assert!(t.on_version_served("09", 500.0).is_none());
        assert!(t.pending().is_some());
        let obs = t.on_version_served("0a", 500.0).unwrap();
        assert_eq!(obs.lag_ms, 400.0);
        assert!(!obs.clamped);
        assert!(t.pending().is_none());
    }

    #[test]
    fn serving_past_the_watermark_also_records() {
        let mut t = E2EServingLagTracker::new();
        t.on_version_ready(Some("0a"), Some(100.0));
        let obs = t.on_version_served("0f", 250.0).unwrap();
        assert_eq!(obs.lag_ms, 150.0);
    }

    #[test]
    fn negative_lag_is_clamped_and_flagged() {
        let mut t = E2EServingLagTracker::new();
        t.on_version_ready(Some("0a"), Some(1000.0));
        let obs = t.on_version_served("0a", 900.0).unwrap();
        assert_eq!(obs.lag_ms, 0.0);
        assert!(obs.clamped);
    }

    #[test]
    fn no_pending_returns_none() {
        let mut t = E2EServingLagTracker::new();
        assert!(t.on_version_served("0a", 100.0).is_none());
    }
}
