//! Phase 4 tests — ViewSyncer dispatch loop, TTL clock, expired queries.

use rust_syncer::view_syncer::*;
use rust_syncer::protocol::{ErrorBody, ErrorKind};
use std::sync::{Arc, Mutex};

// ─── Mock implementations ──────────────────────────────────────────────────

#[derive(Default)]
struct MockPipelineDriver {
    initialized: Mutex<bool>,
    replica_version: Mutex<Option<String>>,
    advance_without_diff_result: Mutex<String>,
    hydrate_result: Mutex<Option<Result<HydrateResult, ErrorBody>>>,
    advance_result: Mutex<Option<Result<AdvanceSyncResult, ErrorBody>>>,
}

impl PipelineDriver for MockPipelineDriver {
    fn initialized(&self) -> bool {
        *self.initialized.lock().unwrap()
    }

    fn init(&self, _client_schema: &serde_json::Value) {
        *self.initialized.lock().unwrap() = true;
    }

    fn reset(&self, _client_schema: &serde_json::Value) {
        *self.initialized.lock().unwrap() = false;
    }

    fn advance_without_diff(&self) -> String {
        self.advance_without_diff_result.lock().unwrap().clone()
    }

    fn replica_version(&self) -> Option<String> {
        self.replica_version.lock().unwrap().clone()
    }

    fn hydrate_and_sync(&self, _params: &HydrateParams) -> Result<HydrateResult, ErrorBody> {
        self.hydrate_result.lock().unwrap()
            .clone()
            .unwrap_or(Ok(HydrateResult::default()))
    }

    fn advance_and_sync(&self, _params: &AdvanceParams) -> Result<AdvanceSyncResult, ErrorBody> {
        self.advance_result.lock().unwrap()
            .clone()
            .unwrap_or(Ok(AdvanceSyncResult::default()))
    }

    fn destroy(&self) {
        *self.initialized.lock().unwrap() = false;
    }

    fn row_set_signature(&self, _query_id: &str) -> Option<String> {
        None
    }
}

#[derive(Default)]
struct MockCVRStore {
    load_result: Mutex<Option<CVRSnapshot>>,
    flushed: Mutex<bool>,
}

impl CVRStoreOps for MockCVRStore {
    fn load(&self, _last_connect_time: i64) -> Result<CVRSnapshot, ErrorBody> {
        self.load_result.lock().unwrap()
            .clone()
            .ok_or_else(|| ErrorBody::internal("no cvr loaded"))
    }

    fn update_ttl_clock(&self, _ttl_clock: i64, _now: i64) {}

    fn flushed(&self) -> bool {
        *self.flushed.lock().unwrap()
    }

    fn wait_flushed(&self) -> Result<(), ErrorBody> {
        Ok(())
    }
}

// ─── TTL Clock tests ───────────────────────────────────────────────────────

#[test]
fn test_ttl_clock_init() {
    let clock = TTLClock::new();
    clock.init(1000, 5000);
    assert_eq!(clock.value(), 1000);
}

#[test]
fn test_ttl_clock_get_advances_clock() {
    let clock = TTLClock::new();
    clock.init(1000, 5000);

    // now=6000, delta=1000, clock=2000
    let val = clock.get(6000);
    assert_eq!(val, 2000);

    // now=7000, delta=1000, clock=3000
    let val = clock.get(7000);
    assert_eq!(val, 3000);
}

#[test]
fn test_ttl_clock_get_updates_base() {
    let clock = TTLClock::new();
    clock.init(1000, 5000);

    clock.get(6000);
    clock.get(6500);

    // delta1=1000, delta2=500, total clock = 1000 + 1000 + 500 = 2500
    assert_eq!(clock.value(), 2500);
}

// ─── Expired query tests ───────────────────────────────────────────────────

#[test]
fn test_internal_queries_never_expire() {
    let query = CVRQuery {
        id: "internal-1".to_string(),
        internal: true,
        deactivated_at: Some(0),
        ttl: Some(100),
        ..Default::default()
    };
    assert!(!is_expired(999999999, &query));
}

#[test]
fn test_active_queries_not_expired() {
    let query = CVRQuery {
        id: "q1".to_string(),
        internal: false,
        deactivated_at: None,
        ..Default::default()
    };
    assert!(!is_expired(999999999, &query));
}

#[test]
fn test_expired_query_past_ttl() {
    let query = CVRQuery {
        id: "q1".to_string(),
        internal: false,
        deactivated_at: Some(1000),
        ttl: Some(500),
        ..Default::default()
    };
    assert!(is_expired(2000, &query)); // 1000 + 500 = 1500 <= 2000
    assert!(!is_expired(1400, &query)); // 1500 > 1400
}

#[test]
fn test_expired_query_clamps_ttl() {
    let query = CVRQuery {
        id: "q1".to_string(),
        internal: false,
        deactivated_at: Some(1000),
        ttl: Some(10_000_000), // > MAX_TTL_MS
        ..Default::default()
    };
    // clamped_ttl = 5_000_000, so 1000 + 5_000_000 = 5_001_000
    assert!(is_expired(5_001_001, &query));
    assert!(!is_expired(5_000_999, &query));
}

#[test]
fn test_has_expired_queries() {
    let mut cvr = CVRSnapshot {
        ttl_clock: 2000,
        ..Default::default()
    };
    cvr.queries.insert("q1".to_string(), CVRQuery {
        id: "q1".to_string(),
        internal: false,
        deactivated_at: Some(1000),
        ttl: Some(500),
        ..Default::default()
    });
    assert!(has_expired_queries(&cvr));

    cvr.queries.insert("q2".to_string(), CVRQuery {
        id: "q2".to_string(),
        internal: false,
        deactivated_at: None,
        ..Default::default()
    });
    assert!(has_expired_queries(&cvr)); // q1 is still expired

    cvr.queries.clear();
    assert!(!has_expired_queries(&cvr));
}

// ─── ViewSyncer construction test ──────────────────────────────────────────

#[test]
fn test_view_syncer_construction() {
    let (tx, rx) = crossbeam_channel::unbounded();
    let pipelines = Arc::new(MockPipelineDriver::default());
    let cvr_store = Arc::new(MockCVRStore::default());
    let ccm = rust_syncer::ConnectionContextManager::new(None, None, None, None, None, None);
    let dc = rust_syncer::DrainCoordinator::new();

    let vs = RustViewSyncer::new(
        "test-cg".to_string(),
        "shard-0".to_string(),
        pipelines.clone(),
        cvr_store.clone(),
        ccm,
        dc,
        None,
        rx,
    );

    assert_eq!(vs.id, "test-cg");
    assert!(!vs.is_initialized());
    assert!(!vs.is_stopped());
    assert!(vs.keepalive());
}

#[test]
fn test_view_syncer_stop() {
    let (_tx, rx) = crossbeam_channel::unbounded();
    let pipelines = Arc::new(MockPipelineDriver::default());
    let cvr_store = Arc::new(MockCVRStore::default());
    let ccm = rust_syncer::ConnectionContextManager::new(None, None, None, None, None, None);
    let dc = rust_syncer::DrainCoordinator::new();

    let mut vs = RustViewSyncer::new(
        "test-cg".to_string(),
        "shard-0".to_string(),
        pipelines,
        cvr_store,
        ccm,
        dc,
        None,
        rx,
    );

    vs.stop();
    assert!(vs.is_stopped());
    assert!(!vs.is_initialized());
}

#[test]
fn test_view_syncer_run_drains_before_init() {
    let (_tx, rx) = crossbeam_channel::unbounded();
    let pipelines = Arc::new(MockPipelineDriver::default());
    let cvr_store = Arc::new(MockCVRStore::default());
    let ccm = rust_syncer::ConnectionContextManager::new(None, None, None, None, None, None);
    let dc = rust_syncer::DrainCoordinator::new();

    // Start draining immediately
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    dc.drain_next_in(0);

    let mut vs = RustViewSyncer::new(
        "test-cg".to_string(),
        "shard-0".to_string(),
        pipelines,
        cvr_store,
        ccm,
        dc,
        None,
        rx,
    );

    // Should stop immediately since draining
    vs.run();
    assert!(vs.is_stopped());
}

#[test]
fn test_view_syncer_run_channel_closed() {
    let (tx, rx) = crossbeam_channel::unbounded();
    let pipelines = Arc::new(MockPipelineDriver::default());
    let cvr_store = Arc::new(MockCVRStore::default());
    let ccm = rust_syncer::ConnectionContextManager::new(None, None, None, None, None, None);
    let dc = rust_syncer::DrainCoordinator::new();

    let mut vs = RustViewSyncer::new(
        "test-cg".to_string(),
        "shard-0".to_string(),
        pipelines,
        cvr_store,
        ccm,
        dc,
        None,
        rx,
    );

    // Set initialized so we enter the main loop
    vs.set_initialized(true);

    // Close the channel
    drop(tx);

    // Should stop when channel is closed
    vs.run();
    assert!(vs.is_stopped());
}
