//! Clone density on the CVR row path — heap allocations per delivered row,
//! counted with dhat (deterministic, unlike wall time).
//!
//! Drives the production chain the syncer runs per streamed row:
//! `ChangeProcessor::on_row_change` (batch + de-dupe) →
//! `CVRQueryDrivenUpdater::received` (row records, patches) →
//! `MultiPoker::add_patch` (one poke body per connected client). The port's
//! per-row work is fixed by TS `#processChanges` / `received` / `addPatch`;
//! what Rust adds is allocation — a `String` id cloned per row, a row-key map
//! cloned per client, a ref-count map cloned twice. Each phase asserts an
//! upper bound on `allocations / row`, set just above the value measured after
//! those clones were removed, so re-introducing one fails here.
//!
//! Report the numbers: `cargo test -p rust-cvr --test clone_density_test -- --nocapture`

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value};

use rust_cvr::change_processor::{ChangeProcessor, RowChangeType};
use rust_cvr::client_handler::{ClientHandler, MultiPoker, PokePartBody, WebSocketSink};
use rust_cvr::cvr::{CVR, CVRQueryDrivenUpdater, RowRecordMap};
use rust_cvr::schema::types::{
    BaseQueryRecord, CVRVersion, ClientQueryRecord, QueryRecord, RowID, RowRecord,
};
use rust_cvr::shards::ShardID;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const ROWS: usize = 5_000;

/// One streamed row as the syncer hands it over: `(row_key, row)`.
type RowInput = (Map<String, Value>, Option<Map<String, Value>>);
const QUERY: &str = "q1";
const TABLE: &str = "issue";

/// Counts frames and drops them — the wire is not what this harness measures.
struct CountingSink {
    frames: Mutex<usize>,
}

impl WebSocketSink for CountingSink {
    fn push(&self, _msg: Value) -> Result<(), String> {
        *self.frames.lock().unwrap() += 1;
        Ok(())
    }
    /// Like the production sink (rust-syncer `ws_sink.rs`), the typed body
    /// leaves the client-group thread as-is and is serialized by the writer
    /// task; the trait's default would build a `Value` tree here instead.
    fn push_poke_part(&self, _body: PokePartBody, _est_bytes: usize) -> Result<(), String> {
        *self.frames.lock().unwrap() += 1;
        Ok(())
    }
    fn fail(&self, _e: String) {}
    fn fail_with_error_body(&self, _body: Value) {}
    fn cancel(&self) {}
}

fn make_cvr() -> CVR {
    let mut cvr = CVR {
        id: "cg1".to_string(),
        version: CVRVersion {
            state_version: "00".to_string(),
            config_version: None,
        },
        last_active: 0,
        ttl_clock: 0,
        replica_version: Some("v1".to_string()),
        clients: BTreeMap::new(),
        queries: BTreeMap::new(),
        client_schema: None,
        profile_id: None,
    };
    cvr.queries.insert(
        QUERY.to_string(),
        QueryRecord::Client(ClientQueryRecord {
            base: BaseQueryRecord {
                id: QUERY.to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
            },
            ast: serde_json::json!({"schema": "s", "table": TABLE}),
            client_state: BTreeMap::new(),
            patch_version: None,
        }),
    );
    cvr
}

fn make_updater() -> CVRQueryDrivenUpdater {
    let mut u = CVRQueryDrivenUpdater::new(make_cvr(), "00".to_string(), "v1".to_string(), None);
    u.track_queries(&[(QUERY, "hash1")], &[]);
    u
}

fn make_clients(n: usize) -> Vec<ClientHandler> {
    (0..n)
        .map(|i| {
            ClientHandler::new(
                "cg1",
                &format!("client{i}"),
                &format!("ws{i}"),
                &ShardID {
                    app_id: "app".to_string(),
                    shard_num: 0,
                },
                None,
                Arc::new(CountingSink {
                    frames: Mutex::new(0),
                }),
            )
        })
        .collect()
}

fn row_key(i: usize) -> Map<String, Value> {
    let mut m = Map::with_capacity(1);
    m.insert("id".to_string(), Value::String(format!("i{i:08}")));
    m
}

fn row(i: usize, version: &str, body: &str) -> Map<String, Value> {
    let mut m = Map::with_capacity(4);
    m.insert("id".to_string(), Value::String(format!("i{i:08}")));
    m.insert(
        "ownerId".to_string(),
        Value::String(format!("u{:04}", i % 50)),
    );
    m.insert("title".to_string(), Value::String(format!("{body} {i}")));
    m.insert("_0_version".to_string(), Value::String(version.to_string()));
    m
}

/// The row records the store would hold after phase A committed at `01`.
fn existing_after_adds() -> RowRecordMap {
    (0..ROWS)
        .map(|i| {
            let id = RowID {
                schema: String::new(),
                table: TABLE.into(),
                row_key: row_key(i),
            };
            let key = rust_cvr::row_key::row_id_string(&id);
            let mut ref_counts = BTreeMap::new();
            ref_counts.insert(QUERY.to_string(), 1);
            (
                key,
                RowRecord {
                    id,
                    row_version: "01".to_string(),
                    patch_version: CVRVersion {
                        state_version: "01".to_string(),
                        config_version: None,
                    },
                    ref_counts: Some(ref_counts),
                },
            )
        })
        .collect()
}

struct Stat {
    rows: usize,
    blocks: u64,
    bytes: u64,
    micros: u128,
}

impl Stat {
    fn blocks_per_row(&self) -> f64 {
        self.blocks as f64 / self.rows.max(1) as f64
    }
}

fn measure(label: &str, rows: usize, f: impl FnOnce()) -> Stat {
    let before = dhat::HeapStats::get();
    let t = std::time::Instant::now();
    f();
    let micros = t.elapsed().as_micros();
    let after = dhat::HeapStats::get();
    let s = Stat {
        rows,
        blocks: after.total_blocks - before.total_blocks,
        bytes: after.total_bytes - before.total_bytes,
        micros,
    };
    println!(
        "{label:<28} rows={:>6} allocs={:>9} bytes={:>11}  allocs/row={:>7.1} bytes/row={:>8.0}  {:>7.1} ms",
        s.rows,
        s.blocks,
        s.bytes,
        s.blocks_per_row(),
        s.bytes as f64 / s.rows.max(1) as f64,
        s.micros as f64 / 1000.0
    );
    s
}

/// One full pass: `ROWS` changes of `kind` through processor → updater →
/// pokers for `n_clients` connected clients, ending the poke.
fn pass(label: &str, n_clients: usize, kind: RowChangeType, existing: &RowRecordMap) -> Stat {
    let mut updater = make_updater();
    let clients = make_clients(n_clients);
    let refs: Vec<&ClientHandler> = clients.iter().collect();
    let pokers = MultiPoker::new(
        &refs,
        CVRVersion {
            state_version: "01".to_string(),
            config_version: None,
        },
        "test",
    );
    let (version, body) = match kind {
        RowChangeType::Add => ("01", "title"),
        RowChangeType::Edit => ("02", "edited"),
        RowChangeType::Remove => ("02", "title"),
    };
    // Built outside the measured window: the syncer hands the processor these
    // maps already built, so only the chain's own allocations are counted.
    let inputs: Vec<RowInput> = (0..ROWS)
        .map(|i| {
            let r = if kind == RowChangeType::Remove {
                None
            } else {
                Some(row(i, version, body))
            };
            (row_key(i), r)
        })
        .collect();
    // The syncer hands over its `RowChange.table` handle; one shared `Arc`
    // per table, cloned per row.
    let table: Arc<str> = Arc::from(TABLE);
    measure(label, ROWS, || {
        let mut processor = ChangeProcessor::new(&mut updater, &pokers);
        for (key, r) in inputs {
            processor
                .on_row_change(kind, QUERY, table.clone(), key, r, existing)
                .expect("on_row_change");
        }
        processor.finish(existing).expect("finish");
        pokers.end(CVRVersion {
            state_version: "01".to_string(),
            config_version: None,
        });
    })
}

/// Attribution run: `CLONE_DENSITY_PHASE=adds CLONE_DENSITY_CLIENTS=3 \
/// CLONE_DENSITY_OUT=/path/dhat.json cargo test --test clone_density_test \
/// -- --ignored clone_density_profile` writes a dhat profile of one phase.
#[test]
#[ignore = "attribution harness; run explicitly with --ignored and CLONE_DENSITY_PHASE"]
fn clone_density_profile() {
    let phase = std::env::var("CLONE_DENSITY_PHASE").expect("CLONE_DENSITY_PHASE");
    let clients: usize = std::env::var("CLONE_DENSITY_CLIENTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let out = std::env::var("CLONE_DENSITY_OUT").unwrap_or_else(|_| "dhat-heap.json".to_string());
    let none = RowRecordMap::new();
    let existing = existing_after_adds();
    let (kind, existing) = match phase.as_str() {
        "adds" => (RowChangeType::Add, &none),
        "edits" => (RowChangeType::Edit, &existing),
        "removes" => (RowChangeType::Remove, &existing),
        other => panic!("unknown phase {other}"),
    };
    let profiler = dhat::Profiler::builder()
        .file_name(&out)
        .trim_backtraces(None)
        .build();
    let s = pass(
        &format!("{phase}/{clients} clients"),
        clients,
        kind,
        existing,
    );
    drop(profiler);
    println!("profile written to {out}; rows={}", s.rows);
}

/// Allocations-per-row ceilings: the value measured after the per-row clones
/// were removed, plus ten percent of slack. A phase over its ceiling means a
/// per-row (or per-client-per-row) clone came back; the failure message
/// prints the measured value. Lower a ceiling when a change removes more;
/// never raise one to make a regression pass.
const CEILINGS: &[(&str, f64)] = &[
    ("adds/1 client", 23.2),
    ("adds/3 clients", 23.3),
    ("edits/1 client", 35.3),
    ("edits/3 clients", 44.2),
    ("removes/1 client", 34.2),
];

#[test]
fn clone_density_report() {
    let _profiler = dhat::Profiler::builder().testing().build();
    let none = RowRecordMap::new();
    let existing = existing_after_adds();
    let stats = vec![
        (
            "adds/1 client",
            pass("adds/1 client", 1, RowChangeType::Add, &none),
        ),
        (
            "adds/3 clients",
            pass("adds/3 clients", 3, RowChangeType::Add, &none),
        ),
        (
            "edits/1 client",
            pass("edits/1 client", 1, RowChangeType::Edit, &existing),
        ),
        (
            "edits/3 clients",
            pass("edits/3 clients", 3, RowChangeType::Edit, &existing),
        ),
        (
            "removes/1 client",
            pass("removes/1 client", 1, RowChangeType::Remove, &existing),
        ),
    ];
    let mut over = Vec::new();
    for (label, s) in &stats {
        if let Some((_, ceiling)) = CEILINGS.iter().find(|(l, _)| l == label)
            && s.blocks_per_row() > *ceiling
        {
            over.push(format!(
                "{label}: {:.1} allocations/row, ceiling {ceiling}",
                s.blocks_per_row()
            ));
        }
    }
    assert!(
        over.is_empty(),
        "clone density regressed on {} phase(s):\n  {}\nA per-row clone came back on the CVR path.",
        over.len(),
        over.join("\n  ")
    );
}
