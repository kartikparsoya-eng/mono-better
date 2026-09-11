//! Regression: the `primary_keys` / `table_specs` table maps must be
//! SHARED with every `Streamer`, never deep-copied per pipeline or per change.
//!
//! TS `new Streamer(primaryKeys, tableSpecs)` stores the caller's `Map`
//! objects by reference (`this.#primaryKeys = primaryKeys`,
//! pipeline-driver.ts:1260-1268) — constructing a Streamer costs nothing
//! beyond the object, which is why TS builds one per companion change (:1512)
//! and one per collector push. The rust port took the maps by VALUE at four
//! sites, so each construction deep-copied the whole table map (one `String`
//! plus one `Vec<String>` per table, ~150 tables in prod):
//!
//!   * `CollectOutput::configure_streaming` — once per pipeline, and the copy
//!     was RETAINED for the pipeline's lifetime, so a client group with N
//!     queries held N copies of one map;
//!   * `engine::push_source_change` — per surviving companion change, per
//!     pipeline, per advance change (the hot one);
//!   * the hydrate streamer and the hydrate-time companion streamer.
//!
//! The maps are read-only inside `Streamer`, so sharing them changes no
//! emitted row; what it removes is the copying. `Streamer::new` now takes
//! `Rc`s — the `new_shared` variant is gone rather than left around to be
//! skipped again by a fifth call site — and `Engine` owns the single `Rc`.
//!
//! Mutation test: revert `configure_streaming` to taking the maps by value with
//! an internal `Rc::new` (passing `(*self.primary_keys).clone()` at the call
//! site) and the holder count stays at 1 however many queries are added, so
//! `every_pipeline_shares_the_engines_one_table_map` fails; revert
//! `push_source_change` to `&HashMap` + `Streamer::new(pk.clone(), ...)` and
//! `an_in_flight_advance_shares_the_map_rather_than_copying_it` fails.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use rusqlite::Connection;

use rust_ivm::builder::ast::Ast;
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::snapshotter::Snapshotter;
use rust_ivm::snapshotter::spec::{ColumnSchema, LiteAndZqlSpec, TableSpec};
use rust_ivm::sqlite::table_source::TableSource;
use rust_ivm::streamer::RowChange;

fn ver(n: usize) -> String {
    format!("v{n:08}")
}

fn db_path(tag: &str) -> String {
    format!(
        "{}/rust-ivm-streamer-sharing-{tag}-{}.db",
        std::env::temp_dir().display(),
        std::process::id()
    )
}

fn seed(db: &str) {
    for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
        let _ = std::fs::remove_file(format!("{db}{suffix}"));
    }
    let conn = Connection::open(db).unwrap();
    let _ = conn.pragma_update(None, "journal_mode", "wal2");
    let _ = conn.pragma_update(None, "journal_mode", "wal");
    conn.execute_batch(&format!(
        r#"
        CREATE TABLE "_zero.replicationConfig" (lock TEXT PRIMARY KEY DEFAULT 'singleton',
            replicaVersion TEXT NOT NULL, publications TEXT NOT NULL);
        CREATE TABLE "_zero.replicationState" (lock TEXT PRIMARY KEY DEFAULT 'singleton',
            stateVersion TEXT NOT NULL);
        CREATE TABLE "_zero.changeLog2" ("stateVersion" TEXT NOT NULL, "table" TEXT NOT NULL,
            "rowKey" TEXT NOT NULL, "op" TEXT NOT NULL, "pos" INTEGER NOT NULL,
            PRIMARY KEY ("stateVersion","pos"));
        CREATE TABLE issues (id TEXT PRIMARY KEY, ownerId TEXT NOT NULL, _0_version TEXT NOT NULL);
        CREATE TABLE comments (id TEXT PRIMARY KEY, issueId TEXT NOT NULL, _0_version TEXT NOT NULL);
        INSERT INTO "_zero.replicationConfig" VALUES ('singleton','{v1}','[]');
        INSERT INTO "_zero.replicationState"  VALUES ('singleton','{v1}');
        INSERT INTO issues   VALUES ('i100','Alice','{v1}');
        INSERT INTO comments VALUES ('c100','i100','{v1}');
        "#,
        v1 = ver(1),
    ))
    .unwrap();
}

fn spec(name: &str, cols: &[&str]) -> LiteAndZqlSpec {
    let columns: HashMap<String, ColumnSchema> = cols
        .iter()
        .map(|c| {
            (
                c.to_string(),
                ColumnSchema {
                    r#type: "TEXT".to_string(),
                    optional: false,
                },
            )
        })
        .collect();
    LiteAndZqlSpec {
        table_spec: TableSpec {
            name: name.to_string(),
            columns: columns.clone(),
            unique_keys: vec![vec!["id".to_string()]],
            min_row_version: None,
        },
        zql_spec: columns,
    }
}

fn ast(table: &str) -> Ast {
    Ast {
        table: table.to_string(),
        order_by: Some(vec![rust_ivm::builder::ast::OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        ..Default::default()
    }
}

struct Fixture {
    db: String,
    snap: Snapshotter,
    eng: Engine,
    syncable: HashMap<String, LiteAndZqlSpec>,
    all_tables: HashSet<String>,
}

impl Fixture {
    fn new(tag: &str) -> Self {
        let db = db_path(tag);
        seed(&db);
        let mut snap = Snapshotter::new(&db, "", None);
        snap.init().unwrap();
        let curr = snap.current_conn().unwrap();

        let cols = |names: &[&str]| -> HashMap<String, ColumnType> {
            names
                .iter()
                .map(|n| (n.to_string(), ColumnType::String { optional: false }))
                .collect()
        };
        let issues = TableSource::new(
            curr.clone(),
            "issues",
            cols(&["id", "ownerId", "_0_version"]),
            vec!["id".to_string()],
        );
        let comments = TableSource::new(
            curr.clone(),
            "comments",
            cols(&["id", "issueId", "_0_version"]),
            vec!["id".to_string()],
        );

        let mut eng = Engine::new(HashMap::from([
            ("issues".to_string(), vec!["id".to_string()]),
            ("comments".to_string(), vec!["id".to_string()]),
        ]));
        eng.register_source(Rc::new(RefCell::new(issues)));
        eng.register_source(Rc::new(RefCell::new(comments)));
        eng.set_unique_keys("issues", vec![vec!["id".to_string()]]);
        eng.set_unique_keys("comments", vec![vec!["id".to_string()]]);
        // `set_table_spec` writes through `Rc::make_mut`; assert below that it
        // leaves the count at 1 (nobody has taken a handle yet).
        eng.set_table_spec("issues", None);
        eng.set_table_spec("comments", None);

        Fixture {
            db,
            snap,
            eng,
            syncable: HashMap::from([
                (
                    "issues".to_string(),
                    spec("issues", &["id", "ownerId", "_0_version"]),
                ),
                (
                    "comments".to_string(),
                    spec("comments", &["id", "issueId", "_0_version"]),
                ),
            ]),
            all_tables: HashSet::from(["issues".to_string(), "comments".to_string()]),
        }
    }

    fn add_query(&mut self, query_id: &str, table: &str) {
        let results = self.eng.add_queries_streaming(
            &[QuerySpec {
                query_id: query_id.into(),
                ast: ast(table),
            }],
            |_rc: &RowChange| {},
        );
        assert_eq!(results.len(), 1, "query {query_id} must hydrate");
    }

    /// One replication step so `start_advance` has a real diff to walk.
    fn write_change(&self, n: usize) {
        let v = ver(n);
        let w = Connection::open(&self.db).unwrap();
        let id = format!("i{}", n + 100);
        w.execute(
            "INSERT INTO issues (id,ownerId,_0_version) VALUES (?,?,?)",
            rusqlite::params![id, "Alice", v],
        )
        .unwrap();
        w.execute(
            r#"INSERT INTO "_zero.changeLog2" ("stateVersion","table","rowKey","op","pos") VALUES (?,?,?,?,?)"#,
            rusqlite::params![v, "issues", format!(r#"{{"id":"{id}"}}"#), "s", 0i64],
        )
        .unwrap();
        w.execute(
            r#"UPDATE "_zero.replicationState" SET stateVersion=? WHERE lock='singleton'"#,
            rusqlite::params![v],
        )
        .unwrap();
    }
}

#[test]
fn every_pipeline_shares_the_engines_one_table_map() {
    let mut f = Fixture::new("pipelines");

    // Nothing built yet: the engine is the only holder. `register_source` /
    // `set_table_spec` mutate through `Rc::make_mut`, which stays in place
    // precisely because no pipeline has taken a handle.
    assert_eq!(
        f.eng.__test_primary_keys_holders(),
        1,
        "a bare engine must be the only holder of its primary-key map"
    );
    assert_eq!(f.eng.__test_table_specs_holders(), 1);

    f.add_query("q1", "issues");
    assert_eq!(
        f.eng.__test_primary_keys_holders(),
        2,
        "one pipeline must SHARE the engine's map, not hold a deep copy of it"
    );
    assert_eq!(f.eng.__test_table_specs_holders(), 2);

    f.add_query("q2", "comments");
    assert_eq!(
        f.eng.__test_primary_keys_holders(),
        3,
        "a second pipeline must share the SAME map — N queries must not mean \
         N copies of the table map"
    );
    assert_eq!(f.eng.__test_table_specs_holders(), 3);
}

#[test]
fn an_in_flight_advance_shares_the_map_rather_than_copying_it() {
    let mut f = Fixture::new("advance");
    f.add_query("q1", "issues");

    let before = f.eng.__test_primary_keys_holders();
    assert_eq!(before, 2, "engine + one pipeline");

    f.write_change(2);

    // `start_advance` hands the stream the maps its per-change companion
    // streamers construct from. Sharing means the count rises by exactly ONE
    // for the whole advance regardless of how many changes it pushes; a
    // copying stream leaves it at `before`.
    let syncable = std::mem::take(&mut f.syncable);
    let all_tables = std::mem::take(&mut f.all_tables);
    let mut stream = f
        .eng
        .start_advance(&mut f.snap, &syncable, &all_tables, None, None)
        .expect("advance must start");
    assert_eq!(
        f.eng.__test_primary_keys_holders(),
        before + 1,
        "an in-flight advance must borrow the shared map, not copy it"
    );
    assert_eq!(f.eng.__test_table_specs_holders(), before + 1);

    // Drain it so the advance is a real one, not just a constructed stream.
    let mut rows = 0;
    for item in stream.by_ref() {
        if matches!(item, rust_ivm::ivm::stream::StreamItem::Data(_)) {
            rows += 1;
        }
    }
    assert_eq!(rows, 1, "the one inserted issue must be delivered");
    assert_eq!(
        f.eng.__test_primary_keys_holders(),
        before + 1,
        "draining the advance must not add holders — the per-change companion \
         streamers share, they do not copy"
    );
    f.eng
        .finish_advance(stream)
        .expect("the advance must finish cleanly");
}
