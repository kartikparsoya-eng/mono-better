//! Heap-profiling advance loop — reproduces the prod per-advance memory creep
//! LOCALLY and attributes it. Stable query set (one EXISTS/join pipeline), a
//! BOUNDED working set churned over many advances: any growth in dhat's live
//! bytes across iterations is a genuine per-advance leak (retained allocation),
//! matching the prod signature (flat CG, memory climbs linearly).
//!
//! Run: cargo test -p rust-ivm --test advance_leak_profile -- --nocapture --ignored
//! dhat writes dhat-heap.json (view at https://nnethercote.github.io/dh_view/dh_view.html)

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{
    Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery, SimpleCondition, ValuePosition,
};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::data::{Row, Value};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;
use rust_ivm::ivm::source::{make_source_change_add, make_source_change_remove};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn str_source(name: &str, cols: &[&str], pk: &[&str]) -> Rc<RefCell<MemorySource>> {
    let columns: HashMap<String, ColumnType> = cols
        .iter()
        .map(|c| (c.to_string(), ColumnType::String { optional: false }))
        .collect();
    Rc::new(RefCell::new(MemorySource::new(
        name,
        columns,
        pk.iter().map(|s| s.to_string()).collect(),
    )))
}

fn add_row(source: &Rc<RefCell<MemorySource>>, pairs: &[(&str, &str)]) {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), Value::Str((*v).into())))
        .collect();
    source.borrow_mut().add_row(map);
}

fn make_row(pairs: &[(&str, &str)]) -> Row {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), Value::Str((*v).into())))
        .collect();
    Arc::new(map)
}

/// issues WHERE EXISTS(users WHERE id = 'u1') correlated ownerId = users.name.
/// `flip` selects the flipped-join execution shape (the prod amplifier).
fn exists_ast(flip: bool) -> Ast {
    let subquery = Ast {
        schema: None,
        table: "users".to_string(),
        alias: Some("users".to_string()),
        where_clause: Some(Condition::Simple(SimpleCondition {
            op: "=".to_string(),
            left: ValuePosition::Column {
                name: "id".to_string(),
            },
            right: ValuePosition::Literal {
                value: Value::Str("u1".into()),
            },
        })),
        related: vec![],
        limit: None,
        order_by: None,
        start: None,
    };
    Ast {
        schema: None,
        table: "issues".to_string(),
        alias: None,
        where_clause: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: RelatedSubquery {
                subquery: Box::new(subquery),
                relationship_name: "users".to_string(),
                parent_key: vec!["ownerId".to_string()],
                child_key: vec!["name".to_string()],
                hidden: false,
                system: None,
            },
            op: "EXISTS".to_string(),
            flip: Some(flip),
            scalar: false,
            plan_id: None,
        })),
        related: vec![],
        limit: None,
        order_by: Some(vec![rust_ivm::builder::ast::OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    }
}

fn run(flip: bool, iters: usize) {
    let _profiler = dhat::Profiler::builder().build();

    let users = str_source("users", &["id", "name"], &["id"]);
    add_row(&users, &[("id", "u1"), ("name", "Alice")]);
    let issues = str_source("issues", &["id", "ownerId"], &["id"]);
    add_row(&issues, &[("id", "i1"), ("ownerId", "Alice")]);
    add_row(&issues, &[("id", "i2"), ("ownerId", "Bob")]);

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(users);
    engine.register_source(issues);
    engine.set_unique_keys("users", vec![vec!["id".to_string()]]);
    engine.set_unique_keys("issues", vec![vec!["id".to_string()]]);

    // Stable query — subscribed once, never removed (prod steady state).
    let specs = vec![QuerySpec {
        query_id: "q".to_string(),
        ast: exists_ast(flip),
    }];
    let _ = engine.add_queries(&specs);

    let baseline = dhat::HeapStats::get();
    println!(
        "flip={flip} baseline after hydrate: curr_bytes={} curr_blocks={}",
        baseline.curr_bytes, baseline.curr_blocks
    );

    // BOUNDED-live, EVER-NEW-KEY churn (prod shape: new rows stream in, old
    // ones age out). Each advance ADDs a fresh issue id and REMOVEs the one
    // from 2 iters ago — only ~2 rows live at once, but the KEYSPACE is
    // monotonic. Any structure keyed by row id / parent that isn't evicted on
    // Remove grows linearly here while live-set stays flat.
    let mut prev_bytes = baseline.curr_bytes;
    for i in 0..iters {
        let id_new = format!("i{}", i + 100);
        let add = make_source_change_add(make_row(&[("id", &id_new), ("ownerId", "Alice")]));
        let _ = engine.advance(&[("issues".to_string(), add)]);
        if i >= 2 {
            let id_old = format!("i{}", i + 100 - 2);
            let rem = make_source_change_remove(make_row(&[("id", &id_old), ("ownerId", "Alice")]));
            let _ = engine.advance(&[("issues".to_string(), rem)]);
        }

        if (i + 1) % 1000 == 0 {
            let s = dhat::HeapStats::get();
            let delta = s.curr_bytes as i64 - prev_bytes as i64;
            println!(
                "  iter {:6}: curr_bytes={:>10} curr_blocks={:>8}  Δ_since_last={:+}",
                i + 1,
                s.curr_bytes,
                s.curr_blocks,
                delta
            );
            prev_bytes = s.curr_bytes;
        }
    }
    let end = dhat::HeapStats::get();
    let grew = end.curr_bytes as i64 - baseline.curr_bytes as i64;
    println!(
        "flip={flip} END: curr_bytes={} curr_blocks={}  grew_since_baseline={:+} bytes over {} advances ({:.1} bytes/advance)",
        end.curr_bytes,
        end.curr_blocks,
        grew,
        iters,
        grew as f64 / iters as f64
    );
    // dhat-heap.json written on _profiler drop → attributes the retained blocks.

    // Self-gating: live bytes must stay FLAT per advance. Before the
    // clear_advance_state fix, the plain-advance path retained one pk_key
    // String per removed row (+1 block, ~46.5 bytes per advance, dhat-
    // attributed to removed_this_advance). Generous jitter budget; a real
    // per-advance retention blows through it within a few hundred advances.
    assert!(
        (grew as f64 / iters as f64) < 4.0,
        "per-advance heap growth: {grew} bytes over {iters} advances"
    );
}

#[test]
#[ignore = "profiling harness; run explicitly with --ignored --nocapture"]
fn advance_leak_exists_noflip() {
    run(false, 20_000);
}

#[test]
#[ignore = "profiling harness; run explicitly with --ignored --nocapture"]
fn advance_leak_exists_flip() {
    run(true, 20_000);
}
