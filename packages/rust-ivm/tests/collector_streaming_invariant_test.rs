//! Precondition: an engine pipeline's collector is ALWAYS in streaming
//! mode, so `CollectOutput.changes` is dead for every pipeline in
//! `Engine::pipelines`.
//!
//! `CollectOutput::push` has two branches (memory_source.rs:1389-1404): with a
//! `stream_config` it accumulates through a `Streamer` into `row_changes`;
//! without one it appends the raw `Change` to `changes`. There is exactly ONE
//! construction path for an engine pipeline — `add_queries_streaming`
//! (engine/mod.rs:854; `add_queries` delegates to it) — and it always calls
//! `configure_streaming`, so the second branch is unreachable from the engine.
//!
//! TS's twin of the clear loop is not a clear at all: `#push` calls
//! `#startAccumulating()` (pipeline-driver.ts:1223), which constructs a FRESH
//! `Streamer` per push, and nulls it out in a `finally` (:1216-1220) so an
//! exception cannot leave accumulated rows visible to the next push. Rust's
//! `CollectOutput` is the graph's terminal `Output` and cannot be swapped per
//! push without rewiring the graph, so it is emptied in place — which is why
//! `row_changes` must keep being cleared (it ports that `finally`) while
//! `changes`, a second sink with no TS counterpart, need not be.
//!
//! That invariant is what licenses the per-push clear loops
//! (`push_source_change` and `advance_streaming`) to stop clearing `changes`:
//! the clear was a SECOND `borrow_mut()` of the same `RefCell` per pipeline
//! per source change, emptying a Vec that is structurally empty — 100
//! pipelines x 1000 changes is 100K borrows of nothing.
//!
//! The clear loops carry a `debug_assert!` for the same invariant, but a
//! `debug_assert` is compiled out of a release build and the loops run on the
//! hot advance path; this test pins it unconditionally.
//!
//! Mutation test: delete the `b.collector.borrow_mut().configure_streaming(...)`
//! call in `add_queries_streaming` and `push` falls into its non-streaming
//! branch — `collector_changes_total` becomes 1 after the advance and
//! `an_engine_pipeline_never_fills_the_non_streaming_changes_buffer` fails on
//! its post-advance assertion (the delivered-row assertion fails too, since a
//! change parked in `changes` is never streamed).

use rust_ivm::ivm::data::RowMap;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{Ast, OrderPart};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::streamer::RowChange;

fn issues_source() -> Rc<RefCell<MemorySource>> {
    let columns: HashMap<String, ColumnType> = [
        ("id".to_string(), ColumnType::String { optional: false }),
        ("owner".to_string(), ColumnType::String { optional: false }),
    ]
    .into_iter()
    .collect();
    Rc::new(RefCell::new(MemorySource::new(
        "issues",
        columns,
        vec!["id".to_string()],
    )))
}

fn row(id: &str, owner: &str) -> RowMap {
    let mut m: RowMap = FxHashMap::default();
    m.insert("id".into(), Value::Str(id.into()));
    m.insert("owner".into(), Value::Str(owner.into()));
    m
}

fn issues_ast() -> Ast {
    Ast {
        table: "issues".to_string(),
        order_by: Some(vec![OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        ..Default::default()
    }
}

#[test]
fn an_engine_pipeline_never_fills_the_non_streaming_changes_buffer() {
    let source = issues_source();
    source.borrow_mut().add_row(row("i1", "alice"));

    let mut eng = Engine::new(HashMap::from([(
        "issues".to_string(),
        vec!["id".to_string()],
    )]));
    eng.register_source(source);

    let results = eng.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast: issues_ast(),
    }]);
    assert_eq!(results.len(), 1, "the query must build");
    assert_eq!(
        results[0].changes.len(),
        1,
        "hydration must vend the seeded row — otherwise the pipeline is not \
         actually wired and the assertions below are vacuous"
    );
    assert_eq!(
        eng.__test_collector_changes_total(),
        0,
        "hydration must leave the non-streaming `changes` buffer empty"
    );

    // Advance: this is the loop the precondition is about. `advance_streaming` runs the
    // clear loop over every pipeline once per source change.
    let mut delivered: Vec<RowChange> = Vec::new();
    let change =
        rust_ivm::ivm::source::make_source_change_add(std::sync::Arc::new(row("i2", "bob")));
    let completed = eng.advance_streaming(&[("issues".to_string(), change)], |rc| {
        delivered.push(rc.clone());
    });
    assert!(completed, "the advance must not be truncated");
    assert_eq!(
        delivered.len(),
        1,
        "the pushed row must be STREAMED (i.e. it went through `row_changes`), \
         not parked in the non-streaming `changes` buffer"
    );
    assert_eq!(
        eng.__test_collector_changes_total(),
        0,
        "a pushed change must never land in the non-streaming `changes` \
         buffer — the per-push clear loop no longer empties it"
    );
}

#[test]
fn the_invariant_holds_for_every_pipeline_when_several_share_a_source() {
    let source = issues_source();
    source.borrow_mut().add_row(row("i1", "alice"));

    let mut eng = Engine::new(HashMap::from([(
        "issues".to_string(),
        vec!["id".to_string()],
    )]));
    eng.register_source(source);

    for qid in ["q1", "q2", "q3"] {
        let results = eng.add_queries(&[QuerySpec {
            query_id: qid.to_string(),
            ast: issues_ast(),
        }]);
        assert_eq!(results.len(), 1, "{qid} must build");
    }
    assert_eq!(eng.__test_collector_changes_total(), 0);

    let mut delivered = 0usize;
    let change =
        rust_ivm::ivm::source::make_source_change_add(std::sync::Arc::new(row("i2", "bob")));
    eng.advance_streaming(&[("issues".to_string(), change)], |_| delivered += 1);
    assert_eq!(
        delivered, 3,
        "all three pipelines must receive the change — the clear loop skipping \
         `changes` must not skip a pipeline"
    );
    assert_eq!(
        eng.__test_collector_changes_total(),
        0,
        "no pipeline may retain a change in the non-streaming buffer"
    );
}
