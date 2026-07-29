//! Unit tests for the StreamSink + Chunker streaming abstraction.
//! Verifies: monotonic chunkIndex, per-query Final, terminal Done,
//! bounded frame size, query-switch flushing.

use rust_ivm::streamer::{Chunker, CollectSink, StreamFrame, RowChange};
use rust_ivm::ivm::change::ChangeType;
use rust_ivm::ivm::data::{Row, Value};
use std::sync::Arc;
use rustc_hash::FxHashMap;

fn make_row_change(qid: &str, table: &str, val: f64) -> RowChange {
    let mut key: FxHashMap<String, Value> = FxHashMap::default();
    key.insert("id".to_string(), Value::F64(val));
    let mut row: FxHashMap<String, Value> = FxHashMap::default();
    row.insert("id".to_string(), Value::F64(val));
    row.insert("name".to_string(), Value::Str(Arc::from("test")));
    RowChange {
        change_type: ChangeType::Add,
        query_id: qid.to_string(),
        table: table.to_string(),
        row_key: Arc::new(key),
        row: Some(Arc::new(row)),
        is_hidden: false,
    }
}

#[test]
fn test_chunk_invariants_single_query() {
    let sink = CollectSink::new();
    let mut chunker = Chunker::new(sink, 3);

    // Push 7 rows for q1, then flush, then done.
    for i in 0..7 {
        chunker.push_row_change("q1", make_row_change("q1", "t", i as f64));
    }
    chunker.flush_query("q1");
    chunker.done();

    let sink = chunker.into_sink();
    let frames = &sink.frames;

    // Should be: 2 Partial (3+3 rows), 1 Partial (1 row from flush), 1 Final, 1 Done
    // Actually: 7 rows / chunk_size=3 = 2 full Partial + 1 remaining row flushed at flush_query
    assert_eq!(frames.len(), 5, "Expected 5 frames, got {}", frames.len());

    // Check monotonic chunk indices
    for (i, frame) in frames.iter().enumerate() {
        let idx = match frame {
            StreamFrame::Partial { chunk_index, .. } => *chunk_index,
            StreamFrame::Final { chunk_index, .. } => *chunk_index,
            StreamFrame::Done { chunk_index } => *chunk_index,
            StreamFrame::Error { chunk_index, .. } => *chunk_index,
        };
        assert_eq!(idx, i, "Frame {} has chunk_index {} expected {}", i, idx, i);
    }

    // First two Partial frames should have 3 rows each
    match &frames[0] {
        StreamFrame::Partial { changes, query_id, .. } => {
            assert_eq!(changes.len(), 3);
            assert_eq!(query_id, "q1");
        }
        _ => panic!("Expected Partial frame at 0"),
    }
    match &frames[1] {
        StreamFrame::Partial { changes, .. } => assert_eq!(changes.len(), 3),
        _ => panic!("Expected Partial frame at 1"),
    }
    // Third Partial has the remaining 1 row
    match &frames[2] {
        StreamFrame::Partial { changes, .. } => assert_eq!(changes.len(), 1),
        _ => panic!("Expected Partial frame at 2"),
    }
    // Final frame
    match &frames[3] {
        StreamFrame::Final { query_id, .. } => assert_eq!(query_id, "q1"),
        _ => panic!("Expected Final frame at 3"),
    }
    // Done frame
    match &frames[4] {
        StreamFrame::Done { .. } => {}
        _ => panic!("Expected Done frame at 4"),
    }
}

#[test]
fn test_chunk_multi_query_switch() {
    let sink = CollectSink::new();
    let mut chunker = Chunker::new(sink, 10);

    // Push 2 rows for q1, then switch to q2
    chunker.push_row_change("q1", make_row_change("q1", "t", 1.0));
    chunker.push_row_change("q1", make_row_change("q1", "t", 2.0));
    chunker.push_row_change("q2", make_row_change("q2", "t", 3.0));
    chunker.push_row_change("q2", make_row_change("q2", "t", 4.0));
    chunker.done();

    let sink = chunker.into_sink();
    let frames = &sink.frames;

    // Should be: Partial(q1, 2 rows), Final(q1), Partial(q2, 2 rows), Final(q2), Done
    assert_eq!(frames.len(), 5, "Expected 5 frames, got {}", frames.len());

    match &frames[0] {
        StreamFrame::Partial { query_id, changes, .. } => {
            assert_eq!(query_id, "q1");
            assert_eq!(changes.len(), 2);
        }
        _ => panic!("Expected Partial(q1) at 0"),
    }
    match &frames[1] {
        StreamFrame::Final { query_id, .. } => assert_eq!(query_id, "q1"),
        _ => panic!("Expected Final(q1) at 1"),
    }
    match &frames[2] {
        StreamFrame::Partial { query_id, changes, .. } => {
            assert_eq!(query_id, "q2");
            assert_eq!(changes.len(), 2);
        }
        _ => panic!("Expected Partial(q2) at 2"),
    }
    match &frames[3] {
        StreamFrame::Final { query_id, .. } => assert_eq!(query_id, "q2"),
        _ => panic!("Expected Final(q2) at 3"),
    }
    match &frames[4] {
        StreamFrame::Done { .. } => {}
        _ => panic!("Expected Done at 4"),
    }
}

#[test]
fn test_chunk_empty_query() {
    let sink = CollectSink::new();
    let mut chunker = Chunker::new(sink, 3);

    chunker.flush_query("q1");
    chunker.done();

    let sink = chunker.into_sink();
    let frames = &sink.frames;

    // No rows → flush_query emits nothing (batch empty), done emits Done
    // Actually: flush_query checks if current_query_id matches, but we never
    // pushed any rows, so current_query_id is None. flush_query does nothing.
    // done() flushes (empty), then Done.
    assert_eq!(frames.len(), 1, "Expected 1 frame (Done), got {}", frames.len());
    match &frames[0] {
        StreamFrame::Done { .. } => {}
        _ => panic!("Expected Done at 0"),
    }
}

#[test]
fn test_chunk_error_mid_stream() {
    let sink = CollectSink::new();
    let mut chunker = Chunker::new(sink, 3);

    chunker.push_row_change("q1", make_row_change("q1", "t", 1.0));
    chunker.push_row_change("q1", make_row_change("q1", "t", 2.0));
    chunker.error("something went wrong".to_string());

    let sink = chunker.into_sink();
    let frames = &sink.frames;

    // Should be: Partial(q1, 2 rows), Error
    assert_eq!(frames.len(), 2, "Expected 2 frames, got {}", frames.len());
    match &frames[0] {
        StreamFrame::Partial { changes, .. } => assert_eq!(changes.len(), 2),
        _ => panic!("Expected Partial at 0"),
    }
    match &frames[1] {
        StreamFrame::Error { message, .. } => assert_eq!(message, "something went wrong"),
        _ => panic!("Expected Error at 1"),
    }
}
