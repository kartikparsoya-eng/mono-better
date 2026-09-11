//! Unit tests for `client_handler.rs`.
//!
//! Kept out of line so the production file stays reviewable. Declared with
//! `#[path]` from `client_handler.rs` under `#[cfg(test)]`, so `use super::*` sees
//! the same private items an inline `mod tests` would.

use super::*;
use std::sync::Mutex as StdMutex;

/// PRECONDITION for moving `pokePart` serialization off the serial
/// client-group thread: `flush_body` builds
/// `serde_json::json!(["pokePart", body])` — a whole `Value` tree, on the
/// CG thread — and the writer task then walks that tree to produce text.
/// Handing the writer the TYPED body instead is only safe if the two
/// routes are byte-identical, which is what this asserts on a body with
/// every optional field populated.
///
/// The two things that could differ, and why they do not: key ORDER —
/// `serde_json`'s `preserve_order` feature is on in all three crates, so a
/// `Value` map keeps the struct's declaration order rather than sorting;
/// and ABSENT fields — `skip_serializing_if = "Option::is_none"` runs in
/// the struct's own `Serialize`, which both routes go through, so a `None`
/// is missing from the `Value` tree in the first place rather than
/// becoming `null`.
///
/// Mutation test: this fails the moment either property stops holding — drop
/// `preserve_order` from Cargo.toml and the `desiredQueriesPatches` /
/// `lastMutationIDChanges` maps come back sorted, which is a different
/// frame on the wire.
#[test]
fn poke_part_serializes_identically_as_a_value_tree_and_as_a_typed_body() {
    let body = PokePartBody {
        poke_id: "poke-1".to_string(),
        got_queries_patch: Some(vec![
            QueryPatchEntry {
                op: "put",
                hash: "zzz".to_string(),
            },
            QueryPatchEntry {
                op: "del",
                hash: "aaa".to_string(),
            },
        ]),
        // Deliberately NOT in sorted order of insertion vs key: a BTreeMap
        // is ordered by key, and the `Value` route must reproduce that same
        // order, not the declaration order of the literal.
        desired_queries_patches: Some(BTreeMap::from([
            (
                "clientB".to_string(),
                vec![QueryPatchEntry {
                    op: "put",
                    hash: "h2".to_string(),
                }],
            ),
            (
                "clientA".to_string(),
                vec![QueryPatchEntry {
                    op: "del",
                    hash: "h1".to_string(),
                }],
            ),
        ])),
        rows_patch: Some(vec![
            RowPatchOp {
                op: "put",
                table_name: "issue".into(),
                // Arc-shared, a float that must stay `1.0`, a null, and a
                // nested object whose key order must survive.
                value: Some(Arc::new(serde_json::json!({
                    "id": "i1",
                    "score": 1.0,
                    "big": 9007199254740993i64,
                    "closed": serde_json::Value::Null,
                    "nested": {"z": 1, "a": 2},
                }))),
                id: None,
            },
            RowPatchOp {
                op: "del",
                table_name: "comment".into(),
                value: None,
                id: Some(serde_json::json!({"id": "c1"})),
            },
        ]),
        last_mutation_id_changes: Some(BTreeMap::from([
            ("cB".to_string(), 7i64),
            ("cA".to_string(), -1i64),
        ])),
        mutations_patch: Some(vec![MutationPatchEntry {
            op: "put",
            mutation: None,
            id: None,
        }]),
    };

    let via_value_tree = serde_json::to_string(&serde_json::json!(["pokePart", &body])).unwrap();
    let typed_directly = serde_json::to_string(&("pokePart", &body)).unwrap();
    assert_eq!(
        via_value_tree, typed_directly,
        "the writer task may only serialize the typed body if it produces \
             the exact bytes the Value tree does"
    );

    // And spot-check the two properties by name, so a failure says WHICH
    // one broke rather than just showing two long strings.
    assert!(
        typed_directly.contains(r#""gotQueriesPatch":[{"op":"put","hash":"zzz"}"#),
        "declaration order and renames must hold: {typed_directly}"
    );
    assert!(
        typed_directly.contains(r#""lastMutationIDChanges":{"cA":-1,"cB":7}"#),
        "BTreeMap key order must survive the Value tree: {typed_directly}"
    );
    assert!(
        !typed_directly.contains("null,\"tableName\""),
        "a None field must be ABSENT, never null: {typed_directly}"
    );
}

struct MockSink {
    messages: Arc<StdMutex<Vec<Value>>>,
    failed: Arc<StdMutex<Option<String>>>,
    cancelled: Arc<StdMutex<bool>>,
}

impl MockSink {
    fn new() -> (Self, Arc<StdMutex<Vec<Value>>>) {
        let messages = Arc::new(StdMutex::new(Vec::new()));
        let sink = Self {
            messages: messages.clone(),
            failed: Arc::new(StdMutex::new(None)),
            cancelled: Arc::new(StdMutex::new(false)),
        };
        (sink, messages)
    }
}

impl WebSocketSink for MockSink {
    fn push(&self, msg: Value) -> Result<(), String> {
        self.messages.lock().unwrap().push(msg);
        Ok(())
    }
    fn fail(&self, e: String) {
        *self.failed.lock().unwrap() = Some(e);
    }
    fn cancel(&self) {
        *self.cancelled.lock().unwrap() = true;
    }
}

struct FailingSink {
    fail_tag: &'static str,
}

impl WebSocketSink for FailingSink {
    fn push(&self, msg: Value) -> Result<(), String> {
        if msg
            .as_array()
            .and_then(|parts| parts.first())
            .and_then(Value::as_str)
            == Some(self.fail_tag)
        {
            Err(format!("intentional {} failure", self.fail_tag))
        } else {
            Ok(())
        }
    }

    fn fail(&self, _e: String) {}

    fn cancel(&self) {}
}

fn make_failing_handler(fail_tag: &'static str) -> ClientHandler {
    ClientHandler::new(
        "cg1",
        "client1",
        "ws1",
        &ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        },
        None,
        Arc::new(FailingSink { fail_tag }),
    )
}

/// Sink that always errors on `push` and counts how many times it was
/// invoked, so a test can assert a dead poker is not re-pushed per patch.
struct CountingFailSink {
    pushes: Arc<std::sync::atomic::AtomicUsize>,
}

impl WebSocketSink for CountingFailSink {
    fn push(&self, _msg: Value) -> Result<(), String> {
        self.pushes.fetch_add(1, AtomicOrdering::SeqCst);
        Err("sink closed".to_string())
    }
    fn fail(&self, _e: String) {}
    fn cancel(&self) {}
}

fn assert_chain_released(handler: &ClientHandler) {
    assert!(
        !handler.poke_chain.load(AtomicOrdering::SeqCst),
        "failed poke must release the per-client chain"
    );
}

fn make_handler() -> (ClientHandler, Arc<StdMutex<Vec<Value>>>) {
    let (sink, messages) = MockSink::new();
    let handler = ClientHandler::new(
        "cg1",
        "client1",
        "ws1",
        &ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        },
        None,
        Arc::new(sink),
    );
    (handler, messages)
}

// Build a handler alongside the sink's fail/cancel observation handles.
#[allow(clippy::type_complexity)] // test helper: a one-off observation tuple
fn make_handler_observing_lifecycle() -> (
    ClientHandler,
    Arc<StdMutex<Option<String>>>,
    Arc<StdMutex<bool>>,
) {
    let failed = Arc::new(StdMutex::new(None));
    let cancelled = Arc::new(StdMutex::new(false));
    let sink = MockSink {
        messages: Arc::new(StdMutex::new(Vec::new())),
        failed: failed.clone(),
        cancelled: cancelled.clone(),
    };
    let handler = ClientHandler::new(
        "cg1",
        "client1",
        "ws1",
        &ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        },
        None,
        Arc::new(sink),
    );
    (handler, failed, cancelled)
}

/// Port of TS `ClientHandler.fail` (client-handler.ts:175-181): LOGS
/// `view-syncer closing connection with error: ${String(e)}` at
/// `getLogLevel(e)`, then forwards to `downstream.fail` — and does NOT
/// cancel.
///
/// Mutation test: the forwarding half passed both before and
/// after — rust's `fail` had the `downstream.fail` call and no log at all,
/// so this test could not have caught the missing line. The level is WARN
/// because the only caller, `send_query_transform_failed_error`, passes
/// `new ProtocolError(error)` in TS (client-handler.ts:368), which takes
/// `getLogLevel`'s `isProtocolError` branch. Drop the `tracing::warn!` and
/// the log assertions fail; move it to `error!` and the level assertion
/// fails.
#[test]
fn fail_logs_at_the_ts_level_then_forwards_to_downstream_fail_only() {
    use std::sync::{Arc as StdArc, Mutex as CapMutex};

    #[derive(Clone)]
    struct Cap(StdArc<CapMutex<Vec<u8>>>);
    impl std::io::Write for Cap {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Cap {
        type Writer = Cap;
        fn make_writer(&'a self) -> Cap {
            self.clone()
        }
    }

    let (handler, failed, cancelled) = make_handler_observing_lifecycle();
    let buf = StdArc::new(CapMutex::new(Vec::<u8>::new()));
    let sub = tracing_subscriber::fmt()
        .with_writer(Cap(buf.clone()))
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    tracing::subscriber::with_default(sub, || handler.fail("boom"));
    let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();

    assert!(
        logged.contains("view-syncer closing connection with error: boom"),
        "TS logs `String(e)` verbatim (client-handler.ts:176); got: {logged}"
    );
    assert!(
        logged.contains("WARN"),
        "a ProtocolError is `getLogLevel` -> warn, not error; got: {logged}"
    );
    assert_eq!(*failed.lock().unwrap(), Some("boom".to_string()));
    assert!(!*cancelled.lock().unwrap(), "fail must not cancel");
}

// Port of TS client-handler.ts:183 `close`: invokes downstream.cancel (a
// clean close), NOT fail.
#[test]
fn close_forwards_to_downstream_cancel_not_fail() {
    let (handler, failed, cancelled) = make_handler_observing_lifecycle();
    handler.close("done");
    assert!(*cancelled.lock().unwrap(), "close must cancel");
    assert_eq!(*failed.lock().unwrap(), None, "close must not fail");
}

fn make_row_patch_put(table: &str, contents: Value) -> PatchToVersion {
    PatchToVersion {
        patch: Patch::Row(RowPatch::Put {
            id: RowID {
                schema: "s".to_string(),
                table: table.into(),
                row_key: Map::new(),
            },
            contents: std::sync::Arc::new(contents),
        }),
        to_version: CVRVersion {
            state_version: "v2".to_string(),
            config_version: Some(1),
        },
    }
}

/// Two overlapping pokes on ONE client must FAIL the second, never block.
///
/// `poke_chain` is rust-only mutual exclusion (INVENTIONS.md I-15): TS
/// keeps `pokeStarted`/`body` as locals in `startPoke`
/// (client-handler.ts:208-210) and relies on the view-syncer `#lock` plus a
/// single JS thread, so it has nothing to contend. Rust's pokers all run on
/// the serial CG thread, which is exactly why the old
/// `while compare_exchange(..).is_err() { thread::yield_now() }` could
/// never make progress: the holder is another `PokeHandler` on THIS thread
/// and only an `.await` point could let it run. A contended chain wedged
/// the client-group thread at 100% CPU indefinitely — no ack, no poke, for
/// every client in the group.
///
/// Mutation test, but by HANGING rather than failing: restore the unbounded
/// `while` loop in `acquire_chain` and this test never returns (verified by
/// running it with a kill timeout). That hang IS the bug.
#[test]
fn a_second_overlapping_poke_fails_instead_of_wedging_the_thread() {
    let (handler, _messages) = make_handler();
    let patch = make_row_patch_put("issue", serde_json::json!({"id": "i1"}));
    let version = CVRVersion {
        state_version: "v2".to_string(),
        config_version: Some(1),
    };

    // Two live pokers over the same client — the state a duplicated poke
    // target produced (see `get_clients` de-duplication).
    let first = handler.start_poke(version.clone());
    let second = handler.start_poke(version.clone());

    // The first admits its patch and TAKES the chain.
    first
        .add_patch(&patch)
        .expect("the first poke must proceed normally");

    // The second cannot, and must say so instead of spinning. `add_patch`
    // has already routed this to `downstream.fail`, so the client
    // reconnects and rehydrates — recoverable, unlike a wedged thread.
    let err = second
        .add_patch(&patch)
        .expect_err("the second overlapping poke must fail, not block");
    assert!(
        err.contains("poke chain already held"),
        "the failure must name the chain contention; got {err:?}"
    );

    // The failed poker must not have claimed the chain, so the FIRST
    // poker's release is still the one that matters.
    drop(second);
    assert!(
        handler.poke_chain.load(AtomicOrdering::SeqCst),
        "dropping the poker that never acquired must not release the chain \
             out from under the holder"
    );
    drop(first);
    assert!(
        !handler.poke_chain.load(AtomicOrdering::SeqCst),
        "the holder's drop must release the chain"
    );
}

#[test]
fn make_row_patch_rejects_unsafe_integer() {
    // Safe integers (<= MAX_SAFE_INTEGER) pass through unchanged.
    let safe = RowPatch::Put {
        id: RowID {
            schema: "s".into(),
            table: "t".into(),
            row_key: Map::new(),
        },
        contents: std::sync::Arc::new(
            serde_json::json!({"id": "1", "big": 9_007_199_254_740_991_i64}),
        ),
    };
    assert!(make_row_patch(&safe).is_ok());

    // A column beyond the safe range (e.g. a snowflake id) must be rejected
    // — matching TS ensureSafeJSON, which throws to fail the connection
    // rather than let the JS client silently truncate the value.
    let unsafe_i = RowPatch::Put {
        id: RowID {
            schema: "s".into(),
            table: "t".into(),
            row_key: Map::new(),
        },
        contents: std::sync::Arc::new(
            serde_json::json!({"id": "1", "big": 9_007_199_254_740_993_u64}),
        ),
    };
    let err = make_row_patch(&unsafe_i).unwrap_err();
    assert!(err.contains("exceeds safe Number range"), "got: {err}");
}

/// F-CH-1: TS `makeRowPatch` del runs `v.parse(id,
/// primaryKeyValueRecordSchema)` (client-handler.ts:434) — rowKey values
/// must be string|number|boolean; null/nested values THROW instead of
/// reaching the client. Pre-fix, Rust passed them through (proven by
/// temp-revert: the unwrap_err below panicked on Ok).
#[test]
fn make_row_patch_del_rejects_non_primitive_row_key() {
    let mut row_key = Map::new();
    row_key.insert("id".to_string(), serde_json::json!("1"));
    row_key.insert("bad".to_string(), Value::Null);
    let del = RowPatch::Del {
        id: RowID {
            schema: "s".into(),
            table: "t".into(),
            row_key,
        },
    };
    let err = make_row_patch(&del).unwrap_err();
    assert!(err.contains("not a primary key value"), "got: {err}");

    // Primitive-only keys still pass (bool/number/string all legal).
    let mut ok_key = Map::new();
    ok_key.insert("id".to_string(), serde_json::json!(7));
    ok_key.insert("flag".to_string(), serde_json::json!(true));
    let ok = RowPatch::Del {
        id: RowID {
            schema: "s".into(),
            table: "t".into(),
            row_key: ok_key,
        },
    };
    assert!(make_row_patch(&ok).is_ok());
}

#[test]
fn test_noop_poke_sends_nothing() {
    let (handler, messages) = make_handler();
    let v2 = CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    };
    // Set base version to v2 (client is caught up).
    *handler.base_version.lock().unwrap() = Some(v2.clone());
    // The first poke on connect is forced (an empty poke) even when caught
    // up — see `force_initial_poke`. Consume it, then verify a *subsequent*
    // caught-up poke is a true NOOP.
    handler.start_poke(v2.clone()).end(v2.clone()).unwrap();
    messages.lock().unwrap().clear();

    let poke = handler.start_poke(v2.clone());
    poke.end(v2).unwrap();
    assert!(messages.lock().unwrap().is_empty());
}

/// The client is AHEAD of the tentative version (Greater case): the
/// returned handler must be a true NOOP even when `end` is called with a
/// final version different from the client's base. Before the fix it was a
/// live handler with `baseCookie: None` — the mismatched `end` emitted a
/// fabricated from-scratch `pokeStart {baseCookie: null}` + `pokeEnd` and
/// REGRESSED the client's cookie. TS returns an object whose
/// addPatch/end/cancel are empty functions (client-handler.ts).
/// Mutation test: `already caught up, not sending poke.` is TS
/// `startPoke`'s line (client-handler.ts:196), compared against the
/// TENTATIVE version. TS's `end` returns SILENTLY in the equivalent
/// situation (`return; // Nothing changed and nothing was sent.`,
/// client-handler.ts:319-325).
///
/// Rust had it on the `end` site only. Because rust's poker is lazy, that
/// branch fires on EVERY no-change advance for every caught-up client,
/// while TS's `startPoke` branch does not (the advance's tentative version
/// is the new stateVersion, strictly ahead of every client base). Measured
/// on the 60-minute prod-replay: 1,718,897 of these INFO lines on rust
/// against 2,983 on TS — 576x the JSON log serialization on the serving
/// path, for byte-identical (empty) client output.
///
/// Move the `tracing::info!` back to `end` and the second half fails; drop
/// it from `start_poke` and the first half fails.
#[test]
fn caught_up_is_logged_at_start_poke_not_at_end() {
    use std::sync::{Arc as StdArc, Mutex as CapMutex};

    #[derive(Clone)]
    struct Cap(StdArc<CapMutex<Vec<u8>>>);
    impl std::io::Write for Cap {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Cap {
        type Writer = Cap;
        fn make_writer(&'a self) -> Cap {
            self.clone()
        }
    }

    const LINE: &str = "already caught up, not sending poke.";
    let v2 = CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    };
    let v3 = CVRVersion {
        state_version: "v3".to_string(),
        config_version: None,
    };

    let capture = |f: &dyn Fn()| -> String {
        let buf = StdArc::new(CapMutex::new(Vec::<u8>::new()));
        let sub = tracing_subscriber::fmt()
            .with_writer(Cap(buf.clone()))
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        tracing::subscriber::with_default(sub, f);
        String::from_utf8(buf.lock().unwrap().clone()).unwrap()
    };

    // (a) startPoke's NOOP branch — TENTATIVE (v2) is not ahead of the
    //     base (v2) and the client has been poked: TS logs here.
    let (handler, _messages) = make_handler();
    *handler.base_version.lock().unwrap() = Some(v2.clone());
    handler.start_poke(v2.clone()).end(v2.clone()).unwrap(); // consume the forced initial poke
    let at_start = capture(&|| {
        handler.start_poke(v2.clone()).end(v2.clone()).unwrap();
    });
    assert!(
        at_start.contains(LINE),
        "TS logs the caught-up line in startPoke (client-handler.ts:196); got: {at_start}"
    );

    // (b) A poke STARTED ahead of the base (tentative v3 > base v2) that
    //     ends with nothing to send (final == base) — the lazy no-change
    //     advance, i.e. the shape that produced 1.7M lines. TS's `end`
    //     returns silently.
    let (handler2, messages2) = make_handler();
    *handler2.base_version.lock().unwrap() = Some(v2.clone());
    handler2.start_poke(v2.clone()).end(v2.clone()).unwrap(); // consume the forced initial poke
    messages2.lock().unwrap().clear();
    let at_end = capture(&|| {
        handler2.start_poke(v3.clone()).end(v2.clone()).unwrap();
    });
    assert!(
        !at_end.contains(LINE),
        "TS's end() returns SILENTLY when nothing changed \
             (client-handler.ts:319-325); rust must not log there. Got: {at_end}"
    );
    assert!(
        messages2.lock().unwrap().is_empty(),
        "and still no frame is sent — the fix is log placement, not behavior"
    );
}

#[test]
fn test_noop_poke_inert_on_mismatched_end() {
    let (handler, messages) = make_handler();
    let v2 = CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    };
    let v3 = CVRVersion {
        state_version: "v3".to_string(),
        config_version: None,
    };
    *handler.base_version.lock().unwrap() = Some(v3.clone());

    let poke = handler.start_poke(v2.clone());
    poke.end(v2).unwrap();
    assert!(
        messages.lock().unwrap().is_empty(),
        "NOOP handler must send nothing on a mismatched end"
    );
    assert_eq!(
        *handler.base_version.lock().unwrap(),
        Some(v3.clone()),
        "NOOP end must not regress the client's base version"
    );

    // The NOOP end must not have consumed `ever_poked`: the first REAL
    // caught-up poke is still forced.
    handler.start_poke(v3.clone()).end(v3).unwrap();
    let msgs = messages.lock().unwrap();
    assert_eq!(msgs.len(), 2, "forced initial poke still fires");
    assert_eq!(msgs[0][0], "pokeStart");
    assert_eq!(msgs[1][0], "pokeEnd");
}

#[test]
fn test_initial_poke_forced_when_caught_up() {
    // Even when the client connects already caught up (base == tentative),
    // the FIRST poke sends an empty pokeStart/pokeEnd so the client learns
    // its got-queries state has been reconciled with the server. Mirrors TS
    // ClientHandler `#everPoked` (zero/v1.9.0).
    let (handler, messages) = make_handler();
    let v2 = CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    };
    *handler.base_version.lock().unwrap() = Some(v2.clone());
    handler.start_poke(v2.clone()).end(v2).unwrap();
    let msgs = messages.lock().unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0][0], "pokeStart");
    assert_eq!(msgs[1][0], "pokeEnd");
}

#[test]
fn test_empty_poke_sends_start_and_end() {
    let (handler, messages) = make_handler();
    let poke = handler.start_poke(CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    });
    poke.end(CVRVersion {
        state_version: "v2".to_string(),
        config_version: Some(1),
    })
    .unwrap();
    let msgs = messages.lock().unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0][0], "pokeStart");
    assert_eq!(msgs[1][0], "pokeEnd");
}

#[test]
fn test_poke_flushes_at_100_parts() {
    let (handler, messages) = make_handler();
    let poke = handler.start_poke(CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    });
    // Add 101 row patches
    for _ in 0..101 {
        poke.add_patch(&make_row_patch_put("t1", serde_json::json!({"id": 1})))
            .unwrap();
    }
    poke.end(CVRVersion {
        state_version: "v2".to_string(),
        config_version: Some(1),
    })
    .unwrap();
    let msgs = messages.lock().unwrap();
    // pokeStart + 1 pokePart (at 100) + 1 pokePart (remaining 1) + pokeEnd
    assert_eq!(msgs.len(), 4);
    assert_eq!(msgs[0][0], "pokeStart");
    assert_eq!(msgs[1][0], "pokePart");
    assert_eq!(msgs[2][0], "pokePart");
    assert_eq!(msgs[3][0], "pokeEnd");
}

/// Load-bearing invariant: client JSON is parsed via serde, whose default
/// 128-level recursion limit rejects pathologically-nested input BEFORE any
/// of our unguarded recursive walks (AST transform, hash, estimate) run. If
/// a future change ever calls `disable_recursion_limit()` on a client path,
/// this test fails — a deliberate tripwire.
#[test]
fn serde_rejects_deeply_nested_client_json() {
    // 200 open brackets ≫ serde's 128 default depth.
    let deep = "[".repeat(200) + &"]".repeat(200);
    let parsed = serde_json::from_str::<Value>(&deep);
    assert!(
        parsed.is_err(),
        "serde must reject >128-deep JSON at parse time (recursion-limit tripwire)"
    );
}

/// The estimator must not stack-overflow even on nesting deeper than its
/// depth cap (defense-in-depth: a Value built programmatically, bypassing
/// the parser). Build the tree directly (not via parse) to exceed the cap.
#[test]
fn estimate_json_bytes_is_depth_bounded() {
    let mut v = serde_json::json!(0);
    for _ in 0..1000 {
        v = Value::Array(vec![v]);
    }
    // Must return without overflowing; value is not asserted (accounting only).
    let _ = estimate_json_bytes(&v);
}

#[test]
fn estimate_json_bytes_tracks_serialized_size() {
    // The estimate must stay within a small factor of the real serialized
    // length across scalars, unicode, and nested JSON — it's an accounting
    // approximation, not exact, but must never wildly under/over-count.
    let samples = [
        serde_json::json!({"id": "1", "n": 42, "b": true, "z": null}),
        serde_json::json!({"name": "héllo wörld", "tags": ["a", "b", "c"]}),
        serde_json::json!({"nested": {"deep": {"arr": [1, 2, 3, {"k": "v"}]}}}),
    ];
    for s in samples {
        let actual = serde_json::to_string(&s).unwrap().len();
        let est = estimate_json_bytes(&s);
        assert!(
            est as f64 >= actual as f64 * 0.5 && est as f64 <= actual as f64 * 2.0,
            "estimate {est} not within 0.5x–2x of actual {actual} for {s}"
        );
    }
}

#[test]
fn poke_flushes_early_on_byte_cap() {
    // Large rows must flush into several parts BEFORE the 100-count
    // threshold — bounding single-frame size. Deterministic against the
    // shipped 256KB default cap (no env/OnceLock dependency): 8 rows of
    // ~50KB ≈ 400KB, well over the cap but only 8 rows (≪ 100).
    let (handler, messages) = make_handler();
    let poke = handler.start_poke(CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    });
    let big = "x".repeat(50 * 1024);
    for _ in 0..8 {
        poke.add_patch(&make_row_patch_put("t1", serde_json::json!({"blob": big})))
            .unwrap();
    }
    poke.end(CVRVersion {
        state_version: "v2".to_string(),
        config_version: Some(1),
    })
    .unwrap();
    let msgs = messages.lock().unwrap();
    let parts = msgs.iter().filter(|m| m[0] == "pokePart").count();
    // 8 rows never reach the 100-count path, so ≥2 parts proves the byte
    // cap flushed mid-stream. (256KB / ~50KB ⇒ a flush around row 5.)
    assert!(
        parts >= 2,
        "byte cap must split ~400KB into multiple parts, got {parts}"
    );
}

#[test]
fn single_oversized_row_still_ships() {
    // A single row larger than the cap is never split or dropped — it ships
    // as one oversized part.
    let (handler, messages) = make_handler();
    let poke = handler.start_poke(CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    });
    let huge = "y".repeat(300 * 1024); // > 256KB default cap
    poke.add_patch(&make_row_patch_put("t1", serde_json::json!({"blob": huge})))
        .unwrap();
    poke.end(CVRVersion {
        state_version: "v2".to_string(),
        config_version: Some(1),
    })
    .unwrap();
    let msgs = messages.lock().unwrap();
    assert!(
        msgs.iter().any(|m| m[0] == "pokePart"),
        "oversized single row must still be delivered as a pokePart"
    );
}

#[test]
fn test_poke_lmids_interception() {
    let (handler, messages) = make_handler();
    let poke = handler.start_poke(CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    });
    poke.add_patch(&make_row_patch_put(
        "app_0.clients",
        serde_json::json!({
            "clientGroupID": "cg1",
            "clientID": "clientA",
            "lastMutationID": 42,
        }),
    ))
    .unwrap();
    poke.end(CVRVersion {
        state_version: "v2".to_string(),
        config_version: Some(1),
    })
    .unwrap();
    let msgs = messages.lock().unwrap();
    // pokeStart + pokePart (with lastMutationIDChanges) + pokeEnd
    assert_eq!(msgs.len(), 3);
    let part = &msgs[1][1];
    assert!(part.get("lastMutationIDChanges").is_some());
    assert_eq!(part["lastMutationIDChanges"]["clientA"], 42);
}

/// F-CH-1: TS `#updateLMIDs` parses the clients row against lmidRowSchema
/// (client-handler.ts:379-383) — a row missing `lastMutationID` (or with
/// wrong types) THROWS, failing the poke; it is not silently skipped.
/// Pre-fix, Rust's `if let (Some, Some, Some)` swallowed it (proven by
/// temp-revert: add_patch returned Ok).
#[test]
fn test_poke_lmids_malformed_clients_row_fails() {
    let (handler, _messages) = make_handler();
    let poke = handler.start_poke(CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    });
    let err = poke
        .add_patch(&make_row_patch_put(
            "app_0.clients",
            serde_json::json!({
                "clientGroupID": "cg1",
                "clientID": "clientA",
                // lastMutationID missing
            }),
        ))
        .unwrap_err();
    assert!(err.contains("lastMutationID"), "got: {err}");
}

#[test]
fn test_mutations_patch_shape() {
    let (handler, messages) = make_handler();
    let poke = handler.start_poke(CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    });
    poke.add_patch(&make_row_patch_put(
        "app_0.mutations",
        serde_json::json!({
            "clientGroupID": "cg1",
            "clientID": "clientA",
            "mutationID": 5,
            "result": {"ok": true},
        }),
    ))
    .unwrap();
    poke.end(CVRVersion {
        state_version: "v2".to_string(),
        config_version: Some(1),
    })
    .unwrap();
    let msgs = messages.lock().unwrap();
    let part = &msgs[1][1];
    let mp = part.get("mutationsPatch").unwrap();
    assert_eq!(mp[0]["op"], "put");
    assert_eq!(mp[0]["mutation"]["id"]["clientID"], "clientA");
    assert_eq!(mp[0]["mutation"]["id"]["id"], 5);
    assert_eq!(mp[0]["mutation"]["result"]["ok"], true);
}

fn make_mutation_del_patch(row_key: Value) -> PatchToVersion {
    PatchToVersion {
        patch: Patch::Row(RowPatch::Del {
            id: RowID {
                schema: "s".to_string(),
                table: "app_0.mutations".into(),
                row_key: row_key.as_object().unwrap().clone(),
            },
        }),
        to_version: CVRVersion {
            state_version: "v2".to_string(),
            config_version: Some(1),
        },
    }
}

/// TS-golden for the mutationsPatch DEL arm (client-handler.ts:267-284):
/// `{op:'del', id:{clientID, id}}` — byte-exact entry shape.
#[test]
fn test_mutations_patch_del_shape() {
    let (handler, messages) = make_handler();
    let poke = handler.start_poke(CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    });
    poke.add_patch(&make_mutation_del_patch(serde_json::json!({
        "clientGroupID": "cg1",
        "clientID": "clientA",
        "mutationID": 7,
    })))
    .unwrap();
    poke.end(CVRVersion {
        state_version: "v2".to_string(),
        config_version: Some(1),
    })
    .unwrap();
    let msgs = messages.lock().unwrap();
    let mp = msgs[1][1].get("mutationsPatch").unwrap();
    assert_eq!(
        mp[0],
        serde_json::json!({"op": "del", "id": {"clientID": "clientA", "id": 7}}),
        "del entry must match the TS shape byte-exactly"
    );
}

/// Port-parity for the TS del-arm asserts (client-handler.ts:268-277):
/// a missing/non-string clientID → 'client id must be a string'; a
/// missing or NEGATIVE mutationID → 'mutation id must be a finite
/// number'. The negative case pins the `id >= 0` guard the rust arm
/// previously lacked (proven failing before the fix).
#[test]
fn test_mutations_patch_del_error_arms() {
    let (handler, _messages) = make_handler();
    let poke = handler.start_poke(CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    });

    let err = poke
        .add_patch(&make_mutation_del_patch(
            serde_json::json!({"mutationID": 7}),
        ))
        .unwrap_err();
    assert!(err.contains("client id must be a string"), "got: {err}");

    let err = poke
        .add_patch(&make_mutation_del_patch(
            serde_json::json!({"clientID": "clientA"}),
        ))
        .unwrap_err();
    assert!(
        err.contains("mutation id must be a finite number"),
        "got: {err}"
    );

    // TS: `assert(!Number.isNaN(id) && Number.isFinite(id) && id >= 0)`
    // — a NEGATIVE mutation id must fail the del, exactly like TS.
    let err = poke
        .add_patch(&make_mutation_del_patch(serde_json::json!({
            "clientID": "clientA",
            "mutationID": -1,
        })))
        .unwrap_err();
    assert!(
        err.contains("mutation id must be a finite number"),
        "got: {err}"
    );
}

#[test]
fn test_cancel_releases_chain() {
    let (handler, messages) = make_handler();
    let poke = handler.start_poke(CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    });
    // Start the poke by adding a patch
    poke.add_patch(&make_row_patch_put("t1", serde_json::json!({"id": 1})))
        .unwrap();
    poke.cancel().unwrap();
    {
        let msgs = messages.lock().unwrap();
        // pokeStart + pokeEnd (cancel)
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[1][0], "pokeEnd");
        assert_eq!(msgs[1][1]["cancel"], true);
    }

    // Verify chain is released — next poke should work
    let poke2 = handler.start_poke(CVRVersion {
        state_version: "v3".to_string(),
        config_version: None,
    });
    poke2
        .end(CVRVersion {
            state_version: "v3".to_string(),
            config_version: Some(1),
        })
        .unwrap();
}

#[test]
fn failed_poke_start_releases_chain() {
    let handler = make_failing_handler("pokeStart");
    let poke = handler.start_poke(CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    });
    assert!(
        poke.end(CVRVersion {
            state_version: "v2".to_string(),
            config_version: Some(1),
        })
        .is_err()
    );
    assert_chain_released(&handler);
}

#[test]
fn failed_poke_part_releases_chain() {
    let handler = make_failing_handler("pokePart");
    let poke = handler.start_poke(CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    });
    poke.add_patch(&make_row_patch_put("t1", serde_json::json!({"id": 1})))
        .unwrap();
    assert!(
        poke.end(CVRVersion {
            state_version: "v2".to_string(),
            config_version: Some(1),
        })
        .is_err()
    );
    assert_chain_released(&handler);
}

#[test]
fn failed_poke_end_and_cancel_release_chain() {
    let handler = make_failing_handler("pokeEnd");
    let poke = handler.start_poke(CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    });
    assert!(
        poke.end(CVRVersion {
            state_version: "v2".to_string(),
            config_version: Some(1),
        })
        .is_err()
    );
    assert_chain_released(&handler);

    let poke = handler.start_poke(CVRVersion {
        state_version: "v3".to_string(),
        config_version: None,
    });
    poke.add_patch(&make_row_patch_put("t1", serde_json::json!({"id": 1})))
        .unwrap();
    assert!(poke.cancel().is_err());
    assert_chain_released(&handler);
}

#[test]
fn multipoker_drops_dead_client_after_first_failure() {
    // One client whose sink always errors, alongside a healthy client. The
    // failing sink must be pushed to at most ONCE across the whole poke (it
    // dies on the first patch's pokeStart), not once per patch — proving the
    // per-poker `dead` short-circuit that mirrors TS's terminal downstream
    // fail. Without it, N patches would produce N pushes + N log lines.
    let pushes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let failing = ClientHandler::new(
        "cg1",
        "bad",
        "ws-bad",
        &ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        },
        None,
        Arc::new(CountingFailSink {
            pushes: pushes.clone(),
        }),
    );
    let (healthy, healthy_msgs) = make_handler();

    let tentative = CVRVersion {
        state_version: "v2".to_string(),
        config_version: Some(1),
    };
    let poker = MultiPoker::new(&[&failing, &healthy], tentative.clone(), "test");

    // Fan out several patches; the failing client dies on the first.
    for i in 0..5 {
        poker.add_patch(&make_row_patch_put("t1", serde_json::json!({"id": i})));
    }
    poker.end(tentative);

    assert_eq!(
        pushes.load(AtomicOrdering::SeqCst),
        1,
        "dead client must be pushed exactly once, not per patch"
    );
    assert!(
        !healthy_msgs.lock().unwrap().is_empty(),
        "healthy client keeps receiving patches after the other dies"
    );
}

/// Mutation test: `any_started()` must report whether a patch was
/// actually SENT, because that is the only condition under which TS's `end()`
/// can raise `Patches were sent but finalVersion ... is not greater than
/// baseVersion` (client-handler.ts:327-334) — the view-syncer gates its
/// discarded-version-bump diagnostic on it. Hard-coding `true` fails the
/// before-patch assertion (the benign case that fired 54x/3.5min in the GKE
/// sandbox); hard-coding `false` fails the after-patch one.
#[test]
fn any_started_tracks_whether_a_patch_was_actually_sent() {
    let (c1, _m1) = make_handler();
    let (c2, _m2) = make_handler();
    let tentative = CVRVersion {
        state_version: "v2".to_string(),
        config_version: Some(1),
    };
    let poker = MultiPoker::new(&[&c1, &c2], tentative, "test");

    assert!(
        !poker.any_started(),
        "no patch admitted yet → no pokeStart went out → TS `pokeStarted` is false"
    );

    poker.add_patch(&make_row_patch_put("t1", serde_json::json!({"id": 1})));
    assert!(
        poker.any_started(),
        "a patch opened the poke (pokeStart emitted) → TS `pokeStarted` is true"
    );
}

/// Port of TS ClientHandler `cancel` fanned across a MultiPoker: once a
/// patch has opened each client's poke (pokeStart), `MultiPoker::cancel`
/// emits a terminal `pokeEnd {cancel:true}` to EVERY live client. Before
/// the patch, `started` is false and cancel is a no-op (PokeHandler::cancel
/// line 415), which is why we add a patch first.
#[test]
fn multipoker_cancel_sends_cancel_frame_to_all_clients() {
    let (c1, m1) = make_handler();
    let (c2, m2) = make_handler();

    let tentative = CVRVersion {
        state_version: "v2".to_string(),
        config_version: Some(1),
    };
    let poker = MultiPoker::new(&[&c1, &c2], tentative, "test");

    // Open each poke (emits pokeStart, sets started=true).
    poker.add_patch(&make_row_patch_put("t1", serde_json::json!({"id": 1})));
    // Then cancel — every live client gets a terminal cancel frame.
    poker.cancel();

    for m in [&m1, &m2] {
        let msgs = m.lock().unwrap();
        let has_cancel = msgs.iter().any(|msg| {
            msg.get(0).and_then(Value::as_str) == Some("pokeEnd")
                && msg
                    .get(1)
                    .and_then(|o| o.get("cancel"))
                    .and_then(Value::as_bool)
                    == Some(true)
        });
        assert!(has_cancel, "each client receives a cancel pokeEnd frame");
    }
}

#[test]
fn patch_assembly_error_releases_chain() {
    let (handler, _messages) = make_handler();
    let poke = handler.start_poke(CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    });
    let malformed_mutation = make_row_patch_put(
        "app_0.mutations",
        serde_json::json!({"mutationID": 1, "result": {"ok": true}}),
    );
    assert!(poke.add_patch(&malformed_mutation).is_err());
    assert_chain_released(&handler);
}

#[test]
fn invalid_final_version_after_patches_releases_chain() {
    let (handler, _messages) = make_handler();
    *handler.base_version.lock().unwrap() = Some(CVRVersion {
        state_version: "v1".to_string(),
        config_version: None,
    });
    let poke = handler.start_poke(CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    });
    poke.add_patch(&make_row_patch_put("t1", serde_json::json!({"id": 1})))
        .unwrap();

    // Once frames have started, ending at the current base is invalid and
    // must not strand the per-client serialization guard.
    assert!(
        poke.end(CVRVersion {
            state_version: "v1".to_string(),
            config_version: None,
        })
        .is_err()
    );
    assert_chain_released(&handler);
}

/// I-21 (rust-only): a `pokePart` also flushes when its ESTIMATED
/// serialized size crosses `DEFAULT_POKE_PART_MAX_BYTES` (256 KiB).
///
/// TS flushes on ONE condition — `if (++partCount >=
/// PART_COUNT_FLUSH_THRESHOLD)` with the threshold at 100
/// (client-handler.ts:109,294). It has no byte accounting at all, so the
/// byte cap, `estimate_row_patch_bytes` and `estimate_json_bytes` have no
/// TS twin; the cap is ON by default, so it moves production frame
/// boundaries. Registered as INVENTIONS.md I-21.
///
/// The client-observable contract an invention may not break: splitting is
/// CONTENT-PRESERVING. More, smaller `pokePart` frames are protocol-legal,
/// but every patch must still arrive exactly once and in the order it was
/// added — the cap may only choose where the boundaries fall.
///
/// 20 rows of ~40 KiB keep the part COUNT well under 100, so a split here
/// can only come from the byte cap.
///
/// Mutation test: set `DEFAULT_POKE_PART_MAX_BYTES` to 0 (or drop the
/// `state.body_est_bytes >= byte_cap` term from the flush condition) and
/// all 20 rows ride in ONE part, failing the split assertion; drop the
/// `state.body_est_bytes +=` accumulation and the same assertion fails.
#[test]
fn a_large_row_burst_flushes_on_the_byte_cap_and_preserves_every_patch() {
    const ROWS: usize = 20;
    const PAYLOAD_BYTES: usize = 40 * 1024;

    let (handler, messages) = make_handler();
    let to_version = CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    };
    let poke = handler.start_poke(to_version.clone());
    for i in 0..ROWS {
        let mut row_key = Map::new();
        row_key.insert("id".to_string(), serde_json::json!(i));
        poke.add_patch(&PatchToVersion {
            patch: Patch::Row(RowPatch::Put {
                id: RowID {
                    schema: "s".to_string(),
                    table: "t".into(),
                    row_key,
                },
                contents: std::sync::Arc::new(serde_json::json!({
                    "id": i,
                    "blob": "x".repeat(PAYLOAD_BYTES),
                })),
            }),
            to_version: to_version.clone(),
        })
        .unwrap();
    }
    poke.end(to_version).unwrap();

    let msgs = messages.lock().unwrap();
    let parts: Vec<&Value> = msgs
        .iter()
        .filter(|m| m.get(0).and_then(|t| t.as_str()) == Some("pokePart"))
        .collect();

    assert!(
        parts.len() > 1,
        "{} rows of {PAYLOAD_BYTES} bytes must cross the 256 KiB byte cap \
             and split into several parts; got {} part(s)",
        ROWS,
        parts.len()
    );
    assert!(
        parts.len() < ROWS,
        "the cap must batch rows, not emit one part per row; got {} parts \
             for {ROWS} rows",
        parts.len()
    );

    // Content preservation: every row exactly once, in the order added.
    let mut ids: Vec<i64> = Vec::new();
    for part in &parts {
        let Some(rows) = part[1].get("rowsPatch").and_then(|r| r.as_array()) else {
            continue;
        };
        for rp in rows {
            // A `put` carries the full contents under `value` (the key
            // included); a `del` carries only `id.rowKey`.
            let id = rp
                .get("value")
                .and_then(|v| v.get("id"))
                .or_else(|| rp.get("id").and_then(|i| i.get("rowKey"))?.get("id"))
                .and_then(|v| v.as_i64())
                .unwrap_or_else(|| panic!("row patch missing its key: op={:?}", rp.get("op")));
            ids.push(id);
        }
    }
    assert_eq!(
        ids,
        (0..ROWS as i64).collect::<Vec<_>>(),
        "splitting on the byte cap must deliver every patch exactly once, \
             in the order it was added — only the part BOUNDARIES may differ"
    );
}

#[test]
fn test_end_advances_base_version() {
    let (handler, _messages) = make_handler();
    let poke = handler.start_poke(CVRVersion {
        state_version: "v2".to_string(),
        config_version: None,
    });
    let final_v = CVRVersion {
        state_version: "v2".to_string(),
        config_version: Some(1),
    };
    poke.end(final_v.clone()).unwrap();
    let bv = handler.version();
    assert_eq!(bv, Some(final_v));
}

#[test]
fn test_patches_below_base_version_skipped() {
    let (handler, messages) = make_handler();
    // Set base version
    {
        let mut bv = handler.base_version.lock().unwrap();
        *bv = Some(CVRVersion {
            state_version: "v2".to_string(),
            config_version: Some(1),
        });
    }
    let poke = handler.start_poke(CVRVersion {
        state_version: "v3".to_string(),
        config_version: None,
    });
    // This patch has to_version <= base_version, should be skipped
    poke.add_patch(&PatchToVersion {
        patch: Patch::Row(RowPatch::Put {
            id: RowID {
                schema: "s".to_string(),
                table: "t".into(),
                row_key: Map::new(),
            },
            contents: std::sync::Arc::new(serde_json::json!({"id": 1})),
        }),
        to_version: CVRVersion {
            state_version: "v2".to_string(),
            config_version: Some(1),
        },
    })
    .unwrap();
    poke.end(CVRVersion {
        state_version: "v3".to_string(),
        config_version: None,
    })
    .unwrap();
    let msgs = messages.lock().unwrap();
    // Only pokeStart + pokeEnd, no pokePart
    assert_eq!(msgs.len(), 2);
}

/// Port of TS `sendQueryTransformFailedError` (client-handler.ts:368):
/// `this.fail(new ProtocolError(error))`. The ProtocolError body reaches the
/// client as EXACTLY one `["error", body]` frame, and the downstream is
/// failed terminally (`downstream.fail`, NOT `cancel` — `close()` is the
/// cancel path, client-handler.ts:183). Caller: rust-syncer sync_engine.rs.
#[test]
fn send_query_transform_failed_error_emits_exact_error_frame_and_fails() {
    let messages = Arc::new(StdMutex::new(Vec::new()));
    let failed = Arc::new(StdMutex::new(None));
    let cancelled = Arc::new(StdMutex::new(false));
    let sink = MockSink {
        messages: messages.clone(),
        failed: failed.clone(),
        cancelled: cancelled.clone(),
    };
    let handler = ClientHandler::new(
        "cg1",
        "client1",
        "ws1",
        &ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        },
        None,
        Arc::new(sink),
    );

    // A TransformFailedBody-shaped error (zero-protocol error body).
    let body = serde_json::json!({
        "kind": "TransformFailed",
        "origin": "zero-cache",
        "message": "failed to transform query",
        "queryHashes": ["qh1"],
    });
    handler.send_query_transform_failed_error(&body);

    // Exactly one frame, byte-shape ["error", body] — not wrapped, not
    // re-keyed, no other frames before/after.
    let msgs = messages.lock().unwrap();
    assert_eq!(
        *msgs,
        vec![serde_json::json!(["error", body])],
        "wire frame must be exactly [\"error\", body]"
    );
    // TS fail() puts the subscription in a terminal failed state.
    assert_eq!(
        failed.lock().unwrap().as_deref(),
        Some("query transform failed"),
        "downstream.fail must fire"
    );
    assert!(
        !*cancelled.lock().unwrap(),
        "must fail the downstream, not cancel it (cancel is the close() path)"
    );
}

#[test]
fn test_normalize_mutation_result_string() {
    let row = serde_json::json!({
        "clientID": "c1",
        "mutationID": 1,
        "result": "{\"ok\":true}",
    });
    let normalized = normalize_mutation_result(&row);
    assert!(normalized.get("result").unwrap().is_object());
    assert_eq!(normalized["result"]["ok"], true);
}

#[test]
fn test_normalize_mutation_result_object() {
    let row = serde_json::json!({
        "clientID": "c1",
        "mutationID": 1,
        "result": {"ok": true},
    });
    let normalized = normalize_mutation_result(&row);
    assert!(normalized.get("result").unwrap().is_object());
}
