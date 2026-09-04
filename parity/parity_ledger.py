#!/usr/bin/env python3
"""
Deterministic TS<->Rust parity ledger.

Given a crate spec (a set of Rust files and the TS files they were ported from),
extract every symbol from both sides, normalize names across the
camelCase<->snake_case boundary, and print exactly what is NOT one-to-one:

  * MATCHED   - symbol exists on both sides (name-normalized)
  * TS-ONLY   - exists in TS, no Rust counterpart  => candidate "not ported"
  * RUST-ONLY - exists in Rust, no TS origin       => candidate "invented / renamed / drift"

This is a *name-level* ledger. It cannot judge whether the bodies agree
("function task wise") - it narrows the surface so a human/agent only has to
deep-read the deltas instead of thousands of lines. Signatures are captured so
arity / async / return differences are eyeballable on matched rows too.

Usage:
    python3 parity/parity_ledger.py cvr > parity/LEDGER-cvr.md
"""

import os
import re
import sys
from collections import defaultdict

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------------------
# Crate specs: which Rust files map to which TS files.
# TS files are the ORIGIN; Rust files are the PORT.
# ---------------------------------------------------------------------------
V = "packages/zero-cache/src/services/view-syncer"
ZC = "packages/zero-cache/src"
CRATES = {
    "cvr": {
        "rust_dir": "packages/rust-cvr/src",
        # TS origin files that were actually ported INTO rust-cvr.
        # NOTE: query-covering.ts is intentionally excluded — it is ported into
        # packages/rust-syncer/src/query_covering.rs, not rust-cvr.
        # shared/src helpers that carry BEHAVIOR are mirrored under rust-cvr
        # `shared/` (rule 3; rust-ivm/syncer reach them through the crate dep).
        "ts_label_rewrites": [("packages/shared/src/", "shared/")],
        "ts_files": [
            "packages/shared/src/string-compare.ts",
            f"{V}/cvr.ts",
            f"{V}/cvr-store.ts",
            f"{V}/row-record-cache.ts",
            f"{V}/row-set-signature.ts",
            f"{V}/ttl-clock.ts",
            f"{V}/client-handler.ts",
            f"{V}/schema/cvr.ts",
            f"{V}/schema/types.ts",
        ],
        # Core files carry the sync/merge algorithms — a missing behavioral
        # symbol here is HIGH risk. schema/* is DDL+zod => structural, LOW risk.
        "core_ts": {"cvr.ts", "cvr-store.ts", "row-record-cache.ts",
                    "client-handler.ts", "row-set-signature.ts", "ttl-clock.ts"},
        # Rust files with no 1:1 TS origin (infra / idiom) => Rust-only here is
        # expected, not drift.
        "infra_rust": {"hash.rs", "tracer.rs", "otel_metrics.rs", "live_count.rs",
                       "parity_check.rs", "row_key.rs", "lib.rs",
                       "change_processor.rs", "ttl.rs", "shards.rs",
                       # module declaration file for the shared/ mirror dir
                       "shared.rs"},
        # TS files that are pure structure (DDL builders + zod codecs). Their fns
        # became inline SQL / serde derives, so they are NOT behavioral gaps.
        "structural_ts": {"schema/cvr.ts", "schema/types.ts"},
        # Confirmed resolutions the fuzzy pass can't infer (logic became inline SQL,
        # a TS fn maps to a Rust enum, or the conversion is identity). Keys are
        # canon() of the TS name.
        "aliases": {
            "convertttlvalues": ("INLINED", "cvr_store.rs upsert SQL: ttl/1000 + null-on-negative"),
            "getttlclock": ("INLINED", "cvr_store.rs SELECT instances.\"ttlClock\" (load path)"),
            "updatettlclock": ("INLINED", "cvr_store.rs UPDATE instances SET lastActive,ttlClock"),
            "ttlclockasnumber": ("IDENTITY", "TTLClock = i64 (ttl_clock.rs); no conversion"),
            "ttlclockfromnumber": ("IDENTITY", "TTLClock = i64 (ttl_clock.rs); no conversion"),
            "cvrerrorkind": ("CVRStoreError enum (cvr_store.rs)", "fn→enum discriminant"),
            "assert": ('Rust assert! macro', 'idiom (D-16)'),
            # TS error classes fold into the CVRStoreError enum; the extractor
            # does not emit enum variants as symbols, so these fuzzy-match noise.
            "clientnotfounderror": ("CVRStoreError::ClientNotFound (cvr_store.rs:47)",
                                    "TS error class → Rust enum variant"),
            "rowsversionbehinderror": ("CVRStoreError::RowsVersionBehind (cvr_store.rs:49)",
                                       "TS error class → Rust enum variant"),
            # Exact 1:1 type alias the extractor skips (it does not emit `type` aliases).
            "rowsetsignatureprovider": ("RowSetSignatureProvider type (cvr.rs:277)",
                                        "exact type alias; extractor skips `type` decls"),
            # valita schema object → serde struct (same shape, `*Schema` suffix dropped).
            "basequeryrecordschema": ("BaseQueryRecord struct (schema/types.rs:338)",
                                      "valita schema → serde struct"),
            # Snapshot-read version guard: rust inlines it into both catchup
            # paths (cvr_store.rs catchup_config_patches ~:1554 and
            # row_record_cache.rs catchup_row_patches), doc-cited to
            # cvr-store.ts:1348/:743-745 incl. the missing-instance-row →
            # EMPTY_CVR_VERSION → ConcurrentModification branch.
            "checkversion": ("INLINED cvr_store.rs catchup version guard",
                             "plain-SELECT re-check of instances.version vs `current`"),
            # Private #-methods (surfaced by the #-method extractor fix,
            # 2026-08-29). Each verified against the rust doc-comment citation.
            "checkversionandownership": ("INLINED cvr_store.rs flush_internal (:700)",
                                         "doc-cited version+ownership guard; Err rolls back tx"),
            "deleteunreferencedrow": ("change_processor.rs delete_unreferenced_rows",
                                      "renamed plural + relocated (doc-cited :201)"),
            "ensureloaded": ("INLINED row_record_cache.rs (:239)", "doc-cited lazy load"),
            "flushdesires": ("INLINED cvr_store.rs flush_internal desires upsert (:959)",
                             "doc-cited"),
            "flushqueries": ("INLINED cvr_store.rs flush_internal queries upsert (:835)",
                             "doc-cited"),
            "lookuprowsforexecutedandremovedqueries": ("INLINED cvr.rs (:1199)", "doc-cited"),
            "updatequeryfields": ("INLINED cvr_store.rs queries json_to_recordset upsert",
                                  "patchVersion/transformationHash/-Version columns"),
        },
        # 2026-08-29 first pin (was unenforced): 3 = census constants +
        # recordSyncFlushStats in otel_metrics.rs + seq_replay SCHEMA copy.
        # 3→5 same day: the #-method extractor surfaced #recordLoad and
        # #recordAsyncFlushStats — same sanctioned otel_metrics.rs counter
        # family as recordSyncFlushStats (infra_rust).
        "max_misfiled": 5,
    },
    "ivm": {
        "rust_dir": "packages/rust-ivm/src",
        "ts_label_prefix": "zql/src/",
        # zqlite/ TS files ported into rust-ivm/src/sqlite/ — label them under
        # `sqlite/` so they mirror-match the rust files (see ts_label).
        "ts_label_rewrites": [("zqlite/src/", "sqlite/")],
        # rust-ivm ports the ZQL IVM engine: the operators (ivm/), the query
        # builder (builder/ + query/), and the query planner (planner/). mutate/
        # is client-side CRUD, not ported into the engine crate.
        "ts_files": [
            "packages/zql/src/ivm/",
            "packages/zql/src/builder/",
            "packages/zql/src/planner/",
            # query/ IS in remit — ~12 of its files are ported into builder/
            # (query_delegate/registry/internals/expression/named/ttl/...). Only
            # the pure client-fluent + TS type-level files have no runtime to
            # port; excluded below so they don't read as false behavioral gaps.
            "packages/zql/src/query/",
            # zqlite/ is the SQLite-backed half of the engine — ported 1:1 into
            # rust-ivm/src/sqlite/ (table_source.rs ← table-source.ts, db.rs ←
            # db.ts, sqlite_cost_model.rs ← sqlite-cost-model.ts, …). WITHOUT
            # this origin the sqlite/*.rs files read as "new/invention" and
            # their symbols (fetch/connect/get_row/…) FALSELY collided with
            # zql/ivm/memory-source.ts, manufacturing a phantom SPLIT. Added
            # 2026-08-31 to resolve memory-source.ts → source.rs as clean 1:1.
            "packages/zqlite/src/",
        ],
        "ts_exclude": (
            "query/query.ts",          # TS type machinery: PullRow/QueryReturn/DeepMerge/…
            "query/create-builder.ts", # client fluent-builder factory
            "planner/planner-debug.ts",  # planner debug-event system — not ported
            "builder/debug-delegate.ts", # debug/instrumentation delegate — not ported
            # zqlite internal helpers folded into their consumers (no 1:1 twin):
            # statement-cache → db.rs, sql/sql-inline → query_builder/view.
            "zqlite/src/internal/",
            "zqlite/src/mod.ts",       # barrel re-export
        ),
        "structural_ts": set(),
        # Triaged 2026-08-24 (3 parallel Explore agents + import-graph checks).
        # Each entry is a behavioral TS symbol confirmed COVERED/N-A in Rust:
        # inlined, renamed, relocated cross-crate, a JS-only idiom Rust drops, or
        # replaced by SQLite. Zero genuine gaps found in the triaged set.
        "aliases": {
            # maybe-split-and-push-edit-change.ts — rust inlines it into the
            # filter_push.rs EDIT arm (predicate-crossing edit → remove+add),
            # per the Rust-only signature-delta note in that file's doc.
            "maybesplitandpusheditchange": ("ivm/filter_push.rs EDIT arm",
                                            "inlined: edit crossing predicate splits into remove/add"),
            # TS generator plumbing (surfaced by the `function*`/`*method`
            # extractor fix): rust restructures coop-yield generators into
            # iterators/direct calls — the LOGIC lives at the cited site.
            "fetchgenerator": ("ivm/snitch.rs fetch",
                               "TS fetch() delegates to *fetchGenerator; rust folds both into fetch"),
            "generaterows": ("INLINED ivm/source.rs fetch scan walk", "generator → iterator"),
            "generatewithconstraint": ("INLINED ivm/source.rs fetch constraint filter",
                                       "generator → iterator"),
            "generatewithoverlayinner": ("INLINED ivm/source.rs apply_source_overlays",
                                         "generator → iterator"),
            "generatewithoverlayinnerunordered": ("INLINED ivm/source.rs apply_source_overlays",
                                                  "unordered overlay arm"),
            "genpushandwrite": ("INLINED sqlite/table_source.rs write_change",
                                "push+write generator → direct calls"),
            "genpushandwritewithsplitedit": ("INLINED sqlite/table_source.rs write_change",
                                             "split-edit arm of write_change"),
            "getchildnodes": ("INLINED ivm/view.rs apply_change_internal child walk",
                              "generator → loop"),
            "runimpl": ("query/query_delegate_base.rs run",
                        "TS module-level default run() impl folds into the trait impls"),
            # view-apply-change.ts → array_view.rs (array maintenance inlined)
            "arraywith": ('ivm/array_view.rs', 'inlined: Vec clone + index assign (D-16)'),
            "insertat": ("array_view.rs Vec::insert", "inlined"),
            "removeat": ("array_view.rs Vec::remove", "inlined"),
            "setproperty": ("array_view.rs field assign", "inlined"),
            "setrefcount": ('ivm/array_view.rs', 'inlined ref-count adjust'),
            "assertarray": ('N/A', 'TS type-guard; Rust View enum (D-16)'),
            "assertnumber": ('N/A', 'TS type-guard; Rust ref_count:usize (D-16)'),
            "assertmetaentry": ('N/A', 'TS type-guard; Rust Entry struct (D-16)'),
            "track": ('N/A', 'JS WeakSet COW -> Rust Rc::make_mut (D-16)'),
            "owns": ('N/A', 'JS WeakSet COW -> Rust Rc::make_mut (D-16)'),
            # builder.ts internals
            "bindstaticparameters": ('CROSS-CRATE rust-syncer auth/read_authorizer.rs bind_static_parameters', 'relocated upstream (AST transform) — was permissions.rs'),
            "resolvefield": ('CROSS-CRATE rust-syncer auth/read_authorizer.rs resolve_field', 'relocated'),
            "isparameter": ('CROSS-CRATE rust-syncer auth/read_authorizer.rs bind_static_parameters', 'inlined into the static-parameter binder'),
            "groupsubqueryconditions": ("builder.rs apply_or .partition", "inlined"),
            "valueposname": ("builder.rs", "inlined"),
            "addedge": ('N/A', 'debug-instrumentation decorator; Rust wires Rc directly (D-14)'),
            "decorateinput": ('N/A', 'debug-instrumentation decorator; not ported (D-14)'),
            "decoratefilterinput": ('N/A', 'debug-instrumentation decorator; not ported (D-14)'),
            # planner algorithm
            "processand": ("planner/planner_builder.rs process_condition", "inlined"),
            "processor": ("planner/planner_builder.rs process_condition", "inlined"),
            "propagateunlimitforflippedjoins": ("planner/planner_graph.rs:298", "renamed"),
            "flipifneeded": ('N/A', 'dead code in TS; planning calls flip() directly (Rust too) (D-16)'),
            # planner-connection.ts *ForDebug + planner-join debug
            "getconstraintsfordebug": ('N/A', 'debug introspection; not ported (D-14)'),
            "getfiltersfordebug": ('N/A', 'debug introspection; not ported (D-14)'),
            "getsortfordebug": ('N/A', 'debug introspection; not ported (D-14)'),
            "getconstraintcostsfordebug": ('N/A', 'debug introspection; not ported (D-14)'),
            "getdebuginfo": ('N/A', 'debug introspection; not ported (D-14)'),
            "getnodename": ('N/A', 'debug introspection; not ported (D-14)'),
            # memory-source.ts → SQLite table_source (in-memory overlay machinery replaced)
            "computeoverlays": ("sqlite/table_source.rs", "-> SQLite (overlays via SQLite tx)"),
            "overlaysforconstraint": ("sqlite/table_source.rs", "-> SQLite"),
            "overlaysformulticonstraint": ("sqlite/table_source.rs", "-> SQLite"),
            "overlaysforstartat": ("sqlite/table_source.rs", "-> SQLite"),
            "overlaysforfilterpredicate": ("sqlite/table_source.rs", "-> SQLite"),
            "setoverlay": ("sqlite/table_source.rs", "-> SQLite"),
            "getindexkeys": ("sqlite/table_source.rs", "-> SQLite index"),
            "fork": ('N/A', 'TS memory-source fork; Rust source is SQLite-backed (D-16)'),
            "tableschema": ("sqlite/table_source.rs", "-> SQLite"),
            "stringify": ('N/A', 'TS memory-source key stringify; Rust uses SQLite keys (D-16)'),
            # stream.ts / misc idioms
            "draingenerator": ('N/A', 'TS generator drain -> Rust Iterator drop/for_each (D-16)'),
            "consume": ("streamer/mod.rs", "-> Rust Iterator consume"),
            "clonedata": ("ivm/memory_storage.rs", "inlined clone"),
            "mergeempty": ("ivm/push_accumulated logic", "inlined"),
            "normalizeundefined": ("ivm/data.rs", "inlined (undefined->null)"),
            "patterntoregexp": ("builder/like.rs get_like_predicate", "predicate closure, not regex"),
            "pinned": ("planner/runtime.rs", "method"),
            "delete": ("array_view.rs Vec::remove", "inlined"),
            "unreachable": ('Rust unreachable!() macro', 'idiom (D-16)'),
            # Private #-methods (surfaced by the #-method extractor fix,
            # 2026-08-29). Verified against doc citations / mirror-file greps.
            "disconnect": ("engine/mod.rs (:1591)", "doc-cited"),
            "fetchchunked": ("INLINED ivm/flipped_join.rs chunked IN() fetch",
                             "get_multi_constraint_chunk_size + chunk loop"),
            "fetchmulti": ("fetch_batched (batched multi-constraint fetch)", "renamed"),
            "firelistener": ("INLINED ivm/array_view.rs flush notify", "listeners Vec"),
            "firelisteners": ("INLINED ivm/array_view.rs flush notify", "listeners Vec"),
            "generatewithfilter": ("INLINED ivm/source.rs fetch filter arm",
                                   "generator → iterator"),
            "getorcreateindex": ("ivm/source.rs (:847)", "doc-cited"),
            "getprimaryindex": ("INLINED ivm/source.rs primary_key()/re-key",
                                "restructured: source keyed on PK, no index registry"),
            "getstart": ("ivm/skip.rs (:83)", "doc-cited"),
            "indexof": ('sqlite/sqlite_stat_fanout.rs find_index_for_columns', 'index lookup by columns'),
            "log": ("ivm/snitch.rs log_message", "renamed — bare `log` is ambiguous"),
            "restoreconnections": ("planner/planner_graph.rs restore_planning_snapshot",
                                   "snapshot capture/restore restructure"),
            "restorefannodes": ("planner/planner_graph.rs restore_planning_snapshot",
                                "snapshot capture/restore restructure"),
            "restorejoins": ("planner/planner_graph.rs restore_planning_snapshot",
                             "snapshot capture/restore restructure"),
            "validatesnapshotshape": ("planner/planner_graph.rs typed PlanState",
                                      "shape validation is the type system's job in rust"),
            "yieldparentwithoverlay": ("ivm/flipped_join.rs generate_with_overlay_no_yield",
                                       "renamed (no coop yield)"),
        },
        # 2026-08-29 first pin (was unenforced): 74 = the enum-shim/type-file
        # folds (change-type-enum → change.rs family), the sanctioned
        # memory-source → source.rs/table_source.rs split, and common-name
        # collision remainders with no mirror pair on either side.
        # 74→71 same day: mid-file-test-mod skip fix re-bound 3 to mirrors.
        # 71→73 (2026-08-31): adding the zqlite/ origin correctly bound the 10
        # sqlite/*.rs files (previously false "inventions"), which surfaced 2
        # more legit cross-binds (query-delegate NewQueryDelegate, table-source
        # symbols → engine/mod) — new visibility, not drift.
        "max_misfiled": 73,
    },
    "syncer": {
        "rust_dir": "packages/rust-syncer/src",
        "ts_label_prefix": "zero-cache/src/",
        # L9 structural ratchet (`--enforce-structure`): resolved pairs whose
        # rust file is not the TS mirror. Baseline 25 (2026-08-28, Stage 5:
        # widened the scope with the newly mirrored infra files, which brings
        # fuzzy-noise pairs of its own; Stage 5 itself RELOCATED the
        # custom/fetch + custom/metrics + observability symbols to their
        # mirrors). Remaining entries = documented folds (auth.ts->CCM,
        # fetch.ts error types->protocol/, rule-3 exception) + fuzzy noise.
        # Any GROWTH means a symbol landed outside its mirrored file — fix the
        # location, don't bump this number without a written exception.
        # 2026-08-29 exception (25 -> 26): pre-existing fuzzy-noise pair —
        # verified NOT introduced by the planner-model/pusher-auth fixes (count
        # is 26 with those changes stashed too). The extra binding is a
        # live-census CONSTANT name-colliding with a TS class (the
        # `pusher.ts::Pusher -> live_count.rs::PUSHER` family from the Stage-5
        # scope widening); the real ports live in their mirrors. The Stage-4
        # ledger re-bind (task #162) should alias these census constants and
        # ratchet back down.
        # 2026-08-29 ratcheted 26→25 after mirror-aware occurrence binding:
        # the remainder is census constants + genuinely relocated fns
        # (is_admin_password_valid in inspect_handler.rs vs config/, #163).
        # 25→30 same day: the #-method + mid-file-test-mod extractor fixes
        # surfaced 5 previously-INVISIBLE bindings — 2 real metric-fn
        # relocations (#recordWebSocketError, #recordViewSyncerLagSamples →
        # observability/metrics.rs; #163 family) + 3 common-name fuzzy
        # collisions (#push/fetch/reset). Growth is new visibility, not drift.
        # 30→31 (2026-08-31, D inspector-metrics port): ONE more common-name
        # fuzzy collision — `InspectorDelegate.addQuery` (inspector-delegate.ts)
        # is correctly ported to `server/inspector_delegate.rs::add_query`, but
        # the fuzzy pass attributes rust `add_query` to `pipeline-driver.ts::
        # addQuery` (a same-name symbol already covered by the "addquery" alias).
        # `inspector-delegate.ts`/`shared/src/tdigest.ts` are deliberately NOT
        # added to `ts_files`: the former would cascade its auth-method fold
        # (isAuthenticated/… live on ViewSyncerService) as new misfiles, and the
        # latter is a `packages/shared` path the ZC-relative file-correspondence
        # can't map (it flags all 19 TDigest methods). The real ports are 1:1
        # filed (server/inspector_delegate.rs, tdigest.rs); this is matcher noise,
        # not drift. See parity/INVENTIONS.md I-10.
        "max_misfiled": 31,
        # rust-syncer replaces the entire TS syncer WORKER process: the WS
        # connection lifecycle (workers/), the view-syncer serving loop +
        # pipeline driver (services/view-syncer/), the read-permission + JWT auth
        # transforms (auth/), and the custom-query relay (custom-queries/,
        # custom/). CVR persistence lives in rust-cvr; the IVM engine in rust-ivm
        # — so their TS origins are intentionally NOT listed here.
        "ts_files": [
            f"{ZC}/auth/jwt.ts",
            f"{ZC}/auth/auth.ts",
            f"{ZC}/auth/read-authorizer.ts",
            f"{ZC}/auth/load-permissions.ts",
            f"{ZC}/workers/connect-params.ts",
            f"{ZC}/workers/connection.ts",
            f"{ZC}/workers/syncer.ts",
            f"{ZC}/workers/syncer-ws-message-handler.ts",
            f"{ZC}/services/view-syncer/client-schema.ts",
            f"{ZC}/services/view-syncer/connection-context-manager.ts",
            f"{ZC}/services/view-syncer/drain-coordinator.ts",
            f"{ZC}/services/view-syncer/e2e-serving-lag.ts",
            f"{ZC}/services/view-syncer/pipeline-driver.ts",
            f"{ZC}/services/view-syncer/query-covering.ts",
            f"{ZC}/services/view-syncer/view-syncer.ts",
            f"{ZC}/custom-queries/transform-query.ts",
            f"{ZC}/custom/fetch.ts",
            f"{ZC}/custom/metrics.ts",
            f"{ZC}/db/lite-tables.ts",
            # L9 Stage 5: infra layer mirrored 1:1
            f"{ZC}/observability/metrics.ts",
            f"{ZC}/config/zero-config.ts",
            f"{ZC}/server/syncer.ts",
            f"{ZC}/server/otel-start.ts",
            f"{ZC}/services/mutagen/pusher.ts",
            f"{ZC}/services/view-syncer/inspect-handler.ts",
        ],
        # Serving-loop / transform algorithms — a missing behavioral symbol here
        # is HIGH risk. Structural/DDL/type files are LOW risk (see structural_ts).
        "core_ts": {
            "view-syncer.ts", "pipeline-driver.ts", "connection.ts", "client-schema.ts",
            "read-authorizer.ts", "syncer-ws-message-handler.ts",
            "connection-context-manager.ts", "jwt.ts", "transform-query.ts",
            "query-covering.ts", "drain-coordinator.ts", "e2e-serving-lag.ts",
        },
        # Rust files with no 1:1 TS origin (transport / process infra / idiom) =>
        # Rust-only here is expected, not drift.
        "infra_rust": {
            "http_server.rs", "lib.rs", "live_count.rs", "metrics.rs",
            "otel.rs", "trace.rs", "ws_sink.rs", "ws_server.rs", "main.rs",
            "protocol.rs",
            # db/specs.rs: the LiteTableSpec/LiteColumnSpec subset of TS
            # db/specs.ts (types only; the zod/DDL half has no rust consumer).
            "specs.rs",
        },
        # Pure structure (lite-table type maps). Their fns became serde/match
        # tables, so they are NOT behavioral gaps.
        "structural_ts": {"lite-tables.ts"},
        # Confirmed resolutions the fuzzy pass can't infer. Filled in during Layer-1
        # triage (5 parallel Explore agents, 2026-08-25). Each key is canon() of the
        # TS name. "COVERED" targets record the CURRENT Rust home (the 1:1-fn refactor
        # then renames the Rust symbol to snake_case(TS) where it diverges). Genuine
        # not-yet-ported symbols are deliberately LEFT UNRESOLVED (the port worklist):
        #   getters queryCount/rowCount, logQueryFailure, randomID, hasErrno/
        #   hasTransientSocketCode, and the cross-CG serving-lag percentile family
        #   (boundReplicaReadyStates/compute*ServingLag*/find/lower/upper/prune/
        #   percentileNearestRank) — the last replaced by the completion-based
        #   e2e-serving-lag histogram in the per-CG Rust arch.
        "aliases": {
            # view-syncer.ts serving loop -> services/view_syncer/view_syncer.rs (async task)
            "contentsandversion": ("view_syncer.rs engine seat (strip _0_version)", "inlined"),
            "elapsedlap": ('N/A', 'per-lap timing via Instant::elapsed() inline (D-14)'),
            "expired": ("view_syncer.rs remove_expired_queries", "TTL/inactivation expiry"),
            "keepalive": ("view_syncer.rs ViewSyncerService.keepalive_until", "field + next_idle_shutdown_delay"),
            "markinitialized": ("view_syncer.rs ViewSyncerService.terminal", "init-state flag; test helper dropped"),
            "readystate": ("view_syncer.rs ViewSyncerService/event loop", "init/drain state flags"),
            "run": ("view_syncer.rs cg_event_loop", "per-CG async serving loop"),
            "shutdownbeforeinitializationerror": ("view_syncer.rs init-fail path", "error on terminal init failure"),
            "start": ("view_syncer.rs ensure_cvr/ViewSyncerService init", "CVR load + ttl seed"),
            "startwithoutyielding": ('N/A', 'no setImmediate; sync Instant::now start (D-15)'),
            "stop": ("view_syncer.rs shutdown()", "per-CG drain + Rehome"),
            "totalelapsed": ('N/A', 'inline Instant::elapsed accumulation (D-14)'),
            "yieldprocess": ('N/A', 'tokio async yield; no global-lock setImmediate (D-15)'),
            # pipeline-driver.ts -> pipeline_driver.rs + rust-ivm (advance gate, ops)
            "addquery": ("rust-ivm engine add_queries", "streaming add (cross-crate)"),
            "advancementresettimelimitms": ("rust-ivm advance_gate.rs", "ported"),
            "advancewithoutdiff": ('CROSS-CRATE rust-ivm snapshotter/snapshotter.rs advance_without_diff', 'ported'),
            "assert": ("Rust assert! macro", "idiom (D-16)"),
            "currentpermissions": ("view_syncer.rs/message_handler perms reload", "perms hot-reload at CG dispatch"),
            "getrowkey": ("rust-ivm streamer get_row_key", "row-key extraction (cross-crate)"),
            "getschema": ("rust-ivm operator get_schema", "trait method (cross-crate)"),
            "minprojectedadvancementsamplechanges": ("rust-ivm advance_gate.rs", "ported"),
            "mustgetprimarykey": ("rust-ivm engine build", "PK validated on build"),
            "projectedadvancementtimems": ("rust-ivm advance_gate.rs", "ported"),
            "queries": ("pipeline_driver.rs running_queries/active_query_ids", "split getters"),
            "replicaversion": ("pipeline_driver.rs snapshotter current_version", "field/getter"),
            "scalarvaluesequal": ("rust-ivm engine scalar_values_equal", "ported (cross-crate)"),
            "setoutput": ("rust-ivm operator set_output", "trait method (cross-crate)"),
            "shouldfinishlateadvancement": ("rust-ivm advance_gate.rs", "ported"),
            "shouldresetprojectedadvancement": ("rust-ivm advance_gate.rs", "ported"),
            "shouldresetslowcurrentchange": ("rust-ivm advance_gate.rs", "ported"),
            "totalhydrationtimems": ("rust-ivm engine total_hydration_time_ms", "ported (cross-crate)"),
            # workers/syncer.ts
            "getwebsocketserveroptions": ("ws_server.rs WebSocketConfig", "compression opts; permessage-deflate NOT supported — registered D-13"),
            # workers/connection.ts transient-socket handling
            "findprotocolerror": ("workers/connection.rs classify_error_log_level", "protocol-error classify"),
            "istransientsocketmessage": ("workers/connection.rs (message substring)", "transient downgrade"),
            # workers/connect-params.ts
            "normalizeheaders": ("ws_server.rs (dup-header join)", "header normalization"),
            # auth/jwt.ts -> auth.rs (whole file ports here; ledger 'DROPPED' is cosmetic)
            "getremotekeyset": ("auth/jwt.rs JWKS_CACHE/lookup_cached_jwk", "cached remote JWKS"),
            "loadjwk": ("auth/jwt.rs serde_json::from_str", "parse JWK"),
            "loadsecret": ("auth/jwt.rs DecodingKey::from_secret", "secret key"),
            "verifytokenimpl": ("auth/jwt.rs verify_sync/verify_with_jwk(s)", "JWT verify (split sync/async)"),
            # auth/auth.ts
            "isprovidedauth": ("services/view_syncer/connection_context_manager.rs is_some_and non-empty", "inlined"),
            # custom/fetch.ts — the api-metric recorders (recordApiAttempt +
            # apiRequestMetricAttrs) live in custom/metrics.rs beside the OTel
            # instruments they drive (api_otel()), a rust OTel-idiom fold:
            # TS splits lazy instrument-accessors (metrics.ts) from the caller
            # that adds attrs (fetch.ts recordApiAttempt); rust holds the
            # instruments statically and records next to them, so splitting the
            # recorder back into fetch.rs would reach into metrics.rs internals.
            # Registered in PARITY-EXCEPTIONS.md (rule 5). (2026-08-31)
            "apiattempts": ("metrics.rs record_api_attempt", "OTel counter"),
            "recordapiattempt": ("metrics.rs record_api_attempt",
                                 "OTel-idiom fold: recorder lives beside its instruments"),
            "apirequestmetricattrs": ("metrics.rs api_request_metric_attrs",
                                      "OTel-idiom fold: attrs helper beside the instruments"),
            "apierrorfromresult": ("custom_queries/transform_query.rs response validation", "error extraction"),
            "apiresponseerrormetricattrs": ("metrics.rs record_api_attempt attrs", "status attrs"),
            "urlmatch": ("custom_queries/transform_query.rs url_match", "URLPattern subset (renamed 1:1)"),
            "compileurlpattern": ('custom_queries/transform_query.rs url_match', 'no separate compile step; url_match matches the raw pattern inline'),
            # workers/connection.ts — Node errno predicates with no tungstenite
            # surface. The transient-MESSAGE path (isTransientSocketMessage) IS
            # ported (connection.rs classify_error_log_level); the errno-CODE path
            # has no equivalent (tungstenite/tokio errors don't carry errno).
            "haserrno": ('N/A', "Node `'errno' in e`; Rust WS stack has no errno (D-15)"),
            "hastransientsocketcode": ('N/A', 'Node EPIPE/ECONNRESET/ECANCELED; no errno in tungstenite (D-15)'),
            # pipeline-driver.ts
            "logqueryfailure": ('inlined', 'streamer error callback lives in rust-ivm; failures logged via tracing at the call sites (D-16)'),
            "randomid": ('N/A', 'TS pipelineRunID debug-correlation id; not ported (D-14)'),
            # custom-queries/transform-query.ts
            "normalizedheaders": ("custom_queries/transform_query.rs normalized_headers", "canonical header hash"),
            # connection-context-manager.ts
            "filterheaders": ('services/view_syncer/connection_context_manager.rs filter_headers', 'header allowlist'),
            "sameconnectionselector": ("services/view_syncer/connection_context_manager.rs set_background_connection", "inlined tuple match"),
            # query-covering.ts
            "jsonequal": ("services/view_syncer/query_covering.rs json_equal", "deep eq w/ JS number semantics"),
            # db/lite-tables.ts
            "keycmp": ("db/lite_tables.rs sort_by len-then-lex", "inlined key compare"),
            # server/otel-start.ts — the mirrored file EXISTS
            # (server/otel_start.rs) but its fn names diverge: node OTel
            # auto-instrumentation vs the rust SDK have no shared surface.
            # Flagged for #163 (infra-layer mirroring), not silently 1:1.
            "startotelauto": ("server/otel_start.rs init_metrics/metrics_enabled",
                              "rust otel init path; node auto-instr has no rust twin"),
            "getinstance": ("N/A — node OtelManager singleton wrapper",
                            "rust init is free fns in server/otel_start.rs"),
            # auth/jwt.ts (surfaced by the async-export extractor fix)
            "createjwkpair": ('N/A — JWK-pair GENERATION helper (tests/tooling)', 'rust only verifies tokens, never mints keys (D-15)'),
            "verifytoken": ("auth/jwt.rs verify_with_jwks / verify_sync cluster",
                            "name-diverged verify path; 1:1 rename pending #163"),
            # custom/fetch.ts — rust splits the API fetch by call-class:
            # transform via post_transform, custom push via the TS loopback
            # relay (Option-A, INVENTIONS.md I-3), so node keeps mutation logic.
            "fetchfromapiserver": ("custom_queries/transform_query.rs post_transform",
                                   "push-class calls go via services/mutagen/pusher.rs relay POST (I-3)"),
            "runworker": ('N/A — node worker bootstrap', 'rust process entry is the invented main/http_server pair (D-15)'),
            # pipeline-driver.ts generators (surfaced by the generator fix)
            "stream": ("CROSS-CRATE rust-ivm streamer/mod",
                       "RowChange streaming lives in the ivm crate's Streamer"),
            "toadds": ('CROSS-CRATE rust-ivm engine/mod.rs', 'hydrate emits Adds directly; no Node→AddChange adaptor needed (D-16)'),
            "accumulate": ("CROSS-CRATE rust-ivm Streamer accumulated buffer",
                           "start/stop folded into the Streamer lifecycle"),
            # Private #-methods (surfaced by the #-method extractor fix,
            # 2026-08-29). Verified against rust doc citations / mirror greps.
            # `#checkForThrashing` is NOT here: it was a REAL gap, ported
            # 2026-08-29 (check_for_thrashing, view_syncer.rs) — exact-binds.
            "addandremovequeries": ("INLINED view_syncer.rs sync_query_pipeline_set",
                                    "add/remove arms of the pipeline-set sync"),
            "addqueryimpl": ("CROSS-CRATE rust-ivm engine add_queries/add_queries_streaming",
                             "pipeline add"),
            "addquerymaterializationservermetric": ('server/inspector_delegate.rs add_metric get_metrics_json_for_query', "ported: per-query TDigests (tdigest.rs); the old 'returns empty TDigests' note was stale (2026-09-03)"),
            "advancepipelines": ("view_syncer.rs (:7321) advance loop", "doc-cited"),
            "checkforshutdownconditionsinlock": ("view_syncer.rs (:2918)",
                                                 "doc-cited; the lock is the CG serial executor (I-1)"),
            "cleanup": ("view_syncer.rs Drop teardown + engine destroy", "I-4 teardown"),
            "closewiththrown": ("workers/connection.rs close_with_error",
                                "renamed: no thrown objects at the rust WS boundary"),
            "createstorage": ("CROSS-CRATE rust-ivm builder (:49) + memory_storage",
                              "operator storage"),
            "deleteclientduetodisconnect": ("view_syncer.rs (:2332)", "doc-cited"),
            "destroypipeline": ("view_syncer.rs pipeline teardown + engine remove_query",
                                "sync_query_pipeline_set removes"),
            "ensurecostmodelexistsifenabled": ("CROSS-CRATE rust-ivm engine ensure_cost_model",
                                               "planner cost model (2026-08-29 wiring)"),
            "faildownstream": ("services/mutagen/pusher.rs fail_downstream + ConnectionSinks::fail_if_current / fail_client_current",
                               "ported 2026-09-03: error frame + close (fb8e74f29, 8e6bbc1fd)"),
            "failmaintenanceconnection": ("view_syncer.rs (:1327)", "doc-cited"),
            "fanoutresponses": ("services/mutagen/pusher.rs fan_out_responses",
                                "ported 2026-09-03 (8e6bbc1fd): the 2xx relay body IS the MutateResponse — the old 'by design' note was wrong"),
            "flushupdater": ("view_syncer.rs (:2882) flush_ops_to_store/flush_to_store",
                             "doc-cited"),
            "getclients": ("view_syncer.rs (:1936) active_clients", "doc-cited"),
            "getsource": ("CROSS-CRATE rust-ivm engine (:372) + source (:96)", "doc-cited"),
            "handlemessageresult": ("workers/connection.rs (:184) handle_result", "doc-cited"),
            "hydrateunchangedqueries": ("INLINED view_syncer.rs sync_query_pipeline_set",
                                        "unchanged-hash arm no-ops; changed hash rehydrates (pinned by test)"),
            "initandresetcommon": ('services/view_syncer/pipeline_driver.rs init_and_reset_common', 'specs+fullTables reload + checkClientSchema (state passed in from the service)'),
            "logquerycoverageshadowsummary": ("services/view_syncer/query_covering.rs (:60)",
                                              "doc-cited"),
            "logquerypipelinelifecycle": ('N/A — logging-only', 'rust uses tracing at the pipeline call sites (D-14)'),
            "processchanges": ("INLINED view_syncer.rs advance path (CROSS-CRATE change_processor)",
                               "doc-cited"),
            "processpush": ("services/mutagen/pusher.rs drainer loop + combine_pushes + validate hook",
                            "one-at-a-time FIFO drain; response handling ported 2026-09-03 (auth-fail + validateConnection)"),
            "proxyinbound": ('workers/connection.rs handle_inbound', 'renamed'),
            "proxyoutbound": ("ws_sink.rs outbound task (I-2)", "per-connection mpsc sender"),
            "removeconnection": ("services/view_syncer/connection_context_manager.rs remove_connection_internal",
                                 "renamed (_internal suffix)"),
            "requesttransform": ("custom_queries/transform_query.rs post_transform", "renamed"),
            "resolvescalarsubqueries": ("CROSS-CRATE rust-ivm sqlite/resolve_scalar_subqueries + engine (:1395)",
                                        "doc-cited"),
            "runbackgroundretransform": ("view_syncer.rs (:1431)", "doc-cited"),
            "runinlockforclient": ("view_syncer.rs (:4465) — CG serial executor replaces the TS #lock (I-1)",
                                   "doc-cited"),
            "runinlockwithcvr": ("INLINED view_syncer.rs CG-thread handlers + lazy CVR load",
                                 "the #lock dissolved into the serial executor (I-1)"),
            "sendquerytransformerrortoclients": ("INLINED view_syncer.rs (:7075) transform_errors fan-out",
                                                 "batch + whole-batch failure arms"),
            "servedversion": ("services/view_syncer/e2e_serving_lag.rs (:75)", "doc-cited"),
            "setgroup": ("INLINED services/view_syncer/connection_context_manager.rs GroupAuthState",
                         "group-state restructure"),
            "shouldadvanceyieldmaybeabortadvance": ("CROSS-CRATE rust-ivm advance_gate", "doc-cited"),
            "shouldyield": ("CROSS-CRATE rust-ivm advance_gate yield decision",
                            "coop-yield ported to the gate"),
            "startaccumulating": ("CROSS-CRATE rust-ivm Streamer accumulated buffer",
                                  "folded into Streamer lifecycle"),
            "stopaccumulating": ("CROSS-CRATE rust-ivm Streamer accumulated buffer",
                                 "folded into Streamer lifecycle"),
            "startlap": ("N/A — TS lock-lap CPU metric; CG serial executor (I-1) replaces the lock",
                         "lap timing not ported (observability)"),
            "stoplap": ("N/A — TS lock-lap CPU metric; CG serial executor (I-1) replaces the lock",
                        "lap timing not ported (observability)"),
            "stopauthmaintenancetimer": ("INLINED view_syncer.rs next_auth_maintenance_at=None",
                                         "timer → deadline field (arm_auth_maintenance)"),
            "streamchanges": ("CROSS-CRATE rust-ivm streamer (:96)", "doc-cited"),
            "streamnodes": ("CROSS-CRATE rust-ivm streamer (:159)", "doc-cited"),
            "throwprojectedadvancementreset": ("CROSS-CRATE rust-ivm advance_gate reset errors",
                                               "advancement-timeout reset (task #145)"),
            "throwslowcurrentchangereset": ("CROSS-CRATE rust-ivm advance_gate reset errors",
                                            "slow-current-change reset"),
            "trackrowsetsignatures": ("CROSS-CRATE rust-ivm engine (:80) + rust-cvr row_set_signature",
                                      "doc-cited"),
            "updatecvrconfig": ("view_syncer.rs (:6905) handle_config_update", "doc-cited"),
        },
    },
}

# Rust method names that are accessors / trait impls, not ported logic.
RUST_IDIOM_NAMES = {
    "new", "from", "default", "drop", "clone", "fmt", "eq", "hash", "get",
    "insert", "id", "base", "base_mut", "as_str", "len", "is_empty", "iter",
    "into", "try_from", "deref", "borrow", "next", "poll", "emit", "inc",
    "dec", "empty", "build",
}
# TS symbol kinds that are structural (types/schemas/DDL), not behavior.
STRUCTURAL_KINDS = {"type", "interface", "const", "enum"}

# ---------------------------------------------------------------------------
# Name normalization: collapse camelCase and snake_case to one canonical key.
#   mergeRefCounts -> mergerefcounts ; merge_ref_counts -> mergerefcounts
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# M10 helper-import aliases (parity/helper_imports.py). Key = `<ts path relative
# to packages/>::<imported symbol>` (`*` = namespace/default import). Value =
# (rust TARGET | non-code resolution, NOTE) under the M9 verification rules —
# a named .rs file must exist and every identifier must occur in it; a non-code
# resolution must cite an I-/D-/GAP-/task/F- id. GAP-n ids are the OPEN gaps
# table in ZERO-DIVERGENCE-PLAN.md.
# ---------------------------------------------------------------------------
HELPER_ALIASES = {
    # ── @rocicorp/otel + tracer: spans/timers are D-14 (observability-only) ──
    "otel/src/enabled.ts::otelEnabled": ("server/otel_start.rs", "exporter enablement env probe (D-14)"),
    "otel/src/enabled.ts::otelLogsEnabled": ("server/otel_start.rs", "exporter enablement env probe (D-14)"),
    "otel/src/enabled.ts::otelMetricsEnabled": ("server/otel_start.rs", "Port of TS otelMetricsEnabled() (D-14)"),
    "otel/src/enabled.ts::otelTracesEnabled": ("server/otel_start.rs", "exporter enablement env probe (D-14)"),
    "otel/src/log-options.ts::logOptions": ("N/A", "LogContext level/format options → tracing env filter, config/zero_config.rs (D-15)"),
    "otel/src/maybe-time.ts::timeSampled": ("N/A", "sampled span timing around table-source fetch (D-14)"),
    "otel/src/span.ts::manualSpan": ("N/A", "otel span emission (D-14)"),
    "otel/src/span.ts::startAsyncSpan": ("N/A", "otel span emission (D-14)"),
    "otel/src/span.ts::startSpan": ("N/A", "otel span emission (D-14)"),
    "zero-cache/src/services/view-syncer/tracer.ts::tracer": ("N/A", "otel tracer handle; rust tracer.rs / trace.rs no-op spans (D-14)"),
    # ── shared/src language idioms (D-16) ──
    "shared/src/arrays.ts::areEqual": ("ivm/exists.rs no_size_reuse", "slice == (D-16)"),
    "shared/src/asserts.ts::assert": ("N/A", "Rust assert!/panic! (D-16)"),
    "shared/src/asserts.ts::assertArray": ("N/A", "match on Value::Array / as_array().expect (D-16)"),
    "shared/src/asserts.ts::assertNumber": ("N/A", "match on Value::F64 (D-16)"),
    "shared/src/asserts.ts::assertString": ("CROSS-CRATE rust-ivm builder/like.rs get_like_predicate", "panic with TS invalidType message on non-string lhs (like.ts:10)"),
    "shared/src/asserts.ts::unreachable": ("N/A", "Rust exhaustive match / unreachable! (D-16)"),
    "shared/src/bigint-json.ts::jsonValueSchema": ("schema/types.rs", "valita JSON value → serde_json::Value (D-17)"),
    "shared/src/bigint-json.ts::stringify": ("N/A", "JSON.stringify with BigInt → serde_json::to_string (D-16)"),
    "shared/src/btree-set.ts::BTreeSet": ("ivm/memory_source.rs", "std BTreeMap-backed ordered rows (D-16)"),
    "shared/src/cache.ts::TimedCache": ("custom_queries/transform_query.rs TRANSFORM_CACHE CACHE_TTL", "5s TTL cache, process-wide keyed by full request identity (transform_query.rs:32)"),
    "shared/src/custom-key-map.ts::CustomKeyMap": ("N/A", "HashMap keyed by row_key.rs row_id_string (D-16)"),
    "shared/src/custom-key-set.ts::CustomKeySet": ("N/A", "HashSet keyed by row_key.rs row_id_string (D-16)"),
    "shared/src/deep-merge.ts::deepMerge": ("N/A", "server-side defineQueries registry merge; rust-ivm query/query_registry.rs is the engine-side registry only (D-16)"),
    "shared/src/deep-merge.ts::isPlainObject": ("N/A", "server-side defineQueries registry merge; rust-ivm query/query_registry.rs (D-16)"),
    "shared/src/error.ts::getErrorMessage": ("N/A", "Display of the error value (`{e}`) — custom_queries/transform_query.rs, custom/fetch.rs (D-16)"),
    "shared/src/has-own.ts::hasOwn": ("N/A", "HashMap::contains_key — ivm/take.rs, ivm/memory_source.rs (D-16)"),
    "shared/src/iterables.ts::once": ("N/A", "std::iter::once (D-16)"),
    "shared/src/iterables.ts::some": ("N/A", "Iterator::any — cvr.rs, cvr_store.rs (D-16)"),
    "shared/src/iterables.ts::toSorted": ("CROSS-CRATE rust-cvr shared/string_compare.rs string_compare", "collect + sort_by(string_compare) (D-16)"),
    "shared/src/json-schema.ts::jsonSchema": ("schema/types.rs", "valita JSON → serde_json::Value (D-17)"),
    "shared/src/json.ts::assertJSONValue": ("N/A", "Value is JSON by construction — client_handler.rs (D-16)"),
    "shared/src/json.ts::deepEqual": ("cvr_store.rs", "derived Eq on RowRecord (cvr_store.rs:716); query_covering.rs Value == (D-16)"),
    "shared/src/must.ts::must": ("N/A", "Option::expect / unwrap (D-16)"),
    "shared/src/object-traversal.ts::getValueAtPath": ("N/A", "server-side defineQueries registry lookup; rust-ivm query/query_registry.rs (D-16)"),
    "shared/src/objects.ts::assignProperty": ("N/A", "map insert — ivm/view_apply_change.rs (D-16)"),
    "shared/src/objects.ts::mapValues": ("N/A", "iterator map/collect — ivm/catch.rs (D-16)"),
    "shared/src/options.ts::flagToEnv": ("config/zero_config.rs", "env-driven option parse (D-15)"),
    "shared/src/options.ts::parseOptions": ("config/zero_config.rs", "env-driven option parse (D-15)"),
    "shared/src/queue.ts::Queue": ("services/mutagen/pusher.rs", "I-3 mpsc channel replaces the async Queue"),
    "shared/src/rand.ts::randInt": ("N/A", "log-only request/worker ids (D-14)"),
    "shared/src/resolved-promises.ts::promiseVoid": ("N/A", "resolved future sentinel (D-16)"),
    "shared/src/sentinels.ts::emptyArray": ("N/A", "&[] sentinel (D-16)"),
    "shared/src/sentinels.ts::identity": ("N/A", "|x| x (D-16)"),
    "shared/src/set-utils.ts::equals": ("N/A", "HashSet == — services/view_syncer/client_schema.rs (D-16)"),
    "shared/src/set-utils.ts::difference": ("N/A", "HashSet::difference — cvr.rs (D-16)"),
    "shared/src/set-utils.ts::intersection": ("N/A", "HashSet::intersection — cvr.rs (D-16)"),
    "shared/src/set-utils.ts::union": ("N/A", "HashSet::union — cvr.rs (D-16)"),
    "shared/src/sleep.ts::sleep": ("N/A", "tokio::time::sleep (D-16)"),
    "shared/src/sorted-entries.ts::sortedEntries": ("custom_queries/transform_query.rs normalized_headers", "sorted header entries for the cache key"),
    "shared/src/testing.ts::assertTesting": ("N/A", "test-only guard; #[cfg(test)] (D-16)"),
    "shared/src/valita.ts::*": ("N/A", "valita schema library → serde derives (D-17)"),
    # ── zero-cache config / process / Node runtime (D-15) ──
    "zero-cache/src/config/network.ts::DEFAULT_PREFERRED_PREFIXES": ("N/A", "multi-NIC address preference for the Node http server (D-15)"),
    "zero-cache/src/config/normalize.ts::assertNormalized": ("config/zero_config.rs", "config normalization (D-15)"),
    "zero-cache/src/config/normalize.ts::isDevelopmentMode": ("services/view_syncer/inspect_handler.rs dev_mode", "NODE_ENV=development admin-password bypass (inspect_handler.rs:61)"),
    "zero-cache/src/db/mode-enum.ts::*": ("N/A", "TransactionPool Mode enum; INLINED READONLY tx in cvr_store.rs (D-16)"),
    "zero-cache/src/db/postgres-type-class-enum.ts::*": ("N/A", "elemPgTypeClass on LiteColumnSpec feeds the replicator's DDL side; no ported file reads it (lite-tables.ts:113 is its only consumer) (D-15)"),
    "zero-cache/src/db/run-transaction.ts::runTx": ("INLINED cvr_store.rs (:772)", "doc-cited SET LOCAL pair"),
    "zero-cache/src/db/sqlite-corruption.ts::registerSQLiteCorruptionDiagnosticTarget": ("N/A", "Node process diagnostic hook (D-15)"),
    "zero-cache/src/db/statements.ts::StatementRunner": ("N/A", "prepared-statement cache over better-sqlite3; rusqlite prepare_cached (D-16)"),
    "zero-cache/src/db/transaction-pool.ts::TransactionPool": ("INLINED cvr_store.rs (:1687)", "doc-cited READONLY pool"),
    "zero-cache/src/db/warmup.ts::warmupConnections": ("main.rs", "best-effort pool warmup before ready (main.rs:246) (D-15)"),
    "zero-cache/src/observability/events.ts::initEventSink": ("N/A", "Node event sink bootstrap (D-15)"),
    "zero-cache/src/server/anonymous-otel-start.ts::recordConnectionAttempted": ("N/A", "anonymous telemetry counter (D-14)"),
    "zero-cache/src/server/anonymous-otel-start.ts::recordConnectionSuccess": ("N/A", "anonymous telemetry counter (D-14)"),
    "zero-cache/src/server/anonymous-otel-start.ts::recordMutation": ("N/A", "anonymous telemetry counter (D-14)"),
    "zero-cache/src/server/anonymous-otel-start.ts::recordRowsSynced": ("N/A", "anonymous telemetry counter (D-14)"),
    "zero-cache/src/server/anonymous-otel-start.ts::setActiveClientGroupsGetter": ("N/A", "anonymous telemetry gauge (D-14)"),
    "zero-cache/src/server/anonymous-otel-start.ts::startAnonymousTelemetry": ("N/A", "anonymous telemetry bootstrap (D-14)"),
    "zero-cache/src/server/logging.ts::createLogContext": ("N/A", "LogContext construction → tracing subscriber (D-15)"),
    "zero-cache/src/server/otel-diag-logger.ts::setupOtelDiagnosticLogger": ("N/A", "otel diag logger (D-14)"),
    "zero-cache/src/services/life-cycle.ts::exitAfter": ("N/A", "Node worker lifecycle (D-15)"),
    "zero-cache/src/services/life-cycle.ts::runUntilKilled": ("N/A", "Node worker lifecycle (D-15)"),
    "zero-cache/src/services/mutagen/mutagen.ts::MutagenService": ("N/A", "CRUD mutagen; rust relays custom pushes only — I-3"),
    "zero-cache/src/services/runner.ts::ServiceRunner": ("workers/cg_executor.rs CGHandle", "per-CG executor replaces the ServiceRunner map — I-1"),
    "zero-cache/src/types/pg.ts::connectPgClient": ("N/A", "postgres.js client; sqlx PgPool in main.rs (D-15)"),
    "zero-cache/src/types/processes.ts::parentWorker": ("N/A", "Node worker IPC (D-15)"),
    "zero-cache/src/types/processes.ts::singleProcessMode": ("N/A", "Node worker IPC (D-15)"),
    "zero-cache/src/types/subscription.ts::Subscription": ("ws_sink.rs", "per-connection downstream channel + writer task — I-4"),
    "zero-cache/src/types/websocket-handoff.ts::installWebSocketReceiver": ("N/A", "Node socket handoff between dispatcher and worker (D-15)"),
    "zero-cache/src/workers/replicator.ts::createNotifierFrom": ("N/A", "replicator worker IPC; rust-ivm snapshotter polls the replica (D-15)"),
    "zero-cache/src/workers/replicator.ts::replicaFileModeSchema": ("N/A", "replicator worker args (D-15)"),
    "zero-cache/src/workers/replicator.ts::replicaFileName": ("N/A", "replicator worker args (D-15)"),
    "zero-cache/src/workers/replicator.ts::subscribeTo": ("N/A", "replicator worker IPC (D-15)"),
    "zero/package.json::*": ("N/A", "package version string for the `connected` frame/otel — INLINED main.rs (D-15)"),
    # ── replica schema helpers ──
    "zero-cache/src/services/replicator/schema/column-metadata.ts::ColumnMetadataStore": ("services/replicator/schema/column_metadata.rs ColumnMetadataStore get_instance get_column", "read side; metadata row wins over the pragma type in read_table_spec"),
    "zero-cache/src/services/replicator/schema/column-metadata.ts::metadataToLiteTypeString": ("services/replicator/schema/column_metadata.rs metadata_to_lite_type_string", "liteTypeString(upstreamType, isNotNull, isEnum, isArray)"),
    "zero-cache/src/services/replicator/schema/replication-state.ts::getSubscriptionState": ("db/lite_tables.rs read_replica_versions", "replicaVersion from _zero.replicationConfig"),
    "zero-cache/src/services/replicator/schema/table-metadata.ts::TableMetadataTracker": ("db/lite_tables.rs read_min_row_versions", "getMinRowVersions"),
    "zero-cache/src/types/error-with-level.ts::ProtocolErrorWithLevel": ("N/A", "log level attached at throw sites; rust derives it from kind — workers/connection.rs classify_error_log_level (D-14)"),
    "zero-cache/src/types/error-with-level.ts::getLogLevel": ("workers/connection.rs classify_error_log_level", "level from ErrorBody kind (D-14)"),
    "zero-cache/src/types/lite.ts::isArray": ("N/A", "elemPgTypeClass derivation only (lite-tables.ts:108-112); no ported file reads elemPgTypeClass (D-15)"),
    "zero-cache/src/types/lite.ts::isEnum": ("N/A", "elemPgTypeClass derivation only (lite-tables.ts:108-112); no ported file reads elemPgTypeClass (D-15)"),
    "zero-cache/src/types/lite.ts::mapLiteDataTypeToZqlSchemaValue": ("db/lite_tables.rs lite_type_to_zql_value_type", "pipe-notation lite type → zql value type"),
    "zero-cache/src/types/lite.ts::nullableUpstream": ("db/lite_tables.rs NOT_NULL_ATTRIBUTE", "INLINED in read_table_spec"),
    "zero-cache/src/types/shards.ts::ALLOWED_APP_ID_CHARACTERS": ("CROSS-CRATE rust-cvr shards.rs ALLOWED_APP_ID_CHARACTERS allowed_app_id_characters", "config/zero_config.rs validated_app_id asserts it at config load (zero-config.ts:38)"),
    "zero-cache/src/types/shards.ts::INVALID_APP_ID_MESSAGE": ("CROSS-CRATE rust-cvr shards.rs INVALID_APP_ID_MESSAGE", "fatal config message"),
    "zero-cache/src/types/shards.ts::getShardID": ("config/zero_config.rs", "ShardID assembled from app_id + shard_num at config load"),
    "zero-cache/src/types/state-version.ts::majorVersionToString": ("INLINED schema/types.rs (:55)", "doc-cited"),
    "zero-cache/src/types/state-version.ts::stateVersionFromString": ("INLINED schema/types.rs (:151)", "doc-cited validation"),
    "zero-cache/src/types/url-params.ts::URLParams": ("workers/connect_params.rs get_string get_integer get_boolean", "URL parameter helpers (connect_params.rs:123)"),
    # ── zero-protocol / zero-schema valita schemas → serde types (D-17) ──
    "zero-protocol/src/ast.ts::astSchema": ("CROSS-CRATE rust-ivm builder/ast.rs Ast", "valita → serde struct (D-17)"),
    "zero-protocol/src/ast.ts::planIdSymbol": ("CROSS-CRATE rust-ivm builder/ast.rs plan_id", "symbol-keyed property → struct field (D-16)"),
    "zero-protocol/src/ast.ts::toStaticParam": ("N/A", "client fluent-builder symbol method; rust-ivm query/expression.rs (D-16)"),
    "zero-protocol/src/client-schema.ts::clientSchemaSchema": ("cvr.rs set_client_schema", "valita → serde_json::Value (D-17)"),
    "zero-protocol/src/data.ts::rowSchema": ("client_handler.rs ensure_safe_json", "valita → Value (D-17), F-CH-1"),
    "zero-protocol/src/error.ts::ProtocolError": ("protocol/error.rs ErrorBody", "thrown error class → ErrorBody Result (D-16)"),
    "zero-protocol/src/error.ts::errorBodySchema": ("protocol/error.rs ErrorBody", "valita union → kind-discriminated Deserialize (error-wire-fixture.json) (D-17)"),
    "zero-protocol/src/error.ts::isProtocolError": ("N/A", "instanceof guard → Result::Err(ErrorBody) (D-16)"),
    "zero-protocol/src/mutate-server.ts::mutateResponseSchema": ("services/mutagen/pusher.rs", "valita → serde (D-17)"),
    "zero-protocol/src/mutation-type-enum.ts::*": ("N/A", "MutationType enum; rust relays custom pushes only — I-3"),
    "zero-protocol/src/mutation.ts::mutationResultSchema": ("client_handler.rs normalize_mutation_result", "valita → Value (D-17)"),
    "zero-protocol/src/primary-key.ts::primaryKeySchema": ("N/A", "valita → Vec<String> (D-17)"),
    "zero-protocol/src/primary-key.ts::primaryKeyValueRecordSchema": ("client_handler.rs make_row_patch", "valita → Value (D-17), client_handler.rs:741"),
    "zero-protocol/src/push.ts::pushErrorSchema": ("protocol/push.rs", "valita → serde (D-17)"),
    "zero-protocol/src/query-server.ts::queryResponseSchema": ("custom_queries/transform_query.rs validation_of", "valita → Value (D-17)"),
    "zero-schema/src/compiled-permissions.ts::permissionsConfigSchema": ("auth/load_permissions.rs load_permissions", "valita → Value (D-17)"),
    # ── zqlite internals ──
    "zqlite/src/internal/sql-inline.ts::compileInline": ("sqlite/sqlite_cost_model.rs inline_sql", "probe SQL with inlined literals"),
    "zqlite/src/internal/sql.ts::compile": ("sqlite/query_builder.rs", "SQL text + params builder (D-16)"),
    "zqlite/src/internal/sql.ts::sql": ("sqlite/query_builder.rs", "sql`` template → String builder (D-16)"),
    "zqlite/src/internal/statement-cache.ts::StatementCache": ("snapshotter/snapshotter.rs", "rusqlite prepare_cached; DEFAULT_MAX_CACHED_STATEMENTS (snapshotter.rs:24) (D-16)"),
}

def canon(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())

# --- token-level similarity, for catching RENAMES that canon() can't ---
# (e.g. cvrErrorKind -> CVRStoreError, rowIDSignatureUnit -> signature_unit,
#  shouldDrain -> should_drain, and file renames like drain-coordinator -> drain)
STOP_TOKENS = {"get", "set", "is", "to", "as", "the", "of", "a", "fn", "id"}

def tokens(name: str):
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", name)  # camel split
    s = s.replace("_", " ").replace("-", " ")
    return {t.lower() for t in s.split() if len(t) >= 2 and t.lower() not in STOP_TOKENS}

def jaccard(a, b):
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)

FUZZY_THRESHOLD = 0.40  # a single shared generic verb (0.33) is not enough

# domain words too common to prove a rename on their own (inspectQueries vs
# delete_queries share only "queries" — not a real match).
COMMON_TOKENS = {
    "query", "queries", "client", "clients", "row", "rows", "patch", "record",
    "records", "value", "values", "desired", "version", "index", "name", "type",
    "data", "cvr", "store", "table", "column", "schema",
}

def distinctive(shared):
    """True if the shared tokens include something specific enough to trust."""
    return any(len(t) >= 4 and t not in COMMON_TOKENS for t in shared)

# ---------------------------------------------------------------------------
# Rust extraction
# ---------------------------------------------------------------------------
RUST_FN = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+(\w+)")
RUST_TYPE = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(struct|enum|trait)\s+(\w+)")
RUST_CONST = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:const|static)\s+(\w+)")
# top-level `type` aliases only (column 0): associated types inside `impl` /
# `trait` bodies are indented and stay excluded. Without this, a TS `type`
# ported as a Rust alias (e.g. `SchemaQuery`) never bound and its mirrored
# file pair read as DROPPED+new.
RUST_TYPE_ALIAS = re.compile(r"^(?:pub(?:\([^)]*\))?\s+)?type\s+(\w+)")

RUST_TEST_MOD = re.compile(r"^\s*(?:pub\s+)?mod\s+tests?\b|^\s*mod\s+test_\w+\b")

def extract_rust(path):
    """Return list of (canon, name, kind, lineno, signature).

    Skips the body of `mod tests { ... }` via brace-depth counting so that
    test-gated *imports* (`#[cfg(test)] use ...`) don't cause us to drop real
    code, while unit-test fns inside the module are still excluded.
    """
    out = []
    with open(path, encoding="utf-8") as f:
        lines = f.readlines()
    skip_close = None       # closing-brace line ending the current test module
    for i, line in enumerate(lines, 1):
        if skip_close is not None:
            if line.rstrip() == skip_close:
                skip_close = None
            continue
        if RUST_TEST_MOD.match(line):
            # Skip to the module's closing brace AT THE SAME INDENT. Brace
            # COUNTING desyncs on braces inside string literals/comments — an
            # unbalanced `}` in a test-fixture string ate everything after
            # view_syncer.rs's MID-FILE tests mod, hiding ~3000 lines of the
            # second production `impl ViewSyncerService` block (catchup_clients,
            # sync_query_pipeline_set, …) from the ledger. rustfmt puts the
            # closing brace alone at the mod's own indent and nothing inside
            # the body sits at that indent, so the indent anchor is exact.
            # `mod tests;` (no body) and one-line `mod tests {}` skip nothing.
            if "{" in line and not line.rstrip().endswith("}"):
                indent = re.match(r"^(\s*)", line).group(1)
                skip_close = indent + "}"
            continue
        m = RUST_FN.match(line)
        if m:
            out.append((canon(m.group(1)), m.group(1), "fn", i, line.strip()))
            continue
        m = RUST_TYPE.match(line)
        if m:
            out.append((canon(m.group(2)), m.group(2), m.group(1), i, line.strip()))
            continue
        m = RUST_CONST.match(line)
        if m:
            out.append((canon(m.group(1)), m.group(1), "const", i, line.strip()))
            continue
        m = RUST_TYPE_ALIAS.match(line)
        if m:
            out.append((canon(m.group(1)), m.group(1), "type", i, line.strip()))
    return out

# ---------------------------------------------------------------------------
# TS extraction
# ---------------------------------------------------------------------------
TS_TOP = re.compile(
    r"^export\s+(?:default\s+)?(?:abstract\s+)?(?:async\s+)?"
    r"(function|const|class|type|interface|enum)(?:\s*\*)?\s+(\w+)"
)
TS_PLAIN_FN = re.compile(r"^(?:async\s+)?function(?:\s*\*)?\s+(\w+)")
# indented class member: `  foo(`, `  async foo(`, `  foo<T>(`, `  static foo(`,
# generator `  *foo(` / `  async *foo(`, private `  #foo(` (rule 2: private TS
# methods port 1:1 under the transliterated name; canon() strips the `#`).
# The name-followed-by-`(`/`<` requirement keeps field decls (`  #input;`,
# `  #output: T = x;`) out.
TS_METHOD = re.compile(
    r"^  (?:public |private |protected |static |readonly |async |get |set |override )*"
    r"(?:\*\s*)?(#?\w+)\s*[(<]"
)
TS_METHOD_ARROW = re.compile(
    r"^  (?:public |private |protected |static |readonly )*"
    r"(?:#?\w+)\s*(?::[^=]+)?=\s*(?:async\s*)?\("
)
# keywords that look like methods but aren't
TS_KW = {
    "if", "for", "while", "switch", "catch", "return", "do", "else",
    "constructor", "function", "typeof", "await", "new", "throw", "yield",
    "case", "super", "this", "void", "in", "of", "as",
}

def extract_ts(path):
    out = []
    with open(path, encoding="utf-8") as f:
        lines = f.readlines()
    for i, line in enumerate(lines, 1):
        m = TS_TOP.match(line)
        if m:
            out.append((canon(m.group(2)), m.group(2), m.group(1), i, line.strip()))
            continue
        m = TS_PLAIN_FN.match(line)
        if m:
            out.append((canon(m.group(1)), m.group(1), "function", i, line.strip()))
            continue
        m = TS_METHOD.match(line)
        if m and m.group(1) not in TS_KW:
            out.append((canon(m.group(1)), m.group(1), "method", i, line.strip()))
    return out

# ---------------------------------------------------------------------------
# Build ledger
# ---------------------------------------------------------------------------
def rel(p):
    return os.path.relpath(p, REPO)

def _is_test_ts(relpath, fname):
    """A TS file that is test/fixture/debug scaffolding, not a parity target."""
    if fname.endswith((".test.ts", ".d.ts", ".bench.ts")):
        return True
    parts = relpath.split(os.sep)
    if "test" in parts or "tests" in parts or "__tests__" in parts:
        return True
    # helper/generator/debug scaffolding kept beside source (not under test/):
    if fname.startswith("test-") or fname.endswith(("-test-util.ts", "-gen.ts")):
        return True
    return False

def expand_ts_files(spec):
    """Repo-relative TS origin paths. An entry ending in '/' is a directory —
    recursively globbed for non-test .ts files (so a crate that ports a whole
    subtree like zql/src/ivm/ needn't list every file). Test/fixture/debug
    scaffolding is skipped, plus any path matching a spec 'ts_exclude' substring
    (e.g. pure client-fluent / TS type-level files that have no runtime to
    port)."""
    excl = spec.get("ts_exclude", ())
    out = []
    for rp in spec["ts_files"]:
        if rp.endswith("/"):
            root = os.path.join(REPO, rp)
            for dp, _d, files in os.walk(root):
                for f in files:
                    relp = os.path.relpath(os.path.join(dp, f), REPO)
                    if f.endswith(".ts") and not _is_test_ts(relp, f) \
                       and not any(x in relp for x in excl):
                        out.append(relp)
        else:
            out.append(rp)
    return sorted(set(out))

def ts_label(spec, path):
    """File label for a TS origin path — strip the crate's ts_label_prefix so
    subdir files read as e.g. 'ivm/join.ts' / 'schema/cvr.ts'. Optional
    `ts_label_rewrites` (list of (find, replace)) is applied FIRST, so a
    cross-package origin ported into a rust subdir mirrors correctly — e.g.
    ivm maps `packages/zqlite/src/` → `sqlite/` so `zqlite/src/table-source.ts`
    labels as `sqlite/table-source.ts` and mirror-matches `sqlite/table_source.rs`."""
    r = rel(path)
    for find, repl in spec.get("ts_label_rewrites", ()):
        if find in r:
            return repl + r.split(find, 1)[-1]
    pref = spec.get("ts_label_prefix", "view-syncer/")
    return r.split(pref, 1)[-1] if pref in r else r

def walk_rs(root):
    out = []
    for dp, _d, files in os.walk(root):
        for f in files:
            if f.endswith(".rs"):
                out.append(os.path.relpath(os.path.join(dp, f), root))
    return sorted(out)

def main():
    crate = sys.argv[1] if len(sys.argv) > 1 else "cvr"
    spec = CRATES[crate]

    rust_syms = {}   # canon -> list of (name, kind, file, line, sig)
    rust_root = os.path.join(REPO, spec["rust_dir"])
    for fn in walk_rs(rust_root):   # recursive; subdir files read as "ivm/join.rs"
        path = os.path.join(rust_root, fn)
        for c, name, kind, ln, sig in extract_rust(path):
            rust_syms.setdefault(c, []).append((name, kind, fn, ln, sig))

    ts_syms = {}
    for rp in expand_ts_files(spec):
        path = os.path.join(REPO, rp)
        if not os.path.exists(path):
            continue
        base = ts_label(spec, path)
        for c, name, kind, ln, sig in extract_ts(path):
            ts_syms.setdefault(c, []).append((name, kind, base, ln, sig))

    core_ts = spec.get("core_ts", set())
    infra_rust = spec.get("infra_rust", set())

    ts_keys = set(ts_syms)
    rust_keys = set(rust_syms)
    matched = sorted(ts_keys & rust_keys)
    ts_only = sorted(ts_keys - rust_keys)
    rust_only = sorted(rust_keys - ts_keys)

    def first(d, k):
        return d[k][0]

    def mirror_of_ts(tf):
        return tf.replace("-", "_").replace(".ts", ".rs")

    # A canon with several occurrences on both sides (common names: `filter`,
    # `destroy`, `getSchema`, `push`) used to bind first-occurrence-first —
    # e.g. filter-operators.ts::filter ⇢ exists.rs::filter — SHADOWING the
    # true ivm/filter.ts::Filter ⇄ ivm/filter.rs::Filter pair, so mirrored
    # files read as sharing zero symbols. Prefer the occurrence pair whose
    # files mirror (rule 3); fall back to first-first as before.
    rep_pair = {}
    for k in matched:
        tlist, rlist = ts_syms[k], rust_syms[k]
        pick = None
        for t in tlist:
            mf = mirror_of_ts(t[2])
            for r in rlist:
                if r[2] == mf:
                    pick = (t, r)
                    break
            if pick:
                break
        rep_pair[k] = pick or (tlist[0], rlist[0])

    aliases = spec.get("aliases", {})  # canon_ts -> (target|"INLINED"|"ABSENT", note)

    # === resolve renames via fuzzy token overlap (greedy global best-first) ===
    cands = []
    for tc in ts_only:
        if tc in aliases:
            continue
        tt = tokens(first(ts_syms, tc)[0])
        for rc in rust_only:
            rt = tokens(first(rust_syms, rc)[0])
            s = jaccard(tt, rt)
            if s >= FUZZY_THRESHOLD and distinctive(tt & rt):
                cands.append((s, tc, rc))
    cands.sort(key=lambda x: (-x[0], x[1], x[2]))
    used_ts, used_rust, fuzzy = set(), set(), {}
    for s, tc, rc in cands:
        if tc in used_ts or rc in used_rust:
            continue
        used_ts.add(tc); used_rust.add(rc); fuzzy[tc] = (rc, s)

    unresolved_ts = [k for k in ts_only if k not in fuzzy and k not in aliases]
    added_rust = [k for k in rust_only if k not in used_rust]

    # === file-structure edges (TS file -> Rust file) from exact + fuzzy pairs ===
    edges = defaultdict(lambda: defaultdict(int))     # tf -> rf -> count
    rust_incoming = defaultdict(set)                  # rf -> {tf}
    # per-Rust-file buckets of resolved pairs
    pairs_by_rf = defaultdict(list)                   # rf -> [(ts_name, rust_name, tag)]
    # Credit EVERY mirror-consistent occurrence pair as a file edge: a canon
    # living in several mirrored file pairs (`filter` in both
    # filter-operators.ts⇄filter_operators.rs AND filter.ts⇄filter.rs) is
    # evidence for EACH pair — a single representative starved the second
    # mirror and left it DROPPED. One row per (canon, TS file); canons with no
    # mirror-consistent pair fall back to the rep_pair as before.
    for k in matched:
        tlist, rlist = ts_syms[k], rust_syms[k]
        credited = set()
        for t in tlist:
            mf = mirror_of_ts(t[2])
            if mf in credited:
                continue
            r = next((r for r in rlist if r[2] == mf), None)
            if r is None:
                continue
            credited.add(mf)
            tn, _, tf, tl, _ = t
            rn, _, rf, rl, _ = r
            edges[tf][rf] += 1; rust_incoming[rf].add(tf)
            pairs_by_rf[rf].append((tf, tn, tl, rn, rl, "exact"))
        if not credited:
            (tn, _, tf, tl, _), (rn, _, rf, rl, _) = rep_pair[k]
            edges[tf][rf] += 1; rust_incoming[rf].add(tf)
            pairs_by_rf[rf].append((tf, tn, tl, rn, rl, "exact"))
    for tc, (rc, s) in fuzzy.items():
        tn, _, tf, tl, _ = first(ts_syms, tc)
        rn, _, rf, rl, _ = first(rust_syms, rc)
        edges[tf][rf] += 1; rust_incoming[rf].add(tf)
        pairs_by_rf[rf].append((tf, tn, tl, rn, rl, f"fuzzy {s:.2f}"))
    # === L9 structural guard (`--enforce-structure`): a resolved pair whose
    # rust file is NOT the mirror of its TS file is MISFILED (rule 3). The
    # threshold is a RATCHET (spec "max_misfiled"): documented exceptions
    # (types-utility folding, metrics counters) are allowed; growth fails CI.
    if "--enforce-structure" in sys.argv:
        def mirror_of(tf):
            return tf.replace("-", "_").replace(".ts", ".rs")
        misfiled = sorted(
            (tf, tn, rf, rn)
            for rf, plist in pairs_by_rf.items()
            for (tf, tn, _tl, rn, _rl, _tag) in plist
            if mirror_of(tf) != rf
        )
        limit = spec.get("max_misfiled")
        print(f"L1 structural guard [{crate}]: {len(misfiled)} misfiled resolved "
              f"symbol(s) (ratchet max {limit})")
        for tf, tn, rf, rn in misfiled:
            print(f"  {tf}::{tn} -> {rf}::{rn}")
        if limit is not None and len(misfiled) > limit:
            print("L1 structural guard: FAIL — misfiled count grew past the ratchet; "
                  "move the symbol(s) to the mirrored file or register the exception "
                  "(spec aliases) and bump max_misfiled WITH justification.")
            sys.exit(1)
        print("L1 structural guard: OK")
        sys.exit(0)

    # pinned aliases that name a Rust file also count as a file edge, so a TS file
    # resolved entirely via aliases (e.g. ttl-clock.ts) isn't mislabelled DROPPED.
    all_rust_files = walk_rs(os.path.join(REPO, spec["rust_dir"]))
    for tc, (tgt, note) in aliases.items():
        if tc not in ts_syms:
            continue
        m = re.search(r"([\w/]+\.rs)", f"{tgt} {note}")
        if not m:
            continue
        tf = first(ts_syms, tc)[2]
        # Resolve the cited filename against the crate's real files: notes often
        # cite the bare name (`view_syncer.rs`), which used to create a PHANTOM
        # file key beside `services/view_syncer/view_syncer.rs` and inflate
        # SPLIT labels. Exact path > unique suffix > mirror-of-TS-file among
        # suffix matches; an unresolvable citation adds no edge.
        cap = m.group(1)
        if cap in all_rust_files:
            rf = cap
        else:
            cands = [fn for fn in all_rust_files if fn.endswith("/" + cap)]
            if len(cands) == 1:
                rf = cands[0]
            elif mirror_of_ts(tf) in cands:
                rf = mirror_of_ts(tf)
            else:
                continue
        edges[tf][rf] += 1; rust_incoming[rf].add(tf)

    # LOC per file
    def loc(path):
        try:
            with open(path, encoding="utf-8") as f:
                return sum(1 for _ in f)
        except OSError:
            return 0
    ts_loc = {}
    for rp in expand_ts_files(spec):
        p = os.path.join(REPO, rp)
        if os.path.exists(p):
            ts_loc[ts_label(spec, p)] = loc(p)
    rust_loc = {fn: loc(os.path.join(REPO, spec["rust_dir"], fn)) for fn in all_rust_files}

    # classify each TS file's relationship
    def rel_kind(tf):
        tgt = edges.get(tf, {})
        if not tgt:
            return "DROPPED", []
        rfs = sorted(tgt.items(), key=lambda x: -x[1])
        top = rfs[0][1]
        # a secondary target only counts as a real split if it's substantial
        sig = [rf for rf, n in rfs[1:] if n >= max(3, top * 0.25)]
        if sig:
            return "SPLIT", rfs
        primary = rfs[0][0]
        return ("MERGED" if len(rust_incoming[primary]) > 1 else "1:1"), rfs

    new_rust = [fn for fn in all_rust_files
                if fn not in rust_incoming and (rust_loc[fn] > 0)]

    # ---------------------------------------------------------------- output
    print(f"# TS ⇄ Rust parity map — `{crate}` crate\n")
    print("_Deterministic. File edges + symbol pairs are derived from **shared symbol "
          "content**, never filenames — so renamed files (e.g. `drain-coordinator.ts`→"
          "`drain.rs`) and renamed symbols (`cvrErrorKind`→`CVRStoreError`) still bind. "
          "Bodies are not compared; behavior drift needs Layer-2 body review._\n")
    print(f"- symbols: TS **{len(ts_keys)}**, Rust **{len(rust_keys)}** · resolved pairs "
          f"**{len(matched)+len(fuzzy)}** (exact {len(matched)} + fuzzy {len(fuzzy)}) "
          f"+ aliases {len(aliases)}")
    structural_ts = spec.get("structural_ts", set())
    unresolved_behav = [k for k in unresolved_ts
                        if first(ts_syms, k)[1] in ("function", "method")
                        and first(ts_syms, k)[2] not in structural_ts]
    print(f"- 🟥 TS UNRESOLVED: **{len(unresolved_ts)}** "
          f"(**{len(unresolved_behav)}** behavioral ⇒ investigate · "
          f"{len(unresolved_ts)-len(unresolved_behav)} structural: zod/DDL/type-alias "
          f"⇒ serde/inline-SQL, expected) · 🟦 Rust-only ADDED: **{len(added_rust)}**\n")
    if unresolved_behav:
        print("> ⚠️ **Behavioral TS symbols with no Rust resolution — check these:** "
              + ", ".join(f"`{first(ts_syms, k)[0]}` ({first(ts_syms, k)[2]})"
                          for k in sorted(unresolved_behav)) + "\n")

    # ---- §1 FILE STRUCTURE DIFF ----
    print("## 1 · File structure diff\n")
    print(f"TS origin files: **{len(ts_loc)}**  ·  Rust files: **{len(all_rust_files)}** "
          f"({len(new_rust)} new)\n")
    print("| TS file (LOC) | rel | Rust file(s) (shared syms) |")
    print("|---|---|---|")
    for tf in sorted(ts_loc):
        kind, rfs = rel_kind(tf)
        rhs = ", ".join(f"`{rf}` ({n})" for rf, n in rfs) or "—"
        print(f"| `{tf}` ({ts_loc[tf]}) | **{kind}** | {rhs} |")
    print("\n**New Rust files (no TS origin — added in the port):**  "
          + (", ".join(f"`{fn}` ({rust_loc[fn]})" for fn in new_rust) or "none"))
    merges = {rf: s for rf, s in rust_incoming.items() if len(s) > 1}
    if merges:
        print("\n**Merges (many TS → one Rust file):**")
        for rf in sorted(merges):
            print(f"- `{rf}` ⟵ " + ", ".join(f"`{t}`" for t in sorted(merges[rf])))

    # ---- §2 PER-FILE FUNCTIONAL DIVERGENCE ----
    print("\n## 2 · Per-file functional divergence\n")
    # attribute unresolved TS symbols to their expected Rust file (via file edge)
    unresolved_by_rf = defaultdict(list)
    orphan_ts = []
    for k in unresolved_ts:
        tn, tk, tf, tl, _ = first(ts_syms, k)
        tgt = edges.get(tf, {})
        rf = max(tgt, key=tgt.get) if tgt else None
        (unresolved_by_rf[rf] if rf else orphan_ts).append((tn, tk, tf, tl))
    added_by_rf = defaultdict(list)
    for k in added_rust:
        rn, rk, rf, rl, _ = first(rust_syms, k)
        added_by_rf[rf].append((rn, rk, rl))

    for rf in all_rust_files:
        pairs = pairs_by_rf.get(rf, [])
        added = added_by_rf.get(rf, [])
        missing = unresolved_by_rf.get(rf, [])
        if not (pairs or added or missing):
            continue
        srcs = ", ".join(f"`{t}`" for t in sorted(rust_incoming.get(rf, []))) or "_(new)_"
        print(f"### `{rf}`  ⟵  {srcs}\n")
        if pairs:
            print("| TS symbol | Rust symbol | match |")
            print("|---|---|---|")
            for tf, tn, tl, rn, rl, tag in sorted(pairs, key=lambda x: x[1].lower()):
                print(f"| `{tn}` ({tf}:{tl}) | `{rn}` (:{rl}) | {tag} |")
        if missing:
            print(f"\n🟥 **TS symbols not resolved into this file ({len(missing)}):** "
                  + ", ".join(f"`{n}`" for n, *_ in sorted(missing)))
        if added:
            print(f"\n🟦 **Rust-only added here ({len(added)}):** "
                  + ", ".join(f"`{n}`" for n, *_ in sorted(added)))
        print()

    # ---- §3 FLAT ONE-TO-ONE MAP ----
    print("## 3 · Flat one-to-one symbol map (every TS symbol resolved)\n")
    print("| TS symbol | origin | → Rust | status |")
    print("|---|---|---|---|")
    for k in sorted(ts_keys, key=lambda k: (first(ts_syms, k)[2], first(ts_syms, k)[3])):
        tn, tk, tf, tl, _ = (rep_pair[k][0] if k in matched else first(ts_syms, k))
        if k in matched:
            rn, _, rf, rl, _ = rep_pair[k][1]
            print(f"| `{tn}` | {tf}:{tl} | `{rn}` {rf}:{rl} | ✅ exact |")
        elif k in fuzzy:
            rc, s = fuzzy[k]; rn, _, rf, rl, _ = first(rust_syms, rc)
            print(f"| `{tn}` | {tf}:{tl} | `{rn}` {rf}:{rl} | 🔁 rename {s:.2f} |")
        elif k in aliases:
            tgt, note = aliases[k]
            print(f"| `{tn}` | {tf}:{tl} | {tgt} | 📌 {note} |")
        else:
            print(f"| `{tn}` | {tf}:{tl} | — | 🟥 UNRESOLVED |")

if __name__ == "__main__":
    main()
