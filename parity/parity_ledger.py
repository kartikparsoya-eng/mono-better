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
        "ts_files": [
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
                       "change_processor.rs", "ttl.rs", "shards.rs"},
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
            "assert": ("assert_new_version (cvr.rs)", "rename"),
        },
    },
    "ivm": {
        "rust_dir": "packages/rust-ivm/src",
        "ts_label_prefix": "zql/src/",
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
        ],
        "ts_exclude": (
            "query/query.ts",          # TS type machinery: PullRow/QueryReturn/DeepMerge/…
            "query/create-builder.ts", # client fluent-builder factory
            "planner/planner-debug.ts",  # planner debug-event system — not ported
            "builder/debug-delegate.ts", # debug/instrumentation delegate — not ported
        ),
        "structural_ts": set(),
        # Triaged 2026-08-24 (3 parallel Explore agents + import-graph checks).
        # Each entry is a behavioral TS symbol confirmed COVERED/N-A in Rust:
        # inlined, renamed, relocated cross-crate, a JS-only idiom Rust drops, or
        # replaced by SQLite. Zero genuine gaps found in the triaged set.
        "aliases": {
            # view-apply-change.ts → array_view.rs (array maintenance inlined)
            "arraywith": ("array_view.rs new_view[pos]=…", "inlined"),
            "insertat": ("array_view.rs Vec::insert", "inlined"),
            "removeat": ("array_view.rs Vec::remove", "inlined"),
            "setproperty": ("array_view.rs field assign", "inlined"),
            "setrefcount": ("array_view.rs inc/dec_ref_count", "inlined"),
            "assertarray": ("N/A", "TS type-guard; Rust View enum"),
            "assertnumber": ("N/A", "TS type-guard; Rust ref_count:usize"),
            "assertmetaentry": ("N/A", "TS type-guard; Rust Entry struct"),
            "track": ("N/A", "JS WeakSet COW -> Rust Rc::make_mut"),
            "owns": ("N/A", "JS WeakSet COW -> Rust Rc::make_mut"),
            # builder.ts internals
            "bindstaticparameters": ("rust-syncer permissions.rs", "relocated upstream (AST transform)"),
            "resolvefield": ("rust-syncer permissions.rs resolve_field", "relocated"),
            "isparameter": ("permissions.rs bind_value", "inlined"),
            "groupsubqueryconditions": ("builder.rs apply_or_filter .partition", "inlined"),
            "valueposname": ("builder.rs", "inlined"),
            "addedge": ("N/A", "debug-instrumentation decorator; Rust wires Rc directly"),
            "decorateinput": ("N/A", "debug-instrumentation decorator; not ported"),
            "decoratefilterinput": ("N/A", "debug-instrumentation decorator; not ported"),
            # planner algorithm
            "processand": ("planner/builder.rs process_condition", "inlined"),
            "processor": ("planner/builder.rs process_condition", "inlined"),
            "propagateunlimitforflippedjoins": ("planner/graph.rs:298", "renamed"),
            "flipifneeded": ("N/A", "dead code in TS; planning calls flip() directly (Rust too)"),
            # planner-connection.ts *ForDebug + planner-join debug
            "getconstraintsfordebug": ("N/A", "debug introspection; not ported"),
            "getfiltersfordebug": ("N/A", "debug introspection; not ported"),
            "getsortfordebug": ("N/A", "debug introspection; not ported"),
            "getconstraintcostsfordebug": ("N/A", "debug introspection; not ported"),
            "getdebuginfo": ("N/A", "debug introspection; not ported"),
            "getnodename": ("N/A", "debug introspection; not ported"),
            # memory-source.ts → SQLite table_source (in-memory overlay machinery replaced)
            "computeoverlays": ("sqlite/table_source.rs", "-> SQLite (overlays via SQLite tx)"),
            "overlaysforconstraint": ("sqlite/table_source.rs", "-> SQLite"),
            "overlaysformulticonstraint": ("sqlite/table_source.rs", "-> SQLite"),
            "overlaysforstartat": ("sqlite/table_source.rs", "-> SQLite"),
            "overlaysforfilterpredicate": ("sqlite/table_source.rs", "-> SQLite"),
            "setoverlay": ("sqlite/table_source.rs", "-> SQLite"),
            "getindexkeys": ("sqlite/table_source.rs", "-> SQLite index"),
            "fork": ("N/A", "TS memory-source fork; Rust source is SQLite-backed"),
            "tableschema": ("sqlite/table_source.rs", "-> SQLite"),
            "stringify": ("N/A", "TS memory-source key stringify; Rust uses SQLite keys"),
            # stream.ts / misc idioms
            "draingenerator": ("N/A", "TS generator drain -> Rust Iterator drop/for_each"),
            "consume": ("streamer/mod.rs", "-> Rust Iterator consume"),
            "clonedata": ("ivm/memory_storage.rs", "inlined clone"),
            "mergeempty": ("ivm/push_accumulated logic", "inlined"),
            "normalizeundefined": ("ivm/data.rs", "inlined (undefined->null)"),
            "patterntoregexp": ("builder/like.rs get_like_predicate", "predicate closure, not regex"),
            "pinned": ("planner/runtime.rs", "method"),
            "delete": ("array_view.rs Vec::remove", "inlined"),
            "unreachable": ("Rust unreachable!() macro", "idiom"),
        },
    },
    "syncer": {
        "rust_dir": "packages/rust-syncer/src",
        "ts_label_prefix": "zero-cache/src/",
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
            f"{ZC}/workers/connect-params.ts",
            f"{ZC}/workers/connection.ts",
            f"{ZC}/workers/syncer.ts",
            f"{ZC}/workers/syncer-ws-message-handler.ts",
            f"{ZC}/services/view-syncer/connection-context-manager.ts",
            f"{ZC}/services/view-syncer/drain-coordinator.ts",
            f"{ZC}/services/view-syncer/e2e-serving-lag.ts",
            f"{ZC}/services/view-syncer/pipeline-driver.ts",
            f"{ZC}/services/view-syncer/query-covering.ts",
            f"{ZC}/services/view-syncer/view-syncer.ts",
            f"{ZC}/custom-queries/transform-query.ts",
            f"{ZC}/custom/fetch.ts",
            f"{ZC}/db/lite-tables.ts",
        ],
        # Serving-loop / transform algorithms — a missing behavioral symbol here
        # is HIGH risk. Structural/DDL/type files are LOW risk (see structural_ts).
        "core_ts": {
            "view-syncer.ts", "pipeline-driver.ts", "connection.ts",
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
            # view-syncer.ts serving loop -> router.rs / sync_engine.rs (async task)
            "contentsandversion": ("sync_engine.rs (strip _0_version)", "inlined"),
            "elapsedlap": ("N/A", "per-lap timing via Instant::elapsed() inline"),
            "expired": ("router.rs remove_expired_queries", "TTL/inactivation expiry"),
            "keepalive": ("router.rs CgState.keepalive_until", "field + next_idle_shutdown_delay"),
            "markinitialized": ("router.rs CgState.terminal", "init-state flag; test helper dropped"),
            "readystate": ("router.rs CgState/event loop", "init/drain state flags"),
            "run": ("router.rs cg_event_loop", "per-CG async serving loop"),
            "shutdownbeforeinitializationerror": ("router.rs init-fail path", "error on terminal init failure"),
            "start": ("router.rs ensure_cvr/CgState init", "CVR load + ttl seed"),
            "startwithoutyielding": ("N/A", "no setImmediate; sync Instant::now start"),
            "stop": ("router.rs shutdown()", "per-CG drain + Rehome"),
            "totalelapsed": ("N/A", "inline Instant::elapsed accumulation"),
            "yieldprocess": ("N/A", "tokio async yield; no global-lock setImmediate"),
            # pipeline-driver.ts -> pipeline_driver.rs + rust-ivm (advance gate, ops)
            "addquery": ("rust-ivm engine add_queries", "streaming add (cross-crate)"),
            "advancementresettimelimitms": ("rust-ivm advance_gate.rs", "ported"),
            "advancewithoutdiff": ("pipeline_driver.rs advance_without_diff", "ported"),
            "assert": ("Rust assert! macro", "idiom"),
            "currentpermissions": ("router.rs/message_handler perms reload", "perms hot-reload at CG dispatch"),
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
            "getwebsocketserveroptions": ("ws_server.rs WebSocketConfig", "compression opts"),
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
            # custom/fetch.ts
            "apiattempts": ("metrics.rs record_api_attempt", "OTel counter"),
            "apierrorfromresult": ("custom_queries/transform_query.rs response validation", "error extraction"),
            "apiresponseerrormetricattrs": ("metrics.rs record_api_attempt attrs", "status attrs"),
            "urlmatch": ("custom_queries/transform_query.rs url_match", "URLPattern subset (renamed 1:1)"),
            "compileurlpattern": ("N/A", "no separate compile step; url_match matches the raw pattern inline"),
            # workers/connection.ts — Node errno predicates with no tungstenite
            # surface. The transient-MESSAGE path (isTransientSocketMessage) IS
            # ported (connection.rs classify_error_log_level); the errno-CODE path
            # has no equivalent (tungstenite/tokio errors don't carry errno).
            "haserrno": ("N/A", "Node `'errno' in e`; Rust WS stack has no errno"),
            "hastransientsocketcode": ("N/A", "Node EPIPE/ECONNRESET/ECANCELED; no errno in tungstenite"),
            # pipeline-driver.ts
            "logqueryfailure": ("inlined", "streamer error callback lives in rust-ivm; failures logged via tracing at the call sites"),
            "randomid": ("N/A", "TS pipelineRunID debug-correlation id; not ported"),
            # custom-queries/transform-query.ts
            "normalizedheaders": ("custom_queries/transform_query.rs normalized_headers", "canonical header hash"),
            # connection-context-manager.ts
            "filterheaders": ("router.rs filtered_query_headers", "header allowlist"),
            "sameconnectionselector": ("services/view_syncer/connection_context_manager.rs set_background_connection", "inlined tuple match"),
            # query-covering.ts
            "jsonequal": ("services/view_syncer/query_covering.rs json_equal", "deep eq w/ JS number semantics"),
            # db/lite-tables.ts
            "keycmp": ("db/lite_tables.rs sort_by len-then-lex", "inlined key compare"),
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
    skip_depth = 0          # >0 => currently inside a test module
    for i, line in enumerate(lines, 1):
        if skip_depth > 0:
            skip_depth += line.count("{") - line.count("}")
            continue
        if RUST_TEST_MOD.match(line):
            # enter skip; account for braces on this same line
            skip_depth = 1 + line.count("{") - line.count("}")
            if skip_depth < 1:
                skip_depth = 1
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
    return out

# ---------------------------------------------------------------------------
# TS extraction
# ---------------------------------------------------------------------------
TS_TOP = re.compile(
    r"^export\s+(?:default\s+)?(?:abstract\s+)?"
    r"(function|const|class|type|interface|enum)\s+(\w+)"
)
TS_PLAIN_FN = re.compile(r"^(?:async\s+)?function\s+(\w+)")
# indented class member: `  foo(`, `  async foo(`, `  foo<T>(`, `  static foo(`
TS_METHOD = re.compile(
    r"^  (?:public |private |protected |static |readonly |async |get |set |override )*"
    r"(\w+)\s*[(<]"
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
    subdir files read as e.g. 'ivm/join.ts' / 'schema/cvr.ts'."""
    pref = spec.get("ts_label_prefix", "view-syncer/")
    r = rel(path)
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
    for k in matched:
        tn, _, tf, tl, _ = first(ts_syms, k)
        rn, _, rf, rl, _ = first(rust_syms, k)
        edges[tf][rf] += 1; rust_incoming[rf].add(tf)
        pairs_by_rf[rf].append((tf, tn, tl, rn, rl, "exact"))
    for tc, (rc, s) in fuzzy.items():
        tn, _, tf, tl, _ = first(ts_syms, tc)
        rn, _, rf, rl, _ = first(rust_syms, rc)
        edges[tf][rf] += 1; rust_incoming[rf].add(tf)
        pairs_by_rf[rf].append((tf, tn, tl, rn, rl, f"fuzzy {s:.2f}"))
    # pinned aliases that name a Rust file also count as a file edge, so a TS file
    # resolved entirely via aliases (e.g. ttl-clock.ts) isn't mislabelled DROPPED.
    for tc, (tgt, note) in aliases.items():
        if tc not in ts_syms:
            continue
        m = re.search(r"([\w/]+\.rs)", f"{tgt} {note}")
        if m:
            tf = first(ts_syms, tc)[2]
            edges[tf][m.group(1)] += 1; rust_incoming[m.group(1)].add(tf)

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
    all_rust_files = walk_rs(os.path.join(REPO, spec["rust_dir"]))
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
        tn, tk, tf, tl, _ = first(ts_syms, k)
        if k in matched:
            rn, _, rf, rl, _ = first(rust_syms, k)
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
