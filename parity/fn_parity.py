#!/usr/bin/env python3
"""fn_parity.py — deterministic FUNCTION-name parity checker (Layer-1, fn-only).

`parity_ledger.py` maps *all* symbols (fns + types + consts) TS⇄Rust. This tool
narrows that to the question the architecture-doc mapping actually claims:

    "Is every ported TS **function** present in Rust under its transliterated
     (camelCase→snake_case) name, in a mirrored file?"

Why fn-only: TS type-level constructs (`interface`, `type`, conditional-type
generics like `DeepMerge`/`AssertQueryDefinitions`) and zod codecs have no 1:1
Rust twin by design — Rust folds them into structs / serde derives / enum
variants. Counting them as "unresolved" (as the full ledger does) drowns the
real signal. This tool reports ONLY `function`/`method` (TS) vs `fn` (Rust).

Matching is by `canon()` (case- and underscore-insensitive), which is exactly
the HARD-RULE-2 fidelity level: `liteTableName`⇄`lite_table_name`,
`#recordWebSocketError`⇄`record_websocket_error` (WebSocket→websocket is a
legitimate single-word transliteration, NOT drift — a strict `_`-per-capital
check would false-positive on every acronym, so we do not do that).

Output per crate:
  * TS functions with NO Rust twin  → real "not ported" candidates (minus the
    spec's `aliases` allow-list of confirmed INLINED/renamed/enum folds, and
    minus `structural_ts` files whose fns became inline SQL/serde).
  * Rust fns with no TS origin       → inventions / renames (informational).
  * counts + a PASS/REVIEW verdict.

Exit non-zero if any crate has un-adjudicated TS-function gaps → usable as a CI
gate alongside `parity_ledger.py --enforce-structure`.

Usage:  python3 parity/fn_parity.py [cvr|ivm|syncer|all]
"""

import os
import sys

from parity_ledger import (
    CRATES,
    REPO,
    canon,
    expand_ts_files,
    extract_rust,
    extract_ts,
    ts_label,
    walk_rs,
)

# Language builtins: TS `assert(...)` (shared/asserts.ts) becomes a Rust
# `assert!`/`debug_assert!` macro or `.expect()` — no ported fn by design.
BUILTIN_CANON = {"assert"}

# TS files whose functions are subsumed by a Rust library / idiom, not ported
# 1:1 (verified 2026-09-01). Keyed by the file's basename.
#   * sqlite/db.ts — a better-sqlite3 `Database` wrapper (`prepare`, `transaction`,
#     `pragma`, `iterate`, `safeIntegers`, …); rusqlite provides all of these
#     natively, so db.rs holds no transliterated twins.
#   * observability/metrics.ts + custom/metrics.ts — an OpenTelemetry
#     meter/instrument registry (`getMeter`, `getOrCreate*`, `recordMs`, `api*`);
#     the Rust `observability/metrics.rs` + `custom/metrics.rs` use the
#     `opentelemetry` crate's own instrument idiom (`INSTRUMENTS`, `record_*`).
LIBRARY_FOLD_FILES = {
    "db.ts": "rusqlite-native (better-sqlite3 Database shim)",
    "metrics.ts": "OTel meter/instrument registry (Rust `opentelemetry` idiom)",
}

# Specific TS functions confirmed (2026-09-01, by reading the Rust source) to be
# ported under a RESTRUCTURED name / merged fn (rule-3 fold), NOT missing. Keyed
# by canon(); value is the Rust home. A future genuinely-missing fn will NOT be
# on this list and so will surface as REVIEW.
CONFIRMED_FOLD = {
    # zqlite/query-builder.ts SQL-gen → build_select_query + helpers (query_builder.rs)
    "constraintstosql": "multi_constraint_to_sql + gather_start_constraints",
    "filterstosql": "condition_to_sql (inline in build_select_query)",
    "orderbytosql": "build_select_query ORDER BY branch",
    "valuepositiontosql": "value_position_to_sql_param",
    "likeconditiontosql": "condition_to_sql LIKE branch",
    "sargableleadingstartbound": "gather_start_constraints",
    "tosqlitetype": "rusqlite ToSql / sqlite_value_to_ivm (table_source.rs)",
    "getjstype": "sqlite_value_to_ivm (table_source.rs)",
    # zqlite/table-source.ts value/PK helpers → rusqlite + sqlite_value_to_ivm
    "tosqlitetypes": "rusqlite ToSql", "tosqlitetypename": "rusqlite ToSql",
    "fromsqlitetype": "sqlite_value_to_ivm", "fromsqlitetypes": "sqlite_value_to_ivm",
    "tosqliterow": "params_from_iter (table_source.rs fetch/update)",
    # services/mutagen/pusher.ts refcounting → combine_pushes (pusher.rs)
    "ref": "combine_pushes", "unref": "combine_pushes", "hasrefs": "combine_pushes",
    "assertarecompatiblepushes": "combine_key_of + combine_pushes",
    # pipeline-driver.ts → engine/view_syncer
    "buildprimarykeys": "client_primary_keys_from_schema (view_syncer.rs:8361)",
    "rowsetsignature": "accumulate_signature + row_signature_unit",
    # custom-queries/transform-query.ts methods → free fns (transform_query.rs)
    "transform": "transform_custom_queries", "validate": "validate_custom_queries",
    # workers/syncer.ts → observability/metrics.rs
    "recordviewsyncerlagsamples": "view_syncer_lag_otel",
    # view-syncer.ts → query_covering.rs (ledger L1 misfiled-map confirmed)
    "findquerycoverageshadowhit": "query_covering.rs::QueryCoverageShadowHit",
    # view-syncer.ts private methods → restructured into the CG event loop (view_syncer.rs)
    "runauthmaintenance": "on_auth_maintenance_tick (view_syncer.rs:1369)",
    "scheduleauthmaintenance": "arm_auth_maintenance (view_syncer.rs:1337)",
    "scheduleshutdown": "cg_event_loop shutdown deadline",
    "hasexpiredqueries": "TTL-expiry branch in the event loop",
    "processtransformedcustomqueries": "custom_queries/transform_query.rs::CustomTransformed",
    "istransformfailederror": "protocol/error.rs::TransformFailedHttpBody",
    # config/zero-config.ts → env-driven config (Rust loads from env, not a JS module)
    "getzeroconfig": "SyncerConfig::from_env (zero_config.rs:148)",
    "getnormalizedzeroconfig": "SyncerConfig::from_env",
    "assertnormalized": "from_env (validation inline)",
    "getserverversion": "build/env constant",
    "warnonce": "warn_if_quota_capped (warn-once idiom)",
    "resetwarnoncestate": "test-only warn-once reset",
    # custom/fetch.ts + custom/metrics.ts OTel instruments / error helpers
    "apiattemptduration": "custom/metrics.rs INSTRUMENTS",
    "apiinflight": "custom/metrics.rs INSTRUMENTS",
    "apifailedbody": "fetch.rs error-body handling",
    "legacypusherrorreason": "fetch.rs error classification",
    # pipeline-driver.ts internals → hydrate/advance public methods
    "hydrateinternal": "IvmPipelines::hydrate (pipeline_driver.rs:625)",
    "fetch": "IvmPipelines::advance / engine fetch",
    # server/syncer.ts bootstrap — folded into main.rs, or intentionally NOT ported
    "getcustomqueryconfig": "main.rs config wiring",
    "initeventsink": "main.rs OTLP/event bootstrap",
    "startanonymoustelemetry": "INTENTIONALLY NOT PORTED (anonymous telemetry, TS-only)",
    "registersqlitecorruptiondiagnostictarget": "INTENTIONALLY NOT PORTED (diagnostic, TS-only)",
    # drain-coordinator.ts getter → struct field
    "draining": "drain_coordinator.rs field",
    # query/ttl.ts → clamp/parse fns (ttl.rs)
    "normalizettl": "clamp_ttl + parse_ttl (query/ttl.rs)",
}

# TS names that are client-facing ZQL query-builder / type-registry API — the
# rust-ivm SERVER engine executes ASTs and never exposes the fluent builder, so
# these are an intentional scope boundary, not a port gap. Keyed by canon().
# (Documented in RUST-SYNCER-ARCHITECTURE.md §8 + parity/PARITY-EXCEPTIONS.md.)
CLIENT_API_CANON = {
    # query-impl.ts / runnable-query-impl.ts / query-internals.ts
    "newquery", "newqueryimpl", "asqueryimpl", "materializeimpl", "preloadimpl",
    "throwquerynotrunnable", "isonehop", "istwohop", "asqueryinternals",
    # query-registry.ts (defineQuery* / getQuery / createQuery family)
    "createquery", "definequery", "definequeries", "definequerywithtype",
    "definequerieswithtype", "getquery", "mustgetquery", "isquery",
    "isquerydefinition", "isqueryregistry", "addcontexttoquery",
    # expression.ts fluent builder helpers
    "eb", "cmplit", "filterfalse", "filtertrue", "filterundefined",
    "isparameterreference",
    # named.ts / schema-query.ts / validate-input.ts client wrappers
    "normalizeparser", "withvalidation", "syncedqueryimpl", "titlecase",
    "syncedquery", "syncedquerywithcontext",
    # query-delegate-base.ts / query-impl.ts / builder.ts / filter.ts client + type machinery
    "arrayviewfactory", "decoratesourceinput", "expressionbuilder", "iscompoundkey",
    "createispredicate", "deepmerge",
}


def fns_by_canon(triples):
    """canon -> set of (name, file) for function-kind symbols only."""
    out = {}
    for canon_key, name, kind, _ln, _sig, *rest in triples:
        out.setdefault(canon_key, set())
    return out


def collect_ts_fns(spec):
    """canon -> {(name, base_file)} for TS function/method symbols."""
    out = {}
    for rp in expand_ts_files(spec):
        path = os.path.join(REPO, rp)
        if not os.path.exists(path):
            continue
        base = ts_label(spec, path)
        for c, name, kind, _ln, _sig in extract_ts(path):
            if kind in ("function", "method"):
                out.setdefault(c, set()).add((name, base))
    return out


def collect_rust_fns(spec):
    """canon -> {(name, file)} for Rust fn symbols (test mods already skipped)."""
    out = {}
    root = os.path.join(REPO, spec["rust_dir"])
    for fn in walk_rs(root):
        for c, name, kind, _ln, _sig in extract_rust(os.path.join(root, fn)):
            if kind == "fn":
                out.setdefault(c, set()).add((name, fn))
    return out


def base_of(label):
    """'zql/src/ivm/foo.ts' or 'schema/foo.ts' -> 'foo.ts'."""
    return label.split("/")[-1]


def is_structural(names, structural):
    """True if EVERY origin file of this canon is a structural_ts file. The spec
    lists them WITH the `schema/` prefix ('schema/cvr.ts'), while ts_label can
    emit either form, so match on full label OR basename."""
    def hit(b):
        return b in structural or base_of(b) in {base_of(s) for s in structural} \
            or any(b.endswith(s) for s in structural)
    return all(hit(b) for (_n, b) in names)


def check_crate(crate):
    spec = CRATES[crate]
    ts = collect_ts_fns(spec)
    rust = collect_rust_fns(spec)
    aliases = {canon(k) if not k.islower() else k: v
               for k, v in spec.get("aliases", {}).items()}
    structural = spec.get("structural_ts", set())

    ts_keys = set(ts)
    rust_keys = set(rust)
    matched = ts_keys & rust_keys
    ts_only = ts_keys - rust_keys
    rust_only = rust_keys - ts_keys

    # Bucket the TS-only (unported) functions, most-specific bucket first.
    gaps = []
    buckets = {"builtin": [], "library-fold": [], "confirmed-fold": [],
               "adjudicated": [], "client-API": [], "structural": []}
    for c in sorted(ts_only):
        names = ts[c]
        if c in BUILTIN_CANON:
            buckets["builtin"].append((c, names)); continue
        if all(base_of(b) in LIBRARY_FOLD_FILES for (_n, b) in names):
            buckets["library-fold"].append((c, names)); continue
        if c in CONFIRMED_FOLD:
            buckets["confirmed-fold"].append((c, names)); continue
        if is_structural(names, structural):
            buckets["structural"].append((c, names)); continue
        if c in aliases:
            buckets["adjudicated"].append((c, names)); continue
        if c in CLIENT_API_CANON:
            buckets["client-API"].append((c, names)); continue
        gaps.append((c, names))

    print(f"\n{'='*72}\n{crate.upper()}  (fn-only Layer-1 parity)\n{'='*72}")
    print(f"  TS functions:          {len(ts_keys)}")
    print(f"  matched in Rust (1:1): {len(matched)}")
    accounted = " · ".join(f"{len(v)} {k}" for k, v in buckets.items() if v)
    print(f"  TS-only unresolved:    {len(ts_only)}  → {len(gaps)} REVIEW"
          + (f" · {accounted}" if accounted else ""))
    print(f"  Rust-only fns:         {len(rust_only)}  (inventions / handlers / renames)")

    if gaps:
        print(f"\n  🟥 TS functions with NO Rust twin — REVIEW ({len(gaps)}):")
        for c, names in gaps:
            shown = ", ".join(sorted(f"{n}  [{b}]" for n, b in names))
            print(f"      - {shown}")
    else:
        print("\n  ✅ every ported TS function has a Rust twin (transliterated "
              "1:1 or an allow-listed fold).")

    return len(gaps), crate


def main():
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    crates = ["cvr", "ivm", "syncer"] if which == "all" else [which]
    total_gaps = 0
    failed = []
    for c in crates:
        n, name = check_crate(c)
        total_gaps += n
        if n:
            failed.append(name)
    print(f"\n{'='*72}")
    if total_gaps == 0:
        print("fn-parity: PASS — 0 un-adjudicated TS-function gaps across "
              f"{', '.join(crates)}.")
        sys.exit(0)
    print(f"fn-parity: REVIEW — {total_gaps} TS-function gap(s) in "
          f"{', '.join(failed)}. Each is either a real un-ported fn (port it) "
          "or a confirmed fold → add to the crate's `aliases`/`CLIENT_API_CANON`.")
    sys.exit(1)


if __name__ == "__main__":
    main()
