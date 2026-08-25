#!/usr/bin/env python3
"""
Layer-2 (body-differential) coverage cross-reference.

Layer 1 (parity_ledger.py) proved which functions EXIST on both sides.
Layer 2 asks the harder question: of the matched *pure/deterministic* functions,
which ones have a **differential fixture** pinning their BODY to TS's actual
output — and which are untested body-wise?

An untested pure function is exactly where a rowKey/schema-corruption-class bug
hides: name matches (Layer-1 green), body silently diverges (no Layer-2 assert).

  COVERED   - reachable (transitive closure over the crate call graph) from a
              differential harness. cvr: parity_check.rs + the flush/inspect/
              catchup PG differentials + the sequence fuzzer (seq_replay.rs).
              syncer: the in-crate `#[cfg(test)]` `*_parity_against_ts` fixtures
              + tests/. Each replays the real API against real-TS goldens.
  GAP       - pure & deterministic, differentiable, but NOT covered => build one
  IO        - async / DB / actor / transport: needs integration diff, not a unit
  NA        - documented non-differentiable (trivial accessor / lifecycle / RNG /
              representation / cross-boundary-only key)
  INFRA     - metrics/observability/trait-decl — no body to differentiate

Usage: python3 parity/layer2_coverage.py [cvr|syncer] > parity/COVERAGE-<crate>.md
"""
import os
import re
import sys
from parity_ledger import extract_rust, canon, CRATES, REPO  # noqa: F401

CRATE = sys.argv[1] if len(sys.argv) > 1 else "cvr"

# ─── per-crate Layer-2 config ────────────────────────────────────────────────
CFG = {
    "cvr": {
        "title": "rust-cvr",
        "harness_note": "COVERED = reachable (transitive closure over the crate call "
            "graph) from a differential harness: parity_check.rs + the flush/inspect/"
            "catchup PG differentials + the sequence fuzzer (seq_replay.rs), which drive "
            "the real API against real-TS goldens with 150+ fuzzed programs + property "
            "tests. Reachability ≠ every-branch-exercised, but it is a tight proxy.",
        # src files whose ENTIRE content seeds COVERED (pure harness / driver)
        "seed_files": ["parity_check.rs", "seq_replay.rs"],
        # tests/ files whose calls seed COVERED (None = every tests/*.rs)
        "seed_tests": ["flush_pg_test.rs", "inspect_pg_test.rs",
                       "catchup_pg_test.rs", "seq_diff_pg_test.rs"],
        # syncer keeps its differentials inline in `#[cfg(test)]`; cvr does not,
        # so cvr leaves the pool + call-graph exactly as the walk finds them.
        "seed_cfg_test": False,
        "infra_files": {"otel_metrics.rs", "live_count.rs", "tracer.rs"},
        "io_files": {"row_record_cache.rs", "change_processor.rs"},
        "io_sig": r"\basync\b|Pool|Executor|Transaction|Handle|Pg|tokio|-> impl",
        "non_diff": {
            "updated_version": "trivial getter — returns `self.base.cvr.version`",
            "has_pending_writes": "trivial getter — `!self.pending.is_empty()`",
            "row_count": "trivial getter — returns `self.row_count`",
            "catchup_reader": "thin handle ctor (clones pool/schema/cvr_id); the "
                              "reader's DB work is covered by the catchup PG differential",
            "close": "lifecycle side-effect (`eprintln!` + `downstream.cancel()`) — no "
                     "differentiable output",
            "send_query_transform_failed_error": "documented TS↔Rust protocol divergence "
                     "(TS `fail(ProtocolError)` channel vs Rust `['error', …]`); byte-parity "
                     "is NOT the contract",
            "force_updates": "set-insert of the already-pinned `row_id_string(id)`; no "
                             "un-pinned logic of its own",
        },
    },
    "syncer": {
        "title": "rust-syncer",
        "harness_note": "COVERED = reachable (transitive closure over the crate call "
            "graph, incl. fn-pointer edges like `.sort_by(cmp_condition)` / "
            "`.any(is_always_false)`) from a differential harness: the in-crate "
            "`*_parity_against_ts` fixtures (jwt / read-authorizer hash goldens / "
            "url_match / query_covering / serving_lag / e2e_serving_lag / parse_int) + the "
            "phase/rowkey/stage integration tests. Reachability ≠ every-branch-exercised.",
        "seed_files": [],
        "seed_tests": None,   # every tests/*.rs
        # the syncer's 6+ differentials are `#[cfg(test)]` `*_parity_against_ts`
        # modules inside the src files; seed COVERED from those blocks and drop
        # the test fns from the surface pool.
        "seed_cfg_test": True,
        "infra_files": {"metrics.rs", "otel.rs", "live_count.rs", "trace.rs"},
        # transport / actor / process host — differentiated (if at all) by the
        # integration tests + the sibling rust-ivm / rust-cvr oracles, not a unit.
        "io_files": {"ws_server.rs", "ws_sink.rs", "http_server.rs", "router.rs",
                     "push_relay.rs", "main.rs", "sync_engine.rs", "connection.rs"},
        "io_sig": (r"\basync\b|Pool|Executor|tokio|TcpListener|WebSocket|reqwest|"
                   r"hyper|axum|-> impl|Sink\b|Sender\b|Receiver\b"),
        "non_diff": {
            "total_queries": "trivial getter — sums query counts over the registry snapshots",
            "total_rows": "trivial getter — sums row counts over the registry snapshots",
            "compute_serving_lag_distribution": "gathers live registry snapshots then calls the "
                "already-pinned `compute_serving_lag_distribution_ms` (serving_lag_parity_against_ts); "
                "the wrapper reads DashMap state, no un-pinned math",
            "row_set_signature": "delegates to `rust_ivm engine.row_set_signature` (covered by the "
                "rust-ivm oracle); the persisted value is asserted by `stage_e_test`",
            "to_error_body": "pure CCMError→wire-`ErrorBody` mapping; the wire shapes are pinned by "
                "`protocol_test` and the mapping is exercised by the phase2 error-path tests — no "
                "single TS `toErrorBody` fn to differentiate against",
        },
    },
}
cfg = CFG[CRATE]
spec = CRATES[CRATE]
RUST_DIR = os.path.join(REPO, spec["rust_dir"])

CALL_RE = re.compile(r"\b([a-z_][a-z0-9_]+)\s*\(")
# fn-pointer passed as a lone call argument: `.any(is_always_false)`,
# `.sort_by(cmp_condition)`. A real call edge the call-syntax regex misses.
FNPTR_RE = re.compile(r"[(,]\s*([a-z_][a-z0-9_]+)\s*[,)]")


def calls_in(text):
    return set(CALL_RE.findall(text))


def edges_in(text):
    """Call edges: direct calls + fn-pointers passed as bare call arguments."""
    return set(CALL_RE.findall(text)) | set(FNPTR_RE.findall(text))


def cfg_test_ranges(src):
    """(start_line, end_line) for each `#[cfg(test)]` block (brace-matched)."""
    ranges = []
    for m in re.finditer(r"#\[cfg\(test\)\]", src):
        i = src.find("{", m.end())
        if i < 0:
            continue
        depth, j = 0, i
        while j < len(src):
            if src[j] == "{":
                depth += 1
            elif src[j] == "}":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        ranges.append((src.count("\n", 0, m.start()) + 1, src.count("\n", 0, j) + 1))
    return ranges


def in_ranges(ln, ranges):
    return any(a <= ln <= b for a, b in ranges)


def fn_call_graph(src, ranges):
    """name -> set(callee names) for fns defined OUTSIDE the given line ranges."""
    out = {}
    for m in re.finditer(r"\bfn\s+([a-z_][a-z0-9_]*)\s*[(<]", src):
        ln = src.count("\n", 0, m.start()) + 1
        if in_ranges(ln, ranges):
            continue
        i = src.find("{", m.end())
        if i < 0:
            continue
        depth, j = 0, i
        while j < len(src):
            if src[j] == "{":
                depth += 1
            elif src[j] == "}":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        out.setdefault(m.group(1), set()).update(edges_in(src[i:j]))
    return out


# ─── build the surface pool + crate call graph + harness seed ────────────────
# Walk RECURSIVELY (os.walk, not os.listdir): the crates have subdirs after the
# 1:1 file refactor; a flat listdir silently drops them (and collides top-level
# cvr.rs with schema/cvr.rs). File labels are RELATIVE to RUST_DIR.
rust_fns = {}          # canon -> (name, rel_file, sig)
name_to_canons = {}    # name -> {canon}
calls_by_name = {}     # name -> {callee names}  (impl-only)
seed_texts = []        # harness code whose calls seed COVERED
rs_files = []
for dirpath, _dirs, files in os.walk(RUST_DIR):
    for f in files:
        if f.endswith(".rs"):
            full = os.path.join(dirpath, f)
            rs_files.append((os.path.relpath(full, RUST_DIR), full))

for rel, full in sorted(rs_files):
    src = open(full, encoding="utf-8").read()
    ranges = cfg_test_ranges(src) if cfg["seed_cfg_test"] else []
    if cfg["seed_cfg_test"]:
        lines = src.split("\n")
        for a, b in ranges:
            seed_texts.append("\n".join(lines[a - 1:b]))
    if rel in cfg["seed_files"]:
        seed_texts.append(src)
    for c, name, kind, ln, sig in extract_rust(full):
        if kind != "fn":
            continue
        if cfg["seed_cfg_test"] and in_ranges(ln, ranges):
            continue
        rust_fns.setdefault(c, (name, rel, sig))
        name_to_canons.setdefault(name, set()).add(c)
    for name, callees in fn_call_graph(src, ranges).items():
        calls_by_name.setdefault(name, set()).update(callees)

TESTS_DIR = os.path.normpath(os.path.join(RUST_DIR, "..", "tests"))
test_files = cfg["seed_tests"]
if test_files is None:
    test_files = (sorted(f for f in os.listdir(TESTS_DIR) if f.endswith(".rs"))
                  if os.path.isdir(TESTS_DIR) else [])
for t in test_files:
    p = os.path.join(TESTS_DIR, t)
    if os.path.exists(p):
        seed_texts.append(open(p, encoding="utf-8").read())

seed = set()
for txt in seed_texts:
    seed |= calls_in(txt)

# transitive closure over crate-defined fn names, seeded from the harnesses
covered_names, frontier = set(), [n for n in seed if n in name_to_canons]
while frontier:
    nm = frontier.pop()
    if nm in covered_names:
        continue
    covered_names.add(nm)
    frontier.extend(callee for callee in calls_by_name.get(nm, ())
                    if callee in name_to_canons and callee not in covered_names)
covered = {c for nm in covered_names for c in name_to_canons[nm]}

# ─── classify each Rust fn ───────────────────────────────────────────────────
IO_SIG = re.compile(cfg["io_sig"])
INFRA_FILES = cfg["infra_files"]
IO_FILES = cfg["io_files"]
NON_DIFFERENTIABLE = cfg["non_diff"]


def classify(c, name, f, sig):
    if c in covered:
        return "COVERED"
    if name in NON_DIFFERENTIABLE:
        return "NA"
    base = os.path.basename(f)
    if base in INFRA_FILES or sig.rstrip().endswith(";") \
       or name.endswith("_for_test") or name in ("drop",):
        return "INFRA"
    if IO_SIG.search(sig) or base in IO_FILES:
        return "IO"
    return "GAP"


rows = []
for c, (name, f, sig) in sorted(rust_fns.items(), key=lambda kv: (kv[1][1], kv[1][0])):
    rows.append((classify(c, name, f, sig), name, f, sig))


def emit(title, want):
    sel = [r for r in rows if r[0] == want]
    print(f"\n## {title} — {len(sel)}\n")
    if not sel:
        print("_none_")
        return
    print("| fn | file | signature |")
    print("|---|---|---|")
    for _, name, f, sig in sel:
        s = sig if len(sig) <= 90 else sig[:87] + "…"
        print(f"| `{name}` | {f} | `{s}` |")


n = len(rows)
nc = sum(1 for r in rows if r[0] == "COVERED")
ng = sum(1 for r in rows if r[0] == "GAP")
ni = sum(1 for r in rows if r[0] == "IO")
nx = sum(1 for r in rows if r[0] == "INFRA")
na = sum(1 for r in rows if r[0] == "NA")
print(f"# {cfg['title']} — Layer-2 (body-differential) coverage\n")
print(f"_{cfg['harness_note']}_\n")
print(f"- Rust fns total **{n}** · ✅ COVERED **{nc}** · 🟥 GAP (pure, untested) "
      f"**{ng}** · ⚙️ IO (integration diff) **{ni}** · ◻️ infra/metrics **{nx}** · "
      f"◻️ documented n/a **{na}**")
print(f"- Body-differential coverage of the **unit-testable pure surface**: "
      f"**{nc}/{nc+ng} = {100*nc/max(1,nc+ng):.0f}%**")

# highest-risk gaps: fns that emit patches / build rowKeys/schemas / mutate state
RISK = ("received", "track", "desired", "unreferenced", "row_patch", "patch",
        "delete_client", "ensure_client", "query_record", "eviction", "row_id",
        "signature", "merge", "schema", "token", "auth", "drain", "classify")
risky = [r for r in rows if r[0] == "GAP" and any(t in r[1] for t in RISK)]
if risky:
    print("\n> ⚠️ **Highest-risk uncovered (build rowKeys/schemas / classify / mutate "
          "state — the corruption class):** "
          + ", ".join(f"`{r[1]}` ({r[2]})" for r in sorted(risky)))

emit("🟥 GAP — pure & deterministic, NO differential fixture (build these)", "GAP")

na_rows = [r for r in rows if r[0] == "NA"]
print(f"\n## ◻️ NON-DIFFERENTIABLE — documented n/a (no un-pinned body) — {len(na_rows)}\n")
if na_rows:
    print("| fn | file | why not a body-differential |")
    print("|---|---|---|")
    for _, name, f, sig in sorted(na_rows, key=lambda r: r[1]):
        print(f"| `{name}` | {f} | {NON_DIFFERENTIABLE[name]} |")
else:
    print("_none_")

emit("✅ COVERED — body pinned to TS fixture", "COVERED")
emit("⚙️ IO — async/DB/actor/transport, use the integration diff", "IO")
