#!/usr/bin/env python3
"""
Layer-2 coverage cross-reference for the rust-cvr crate.

Layer 1 (parity_ledger.py) proved which functions EXIST on both sides.
Layer 2 asks the harder question: of the matched *pure/deterministic* functions,
which ones have a **differential fixture** (parity_check.rs) pinning their BODY to
TS's actual output — and which are untested body-wise?

An untested pure function is exactly where a rowKey-corruption-class bug hides:
name matches (Layer-1 green), body silently diverges (no Layer-2 assert).

  COVERED   - reachable (transitively) from a differential harness: parity_check.rs
              + the flush/inspect/catchup PG differentials + the sequence fuzzer
              (seq_replay.rs), each replaying the real API against real-TS goldens
  GAP       - pure & deterministic, differentiable, but NOT covered  => build a fixture
  IO        - async / DB / actor: needs integration diff (ART mirror), not unit fixture
  NA        - documented non-differentiable (trivial accessor / lifecycle / divergence)

Usage: python3 parity/layer2_coverage.py > parity/COVERAGE-cvr.md
"""
import os
import re
from parity_ledger import extract_rust, canon, CRATES, REPO

spec = CRATES["cvr"]
RUST_DIR = os.path.join(REPO, spec["rust_dir"])

# --- all Rust fns in the crate (name-keyed) ---
# Walk RECURSIVELY (os.walk, not os.listdir): the crate has subdirs (schema/,
# bin/) after the 1:1 file refactor, and a flat listdir silently drops them —
# under-counting the pool and colliding top-level cvr.rs with schema/cvr.rs. The
# file label is the path RELATIVE to RUST_DIR so subdir files stay distinct.
rust_fns = {}   # canon -> (name, rel_file, sig)
rs_files = []
for dirpath, _dirs, files in os.walk(RUST_DIR):
    for f in files:
        if f.endswith(".rs"):
            full = os.path.join(dirpath, f)
            rs_files.append((os.path.relpath(full, RUST_DIR), full))
for rel, full in sorted(rs_files):
    for c, name, kind, ln, sig in extract_rust(full):
        if kind == "fn":
            rust_fns.setdefault(c, (name, rel, sig))

# --- which fns do the Layer-2 differential HARNESSES exercise? ---------------
# cvr's body-differential coverage is spread across FIVE harnesses, not just the
# in-memory parity_check.rs: the PG differentials (flush/inspect/catchup) and the
# stateful sequence fuzzer (seq_diff_pg_test.rs, driven by src/seq_replay.rs)
# replay the REAL updater/store API against real-TS goldens over live PG. A fn
# whose name is *called* in any harness has its body pinned. We then take the
# TRANSITIVE closure over the crate's own call graph: a private helper reached
# from a harness-driven fn is pinned too — a divergence in it changes the driven
# fn's differential output. NB reachability ≠ every-branch-exercised, but the
# harnesses include 150+ fuzzed programs + property tests, so it's a tight proxy.
CALL_RE = re.compile(r"\b([a-z_][a-z0-9_]+)\s*\(")

def calls_in(text):
    return set(CALL_RE.findall(text))

def fn_call_graph(path):
    """name -> set(callee names) for every fn defined in `path` (brace-matched body)."""
    src = open(path, encoding="utf-8").read()
    out = {}
    for m in re.finditer(r"\bfn\s+([a-z_][a-z0-9_]*)\s*[(<]", src):
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
        out.setdefault(m.group(1), set()).update(calls_in(src[i:j]))
    return out

# crate call graph (union callees per fn name) + name -> canons
calls_by_name, name_to_canons = {}, {}
for c, (name, rel, sig) in rust_fns.items():
    name_to_canons.setdefault(name, set()).add(c)
for rel, full in rs_files:
    for name, callees in fn_call_graph(full).items():
        calls_by_name.setdefault(name, set()).update(callees)

HARNESS_PATHS = [os.path.join(RUST_DIR, "parity_check.rs"),
                 os.path.join(RUST_DIR, "seq_replay.rs")]
TESTS_DIR = os.path.normpath(os.path.join(RUST_DIR, "..", "tests"))
for t in ("flush_pg_test.rs", "inspect_pg_test.rs", "catchup_pg_test.rs",
          "seq_diff_pg_test.rs"):
    HARNESS_PATHS.append(os.path.join(TESTS_DIR, t))

seed = set()
for h in HARNESS_PATHS:
    if os.path.exists(h):
        seed |= calls_in(open(h, encoding="utf-8").read())

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

# --- classify each Rust fn ---
IO_SIG = re.compile(r"\basync\b|Pool|Executor|Transaction|Handle|Pg|tokio|-> impl")
INFRA_FILES = {"otel_metrics.rs", "live_count.rs", "tracer.rs"}

# Explicitly non-differentiable: trivial state accessors + lifecycle side-effects
# + one documented protocol divergence. None has an un-pinned differentiable body.
# Listed by name (NOT auto-detected) so every exclusion is auditable — the same
# transparency the syncer COVERAGE doc's NON-DIFFERENTIABLE table gives.
NON_DIFFERENTIABLE = {
    "updated_version": "trivial getter — returns `self.base.cvr.version`",
    "has_pending_writes": "trivial getter — `!self.pending.is_empty()`",
    "row_count": "trivial getter — returns `self.row_count`",
    "catchup_reader": "thin handle ctor (clones pool/schema/cvr_id); the reader's "
                      "DB work is covered by the catchup PG differential",
    "close": "lifecycle side-effect (`eprintln!` + `downstream.cancel()`) — no "
             "differentiable output",
    "send_query_transform_failed_error": "documented TS↔Rust protocol divergence "
             "(TS `fail(ProtocolError)` channel vs Rust `['error', …]`); byte-parity "
             "is NOT the contract",
    "force_updates": "set-insert of the already-pinned `row_id_string(id)`; no "
                     "un-pinned logic of its own",
}

def classify(c, name, f, sig):
    if c in covered:
        return "COVERED"
    if name in NON_DIFFERENTIABLE:
        return "NA"
    # non-differentiable: metrics/infra, trait-method declarations, test helpers
    if f in INFRA_FILES or sig.rstrip().endswith(";") \
       or name.endswith("_for_test") or name in ("drop",):
        return "INFRA"
    if IO_SIG.search(sig) or f in ("row_record_cache.rs", "change_processor.rs"):
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
print("# rust-cvr — Layer-2 (body-differential) coverage\n")
print("_Which Rust fns have their BODY pinned to TS output. COVERED = reachable "
      "from a differential harness (parity_check.rs + the flush/inspect/catchup PG "
      "differentials + the sequence fuzzer via seq_replay.rs), taking the "
      "transitive closure over the crate call graph. Reachability ≠ every-branch-"
      "exercised, but the harnesses drive the real API over real-TS goldens with "
      "150+ fuzzed programs + property tests._\n")
print(f"- Rust fns total **{n}** · ✅ COVERED **{nc}** · 🟥 GAP (pure, untested) "
      f"**{ng}** · ⚙️ IO (integration diff) **{ni}** · ◻️ infra/metrics **{nx}** · "
      f"◻️ documented n/a **{na}**")
print(f"- Body-differential coverage of the **unit-testable pure surface**: "
      f"**{nc}/{nc+ng} = {100*nc/max(1,nc+ng):.0f}%**")

# highest-risk gaps: fns that emit patches / build rowKeys / mutate CVR
RISK = ("received", "track", "desired", "unreferenced", "row_patch", "patch",
        "delete_client", "ensure_client", "query_record", "eviction", "row_id",
        "signature", "merge")
risky = [r for r in rows if r[0] == "GAP"
         and any(t in r[1] for t in RISK)]
if risky:
    print("\n> ⚠️ **Highest-risk uncovered (emit patches / build rowKeys / mutate CVR "
          "— the corruption class):** "
          + ", ".join(f"`{r[1]}` ({r[2]})" for r in sorted(risky)))

emit("🟥 GAP — pure & deterministic, NO differential fixture (build these)", "GAP")

# documented n/a — explicit allowlist, each with its reason
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
emit("⚙️ IO — async/DB/actor, use the ART mirror not a unit fixture", "IO")
