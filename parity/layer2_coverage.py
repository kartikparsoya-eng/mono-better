#!/usr/bin/env python3
"""
Layer-2 coverage cross-reference for the rust-cvr crate.

Layer 1 (parity_ledger.py) proved which functions EXIST on both sides.
Layer 2 asks the harder question: of the matched *pure/deterministic* functions,
which ones have a **differential fixture** (parity_check.rs) pinning their BODY to
TS's actual output — and which are untested body-wise?

An untested pure function is exactly where a rowKey-corruption-class bug hides:
name matches (Layer-1 green), body silently diverges (no Layer-2 assert).

  COVERED   - called/asserted by parity_check.rs against a TS-generated fixture
  GAP       - pure & deterministic, differentiable, but NOT covered  => build a fixture
  IO        - async / DB / actor: needs integration diff (ART mirror), not unit fixture

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

# --- which fns does the Layer-2 harness actually exercise? ---
pc = open(os.path.join(RUST_DIR, "parity_check.rs"), encoding="utf-8").read()
called = set(re.findall(r"\b([a-z_][a-z0-9_]+)\s*\(", pc))
covered = {c for c in rust_fns if rust_fns[c][0] in called}

# --- classify each Rust fn ---
IO_SIG = re.compile(r"\basync\b|Pool|Executor|Transaction|Handle|Pg|tokio|-> impl")
INFRA_FILES = {"otel_metrics.rs", "live_count.rs", "trace.rs"}

def classify(c, name, f, sig):
    if c in covered:
        return "COVERED"
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
print("# rust-cvr — Layer-2 (body-differential) coverage\n")
print("_Which Rust fns have their BODY pinned to TS output via `parity_check.rs`._\n")
print(f"- Rust fns total **{n}** · ✅ COVERED **{nc}** · 🟥 GAP (pure, untested) "
      f"**{ng}** · ⚙️ IO (integration diff) **{ni}** · ◻️ infra/metrics (n/a) **{nx}**")
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
emit("✅ COVERED — body pinned to TS fixture", "COVERED")
emit("⚙️ IO — async/DB/actor, use the ART mirror not a unit fixture", "IO")
