#!/usr/bin/env bash
# M6 — branch/condition coverage over the ported control-flow (parity/ layer).
#
# "Ported the function" is not the same as "ported every branch of the function".
# Each divergence of 2026-09-02 was an UNTAKEN BRANCH: a TS `if` whose rust twin
# was missing or never exercised. Line coverage hides that — a gate can be 100%
# line-covered with only one side of every condition ever taken. `cargo llvm-cov`
# reports REGION coverage, which counts each arm separately, so an unexercised
# branch shows up.
#
# Scoped to the ported state machines where a dropped guard actually costs
# something; a whole-crate run is slow and its number is dominated by plumbing.
# Widen with FILES=..., set the bar with THRESHOLD=...
#
# Usage:
#   parity/branch_coverage.sh
#   THRESHOLD=70 parity/branch_coverage.sh
#   FILES="view_syncer.rs pipeline_driver.rs" parity/branch_coverage.sh
#
# NOTE: paths are rust-syncer-relative. `advance_gate.rs` lives in rust-ivm, not
# here — naming a non-existent path is a FAILURE below, not a silent skip (that
# is exactly how a wrong crate path hides in a green gate).
#
# NOTE: this reports and gates; it does not tell you WHICH branch is missing.
# Use `--html` (printed at the end) to open the annotated source.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CRATE="$ROOT/packages/rust-syncer"
THRESHOLD="${THRESHOLD:-75}"

# Ported control-flow whose branches carry client-visible behavior.
FILES="${FILES:-\
services/view_syncer/view_syncer.rs \
services/view_syncer/connection_context_manager.rs \
services/view_syncer/pipeline_driver.rs \
custom_queries/transform_query.rs}"

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
  echo "cargo-llvm-cov not installed; installing (one-time)..."
  cargo install cargo-llvm-cov --locked || { echo "install failed"; exit 2; }
fi

cd "$CRATE"
unset SQLITE3_STATIC SQLITE3_LIB_DIR SQLITE3_INCLUDE_DIR PKG_CONFIG_LIBDIR

echo "== M6 region (branch) coverage over the ported state machines =="
cargo +1.90.0 llvm-cov --locked --no-default-features --lib \
  --json --output-path /tmp/m6-cov.json -- --test-threads=1 >/dev/null 2>/tmp/m6-cov.err
rc=$?
if [ $rc -ne 0 ]; then
  echo "M6: coverage run failed (exit $rc)"; tail -20 /tmp/m6-cov.err; exit $rc
fi

python3 - "$THRESHOLD" $FILES <<'PY'
import json, sys
threshold = float(sys.argv[1])
wanted = sys.argv[2:]
data = json.load(open("/tmp/m6-cov.json"))
files = data["data"][0]["files"]
worst = 0.0
rows = []
for f in files:
    name = f["filename"]
    if not any(name.endswith(w) for w in wanted):
        continue
    s = f["summary"]["regions"]
    pct = s["percent"]
    rows.append((pct, s["covered"], s["count"], name.split("/src/")[-1]))
rows.sort()
missing = [w for w in wanted
           if not any(f["filename"].endswith(w) for f in files)]
if missing:
    print("M6: FILES entries that matched NO file in the coverage report "
          "(wrong path or wrong crate): " + ", ".join(missing))
    sys.exit(1)
if not rows:
    print("M6: no matching files in the coverage report (check FILES)")
    sys.exit(1)
for pct, cov, tot, name in rows:
    mark = "ok " if pct >= threshold else "LOW"
    print(f"  [{mark}] {pct:6.2f}%  {cov:5d}/{tot:5d} regions  {name}")
worst = rows[0][0]
print(f"\nM6 worst region coverage: {worst:.2f}% (threshold {threshold}%)")
print("  Inspect uncovered branches: cargo llvm-cov --no-default-features --lib --html")
sys.exit(0 if worst >= threshold else 1)
PY
rc=$?
[ $rc -eq 0 ] && echo "M6 branch coverage: PASS" || echo "M6 branch coverage: FAIL (an unexercised branch is a branch nothing pins)"
exit $rc
