#!/usr/bin/env bash
# M7 — mutation testing harness (parity/ layer).
#
# AGENTS rule 7 (prove-on-revert) is a MANUAL mutation test per fix: revert the fix,
# confirm the test fails. cargo-mutants automates that at scale — it mutates guards,
# comparisons, return values, and error handling across a module and reports which
# mutants SURVIVE (i.e. no test failed). A surviving mutant = behavior the suite did
# not specify = a weak/missing test, the class that let the hydrate_unchanged_queries
# gate ship untested for so long.
#
# Scoped by default to correctness-critical, control-flow-heavy files (fast enough to
# run in CI-adjacent time); pass FILES=... to widen. A full-crate run is expensive
# (recompile+test per mutant) — do that offline, not in the inner loop.
#
# Usage:
#   parity/mutation_smoke.sh                 # default critical set
#   FILES="src/services/view_syncer/advance_gate.rs" parity/mutation_smoke.sh
#   THRESHOLD=0.80 parity/mutation_smoke.sh  # fail if kill-rate below
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CRATE="$ROOT/packages/rust-syncer"
THRESHOLD="${THRESHOLD:-0.75}"

# Correctness-critical, gate/branch-dense files (where a dropped guard hides).
FILES="${FILES:-\
src/services/view_syncer/advance_gate.rs \
src/custom_queries/transform_query.rs \
src/services/view_syncer/connection_context_manager.rs}"

if ! command -v cargo-mutants >/dev/null 2>&1; then
  echo "cargo-mutants not installed; installing (one-time)..."
  cargo install cargo-mutants --locked || { echo "install failed"; exit 2; }
fi

cd "$CRATE"
unset SQLITE3_STATIC SQLITE3_LIB_DIR SQLITE3_INCLUDE_DIR PKG_CONFIG_LIBDIR

file_args=()
for f in $FILES; do file_args+=(--file "$f"); done

echo "== M7 mutation smoke over: $FILES =="
# --no-default-features matches how rust-syncer builds/tests in local CI.
cargo +1.90.0 mutants "${file_args[@]}" \
  -- --no-default-features 2>&1 | tee /tmp/mutants.out

# Kill-rate gate. cargo-mutants prints a summary like "N caught, M missed, ...".
caught=$(grep -oiE "[0-9]+ caught" /tmp/mutants.out | tail -1 | grep -oE "[0-9]+" || echo 0)
missed=$(grep -oiE "[0-9]+ missed" /tmp/mutants.out | tail -1 | grep -oE "[0-9]+" || echo 0)
total=$((caught + missed))
if [ "$total" -eq 0 ]; then echo "M7: no mutants generated (check FILES)"; exit 0; fi
rate=$(python3 -c "print(f'{$caught/$total:.2f}')")
echo "M7 kill-rate: $caught/$total = $rate (threshold $THRESHOLD)"
python3 -c "import sys; sys.exit(0 if $rate >= $THRESHOLD else 1)" \
  && echo "M7 mutation smoke: PASS" || { echo "M7 mutation smoke: FAIL (survivors = unspecified behavior; strengthen tests)"; exit 1; }
