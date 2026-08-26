#!/usr/bin/env bash
# In-repo test-coverage baseline for the three rust crates — the fast loop
# for coverage-driven test enrichment (the heavy end-to-end twin is the
# coverage-instrumented ART image, xyne-art tools/coverage-report.sh).
#
# Per-crate env quirks (from CI memory — wrong env silently skips suites):
#   rust-syncer  --no-default-features, SQLITE3_* must be UNSET
#   rust-cvr     TEST_CVR_PG_URI required or the PG suites self-skip
#                (their paths would then read as uncovered)
#   rust-ivm     wal2 static SQLite env + --test-threads=1
#
# Output: parity/coverage/<crate>/{summary.txt,uncovered-functions.txt,lcov.info}
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/parity/coverage"
mkdir -p "$OUT"
: "${TEST_CVR_PG_URI:?set TEST_CVR_PG_URI (disposable PG) so rust-cvr PG suites count as covered}"

run_cov() { # crate, extra cargo args...
  local crate="$1"; shift
  echo "══ $crate ══"
  mkdir -p "$OUT/$crate"
  (cd "$ROOT/packages/$crate" && cargo llvm-cov --all-targets "$@" \
      --summary-only 2>&1 | tail -8 | tee "$OUT/$crate/summary.txt")
  local s1=${PIPESTATUS[0]}
  (cd "$ROOT/packages/$crate" && cargo llvm-cov report --lcov \
      --output-path "$OUT/$crate/lcov.info")
  # Uncovered functions: lcov FNDA:0 records.
  awk -F'[:,]' '/^SF:/{f=$2} /^FNDA:0,/{print f "\t" $3}' \
      "$OUT/$crate/lcov.info" | sort -u > "$OUT/$crate/uncovered-functions.txt"
  echo "  uncovered functions: $(wc -l < "$OUT/$crate/uncovered-functions.txt")"
  return "$s1"
}

fail=0

env -u SQLITE3_LIB_DIR -u SQLITE3_INCLUDE_DIR -u SQLITE3_STATIC -u PKG_CONFIG_LIBDIR \
  bash -c "$(declare -f run_cov); OUT='$OUT' ROOT='$ROOT' run_cov rust-syncer --no-default-features" || fail=1

run_cov rust-cvr || fail=1

SQLITE3_LIB_DIR="$ROOT/packages/rust-ivm/wal2-sqlite/build" \
SQLITE3_INCLUDE_DIR="$ROOT/packages/rust-ivm/wal2-sqlite/build" \
SQLITE3_STATIC=1 PKG_CONFIG_LIBDIR="" \
  run_cov rust-ivm -- --test-threads=1 || fail=1

echo
echo "reports: $OUT/<crate>/{summary.txt,uncovered-functions.txt}"
exit "$fail"
