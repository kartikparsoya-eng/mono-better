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
  # --summary-only must precede any `--` test-binary args in "$@".
  (cd "$ROOT/packages/$crate" && cargo llvm-cov --all-targets --summary-only \
      "$@" 2>&1 | tail -8 | tee "$OUT/$crate/summary.txt")
  local s1=${PIPESTATUS[0]}
  (cd "$ROOT/packages/$crate" && cargo llvm-cov report --lcov \
      --output-path "$OUT/$crate/lcov.info")
  # Uncovered functions: the same logical fn appears once per test binary under
  # a different v0 crate disambiguator (Cs<base62>_), so a raw FNDA:0 grep
  # overstates uncoverage 2-3x. Normalize the disambiguator and count a fn as
  # uncovered only if its hit total is 0 across ALL compilation units.
  awk '/^SF:/{f=substr($0,4)}
       /^FNDA:/{line=substr($0,6); c=index(line,","); h=substr(line,1,c-1)+0
                sym=substr(line,c+1); gsub(/Cs[A-Za-z0-9]+_/,"Cs_",sym)
                k=f "\t" sym; hits[k]+=h}
       END{for(k in hits) if(hits[k]==0) print k}' \
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
