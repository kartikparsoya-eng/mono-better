#!/usr/bin/env bash
# Regenerate the checked-in sequence-differential corpus goldens.
#
# For every corpus/*.json program (produced by `node gen.mjs --corpus N`), drive
# the REAL TS updaters via run-ts.mjs and freeze the resulting trace as
# corpus/<name>.trace.json. The CI gate (tests/seq_diff_pg_test.rs) replays each
# program through the Rust engine and asserts its trace matches the frozen golden.
#
# Usage: TEST_CVR_PG_URI=... ./refresh-goldens.sh
set -euo pipefail
cd "$(dirname "$0")"

if [[ -z "${TEST_CVR_PG_URI:-}" ]]; then
  echo "TEST_CVR_PG_URI unset" >&2
  exit 2
fi

shopt -s nullglob

n=0
# Both the config corpus (prog-*) and the query/received-rows corpus (qprog-*).
for p in corpus/prog-*.json corpus/qprog-*.json; do
  case "$p" in *.trace.json) continue;; esac
  out="${p%.json}.trace.json"
  npx tsx run-ts.mjs "$p" > "$out"
  n=$((n + 1))
done
echo "refreshed $n golden traces"
