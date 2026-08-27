#!/usr/bin/env bash
# local-rust-ci.sh — replicate .github/workflows/rust-syncer.yml EXACTLY so a
# push is verified locally first (real exit-code checks; NEVER pipe to tail).
# Pinned toolchain 1.90.0 so clippy lints match CI. Run from repo root.
set -uo pipefail
TC="${TC:-1.90.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
fail=0
step() { echo; echo "== $* =="; }
chk()  { if [ "$1" -ne 0 ]; then echo "FAIL ($2)"; fail=1; else echo "ok ($2)"; fi; }

step "build static WAL2 SQLite (exports SQLITE3_* for ivm/cvr)"
WAL2="$ROOT/packages/rust-ivm/wal2-sqlite"
( cd "$WAL2" && cc -O2 -fPIC -c sqlite3.c -o sqlite3.o \
    -DSQLITE_THREADSAFE=2 -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_COLUMN_METADATA \
    -DSQLITE_ENABLE_DBSTAT_VTAB -DSQLITE_DQS=0 && ar rcs libsqlite3.a sqlite3.o )
export SQLITE3_LIB_DIR="$WAL2" SQLITE3_INCLUDE_DIR="$WAL2" SQLITE3_STATIC=1
# TEST_CVR_PG_URI: set to run PG-gated tests; unset => they skip+pass.
export TEST_CVR_PG_URI="${TEST_CVR_PG_URI:-}"

for c in rust-ivm rust-cvr; do
  step "$c — fmt / clippy --all-targets -D warnings / test"
  ( cd "packages/$c"
    cargo +$TC fmt --check ); chk $? "$c fmt"
  ( cd "packages/$c"
    cargo +$TC clippy --locked --all-targets -- -D warnings ); chk $? "$c clippy"
  if [ "$c" = rust-ivm ]; then TESTFLAGS="--tests"; else TESTFLAGS=""; fi
  ( cd "packages/$c"
    cargo +$TC test --locked $TESTFLAGS -- --test-threads=1 ); chk $? "$c test"
done

step "rust-syncer — fmt / clippy --no-default-features / test (UNSET SQLITE3_*)"
( cd packages/rust-syncer
  unset SQLITE3_STATIC SQLITE3_LIB_DIR SQLITE3_INCLUDE_DIR PKG_CONFIG_LIBDIR
  cargo +$TC fmt --check ); chk $? "syncer fmt"
( cd packages/rust-syncer
  unset SQLITE3_STATIC SQLITE3_LIB_DIR SQLITE3_INCLUDE_DIR PKG_CONFIG_LIBDIR
  cargo +$TC clippy --locked --no-default-features --all-targets -- -D warnings ); chk $? "syncer clippy"
( cd packages/rust-syncer
  unset SQLITE3_STATIC SQLITE3_LIB_DIR SQLITE3_INCLUDE_DIR PKG_CONFIG_LIBDIR
  cargo +$TC test --locked --no-default-features -- --test-threads=1 ); chk $? "syncer test"

step "rust-ivm — teardown integrity soak"
( cd packages/rust-ivm
  cargo +$TC test --locked --test teardown_gate_test -- --test-threads=1 ); chk $? "ivm teardown soak"

step "parity — L3 call-topology guard (ordering-sensitive emissions in sanctioned context)"
python3 "$ROOT/parity/call_topology.py"; chk $? "L3 call-topology"

echo; [ $fail -eq 0 ] && echo "LOCAL CI: PASS" || echo "LOCAL CI: FAIL"
exit $fail
