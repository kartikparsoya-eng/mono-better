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

step "parity — vendored SQLite == the SQLite the TS zero-cache runs"
# The planner's cost model IS SQLite's own estimate (scanstatus EST + stat1/stat4),
# so rust and TS must link the SAME SQLite or their plans can legitimately differ
# for identical data. TS's is whatever @rocicorp/zero-sqlite3 bundles; ours is
# packages/rust-ivm/wal2-sqlite. Compare SQLITE_SOURCE_ID, not just the version.
# Skips (passes) when node_modules is absent.
ZS=$(ls -d "$ROOT"/node_modules/.pnpm/@rocicorp+zero-sqlite3@*/node_modules/@rocicorp/zero-sqlite3/deps/sqlite3 2>/dev/null | tail -1)
if [ -n "$ZS" ] && [ -f "$ZS/sqlite3.h" ]; then
  src_id() { grep -m1 '^#define SQLITE_SOURCE_ID' "$1" | sed 's/.*"\(.*\)".*/\1/'; }
  ours=$(src_id "$WAL2/sqlite3.h"); theirs=$(src_id "$ZS/sqlite3.h")
  if [ "$ours" = "$theirs" ]; then
    echo "vendored SQLite matches zero-sqlite3: $ours"
    chk 0 "sqlite source-id parity"
  else
    echo "MISMATCH — rust links a different SQLite than the TS zero-cache."
    echo "  rust  (packages/rust-ivm/wal2-sqlite): $ours"
    echo "  TS    (@rocicorp/zero-sqlite3):        $theirs"
    echo "  Fix:  cp $ZS/sqlite3.{c,h} $ZS/sqlite3ext.h $WAL2/"
    chk 1 "sqlite source-id parity"
  fi
else
  echo "SKIP: @rocicorp/zero-sqlite3 not installed (run pnpm install)"
fi
# TEST_CVR_PG_URI: set to run PG-gated tests; unset => they skip+pass.
#
# Auto-discover a local Postgres so the PG-gated tests actually RUN by default.
# They are the ONLY coverage for the CVR store <-> row-cache <-> catchup seam,
# and on 2026-09-02 a re-entrant `tokio::sync::Mutex` lock in the catchup path
# shipped green precisely because this was unset: every PG test skipped, the
# in-process suites passed, and the deadlock only surfaced on the ART sandbox as
# "no rows served". Skipping is still allowed, but it is now LOUD.
if [ -z "${TEST_CVR_PG_URI:-}" ] && command -v psql >/dev/null 2>&1 \
   && psql "postgresql://localhost/postgres" -tAc "select 1" >/dev/null 2>&1; then
  psql "postgresql://localhost/postgres" -tAc \
    "select 1 from pg_database where datname='rust_cvr_test'" 2>/dev/null | grep -q 1 \
    || createdb rust_cvr_test >/dev/null 2>&1 || true
  TEST_CVR_PG_URI="postgresql://localhost/rust_cvr_test"
  echo "PG-gated tests: auto-discovered local Postgres -> $TEST_CVR_PG_URI"
fi
export TEST_CVR_PG_URI="${TEST_CVR_PG_URI:-}"
if [ -z "$TEST_CVR_PG_URI" ]; then
  echo ""
  echo "  ############################################################"
  echo "  # WARNING: TEST_CVR_PG_URI is unset.                       #"
  echo "  # Every PG-gated test SKIPPED (they pass vacuously).       #"
  echo "  # A CVR/catchup deadlock shipped this way on 2026-09-02.   #"
  echo "  # A green run here does NOT cover the CVR store seam.      #"
  echo "  ############################################################"
  echo ""
fi

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

step "parity — L1 structural guard (misfiled-symbol ratchet, L9)"
python3 "$ROOT/parity/parity_ledger.py" syncer --enforce-structure > /tmp/l1-structure.out 2>&1; chk $? "L1 structural ratchet"
tail -1 /tmp/l1-structure.out

step "parity — M5 unverified-claim guard (parity assertions must cite a .ts source)"
python3 "$ROOT/parity/ban_unverified_claims.py"; chk $? "M5 unverified-claim ratchet"

step "parity — M3 state-flag registry (TS lifecycle flags have rust counterparts)"
python3 "$ROOT/parity/state_flag_registry.py"; chk $? "M3 state-flag registry"

step "parity — M2 call-guard parity (TS-gated calls are gated in rust too)"
python3 "$ROOT/parity/call_guard_parity.py"; chk $? "M2 call-guard parity"

step "parity — M8 signature differential (mirrored-file twin, 1:1, same parameters)"
python3 "$ROOT/parity/signature_diff.py"; chk $? "M8 signature differential"

step "parity — M9 alias-note guard (every ledger 📌 alias names an existing rust twin or cites an I-/D-/task/F- id)"
python3 "$ROOT/parity/alias_guard.py"; chk $? "M9 alias guard"

step "parity — M10 helper-import ledger (every shared/types helper a ported file imports has a rust twin or a verified alias)"
python3 "$ROOT/parity/helper_imports.py"; chk $? "M10 helper-import ledger"

echo; [ $fail -eq 0 ] && echo "LOCAL CI: PASS" || echo "LOCAL CI: FAIL"
exit $fail
