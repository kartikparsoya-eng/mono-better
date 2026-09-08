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
# The ivm/cvr test lib carries the SAME compile defines as zero-sqlite3
# deps/defines.gypi and rust-syncer/build.rs (checked below), so the tests run
# the SQLite configuration the production binary runs.
( cd "$WAL2" && cc -O2 -fPIC -c sqlite3.c -o sqlite3.o \
    -DSQLITE_DEFAULT_CACHE_SIZE=-16000 -DSQLITE_DEFAULT_FOREIGN_KEYS=1 -DSQLITE_DEFAULT_MEMSTATUS=0 -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1 -DSQLITE_DQS=0 -DSQLITE_ENABLE_COLUMN_METADATA -DSQLITE_ENABLE_DBSTAT_VTAB -DSQLITE_ENABLE_DESERIALIZE -DSQLITE_ENABLE_FTS3 -DSQLITE_ENABLE_FTS3_PARENTHESIS -DSQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_GEOPOLY -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_ENABLE_PERCENTILE -DSQLITE_ENABLE_RTREE -DSQLITE_ENABLE_STAT4 -DSQLITE_ENABLE_STMT_SCANSTATUS -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT -DSQLITE_LIKE_DOESNT_MATCH_BLOBS -DSQLITE_OMIT_DEPRECATED -DSQLITE_OMIT_PROGRESS_CALLBACK -DSQLITE_OMIT_SHARED_CACHE -DSQLITE_OMIT_TCL_VARIABLE -DSQLITE_SOUNDEX -DSQLITE_STAT4_SAMPLES=128 -DSQLITE_THREADSAFE=2 -DSQLITE_TRACE_SIZE_LIMIT=32 -DSQLITE_USE_URI=1 \
    && ar rcs libsqlite3.a sqlite3.o )
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

step "parity — vendored SQLite compile DEFINES == zero-sqlite3 deps/defines.gypi"
# Same SQLite source is not enough: the TS build's compile flags set its runtime
# behavior (SQLITE_DEFAULT_MEMSTATUS=0 = no global malloc mutex; THREADSAFE=2;
# 16 MB page cache; DQS=0; ...). rust-syncer/build.rs must carry every define
# defines.gypi carries, with the same value. 2026-09-07: MEMSTATUS was missing —
# every sqlite3Malloc took SQLite's mem0 mutex, 110 CG threads serialized on it,
# rust's capacity knee was 110 vs TS 230. Skips (passes) when node_modules is absent.
ZG=$(ls "$ROOT"/node_modules/.pnpm/@rocicorp+zero-sqlite3@*/node_modules/@rocicorp/zero-sqlite3/deps/defines.gypi 2>/dev/null | tail -1)
if [ -n "$ZG" ]; then
  python3 - "$ZG" "$ROOT/packages/rust-syncer/build.rs" "$ROOT/scripts/local-rust-ci.sh" "$ROOT/packages/rust-ivm/scripts/build-wal2-static-lib.sh" <<'PY'
import re, sys
gypi, build, ci, wal2 = (open(a).read() for a in sys.argv[1:5])
defs = re.findall(r"'(SQLITE_[A-Z0-9_]+)(?:=([^']+))?'", gypi)
def dflags(text): return {m.group(1): m.group(2) for m in re.finditer(r'-D(SQLITE_[A-Z0-9_]+)(?:=(\S+))?', text)}
places = {
    "rust-syncer/build.rs": {m.group(1): m.group(2) for m in re.finditer(r'\.define\("(SQLITE_[A-Z0-9_]+)",\s*(?:"([^"]*)"|None)\)', build)},
    "local-rust-ci.sh cc": dflags(ci[ci.index("cc -O2 -fPIC -c sqlite3.c"):ci.index("ar rcs libsqlite3.a")]),
    "rust-ivm/scripts/build-wal2-static-lib.sh": dflags(wal2),
}
bad = []
for label, have in places.items():
    for name, val in defs:
        if name not in have: bad.append(f"{label}: {name} MISSING"); continue
        if (have[name] or "") != (val or ""): bad.append(f"{label}: {name}={have[name]!r} but defines.gypi says {val!r}")
print(f"defines.gypi: {len(defs)} defines; " + ("mirrored in all 3 places" if not bad else f"{len(bad)} drift(s)"))
for b in bad: print("  ", b)
sys.exit(1 if bad else 0)
PY
  chk $? "sqlite compile-define parity"
else
  echo "SKIP: @rocicorp/zero-sqlite3 not installed (run pnpm install)"
fi

step "image — Dockerfile must not cap glibc malloc arenas (MALLOC_ARENA_MAX)"
# With mimalloc serving Rust + SQLite (I-13), glibc malloc only serves glibc's
# own internals (DNS, thread TLS, dl); a 2-arena cap turns those into a
# process-wide lock. 2026-09-07 A/B at 110 conns: cap 2 -> 64 cut connect p50
# 5.45 -> 4.65 s and steady p95 1.62 -> 0.57 s.
if grep -nE '^ENV MALLOC_ARENA_MAX=([0-7])\b' "$ROOT/Dockerfile"; then
  chk 1 "Dockerfile MALLOC_ARENA_MAX cap"
else
  chk 0 "Dockerfile MALLOC_ARENA_MAX cap"
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

# The three crates run CONCURRENTLY. Same commands, same order WITHIN a crate —
# only the scheduling changes, so this still mirrors rust-syncer.yml exactly.
# They are separate cargo packages with separate target dirs, so there is no
# lock contention and no shared cache to invalidate; the phases were previously
# serial on a 14-core box whose load sat at ~4 because a test binary links
# single-threaded (rust-ivm alone builds 104 of them). Each crate writes its own
# log; the logs are replayed IN ORDER after the join so the output reads the
# same as the serial version. Set CI_SERIAL=1 to fall back to one at a time.
CILOG="${TMPDIR:-/tmp}/local-rust-ci.$$"
mkdir -p "$CILOG"

crate_phase() {   # $1 = crate; writes $CILOG/$1.log and $CILOG/$1.rc
  local c="$1" rc=0 t
  {
    echo; echo "== $c — fmt / clippy --all-targets -D warnings / test =="
    if [ "$c" = rust-syncer ]; then
      unset SQLITE3_STATIC SQLITE3_LIB_DIR SQLITE3_INCLUDE_DIR PKG_CONFIG_LIBDIR
    fi
    cd "$ROOT/packages/$c" || return 1
    cargo +$TC fmt --check; t=$?; [ $t -eq 0 ] && echo "ok ($c fmt)" || { echo "FAIL ($c fmt)"; rc=1; }
    if [ "$c" = rust-syncer ]; then
      cargo +$TC clippy --locked --no-default-features --all-targets -- -D warnings
    else
      cargo +$TC clippy --locked --all-targets -- -D warnings
    fi
    t=$?; [ $t -eq 0 ] && echo "ok ($c clippy)" || { echo "FAIL ($c clippy)"; rc=1; }
    case "$c" in
      rust-ivm)    cargo +$TC test --locked --tests -- --test-threads=1 ;;
      rust-cvr)    cargo +$TC test --locked -- --test-threads=1 ;;
      rust-syncer) cargo +$TC test --locked --no-default-features -- --test-threads=1 ;;
    esac
    t=$?; [ $t -eq 0 ] && echo "ok ($c test)" || { echo "FAIL ($c test)"; rc=1; }
    if [ "$c" = rust-ivm ]; then
      echo; echo "== rust-ivm — teardown integrity soak =="
      cargo +$TC test --locked --test teardown_gate_test -- --test-threads=1
      t=$?; [ $t -eq 0 ] && echo "ok (ivm teardown soak)" || { echo "FAIL (ivm teardown soak)"; rc=1; }
    fi
  } > "$CILOG/$c.log" 2>&1
  echo "$rc" > "$CILOG/$c.rc"
}

if [ "${CI_SERIAL:-0}" = 1 ]; then
  for c in rust-ivm rust-cvr rust-syncer; do crate_phase "$c"; done
else
  step "rust-ivm / rust-cvr / rust-syncer — running concurrently (logs replayed in order)"
  for c in rust-ivm rust-cvr rust-syncer; do crate_phase "$c" & done
  wait
fi
for c in rust-ivm rust-cvr rust-syncer; do
  cat "$CILOG/$c.log"
  [ "$(cat "$CILOG/$c.rc" 2>/dev/null || echo 1)" -eq 0 ] || fail=1
done
rm -rf "$CILOG"

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

step "parity — M11 prod-path reachability (a ported symbol prod cannot reach is not parity)"
python3 "$ROOT/parity/prod_reachability.py"; chk $? "M11 prod-path reachability"

step "parity — M14 log differential (a rust log line with no TS twin, or a twin at a different severity, is a divergence operators see)"
python3 "$ROOT/parity/log_differential.py"; chk $? "M14 log differential"

echo; [ $fail -eq 0 ] && echo "LOCAL CI: PASS" || echo "LOCAL CI: FAIL"
exit $fail
