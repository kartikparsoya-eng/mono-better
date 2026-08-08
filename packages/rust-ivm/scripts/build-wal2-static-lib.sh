#!/usr/bin/env bash
# Compile the wal2 SQLite fork into a static lib for rust builds/tests.
#
# Shared by build-local-wal2.sh (local addon builds) and CI's rust-test job
# (cargo test must link a SQLite with STMT_SCANSTATUS + STAT4 — the system
# libsqlite3 lacks both, and the cost model hand-binds sqlite3_stmt_scanstatus_v2).
#
# LEAN define set: wal2 + JSON1 + snapshot + the planner's SCANSTATUS/STAT4.
# NOT define-identical to Dockerfile stage 1 (perf/robustness flags). RULE:
# any define the ENGINE READS (scanstatus, stat4) must match Dockerfile stage 1.
#
# Output: $RUST_IVM_DIR/wal2-sqlite/build/{libsqlite3.a,sqlite3.h,sqlite3ext.h}
set -euo pipefail

RUST_IVM_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$RUST_IVM_DIR/wal2-sqlite/build"
mkdir -p "$BUILD"

cc -O2 -ffp-contract=off -fPIC -c "$RUST_IVM_DIR/wal2-sqlite/sqlite3.c" -o "$BUILD/sqlite3.o" \
   -DSQLITE_THREADSAFE=2 -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_RTREE \
   -DSQLITE_OMIT_LOAD_EXTENSION -DSQLITE_ENABLE_SNAPSHOT \
   -DSQLITE_ENABLE_STMT_SCANSTATUS \
   -DSQLITE_ENABLE_STAT4 -DSQLITE_STAT4_SAMPLES=128
ar rcs "$BUILD/libsqlite3.a" "$BUILD/sqlite3.o"
cp "$RUST_IVM_DIR/wal2-sqlite/sqlite3.h" "$RUST_IVM_DIR/wal2-sqlite/sqlite3ext.h" "$BUILD/"
echo "$BUILD"
