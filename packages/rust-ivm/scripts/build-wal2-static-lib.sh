#!/usr/bin/env bash
# Compile the wal2 SQLite fork into a static lib for rust builds/tests.
#
# Used by CI's rust-test job and by local `cargo test` runs
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

# -D list = @rocicorp/zero-sqlite3 deps/defines.gypi, mirrored 1:1 (scripts/local-rust-ci.sh
# checks it) so ivm/cvr tests run the SQLite configuration the production binary runs;
# OMIT_LOAD_EXTENSION + ENABLE_SNAPSHOT are test-lib extras kept from before.
cc -O2 -ffp-contract=off -fPIC -c "$RUST_IVM_DIR/wal2-sqlite/sqlite3.c" -o "$BUILD/sqlite3.o" \
   -DSQLITE_DEFAULT_CACHE_SIZE=-16000 -DSQLITE_DEFAULT_FOREIGN_KEYS=1 -DSQLITE_DEFAULT_MEMSTATUS=0 -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1 -DSQLITE_DQS=0 -DSQLITE_ENABLE_COLUMN_METADATA -DSQLITE_ENABLE_DBSTAT_VTAB -DSQLITE_ENABLE_DESERIALIZE -DSQLITE_ENABLE_FTS3 -DSQLITE_ENABLE_FTS3_PARENTHESIS -DSQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_GEOPOLY -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_ENABLE_PERCENTILE -DSQLITE_ENABLE_RTREE -DSQLITE_ENABLE_STAT4 -DSQLITE_ENABLE_STMT_SCANSTATUS -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT -DSQLITE_LIKE_DOESNT_MATCH_BLOBS -DSQLITE_OMIT_DEPRECATED -DSQLITE_OMIT_PROGRESS_CALLBACK -DSQLITE_OMIT_SHARED_CACHE -DSQLITE_OMIT_TCL_VARIABLE -DSQLITE_SOUNDEX -DSQLITE_STAT4_SAMPLES=128 -DSQLITE_THREADSAFE=2 -DSQLITE_TRACE_SIZE_LIMIT=32 -DSQLITE_USE_URI=1 -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_RTREE \
   -DSQLITE_OMIT_LOAD_EXTENSION -DSQLITE_ENABLE_SNAPSHOT
ar rcs "$BUILD/libsqlite3.a" "$BUILD/sqlite3.o"
cp "$RUST_IVM_DIR/wal2-sqlite/sqlite3.h" "$RUST_IVM_DIR/wal2-sqlite/sqlite3ext.h" "$BUILD/"
echo "$BUILD"
