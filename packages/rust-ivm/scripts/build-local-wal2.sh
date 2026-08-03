#!/usr/bin/env bash
# Build the rust-ivm napi addon LOCALLY with the wal2 SQLite fork statically
# linked — so the snapshotter/driver tests (which open a `journal_mode = wal2`
# replica) actually run on a dev machine instead of only in Docker/CI.
#
# WHY: the addon links SQLite via rusqlite/libsqlite3-sys. By default that's the
# system SQLite (macOS's), which has no wal2 → `initSnapshotter` fails with
# "pragma journal_mode: unable to open database file", and every snapshotter-
# dependent test is un-runnable locally. That masked real bugs in exactly the
# driver/snapshotter seam (see the rowKey/primaryKey regression). This script is
# the local analog of Dockerfile stage 1 + stage 3.
#
# Usage:  packages/rust-ivm/scripts/build-local-wal2.sh
# Then:   RUST_IVM_ADDON_PATH=<repo>/packages/rust-ivm/napi/rust-ivm.node \
#           pnpm --filter zero-cache test rust-ivm-driver
set -euo pipefail

RUST_IVM_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$RUST_IVM_DIR/wal2-sqlite/build"
mkdir -p "$BUILD"

# 1. Compile the wal2 fork into a static lib. Defines match Dockerfile stage 1,
#    plus SQLITE_ENABLE_STMT_SCANSTATUS so the planner cost model can link.
echo "[1/3] compiling wal2 SQLite (static)…"
cc -O2 -ffp-contract=off -fPIC -c "$RUST_IVM_DIR/wal2-sqlite/sqlite3.c" -o "$BUILD/sqlite3.o" \
   -DSQLITE_THREADSAFE=2 -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_RTREE \
   -DSQLITE_OMIT_LOAD_EXTENSION -DSQLITE_ENABLE_SNAPSHOT -DSQLITE_ENABLE_WAL2_COREAD \
   -DSQLITE_ENABLE_STMT_SCANSTATUS
ar rcs "$BUILD/libsqlite3.a" "$BUILD/sqlite3.o"
cp "$RUST_IVM_DIR/wal2-sqlite/sqlite3.h" "$RUST_IVM_DIR/wal2-sqlite/sqlite3ext.h" "$BUILD/"

# 2. Build the addon, forcing libsqlite3-sys to STATIC-link our lib. The empty
#    PKG_CONFIG_LIBDIR is essential: without it libsqlite3-sys's pkg-config probe
#    finds a system/homebrew sqlite3.pc and links the system dylib instead.
echo "[2/3] building addon (static wal2 link)…"
EMPTY_PC="$(mktemp -d)"
export SQLITE3_LIB_DIR="$BUILD" SQLITE3_INCLUDE_DIR="$BUILD" SQLITE3_STATIC=1
export PKG_CONFIG_LIBDIR="$EMPTY_PC"
cd "$RUST_IVM_DIR/napi"
cargo clean -p libsqlite3-sys >/dev/null 2>&1 || true
rm -f target/release/librust_ivm_napi.dylib
cargo build --release --features rust-ivm/wal2_coread

# 3. Publish + verify it is NOT dynamically linking system sqlite.
cp target/release/librust_ivm_napi.dylib rust-ivm.node
echo "[3/3] verifying static link…"
if otool -L rust-ivm.node 2>/dev/null | grep -qi 'libsqlite3.dylib'; then
  echo "ERROR: addon still dynamically links system libsqlite3 — wal2 NOT active." >&2
  exit 1
fi
echo "OK: $RUST_IVM_DIR/napi/rust-ivm.node (static wal2, $(wc -c < rust-ivm.node) bytes)"
