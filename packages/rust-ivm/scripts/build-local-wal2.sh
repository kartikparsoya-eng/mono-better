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

# 1. Compile the wal2 fork into a static lib. This is a LEAN local build — the
#    minimum defines for wal2 + JSON1 + snapshot + the planner's SCANSTATUS +
#    STAT4. STAT4 (+ the 128-sample count) MUST match the prod Dockerfile and
#    @rocicorp/zero-sqlite3 (deps/defines.gypi): the planner cost model reads
#    SQLITE_SCANSTAT_EST, whose value depends on the stat4 histograms — without
#    identical stats machinery, rust-vs-TS flip-decision parity tests would
#    diverge locally. Still NOT fully define-identical to Dockerfile stage 1
#    (perf/robustness flags like DQS=0 don't affect planning). Correctness
#    (wal2 file format, value semantics) is unaffected — that is what the
#    differential suite checks.
echo "[1/3] compiling wal2 SQLite (static)…"
cc -O2 -ffp-contract=off -fPIC -c "$RUST_IVM_DIR/wal2-sqlite/sqlite3.c" -o "$BUILD/sqlite3.o" \
   -DSQLITE_THREADSAFE=2 -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_RTREE \
   -DSQLITE_OMIT_LOAD_EXTENSION -DSQLITE_ENABLE_SNAPSHOT \
   -DSQLITE_ENABLE_STMT_SCANSTATUS \
   -DSQLITE_ENABLE_STAT4 -DSQLITE_STAT4_SAMPLES=128
ar rcs "$BUILD/libsqlite3.a" "$BUILD/sqlite3.o"
cp "$RUST_IVM_DIR/wal2-sqlite/sqlite3.h" "$RUST_IVM_DIR/wal2-sqlite/sqlite3ext.h" "$BUILD/"

# 2. Build the addon, forcing libsqlite3-sys to STATIC-link our lib. The empty
#    PKG_CONFIG_LIBDIR is essential: without it libsqlite3-sys's pkg-config probe
#    finds a system/homebrew sqlite3.pc and links the system dylib instead.
echo "[2/3] building addon (static wal2 link)…"
EMPTY_PC="$(mktemp -d)"
trap 'rm -rf "$EMPTY_PC"' EXIT
export SQLITE3_LIB_DIR="$BUILD" SQLITE3_INCLUDE_DIR="$BUILD" SQLITE3_STATIC=1
export PKG_CONFIG_LIBDIR="$EMPTY_PC"
cd "$RUST_IVM_DIR/napi"
cargo clean -p libsqlite3-sys >/dev/null 2>&1 || true
rm -f target/release/librust_ivm_napi.dylib target/release/librust_ivm_napi.so
cargo build --release

# 3. Publish + verify it is NOT dynamically linking system sqlite.
case "$(uname -s)" in
  Darwin)
    NATIVE_LIB="target/release/librust_ivm_napi.dylib"
    ;;
  Linux)
    NATIVE_LIB="target/release/librust_ivm_napi.so"
    ;;
  *)
    echo "ERROR: unsupported platform: $(uname -s)" >&2
    exit 1
    ;;
esac
cp "$NATIVE_LIB" rust-ivm.node
# Copying a Mach-O dylib can leave a signature that `codesign -v` accepts but
# macOS kills when Node maps it as an addon. Re-sign the published artifact,
# not only Cargo's source dylib.
if [[ "$(uname -s)" == "Darwin" ]]; then
  codesign --force --sign - rust-ivm.node
fi
echo "[3/3] verifying static link…"
if [[ "$(uname -s)" == "Darwin" ]]; then
  if otool -L rust-ivm.node | grep -qi 'libsqlite3.dylib'; then
    echo "ERROR: addon still dynamically links system libsqlite3; wal2 is not active." >&2
    exit 1
  fi
else
  if ldd rust-ivm.node | grep -Eqi 'libsqlite3\.so'; then
    echo "ERROR: addon still dynamically links system libsqlite3; wal2 is not active." >&2
    exit 1
  fi
fi
echo "OK: $RUST_IVM_DIR/napi/rust-ivm.node (static wal2, $(wc -c < rust-ivm.node) bytes)"
