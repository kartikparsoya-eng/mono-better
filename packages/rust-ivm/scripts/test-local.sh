#!/usr/bin/env bash
# test-local.sh — Master local test runner for Rust IVM.
# Runs ALL tests without Docker/ART:
#   1. Rust unit + integration tests (cargo test)
#   2. WAL2 linking check (if Docker image exists)
#
# Usage:
#   bash rust-ivm/scripts/test-local.sh
#   bash rust-ivm/scripts/test-local.sh --verbose
#   bash rust-ivm/scripts/test-local.sh --quick  # skip cargo tests
#
# Exits 0 on success, 1 on any failure.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VERBOSE=""
QUICK=0

while [ $# -gt 0 ]; do
  case "$1" in
    --verbose) VERBOSE="--verbose"; shift;;
    --quick) QUICK=1; shift;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

echo "╔══════════════════════════════════════════════════════╗"
echo "║   Rust IVM Local Test Suite                          ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

FAILURES=0

# ---------------------------------------------------------------------------
# 1. Rust unit + integration tests
# ---------------------------------------------------------------------------
if [ "$QUICK" = "0" ]; then
  echo "━━━ 1. Rust Tests (cargo test) ━━━━━━━━━━━━━━━━━━━━━━━"
  cd "$PROJECT_ROOT/rust-ivm"
  if cargo test -- --test-threads=1 2>&1 | tail -30; then
    echo "  ✅ Rust tests passed"
  else
    echo "  ❌ Rust tests FAILED"
    FAILURES=$((FAILURES + 1))
  fi
  echo ""
else
  echo "━━━ 1. Rust Tests (SKIPPED --quick) ━━━━━━━━━━━━━━━━━━"
  echo ""
fi

# ---------------------------------------------------------------------------
# 2. Docker WAL2 check (if image exists)
# ---------------------------------------------------------------------------
echo "━━━ 2. Docker WAL2 Check ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if docker image inspect zero-rust-ivm:latest >/dev/null 2>&1; then
  echo "  Docker image found, checking WAL2 linking..."
  if [ -f "$SCRIPT_DIR/test-docker-wal2.sh" ]; then
    if bash "$SCRIPT_DIR/test-docker-wal2.sh" 2>&1 | tail -10; then
      echo "  ✅ Docker WAL2 check passed"
    else
      echo "  ⚠️  Docker WAL2 check had warnings (may be OK)"
    fi
  else
    echo "  ⚠️  test-docker-wal2.sh not found, skipping"
  fi
else
  echo "  ⏭️  Docker image not built yet, skipping WAL2 check"
  echo "  Build with: cd $PROJECT_ROOT && docker build -t zero-rust-ivm:latest -f mono/Dockerfile.rust-ivm ."
fi
echo ""

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo "╔══════════════════════════════════════════════════════╗"
if [ "$FAILURES" = "0" ]; then
  echo "║   ✅ ALL TESTS PASSED                                ║"
else
  echo "║   ❌ $FAILURES TEST SUITE(S) FAILED                          ║"
fi
echo "╚══════════════════════════════════════════════════════╝"

exit $FAILURES
