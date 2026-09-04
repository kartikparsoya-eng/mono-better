#!/bin/bash
# Local test runner — runs all Rust IVM tests without Docker or ART.
#
# Usage:
#   ./rust-ivm/scripts/run-local-tests.sh              # Rust tests
#   ./rust-ivm/scripts/run-local-tests.sh --rust-only   # Rust tests only
#   ./rust-ivm/scripts/run-local-tests.sh --docker      # Also run Docker WAL2 test
#   ./rust-ivm/scripts/run-local-tests.sh --all         # Everything including Docker

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

TOTAL_PASS=0
TOTAL_FAIL=0

run_rust_tests() {
  echo -e "\n${BLUE}=== Rust Integration Tests ===${NC}"
  echo "Running: cargo test -- --test-threads=1"
  echo ""

  cd "$PROJECT_ROOT/rust-ivm"

  # Force a clean rebuild of the crate before the authoritative test run.
  # A stale incremental .rlib has masked real fixture-oracle divergences;
  # never trust the fixture suite against an incremental build.
  cargo clean -p rust-ivm 2>/dev/null || true

  if cargo test -- --test-threads=1 2>&1; then
    echo -e "${GREEN}Rust tests: PASS${NC}"
    TOTAL_PASS=$((TOTAL_PASS + 1))
  else
    echo -e "${RED}Rust tests: FAIL${NC}"
    TOTAL_FAIL=$((TOTAL_FAIL + 1))
  fi
}

run_docker_tests() {
  echo -e "\n${BLUE}=== Docker WAL2 Verification ===${NC}"

  CONTAINER="xyne-sandbox-rust-test-zero-cache"

  if ! docker ps --format '{{.Names}}' | grep -q "$CONTAINER"; then
    echo -e "${YELLOW}Container $CONTAINER not running. Trying docker run...${NC}"
    if docker run --rm zero-rust-ivm:latest bash /app/mono/rust-ivm/scripts/test-docker-wal2.sh 2>&1; then
      echo -e "${GREEN}Docker WAL2 tests: PASS${NC}"
      TOTAL_PASS=$((TOTAL_PASS + 1))
    else
      echo -e "${RED}Docker WAL2 tests: FAIL${NC}"
      TOTAL_FAIL=$((TOTAL_FAIL + 1))
    fi
  else
    echo "Running inside container $CONTAINER..."
    if docker exec "$CONTAINER" bash /app/mono/rust-ivm/scripts/test-docker-wal2.sh 2>&1; then
      echo -e "${GREEN}Docker WAL2 tests: PASS${NC}"
      TOTAL_PASS=$((TOTAL_PASS + 1))
    else
      echo -e "${RED}Docker WAL2 tests: FAIL${NC}"
      TOTAL_FAIL=$((TOTAL_FAIL + 1))
    fi
  fi
}

# Parse args
RUST=true
DOCKER=false

for arg in "$@"; do
  case $arg in
    --rust-only)  RUST=true;  DOCKER=false ;;
    --docker)     DOCKER=true ;;
    --all)        RUST=true;  DOCKER=true ;;
  esac
done

echo "Rust IVM Local Test Runner"
echo "=========================="
echo "Project root: $PROJECT_ROOT"

if $RUST; then run_rust_tests; fi
if $DOCKER; then run_docker_tests; fi

echo ""
echo "=========================="
echo -e "${GREEN}Total passed: $TOTAL_PASS${NC}"
echo -e "${RED}Total failed: $TOTAL_FAIL${NC}"
echo "=========================="

exit $TOTAL_FAIL
