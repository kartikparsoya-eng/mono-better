#!/usr/bin/env bash
set -euo pipefail

# Fast iteration script: build Docker, restart containers, smoke test.
# Usage:
#   ./fast-iterate.sh          # build + restart + smoke test (~2 min)
#   ./fast-iterate.sh --art    # also run full ART oracle (~5 min)
#   ./fast-iterate.sh --quick  # skip build, just restart + smoke test (~30s)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
SANDBOX_DIR="/Users/kartik.parsoya/Documents/xy-repo/xyne-spaces/.sandboxes/rust-test"
ART_DIR="/Users/kartik.parsoya/Documents/xyne-art"

RUN_ART=false
SKIP_BUILD=false

for arg in "$@"; do
  case "$arg" in
    --art)    RUN_ART=true ;;
    --quick)  SKIP_BUILD=true ;;
    *) echo "Unknown arg: $arg"; exit 1 ;;
  esac
done

# ---- Build ----
if [ "$SKIP_BUILD" = false ]; then
  echo "=== Building Docker image (with BuildKit cache) ==="
  BUILD_START=$(date +%s)
  cd "$REPO_DIR"
  DOCKER_BUILDKIT=1 docker build \
    --progress=plain \
    -t zero-rust-ivm:latest \
    -f mono/Dockerfile.rust-ivm . 2>&1 | tail -5
  BUILD_END=$(date +%s)
  echo "Build took $((BUILD_END - BUILD_START))s"
  
  # Clean stale images
  docker images --filter "dangling=true" --format "{{.ID}}" | xargs docker rmi -f 2>/dev/null || true
fi

# ---- Restart containers ----
echo "=== Restarting containers ==="
cd "$SANDBOX_DIR"
docker compose -f docker-compose.yml -f docker-compose.override.yml down 2>&1 | tail -3
docker compose -f docker-compose.yml -f docker-compose.override.yml up -d 2>&1 | tail -5

# ---- Wait for health ----
echo "=== Waiting for health ==="
for i in $(seq 1 30); do
  STATUS=$(docker inspect --format='{{.State.Health.Status}}' xyne-sandbox-rust-test-zero-cache 2>/dev/null || echo "none")
  if [ "$STATUS" = "healthy" ]; then
    echo "Healthy after ${i}s"
    break
  fi
  sleep 2
done

if [ "$STATUS" != "healthy" ]; then
  echo "FAILED: container not healthy after 60s"
  docker logs xyne-sandbox-rust-test-zero-cache 2>&1 | tail -20
  exit 1
fi

# ---- Smoke test: wait for replica sync, check for crashes ----
echo "=== Smoke test (30s) ==="
sleep 15  # wait for change-streamer to sync replica

# Check if container is still running
CONTAINER_STATUS=$(docker inspect --format='{{.State.Status}}' xyne-sandbox-rust-test-zero-cache 2>/dev/null || echo "none")
if [ "$CONTAINER_STATUS" != "running" ]; then
  echo "CRASHED: container status=$CONTAINER_STATUS"
  docker logs xyne-sandbox-rust-test-zero-cache 2>&1 | grep -iE "panic|fatal|SIGABRT|crash" | tail -10
  exit 1
fi

# Check for rust-ivm logs (indicates queries are being processed)
RUST_LOGS=$(docker logs xyne-sandbox-rust-test-zero-cache 2>&1 | grep "\[rust-ivm\]" | wc -l | tr -d ' ')
echo "Rust IVM log lines: $RUST_LOGS"

# Check for crashes in logs
CRASH_COUNT=$(docker logs xyne-sandbox-rust-test-zero-cache 2>&1 | grep -ciE "panic|fatal.*abort|SIGABRT" || true)
if [ "$CRASH_COUNT" -gt "0" ]; then
  echo "WARNING: $CRASH_COUNT crash-like lines in logs"
  docker logs xyne-sandbox-rust-test-zero-cache 2>&1 | grep -iE "panic|fatal.*abort|SIGABRT" | tail -5
fi

# Check for fetched nodes
FETCHED=$(docker logs xyne-sandbox-rust-test-zero-cache 2>&1 | grep "\[rust-ivm\].*fetched [1-9]" | wc -l | tr -d ' ')
echo "Queries with non-zero fetch: $FETCHED"

echo "=== Smoke test PASSED ==="

# ---- Optional: Full ART ----
if [ "$RUN_ART" = true ]; then
  echo "=== Running full ART oracle ==="
  cd "$ART_DIR"
  ART_START=$(date +%s)
  ./run-art-local.sh --oracle --clean 2>&1 | tail -30
  ART_END=$(date +%s)
  echo "ART took $((ART_END - ART_START))s"
fi
