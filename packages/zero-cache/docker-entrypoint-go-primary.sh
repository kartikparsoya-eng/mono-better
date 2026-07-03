#!/bin/sh
# Container entrypoint for the Go-primary zero-cache image.
#
# Two transports (ZERO_GO_SIDECAR_TRANSPORT, baked by the TRANSPORT build
# arg):
#   napi   — each syncer worker dlopens libgoivm.so IN-PROCESS; no shared
#            sidecar process exists, so exec zero-cache directly. (H3:
#            previously the socket sidecar was spawned anyway and sat
#            idle — nobody connects in napi mode — while container boot
#            lived or died with it and it claimed GOMEMLIMIT/conn
#            budgets alongside the real in-process engines.)
#   socket — one shared go-ivm-sidecar per container, spawned here before
#            zero-cache starts. Every zero-cache syncer worker connects
#            to it via SidecarManager's externallyManaged=true path, per
#            go-ivm/REVIEW-final.md and feedback_shared_sidecar_tuning.md.
#
# `set -e` is intentionally NOT used: the socket-poll loop below
# uses explicit checks so a transient missing socket doesn't kill
# the container before we've waited long enough.

if [ "${ZERO_GO_SIDECAR_TRANSPORT:-socket}" = "napi" ]; then
  echo "[entrypoint] transport=napi — in-process engine (libgoivm.so); not spawning shared sidecar"
  exec npx tsx ./src/server/runner/main.ts
fi

SOCKET_PATH="${ZERO_GO_SIDECAR_SOCKET_PATH:-/tmp/go-ivm-shared.sock}"
SIDECAR_BIN="${ZERO_GO_SIDECAR_BINARY_PATH:-/opt/go-ivm/go-ivm-sidecar}"

# Clean up any stale socket from a prior container instance with the
# same name (rare, but cheap insurance).
rm -f "$SOCKET_PATH"

echo "[entrypoint] spawning shared sidecar: $SIDECAR_BIN $SOCKET_PATH"
"$SIDECAR_BIN" "$SOCKET_PATH" &
SIDECAR_PID=$!

# Wait up to ~7.5s for the socket to appear before declaring failure.
# Shared-sidecar mode init typically takes <500ms; 15 polls × 500ms
# is conservative but bounded so a broken binary fails fast instead
# of hanging the container.
#
# Order matters: check PID *before* the socket so a sidecar that
# refused-and-exited cannot ride a stale/half-bound socket into a
# false success. Observed in prod (GKE 2026-06-01 09:50 IST) when
# the sidecar refused on misconfigured env vars but entrypoint still
# announced "sidecar socket ready" 0.5s later, letting zero-cache
# boot with no IVM sidecar and no drift-audit running.
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
  if ! kill -0 "$SIDECAR_PID" 2>/dev/null; then
    echo "[entrypoint] ERROR: sidecar exited before socket appeared (pid=$SIDECAR_PID)"
    exit 1
  fi
  if [ -S "$SOCKET_PATH" ]; then
    echo "[entrypoint] sidecar socket ready after ${i} poll(s) (pid=$SIDECAR_PID)"
    break
  fi
  sleep 0.5
done

if [ ! -S "$SOCKET_PATH" ]; then
  echo "[entrypoint] ERROR: sidecar socket never appeared at $SOCKET_PATH; aborting"
  kill "$SIDECAR_PID" 2>/dev/null || true
  exit 1
fi

# Final guard: the sidecar may bind the socket and then crash during
# its own initialization (between the last poll and exec). Verify the
# process is still alive before handing off so a dead sidecar can't
# be papered over by a leftover socket.
if ! kill -0 "$SIDECAR_PID" 2>/dev/null; then
  echo "[entrypoint] ERROR: sidecar exited after socket appeared (pid=$SIDECAR_PID)"
  exit 1
fi

# Hand off to zero-cache. exec replaces this shell with the node
# process so signals (SIGTERM from orchestrator) reach zero-cache
# directly. The Go sidecar continues running in the background;
# SidecarManager in externallyManaged mode doesn't kill it on
# shutdown (the container is the owner — kernel reaps the
# orphaned sidecar when the container stops).
echo "[entrypoint] starting zero-cache"
exec npx tsx ./src/server/runner/main.ts
