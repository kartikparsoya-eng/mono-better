#!/bin/bash
# fuzz-cycle.sh — run a 10-minute fuzz every 15 minutes, forever.
# Duty cycle 40% (was 17%): fuzzer is lightweight (TS oracle + 5ms Rust replay),
# won't starve loop workers. At 0.40s/seed this yields ~14K seeds/hr.
# Start: nohup bash agentic/fuzz/fuzz-cycle.sh >> agentic/logs/fuzz-cycle.out 2>&1 &
# Stop:  pkill -f fuzz-cycle.sh; pkill -f fuzz-loop.mjs
cd "$(dirname "$0")/../.." || exit 1   # rust-ivm root
while true; do
  # Rebuild the replay binary each cycle so the fuzzer always tests CURRENT
  # code. fuzz-loop.mjs runs the prebuilt target/debug/replay and never
  # rebuilds it, so a stale binary otherwise re-reports already-fixed panics
  # as fresh "rust-crash" divergences (false positives). Skip the fuzz round
  # if the build breaks rather than fuzz a stale/missing binary.
  if cargo build --bin replay; then
    node agentic/fuzz/fuzz-loop.mjs --minutes 10 --max-findings 5
  else
    echo "$(date -u +%FT%TZ) fuzz-cycle: cargo build --bin replay FAILED — skipping this round"
  fi
  sleep 300  # 5 min
done
