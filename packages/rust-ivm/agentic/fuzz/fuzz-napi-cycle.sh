#!/bin/bash
# fuzz-napi-cycle.sh — run napi/TableSource differential fuzzer in cycles.
# 10 min on, 5 min off, forever. Exercises the napi addon over SQLite TableSource.
# Start: nohup bash agentic/fuzz/fuzz-napi-cycle.sh >> agentic/logs/fuzz-napi-cycle.out 2>&1 &
# Stop:  pkill -f fuzz-napi-cycle.sh; pkill -f fuzz-napi-loop.mjs
cd "$(dirname "$0")/../.." || exit 1   # rust-ivm root

while true; do
  # Rebuild the napi addon each cycle so the fuzzer always tests CURRENT code.
  # The addon is a .node file that JS requires directly — a stale build would
  # re-report already-fixed bugs as fresh divergences (false positives).
  if npx napi build --release --cwd napi 2>&1 | tail -1 | grep -q "Finished"; then
    cp napi/target/aarch64-apple-darwin/release/librust_ivm_napi.dylib napi/rust-ivm.node
    node agentic/fuzz/fuzz-napi-loop.mjs --minutes 10 --max-findings 5
  else
    echo "$(date -u +%FT%TZ) fuzz-napi-cycle: napi build FAILED — skipping this round"
  fi
  sleep 300  # 5 min
done
