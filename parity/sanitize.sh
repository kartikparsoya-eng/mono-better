#!/usr/bin/env bash
# Axis-5 sanitizers + Axis-4 leak detection (PROFILING.md): run the crate test
# suites under ASan(+LSan) and TSan. Sanitizers need nightly rustc and are far
# more reliable on linux targets, so everything runs inside a rust:nightly
# container — identical behavior on mac and linux hosts.
#
#   parity/sanitize.sh                 # ASan+LSan on all three crates
#   parity/sanitize.sh tsan            # TSan (rust-syncer only: the one crate
#                                      #  with real cross-thread concurrency;
#                                      #  5-15x slowdown)
#
# Notes:
# - rust-cvr PG suites self-skip without TEST_CVR_PG_URI (pass it through
#   with host.docker.internal if you want them sanitized too).
# - rust-ivm runs its non-wal2 path here (the wal2 static lib is a host
#   build artifact); the wal2-specific branches are covered by the normal
#   suite + the coverage image, not by sanitizers.
# - LSan is ON by default under ASan: Rc/RefCell cycles (the class behind
#   the G6 leak) and Box::leak show up with allocation stacks at exit.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:-asan}"

IMG=rustlang/rust:nightly-slim
DOCKER_RUN=(docker run --rm -v "$ROOT/packages":/pkg -w /pkg
  -e CARGO_TARGET_DIR=/tmp/sanitize-target "$IMG" bash -lc)

case "$MODE" in
  asan)
    "${DOCKER_RUN[@]}" '
      set -e
      apt-get update -qq && apt-get install -y -qq clang pkg-config libssl-dev >/dev/null
      rustup component add rust-src >/dev/null 2>&1
      export RUSTFLAGS="-Zsanitizer=address"
      T=$(rustc -vV | sed -n "s/host: //p")
      for crate in rust-cvr rust-ivm rust-syncer; do
        echo "══ ASan+LSan: $crate ══"
        extra=""
        [ "$crate" = rust-syncer ] && extra="--no-default-features"
        cargo +nightly test -Zbuild-std --target "$T" $extra \
          --manifest-path "$crate/Cargo.toml" --lib -- --test-threads=1
      done
    '
    ;;
  tsan)
    "${DOCKER_RUN[@]}" '
      set -e
      apt-get update -qq && apt-get install -y -qq clang pkg-config libssl-dev >/dev/null
      rustup component add rust-src >/dev/null 2>&1
      export RUSTFLAGS="-Zsanitizer=thread -Ctarget-feature=-crt-static"
      T=$(rustc -vV | sed -n "s/host: //p")
      echo "══ TSan: rust-syncer ══"
      cargo +nightly test -Zbuild-std --target "$T" --no-default-features \
        --manifest-path rust-syncer/Cargo.toml --lib -- --test-threads=1
    '
    ;;
  *) echo "usage: $0 [asan|tsan]" >&2; exit 2;;
esac
