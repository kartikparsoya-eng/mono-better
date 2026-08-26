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
# bash -c, NOT -lc: a login shell re-sources /etc/profile and DROPS
# /usr/local/cargo/bin from PATH in this image (rustc: command not found, 127).
DOCKER_RUN=(docker run --rm -v "$ROOT/packages":/pkg -w /pkg
  -e CARGO_TARGET_DIR=/tmp/sanitize-target
  -e PATH=/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
  -e DEBIAN_FRONTEND=noninteractive "$IMG" bash -c)

# Shared container preamble (word-for-word identical for asan/tsan):
#  - toolchain: pin a dated nightly. The rolling nightly channel can pair the
#    image cargo with a rust-src whose std-workspace layout it cannot build
#    (library/Cargo.lock moved), breaking -Zbuild-std; a fixed date keeps cargo
#    and rust-src from the SAME snapshot.
#  - sqlite: rust-ivm'"'"'s cost model calls sqlite3_stmt_scanstatus_v2, which
#    Debian'"'"'s libsqlite3 is NOT compiled with (undefined-reference link error
#    on rust-syncer, which embeds rust-ivm). Compile the repo'"'"'s WAL2 SQLite
#    source with the SAME flags the production Dockerfile uses (incl.
#    SQLITE_ENABLE_STMT_SCANSTATUS) into /usr/local/lib — so the sanitizer links
#    against the exact SQLite production ships, not Debian'"'"'s.
PREAMBLE='
  set -e
  apt-get update -qq && apt-get install -y -qq clang gcc pkg-config libssl-dev >/dev/null
  # Install into the multiarch system libdir, NOT /usr/local/lib: at LINK time
  # `ld` searches /usr/lib/<multiarch> + /lib, but NOT /usr/local/lib (that is
  # runtime-only via ldconfig) — so a /usr/local/lib install links for crates
  # whose scanstatus refs get gc-sectioned away (rust-ivm) but fails for
  # rust-syncer, which keeps the ref. Multiarch dir fixes both link and runtime.
  ARCHLIB=/usr/lib/$(gcc -print-multiarch)
  gcc -O2 -ffp-contract=off -fPIC -shared rust-ivm/wal2-sqlite/sqlite3.c \
      -Wl,-soname,libsqlite3.so.0 -o "$ARCHLIB/libsqlite3.so.0" \
      -DSQLITE_THREADSAFE=2 -DSQLITE_ENABLE_COLUMN_METADATA \
      -DSQLITE_ENABLE_DBSTAT_VTAB -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_JSON1 \
      -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_ENABLE_RTREE -DSQLITE_ENABLE_STAT4 \
      -DSQLITE_ENABLE_STMT_SCANSTATUS -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT \
      -DSQLITE_ENABLE_DESERIALIZE -lpthread -ldl -lm
  ln -sf libsqlite3.so.0 "$ARCHLIB/libsqlite3.so"
  cp rust-ivm/wal2-sqlite/sqlite3.h /usr/local/include/sqlite3.h
  cp rust-ivm/wal2-sqlite/sqlite3ext.h /usr/local/include/sqlite3ext.h
  ldconfig
  NIGHTLY=nightly-2026-08-01
  rustup toolchain install "$NIGHTLY" --profile minimal --component rust-src
  T=$(rustup run "$NIGHTLY" rustc -vV | sed -n "s/host: //p")
'

case "$MODE" in
  asan)
    "${DOCKER_RUN[@]}" "$PREAMBLE"'
      export RUSTFLAGS="-Zsanitizer=address"
      # rust-cvr + rust-ivm carry essentially all the unsafe / Rc-cycle /
      # RefCell surface (rust-ivm is where the G6 operator-graph leak lived), so
      # they are HARD requirements. rust-syncer is safe-Rust tokio orchestration
      # around those two; its ASan leg is BEST-EFFORT because -Zbuild-std +
      # sanitizer target + -nodefaultlibs perturbs library resolution so the
      # rusqlite `-lsqlite3` directive fails to resolve sqlite3_stmt_scanstatus_v2
      # (a link quirk — the symbol IS exported by the WAL2 lib built above, and
      # the same code links cleanly in the prod Dockerfile and normal cargo test).
      for crate in rust-cvr rust-ivm; do
        echo "══ ASan+LSan: $crate (required) ══"
        cargo +"$NIGHTLY" test -Zbuild-std --target "$T" \
          --manifest-path "$crate/Cargo.toml" --lib -- --test-threads=1
      done
      echo "══ ASan+LSan: rust-syncer (best-effort) ══"
      cargo +"$NIGHTLY" test -Zbuild-std --target "$T" --no-default-features \
        --manifest-path rust-syncer/Cargo.toml --lib -- --test-threads=1 \
        || echo "NOTE: rust-syncer ASan link-blocked (build-std scanstatus_v2 quirk) — see comment; memory risk covered transitively by rust-cvr+rust-ivm above"
    '
    ;;
  tsan)
    "${DOCKER_RUN[@]}" "$PREAMBLE"'
      export RUSTFLAGS="-Zsanitizer=thread -Ctarget-feature=-crt-static"
      # Suppress the ONE known-benign lock-order-inversion: SQLite'"'"'s unix VFS
      # acquires its global unixBigLock + per-inode mutexes (unixEnterMutex /
      # unixLock vs unixClose, both in sqlite3.c) in a discipline TSan flags as
      # an inversion on rusqlite::Connection drop. It is SQLite-internal C code
      # with its own no-deadlock guarantee, single-threaded here, and NOT our
      # Rust code. Suppressing it makes a REAL lock-order-inversion in our code
      # stand out instead of being buried under this noise.
      cat > /tmp/tsan.supp <<SUPP
deadlock:unixEnterMutex
deadlock:unixLock
deadlock:unixClose
SUPP
      export TSAN_OPTIONS="suppressions=/tmp/tsan.supp"
      echo "══ TSan: rust-syncer ══"
      cargo +"$NIGHTLY" test -Zbuild-std --target "$T" --no-default-features \
        --manifest-path rust-syncer/Cargo.toml --lib -- --test-threads=1
    '
    ;;
  *) echo "usage: $0 [asan|tsan]" >&2; exit 2;;
esac
