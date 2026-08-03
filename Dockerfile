# =============================================================================
# Zero-Cache with Rust IVM — drop-in replacement for zero 1.7
# =============================================================================
# Build context: root of the mono-v1.7 repo (i.e. this directory).
#
# Multi-stage:
#   1. Build WAL2 SQLite static library
#   2. Build Litestream executables (rocicorp fork + v5)
#   3. Build the Rust IVM NAPI addon
#   4. Runtime — zero-cache from source via tsx + .node addon baked in
#
# Deployment-hardened checklist (learned from sandbox incidents):
#   * Schema/protocol version matches target env (handled outside image).
#   * All envs required at runtime are baked in with sane defaults.
#   * Litestream executables present so backup replicator starts.
#   * WORKDIR is explicit; entry point uses a stable relative path.
#   * USER set so OTel user resolver does not panic.
#   * Sync-worker count defaults to core-aware value, not unbounded.
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1: WAL2 SQLite static library
# ---------------------------------------------------------------------------
FROM rust:1-slim-bookworm AS sqlite-builder

RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY packages/rust-ivm/wal2-sqlite/ ./sqlite3/

RUN gcc -O2 -ffp-contract=off -fPIC -c sqlite3/sqlite3.c -o /tmp/sqlite3.o \
        -DSQLITE_THREADSAFE=2 \
        -DSQLITE_ENABLE_FTS5 \
        -DSQLITE_ENABLE_JSON1 \
        -DSQLITE_ENABLE_RTREE \
        -DSQLITE_OMIT_LOAD_EXTENSION \
        -DSQLITE_ENABLE_SNAPSHOT \
        -DSQLITE_ENABLE_WAL2_COREAD \
        -DSQLITE_ENABLE_STMT_SCANSTATUS \
    && ar rcs /usr/lib/libsqlite3.a /tmp/sqlite3.o \
    && cp sqlite3/sqlite3.h /usr/include/sqlite3.h \
    && cp sqlite3/sqlite3ext.h /usr/include/sqlite3ext.h

# ---------------------------------------------------------------------------
# Stage 2: Litestream executables (matches packages/zero/Dockerfile)
# ---------------------------------------------------------------------------
FROM golang:1.25.10@sha256:cd05a378aaf011e8056745363e5c40f4f2bef0fa4d9bf19b9c38316079c332ff AS litestream-builder

WORKDIR /src/
RUN git clone --depth 1 --branch zero@v0.0.9 https://github.com/rocicorp/litestream.git
WORKDIR /src/litestream/

ARG LITESTREAM_VERSION=0.3.13+z0.0.9
ENV GOTOOLCHAIN=local

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags "-s -w -X 'main.Version=${LITESTREAM_VERSION}' -extldflags '-static'" -tags osusergo,netgo,sqlite_omit_load_extension -o /usr/local/bin/litestream ./cmd/litestream

FROM litestream/litestream:0.5.11 AS litestream-v5

# ---------------------------------------------------------------------------
# Stage 3: Build the Rust IVM NAPI addon
# ---------------------------------------------------------------------------
FROM rust:1-slim-bookworm AS napi-builder

COPY --from=sqlite-builder /usr/lib/libsqlite3.a /usr/lib/libsqlite3.a
COPY --from=sqlite-builder /usr/include/sqlite3.h /usr/include/sqlite3.h
COPY --from=sqlite-builder /usr/include/sqlite3ext.h /usr/include/sqlite3ext.h

RUN apt-get update && apt-get install -y --no-install-recommends pkg-config && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY packages/rust-ivm/ ./rust-ivm/

WORKDIR /build/rust-ivm/napi
# --features rust-ivm/wal2_coread: this system SQLite is the SQLITE_ENABLE_WAL2_COREAD
# fork, so the sqlite3_wal2_coread_* symbols are present. The feature makes the
# parallel read pool CO-READ at curr's frame (sharing its read-mark) instead of
# each pool conn claiming its own — the fix that lets the pool stay enabled under
# churn without exhausting wal2's read-mark slots (see read_pool.rs).
RUN cargo build --release --features rust-ivm/wal2_coread
RUN cp target/release/librust_ivm_napi.so rust-ivm.node

# ---------------------------------------------------------------------------
# Stage 4: Runtime
# ---------------------------------------------------------------------------
FROM node:22-bookworm-slim

ARG ZERO_VERSION=1.7.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*

RUN corepack enable && corepack prepare pnpm@11.5.3 --activate

WORKDIR /app

# Copy the monorepo source
COPY . ./mono/

# Copy the built NAPI addon into the package tree so the driver default
# fallback resolves it.
COPY --from=napi-builder /build/rust-ivm/napi/rust-ivm.node ./mono/packages/rust-ivm/napi/rust-ivm.node

# Copy Litestream executables and config
COPY --from=litestream-builder /usr/local/bin/litestream /usr/local/bin/litestream
COPY --from=litestream-v5 /usr/local/bin/litestream /usr/local/bin/litestream-v5
RUN cp /app/mono/packages/zero-cache/src/services/litestream/config.yml /etc/litestream.yml

# Install dependencies
WORKDIR /app/mono
RUN pnpm install --frozen-lockfile
RUN pnpm add -w tsx@4

# Required/sane defaults — DO NOT ask Shivral to remember these.
ENV USE_RUST_IVM=true
# Absolute path to the native addon (baked below at COPY). The driver's relative
# fallback ('../../../../packages/rust-ivm/napi/rust-ivm.node') mis-resolves in
# this layout (doubled 'packages') and silently falls back to TS — so without
# this env the rust engine never loads. Must match the COPY destination.
ENV RUST_IVM_ADDON_PATH=/app/mono/packages/rust-ivm/napi/rust-ivm.node
# Read-level parallelism (frame-pinned pool). RE-ENABLED (4) now that the pool
# CO-READS at curr's frame via the wal2_coread feature (built above): the K pool
# connections share curr's single -shm read-mark instead of each claiming its
# own, so they no longer exhaust wal2's fixed read-mark slots under churn (the
# cause of the "prev db has advanced past X" slips). Before coread, LANES=4 drove
# 33 stale-snapshot slips per 50c churn load vs 2 at LANES=0; with coread the
# pool is expected at ~0 like the Go port (which uses the same coread API).
# Guarded: if coread capture/arm ever fails at runtime, the pool falls back to
# the old independent BEGIN CONCURRENT pin (read_pool.rs), and the self-heal
# reset (diff.rs) still absorbs any residual slip.
ENV RUST_IVM_READ_LANES=4
# Native query planner (cost model + flip decision). Dark behind this flag;
# when enabled, Rust runs the planner on its own DB connection instead of
# round-tripping to JS for planQuery.
ENV RUST_IVM_PLANNER=1
# Bounded TSFN queue depth for per-row streaming delivery. 1 = actor parks after
# every row until JS drains it; a busy main thread then stalls delivery per-row
# (microbench: 0.5–5ms bursts inflate per-row 180–750×). K=64 lets the actor run
# 64 rows ahead without parking → 134–166× faster delivery under a busy loop,
# output byte-identical (FIFO queue preserves order). O(64) NapiRowChanges in
# flight per stream (bounded). Enabled here; set to 1 to revert instantly.
ENV RUST_IVM_TSFN_QUEUE=64
# Distribute client groups across sync workers by count (round-robin) instead
# of by CG-id hash. Sticky per CG within a process lifetime; evens out load
# when hash bucketing leaves workers lopsided.
ENV ZERO_ROUND_ROBIN_ROUTING=1
# Hydration cursor page size (default 10000). Smaller pages = more, lighter
# frames during cold hydrate.
ENV ZERO_CURSOR_PAGE_SIZE=100
ENV UV_THREADPOOL_SIZE=16
ENV ZERO_IN_CONTAINER=1
ENV ZERO_LOG_FORMAT=json
ENV ZERO_SERVER_VERSION=${ZERO_VERSION}
ENV ZERO_LITESTREAM_EXECUTABLE=/usr/local/bin/litestream
ENV ZERO_LITESTREAM_EXECUTABLE_V5=/usr/local/bin/litestream-v5
ENV ZERO_LITESTREAM_CONFIG_PATH=/etc/litestream.yml
# OTel needs a user; otherwise user.current() throws in containers.
ENV USER=zero-cache
# OTel: omit endpoint in sandbox (no collector). Set per-env if a collector
# exists. Leaving unset avoids ECONNREFUSED on 127.0.0.1:4318.
ENV OTEL_EXPORTER_OTLP_ENDPOINT=
# Set a safe cap on sync workers. Override per env if cores differ.
ENV ZERO_NUM_SYNC_WORKERS=8
# Bound JS heap to avoid OOM death spirals on unbounded hydration paths.
ENV NODE_OPTIONS="--import tsx --no-warnings --max-old-space-size=4096"
ENV PATH="/app/mono/node_modules/.bin:${PATH}"

EXPOSE 4848 4849

WORKDIR /app/mono

ENTRYPOINT ["node", "--import", "tsx", "--no-warnings"]
CMD ["./packages/zero-cache/src/server/runner/main.ts"]
