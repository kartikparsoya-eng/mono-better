# =============================================================================
# Zero-Cache with Rust IVM — drop-in replacement for zero 1.7
# =============================================================================
# Build context: root of the mono-v1.7 repo (i.e. this directory).
#
# Multi-stage:
#   1. Build the WAL2 SQLite shared library used by the TS control path
#   2. Build Litestream executables (rocicorp fork + v5)
#   3. Build the full Rust syncer binary (WAL2-only production features)
#   4. Rebuild zero-sqlite3 against the same WAL2 shared library
#   5. Runtime — zero-cache dispatcher + the full Rust syncer binary
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
# Stage 1: WAL2 SQLite shared library
# ---------------------------------------------------------------------------
FROM rust:1-slim-bookworm@sha256:99e09cb2284e2ddbb73a995deee3e91783fd04d177602ccf6eab326d778ee777 AS sqlite-builder

RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY packages/rust-ivm/wal2-sqlite/ ./sqlite3/

RUN gcc -O2 -ffp-contract=off -fPIC -shared sqlite3/sqlite3.c \
        -Wl,-soname,libsqlite3.so.0 -o /usr/local/lib/libsqlite3.so.0 \
        -DHAVE_INT16_T=1 -DHAVE_INT32_T=1 -DHAVE_INT8_T=1 \
        -DHAVE_STDINT_H=1 -DHAVE_UINT16_T=1 -DHAVE_UINT32_T=1 \
        -DHAVE_UINT8_T=1 -DHAVE_USLEEP=1 \
        -DSQLITE_DEFAULT_CACHE_SIZE=-16000 \
        -DSQLITE_DEFAULT_FOREIGN_KEYS=1 \
        -DSQLITE_DEFAULT_MEMSTATUS=0 \
        -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1 \
        -DSQLITE_DQS=0 \
        -DSQLITE_THREADSAFE=2 \
        -DSQLITE_ENABLE_COLUMN_METADATA \
        -DSQLITE_ENABLE_DBSTAT_VTAB \
        -DSQLITE_ENABLE_DESERIALIZE \
        -DSQLITE_ENABLE_FTS3 \
        -DSQLITE_ENABLE_FTS3_PARENTHESIS \
        -DSQLITE_ENABLE_FTS4 \
        -DSQLITE_ENABLE_FTS5 \
        -DSQLITE_ENABLE_GEOPOLY \
        -DSQLITE_ENABLE_JSON1 \
        -DSQLITE_ENABLE_MATH_FUNCTIONS \
        -DSQLITE_ENABLE_PERCENTILE \
        -DSQLITE_ENABLE_RTREE \
        -DSQLITE_ENABLE_STAT4 \
        -DSQLITE_ENABLE_STMT_SCANSTATUS \
        -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT \
        -DSQLITE_LIKE_DOESNT_MATCH_BLOBS \
        -DSQLITE_OMIT_DEPRECATED \
        -DSQLITE_OMIT_PROGRESS_CALLBACK \
        -DSQLITE_OMIT_SHARED_CACHE \
        -DSQLITE_OMIT_TCL_VARIABLE \
        -DSQLITE_SOUNDEX \
        -DSQLITE_STAT4_SAMPLES=128 \
        -DSQLITE_TRACE_SIZE_LIMIT=32 \
        -DSQLITE_USE_URI=1 \
        -lpthread -ldl -lm \
    && ln -s libsqlite3.so.0 /usr/local/lib/libsqlite3.so \
    && cp sqlite3/sqlite3.h /usr/local/include/sqlite3.h \
    && cp sqlite3/sqlite3ext.h /usr/local/include/sqlite3ext.h

# ---------------------------------------------------------------------------
# Stage 2: Litestream executables (matches packages/zero/Dockerfile)
# ---------------------------------------------------------------------------
FROM golang:1.25.10@sha256:cd05a378aaf011e8056745363e5c40f4f2bef0fa4d9bf19b9c38316079c332ff AS litestream-builder

WORKDIR /src/
RUN git clone --depth 1 --branch zero@v0.0.9 https://github.com/rocicorp/litestream.git
WORKDIR /src/litestream/

ARG LITESTREAM_VERSION=0.3.13+z0.0.9
ENV GOTOOLCHAIN=local

RUN go get google.golang.org/grpc@v1.82.1 google.golang.org/api@v0.291.0 \
    && go mod tidy

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags "-s -w -X 'main.Version=${LITESTREAM_VERSION}' -extldflags '-static'" -tags osusergo,netgo,sqlite_omit_load_extension -o /usr/local/bin/litestream ./cmd/litestream

FROM golang:1.25.10@sha256:cd05a378aaf011e8056745363e5c40f4f2bef0fa4d9bf19b9c38316079c332ff AS litestream-v5-builder

WORKDIR /src/
RUN git clone --depth 1 --branch v0.5.11 https://github.com/benbjohnson/litestream.git
WORKDIR /src/litestream/
RUN go get google.golang.org/grpc@v1.82.1 google.golang.org/api@v0.291.0 \
    && go mod tidy
ARG LITESTREAM_V5_VERSION=0.5.11
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    go build -ldflags "-s -w -X 'main.Version=${LITESTREAM_V5_VERSION}'" \
      -o /usr/local/bin/litestream ./cmd/litestream

# ---------------------------------------------------------------------------
# Stage 3: Build the full Rust syncer binary
# ---------------------------------------------------------------------------
FROM rust:1-slim-bookworm@sha256:99e09cb2284e2ddbb73a995deee3e91783fd04d177602ccf6eab326d778ee777 AS rust-syncer-builder

RUN apt-get update && apt-get install -y --no-install-recommends gcc pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY packages/rust-cvr/ ./rust-cvr/
COPY packages/rust-ivm/ ./rust-ivm/
COPY packages/rust-syncer/ ./rust-syncer/

# Optional extra cargo features for the syncer build (empty by default → the
# normal production binary). Set `--build-arg RUST_SYNCER_FEATURES=dhat-heap` to
# produce a heap-profiling binary that dumps a dhat profile on graceful shutdown
# (leak hunts only; dhat intercepts every allocation, so never ship it).
ARG RUST_SYNCER_FEATURES=""

# `--no-default-features` disables the plain-WAL test escape hatch. The binary's
# build.rs statically links the WAL2 amalgamation, so it reads the exact replica
# format produced by zero-cache without depending on the runtime system SQLite.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/rust-syncer/target \
    cargo build --release --no-default-features \
      ${RUST_SYNCER_FEATURES:+--features "$RUST_SYNCER_FEATURES"} \
      --manifest-path rust-syncer/Cargo.toml \
    && cp rust-syncer/target/release/rust-syncer /usr/local/bin/rust-syncer

# ---------------------------------------------------------------------------
# Stage 4: Build zero-sqlite3 against the SAME shared WAL2 library
# ---------------------------------------------------------------------------
FROM node:22-bookworm-slim@sha256:f32b81066cde10a75dbac96646099533316d94bac4150c55da1636e1f0ffdc46 AS zero-sqlite-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=sqlite-builder /usr/local/lib/libsqlite3.so* /usr/local/lib/
COPY --from=sqlite-builder /usr/local/include/sqlite3*.h /usr/local/include/
COPY packages/rust-ivm/zero-sqlite3-shared.binding.gyp /tmp/binding.gyp

WORKDIR /build
RUN npm install --ignore-scripts @rocicorp/zero-sqlite3@1.1.4 node-gyp@11 \
    && cp /tmp/binding.gyp node_modules/@rocicorp/zero-sqlite3/binding.gyp \
    && node node_modules/@rocicorp/zero-sqlite3/deps/gen-unicode-case.mjs \
         > node_modules/@rocicorp/zero-sqlite3/src/util/unicode_case_data.h \
    && npx node-gyp rebuild --release \
         --directory node_modules/@rocicorp/zero-sqlite3 \
    && cp node_modules/@rocicorp/zero-sqlite3/build/Release/better_sqlite3.node /tmp/better_sqlite3.node \
    && ldd /tmp/better_sqlite3.node | grep '/usr/local/lib/libsqlite3.so.0'

# ---------------------------------------------------------------------------
# Stage 5: Runtime
# ---------------------------------------------------------------------------
FROM node:22-bookworm-slim@sha256:f32b81066cde10a75dbac96646099533316d94bac4150c55da1636e1f0ffdc46

ARG ZERO_VERSION=1.7.0
ARG GIT_REVISION=unknown

LABEL org.opencontainers.image.base.name="node:22-bookworm-slim" \
      org.opencontainers.image.base.digest="sha256:f32b81066cde10a75dbac96646099533316d94bac4150c55da1636e1f0ffdc46" \
      org.opencontainers.image.revision="${GIT_REVISION}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN corepack enable && corepack prepare pnpm@11.5.3 --activate

WORKDIR /app

# Copy the monorepo source
COPY . ./mono/

COPY --from=rust-syncer-builder /usr/local/bin/rust-syncer /usr/local/bin/rust-syncer
COPY --from=sqlite-builder /usr/local/lib/libsqlite3.so* /usr/local/lib/
COPY --from=zero-sqlite-builder /tmp/better_sqlite3.node /tmp/better_sqlite3.node

# Copy Litestream executables and config
COPY --from=litestream-builder /usr/local/bin/litestream /usr/local/bin/litestream
COPY --from=litestream-v5-builder /usr/local/bin/litestream /usr/local/bin/litestream-v5
RUN cp /app/mono/packages/zero-cache/src/services/litestream/config.yml /etc/litestream.yml

# Install only the runtime dependency closure for zero-cache. Keep the root
# package in the filter because it owns the pinned tsx runtime loader. The
# server source imports ast-to-zql directly, outside pnpm's declared graph.
WORKDIR /app/mono
RUN --mount=type=cache,id=pnpm-store,target=/root/.local/share/pnpm/store \
    pnpm install --frozen-lockfile --prod \
      --filter @rocicorp/mono --filter zero-cache... --filter ast-to-zql... \
    && zero_sqlite=$(find node_modules/.pnpm -path '*/@rocicorp/zero-sqlite3' -type d -print -quit) \
    && test -n "$zero_sqlite" \
    && mkdir -p "$zero_sqlite/build/Release" \
    && cp /tmp/better_sqlite3.node "$zero_sqlite/build/Release/better_sqlite3.node" \
    && rm /tmp/better_sqlite3.node \
    && ldconfig \
    && ldd "$zero_sqlite/build/Release/better_sqlite3.node" | grep '/usr/local/lib/libsqlite3.so.0' \
    && rm -rf /root/.cache \
       /usr/local/lib/node_modules/npm \
       /usr/local/bin/npm /usr/local/bin/npx \
       packages/rust-ivm/agentic packages/rust-ivm/wal2-sqlite \
       packages/rust-ivm/src packages/rust-ivm/tests

# =============================================================================
# Baked env — IMAGE-INTERNAL ONLY.
#
# This is the dedicated full-Rust candidate image; selecting it is the rollout
# opt-in (the TS control uses the upstream image). We bake ONLY what the image
# itself knows or provides: engine selection, in-image binary/config paths, and
# runtime plumbing. All OPERATIONAL config — DB/auth secrets, URLs, ports,
# routing, worker/shard counts, CVR conns, OTEL endpoint — is supplied by the
# DEPLOYMENT (k8s manifest + secrets), exactly like the TS control image. What
# used to be baked and moved out is listed at the bottom.
# =============================================================================

# --- Engine selection + in-image paths (only the image can know these) -------
ENV ZERO_SYNCER=rust
ENV ZERO_RUST_SYNCER_PATH=/usr/local/bin/rust-syncer
ENV ZERO_LITESTREAM_EXECUTABLE=/usr/local/bin/litestream
ENV ZERO_LITESTREAM_EXECUTABLE_V5=/usr/local/bin/litestream-v5
ENV ZERO_LITESTREAM_CONFIG_PATH=/etc/litestream.yml
ENV ZERO_SERVER_VERSION=${ZERO_VERSION}

# --- Runtime plumbing the image must provide ---------------------------------
# OTel needs a resolvable user; user.current() throws in containers otherwise.
ENV USER=zero-cache
# Bound JS heap to avoid OOM death spirals on unbounded hydration paths.
ENV NODE_OPTIONS="--import tsx --no-warnings --max-old-space-size=4096"
ENV PATH="/app/mono/node_modules/.bin:${PATH}"

# --- Safe, non-conflicting image defaults (a deployment may still override) ---
# Cap glibc malloc arenas: the multi-threaded rust-syncer (sharded executors +
# tokio) otherwise gets up to 8*cores arenas whose fragmentation retains freed
# pipeline memory as ever-growing RSS (reads as a leak in the ART G6 gate and in
# prod dashboards). Two arenas keep contention negligible at our thread counts
# while bounding retention; pairs with the in-process malloc_trim task.
ENV MALLOC_ARENA_MAX=2
# node libuv threadpool — sized for litestream spawns + fs work, not node's 4.
ENV UV_THREADPOOL_SIZE=16
ENV ZERO_IN_CONTAINER=1
ENV ZERO_LOG_FORMAT=json
# OTel endpoint intentionally empty; the deployment sets it when a collector
# exists (leaving unset avoids ECONNREFUSED on 127.0.0.1:4318).
ENV OTEL_EXPORTER_OTLP_ENDPOINT=

# --- Dispatcher/runtime tuning defaults (a deployment may override) ----------
# CG→worker routing at the TS DISPATCHER (worker-dispatcher.ts): round-robin,
# distributed evenly by count and sticky per CG within a process lifetime,
# instead of the default CG-id hash. This is a SEPARATE layer from the
# rust-syncer's own CG→shard placement, which is ALWAYS least-loaded
# (router.rs `place_cg`, no flag). `ZERO_LEAST_LOADED_ROUTING` is not implemented
# on this line, so this does not conflict with it.
ENV ZERO_ROUND_ROBIN_ROUTING=1
# Hydration cursor page size (code default 10000): smaller = more, lighter
# frames during cold hydrate.
ENV ZERO_CURSOR_PAGE_SIZE=100
# Sync-worker (process) count. Override per env if cores differ.
ENV ZERO_NUM_SYNC_WORKERS=4

# --- Left to the DEPLOYMENT (not baked) --------------------------------------
#   ZERO_SYNCER_SHARDS — rust executor shards PER worker; unset ⇒ host*2 clamped
#   16..64 (host-adaptive). Pin in the manifest to match CGs-per-instance.

EXPOSE 4848 4849

WORKDIR /app/mono

ENTRYPOINT ["node", "--import", "tsx", "--no-warnings"]
CMD ["./packages/zero-cache/src/server/runner/main.ts"]
