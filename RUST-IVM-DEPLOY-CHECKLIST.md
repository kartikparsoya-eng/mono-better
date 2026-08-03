# Rust IVM Deploy Checklist

Image: `ghcr.io/<repo>/zero-cache-rust-ivm:<branch>-<sha>`  
Branch: `rust-ivm-v1.7.0`

Send Shivral **exactly one message** containing: image tag, target env, one-pod rollout plan, and the checked-off list below.

## Before building the image

- [ ] `mono-v1.7` is on `rust-ivm-v1.7.0` and `git status` is clean.
- [ ] `rust-ivm` changes are in `packages/rust-ivm/` (single branch of truth).
- [ ] `packages/zero-cache/src/services/view-syncer/rust-ivm-driver.ts` does not contain random agent-added imports.
- [ ] Target env's base Zero version is known (e.g. `1.7.0`) and this branch is rebased on top of it.
- [ ] Schema version in this image matches target env DB.
- [ ] Sync protocol version matches target env servers/clients.
- [ ] Syncer and replicator are the **same version** — version mismatch breaks the replicator immediately.

## Build & CI

- [ ] `cargo test -- --test-threads=1` passes in `packages/rust-ivm`.
- [ ] `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` pass.
- [ ] NAPI addon builds and produces `packages/rust-ivm/napi/rust-ivm.node`.
- [ ] Docker image built from `mono-v1.7/Dockerfile` (context = `mono-v1.7` root).
- [ ] CI workflow `Rust IVM` is green on the commit you are deploying.

## Image contents verification

- [ ] `/usr/local/bin/litestream` exists.
- [ ] `/usr/local/bin/litestream-v5` exists.
- [ ] `/app/mono/packages/rust-ivm/napi/rust-ivm.node` exists.
- [ ] `USER=zero-cache` is set in env.
- [ ] `ZERO_LITESTREAM_EXECUTABLE=/usr/local/bin/litestream` is set.
- [ ] `OTEL_EXPORTER_OTLP_ENDPOINT` is set to the actual collector (or left empty for sandbox).
- [ ] `ZERO_NUM_SYNC_WORKERS` is ≤ target pod CPU core limit (image default 4).
- [ ] No `RUST_IVM_READ_LANES` override is present; hydration is serial.
- [ ] `RUST_IVM_TSFN_QUEUE=64` and `RUST_IVM_STREAM_CREDIT=64` are both set.
- [ ] No `RUST_IVM_TSFN_BATCH` override is present; batch hydration is removed.
- [ ] Planner enablement matches the `PipelineDriver` constructor flag.

## Local / sandbox validation

- [ ] Image runs locally with a similar Postgres + upstream setup.
- [ ] Stress-tested with multiple client groups / connections.
- [ ] No unbounded-memory hydration paths (heap stays under limit).
- [ ] No false-drift rehydration loops observed.
- [ ] Backup replicator pod starts without `Missing --litestream-executable`.
- [ ] Pod logs show the correct image tag was pulled (not a stale image).

## Rollout message template

```
Env: <sandbox|pre-prod>
Image: ghcr.io/<repo>/zero-cache-rust-ivm:rust-ivm-v1.7.0-<sha>
Rollout: one pod first, compare against existing pods, scale if healthy.
Checks: schema v<>, sync protocol v<>, syncer=replicator v<>, workers=<>, heap=4Gi, serial hydration, planner parity.
```
