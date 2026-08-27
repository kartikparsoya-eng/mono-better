# L8 — traffic-driven path differential: capture + run recipe

Goal: prove rust WALKS the same code TS walks under identical traffic (the
unwired-port / divergent-branch class that fixtures and frame-diffs can't see).
Joiner: `parity/layer8_path_diff.py` (self-test: `--self-test`).

## 1. Build the instrumented rust image

```bash
docker build -t zero-cache-rust-syncer:l8cov \
  --build-arg GIT_REVISION=$(git rev-parse --short HEAD)-l8cov \
  --build-arg RUST_SYNCER_COVERAGE=1 .          # -> -C instrument-coverage
```

## 2. Launch the two capture containers (sandbox-net)

Both replicate fresh from the sandbox upstream under their OWN app IDs
(`sandbox_rust_test_l8t` / `sandbox_rust_test_l8r`) so live CVR schemas are
untouched. **Cleanup (step 6) must drop their replication slots + schemas.**

```bash
mkdir -p /tmp/l8cov/{ts,rust}

# TS side: same image/rev, ZERO_SYNCER=ts + V8 coverage (flushed on graceful exit)
docker inspect xyne-sandbox-rust-test-zero-cache-ts \
  --format '{{range .Config.Env}}{{println .}}{{end}}' \
  | grep -v '^PATH=\|^NODE_VERSION\|^YARN_VERSION\|^USER=\|^ZERO_APP_ID=' > /tmp/l8-ts-env.txt
{ echo ZERO_APP_ID=sandbox_rust_test_l8t; echo NODE_V8_COVERAGE=/coverage; } >> /tmp/l8-ts-env.txt
docker run -d --name l8ts-zero-cache --network sandbox-net \
  -v /tmp/l8cov/ts:/coverage -v l8ts_zero:/var/zero \
  --env-file /tmp/l8-ts-env.txt zero-cache-rust-syncer:local

# rust side: instrumented image; %c = LLVM continuous mode (mmap-backed profraw,
# survives a non-graceful child exit — rust-syncer is a child of the node runner)
docker inspect xyne-sandbox-rust-test-zero-cache-art \
  --format '{{range .Config.Env}}{{println .}}{{end}}' \
  | grep -v '^PATH=\|^NODE_VERSION\|^YARN_VERSION\|^USER=\|^ZERO_APP_ID=' > /tmp/l8-rust-env.txt
{ echo ZERO_APP_ID=sandbox_rust_test_l8r; \
  echo 'LLVM_PROFILE_FILE=/coverage/rust-%p%c.profraw'; } >> /tmp/l8-rust-env.txt
docker run -d --name l8rust-zero-cache --network sandbox-net \
  -v /tmp/l8cov/rust:/coverage -v l8rust_zero:/var/zero \
  --env-file /tmp/l8-rust-env.txt zero-cache-rust-syncer:l8cov
```

Wait for both `/healthz` (initial replication takes ~1-2 min on the sandbox DB).

## 3. Drive IDENTICAL traffic (xyne-art diff_oracle, full catalog)

```bash
cd ~/Documents/xyne-art
RUST_IP=$(docker inspect l8rust-zero-cache --format '{{(index .NetworkSettings.Networks "sandbox-net").IPAddress}}')
TS_IP=$(docker inspect l8ts-zero-cache  --format '{{(index .NetworkSettings.Networks "sandbox-net").IPAddress}}')
python3 harness/diff_oracle.py \
  --primary ws://$RUST_IP:4848 --mirror ws://$TS_IP:4848 \
  --id-pool harness/id-pool.sandbox.json \
  --client-schema harness/client-schema.json \
  --auth-token "$JWT" --extra-param userID=$UID \
  --full-catalog --pairs 2 --duration 120 \
  --enable-mutations --i-know-this-writes --mutate-url auto
```

(Convergence verdict comes for free; L8 only needs the traffic.)

## 4. Collect coverage

```bash
docker stop -t 60 l8ts-zero-cache l8rust-zero-cache   # graceful → V8 flush
# rust: merge + export INSIDE the builder base image (LLVM must match rustc)
docker cp l8rust-zero-cache:/usr/local/bin/rust-syncer /tmp/l8cov/rust-syncer
docker run --rm -v /tmp/l8cov:/cov \
  $(grep -m1 'FROM rust:1-slim' Dockerfile | awk '{print $2}') bash -c '
    rustup component add llvm-tools >/dev/null 2>&1
    B=$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | grep host | cut -d" " -f2)/bin
    $B/llvm-profdata merge -sparse /cov/rust/*.profraw -o /cov/rust.profdata
    $B/llvm-cov export -instr-profile /cov/rust.profdata /cov/rust-syncer \
      > /cov/rust-cov.json'
```

## 5. Join + report

```bash
python3 parity/layer8_path_diff.py \
  --ts-cov /tmp/l8cov/ts --rust-cov /tmp/l8cov/rust-cov.json \
  --json parity/coverage/l8-rows.json > parity/L8-PATH-DIFF.md
```

Buckets: `TS-HOT/RUST-COLD` = divergence candidates (triage every one);
`RUST-HOT/TS-COLD` = extra rust paths; `BOTH-COLD` = traffic gap only.
`--strict` (exit 1 on any TS-HOT/RUST-COLD) once the baseline is triaged.

## 6. Cleanup (slots + schemas + containers — slot bloat is a real prod hazard)

```bash
docker rm -f l8ts-zero-cache l8rust-zero-cache
docker volume rm l8ts_zero l8rust_zero
docker exec xyne-sandbox-postgres psql -U xyne -d sandbox_rust_test_db -c "
  SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
  WHERE slot_name LIKE '%l8t%' OR slot_name LIKE '%l8r%';"
docker exec xyne-sandbox-postgres psql -U xyne -d sandbox_rust_test_db -c "
  DROP SCHEMA IF EXISTS sandbox_rust_test_l8t_0, \"sandbox_rust_test_l8t_0/cvr\",
  \"sandbox_rust_test_l8t_0/cdc\", sandbox_rust_test_l8r_0,
  \"sandbox_rust_test_l8r_0/cvr\", \"sandbox_rust_test_l8r_0/cdc\" CASCADE;"
```

## Interpretation contract

- The covered-SET is deterministic for identical traffic; COUNTS are not
  (concurrency/retries) — only ≥100× ratios are flagged.
- Path parity is bounded by the traffic driven: BOTH-COLD pairs are untested,
  not proven equal. Widen the catalog / add mutations to shrink it.
- Same path ≠ same values: within-path value divergence stays L2's job.
