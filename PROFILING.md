# Profiling & correctness toolkit — the 11 axes, wired for THIS repo

One entry per axis: the mechanism, what is wired in this repo, and the exact
command. Everything works on macOS and Linux (docker where the tool is
linux-native). "Deploy-time" means the tool is baked into / reachable on a
running image for initial-testing environments.

| # | Axis | Local | Deploy-time |
|---|------|-------|-------------|
| 1 | Coverage | `parity/repo-coverage.sh` | coverage image + `xyne-art tools/coverage-report.sh` |
| 2 | CPU | `samply` | `/debug/pprof/flamegraph` (feature `profiling`) |
| 3 | Heap alloc | `dhat-heap` feature | same (graceful-shutdown dump) + `/census` |
| 4 | Leaks | `parity/sanitize.sh` (LSan) | `/census` live-object counters |
| 5 | Sanitizers | `parity/sanitize.sh [asan\|tsan]` | — (never prod) |
| 6 | Tracing | `RUST_LOG` spans (`tracing`) | OTLP metrics → sandbox collector |
| 7 | Fuzzing | `cargo +nightly fuzz` + IVM differential loops | — |
| 8 | Microbench | `cargo bench` (criterion) | art-baseline.json (prod-level twin) |
| 9 | I/O / syscalls | recipes below | `/statz`, pool/queue gauges |
| 10 | Wire | `tshark` recipe + diff-surface G35/G36 | encryption-proxy mirror |
| 11 | APM | sandbox otel stack | `/metrics`, `/statz`, `/readyz`, `/healthz`, G41 gate |

## 1 · Coverage — "did tests execute every branch?"

- **In-repo (fast loop):** `TEST_CVR_PG_URI=... parity/repo-coverage.sh` →
  `parity/coverage/<crate>/{summary.txt,uncovered-functions.txt,lcov.info}`.
  Per-crate env quirks are encoded in the script — wrong env makes suites
  silently self-skip and read as uncovered.
- **End-to-end (what does the LIVE system trigger):**
  `docker build --build-arg RUST_SYNCER_COVERAGE=1 -t zero-cache-rust-syncer:coverage .`
  → deploy with the sandbox `docker-compose.coverage.yml` overlay → drive with
  the correctness suites → `xyne-art/tools/coverage-report.sh`.

## 2 · CPU — "what's consuming CPU?"

- **Local (any binary/test):** `samply record cargo test -p rust-ivm --test <t>`
  or `samply record target/release/rust-syncer ...` → Firefox Profiler UI.
  Works mac + linux, no root.
- **Deploy-time (the rust syncer in a container):** build with
  `RUST_SYNCER_FEATURES=profiling`, then
  `curl 'http://<status-port>/debug/pprof/flamegraph?seconds=30' > prof.svg`.
  In-process sampling (pprof-rs, 99Hz), only active during the request —
  safe to leave enabled in initial-testing deploys. This is the rust analog
  of the Node inspector endpoints the ART harness probes (task B1).
- Node side: `node --cpu-prof` / `clinic doctor` on the dispatcher workers.

## 3 · Heap allocation — "what allocates?"

- `--features dhat-heap` (rust-syncer): dhat global allocator, dumps
  `dhat-heap.json` on graceful shutdown → view at
  https://nnethercote.github.io/dh_view/dh_view.html. Image variant:
  `--build-arg RUST_SYNCER_FEATURES=dhat-heap` (leak hunts only — every
  allocation is intercepted; never ship).
- `GET /census` — live-object counters across all three crates (the G6
  leak-hunt instrument); poll during load to see which counter climbs.
- `GET /heapz` — TS-compat heap snapshot surface.

## 4 · Leaks — "is memory freed?"

- **LSan (byte-exact):** `parity/sanitize.sh` — ASan runs include LeakSanitizer;
  Rc/RefCell cycles (the class behind the G6 engine leak) and `Box::leak`
  report with allocation stacks at process exit.
- **Statistical / long-run:** dhat (above) + the ART G6 RSS-slope gate with
  `--soak`.

## 5 · Sanitizers — "memory/concurrency UB?"

- `parity/sanitize.sh` → ASan+LSan across all three crates' lib tests.
- `parity/sanitize.sh tsan` → ThreadSanitizer on rust-syncer (the crate with
  real cross-thread concurrency; 5-15× slowdown). Dockerized rust:nightly, so
  identical on mac/linux hosts. CI-able; never production.

## 6 · Tracing — "what happened, in order?"

- In-process: the `tracing` crate everywhere; `RUST_LOG=rust_syncer=debug`
  (or per-target filters) for span-level logs. `trace.rs` `note()` markers on
  conn-open/close paths.
- Cross-service: OTLP **metrics** export is wired (`otel.rs`, enabled by
  `OTEL_EXPORTER_OTLP_ENDPOINT`; the sandbox collector at `otel-collector:4318`
  → prometheus :9464; diffed by gate G41). Span/trace OTLP export is NOT yet
  wired on the rust side — Node workers export traces already. Open item.

## 7 · Fuzzing — "what inputs break the parsers?"

- **Coverage-guided (crash/UB hunting):** `packages/rust-syncer/fuzz/` —
  `cargo install cargo-fuzz`, then from `packages/rust-syncer`:
  `cargo +nightly fuzz run parse_upstream` (ws message parser — the G36
  surface), `connect_params` (client-controlled URL), `version_string`
  (client-supplied cookie; a panic there would kill a whole CG task).
- **Differential (parity hunting):** `rust-ivm/agentic/fuzz/` — evolves query
  inputs against the live TS oracle; a different objective, keep both.

## 8 · Microbenchmarks — "is the port 1:1 in performance?"

- `cd packages/rust-cvr && cargo bench` — criterion; compare runs with
  `cargo bench -- --save-baseline before` / `--baseline before`.
  Exemplar: `benches/version_bench.rs` (version-string codec). Add a bench
  beside any hot unit you touch.
- Prod-level twin: the ART latency gates (G5/G25/G42) against
  `art-baseline.json`.

## 9 · I/O & syscalls — "why is it waiting?"

- macOS: `sudo dtruss -c -p <pid>` (syscall counts); `sample <pid> 10`.
- Linux/container: `strace -c -f -p <pid>`;
  `docker run --pid=container:<name> ... strace`. eBPF (`bpftrace`,
  `biolatency`, `tcplife`) on linux hosts.
- Async stalls: the pool/queue gauges on `/metrics`
  (`zero_sync_*`, pool size/idle, ws queued frames/bytes) localize waits
  without a tracer. (`tokio-console` would need `tokio_unstable` — not wired.)
- Case history: the B-POOL acquire convoy and the TCP_NODELAY +50ms were both
  found with exactly this axis's methods (pool gauges; constant-delta timing).

## 10 · Wire — "what's on the wire?"

- `tshark -i lo0 -f 'tcp port 4850' -Y websocket -T fields -e websocket.payload.text`
  against the sandbox's direct rust port; TS twin on :4849.
- Protocol-level differential is automated: diff-surface **G35** (poke
  streams), **G36** (error frames + close codes), G42 (timing) — usually
  faster than raw capture.
- TLS paths: `mitmproxy`; the encryption-proxy has its own E2E suite.

## 11 · Production observability

- Rust status port: `/metrics` (prometheus), `/statz` (admin-gated),
  `/readyz` (PG+replica readiness), `/healthz` (liveness), `/census`.
- Sandbox APM: otel-collector → prometheus (:9464); instrument-set parity is
  ITSELF gated (G41), so missing telemetry is caught per run, not by audit.
- Serving-lag: `zero.sync.e2e_serving_lag` exported by the syncer.

## Env-quirk cheat sheet (things that silently break measurements)

- rust-syncer tests: `--no-default-features` + `SQLITE3_*` UNSET.
- rust-cvr PG suites: need `TEST_CVR_PG_URI` (else self-skip → false gaps).
- rust-ivm full suite: wal2 static env + `--test-threads=1`.
- Never run CPU-heavy tooling while an ART gate is measuring latency — it
  contaminated a G22/G25 run once already. Same for laptop sleep: wrap long
  runs in `caffeinate -dims` and keep the lid open.
