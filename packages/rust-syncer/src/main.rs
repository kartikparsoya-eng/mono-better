//! rust-syncer — full Rust syncer binary entry point.
//!
//! Replaces the TS syncer worker process. Launched by `main.ts` when
//! `ZERO_SYNCER=rust` is set.
//!
//! Configuration is via environment variables:
//! - `PORT` — WebSocket listen port
//! - `HTTP_PORT` — HTTP server port (for /statz, /heapz, /notify)
//! - `REPLICA_FILE` — SQLite replica file path
//! - `CVR_PG_URI` — CVR Postgres connection string
//! - `TASK_ID` — Task ID
//! - `SHARD` — Shard ID
//! - `AUTH_JWK` — JWT JWK (optional)
//! - `AUTH_JWKS_URL` — JWT JWKS URL (optional)
//! - `AUTH_SECRET` — JWT secret (optional)
//! - `MUTAGEN_URL` — Mutagen service URL (optional)
//! - `PUSHER_URL` — Pusher service URL (optional)
//! - `MAX_CLIENT_GROUPS` — Client-group memory backstop (default 1000)
//! - `ZERO_SLOW_HYDRATE_THRESHOLD_MS` — Slow-query warn threshold (default 1000)
//! - `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_METRICS_EXPORTER` — enable OTLP metrics
//!   push (standard OpenTelemetry env; mirrors the TS syncer)
//! - `ZERO_APP_ID` / `APP_ID` — Application id (schema prefix); default `zero`
//! - `ZERO_ADMIN_PASSWORD` — Inspector protocol admin password (optional)
//! - `ZERO_SERVER_VERSION` — Server version for the inspector `version` op
//!   (defaults to the crate version)

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use rust_syncer::Syncer;
use rust_syncer::config::zero_config::SyncerConfig;
use rust_syncer::http_server::{bind_http_listener, serve_http};
use rust_syncer::server::syncer::RealServicesFactory;
use rust_syncer::ws_server::{WsServerConfig, bind_ws_listener, serve_ws_with_config};

/// Which shutdown signal arrived — they drain differently: SIGTERM (deploys,
/// sent by the zero-cache ProcessManager) gets the staggered rehome; SIGINT
/// (dev ctrl-C) stays an immediate shutdown.
enum ShutdownSignal {
    Interrupt,
    Terminate,
}

/// Resolve when the process receives a shutdown signal (Ctrl-C / SIGINT, or
/// SIGTERM from the ProcessManager). Used to trigger a graceful drain.
async fn shutdown_signal() -> ShutdownSignal {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => ShutdownSignal::Interrupt,
        () = term => ShutdownSignal::Terminate,
    }
}

/// dhat's heap-profiling allocator, installed only under `--features dhat-heap`.
/// It intercepts every Rust allocation so `dhat::Profiler` can attribute retained
/// blocks. Left uninstalled by default so production runs use the system allocator.
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    // INVENTIONS.md I-13: SQLite must allocate through mimalloc too, and the
    // hook is only accepted before the first sqlite3_initialize — so it is the
    // first statement of the process (before any Connection::open below).
    #[cfg(not(feature = "dhat-heap"))]
    if let Err(rc) = rust_syncer::alloc::route_sqlite_malloc_through_mimalloc() {
        eprintln!("[alloc] SQLITE_CONFIG_MALLOC rejected (rc={rc}); SQLite stays on glibc malloc");
    }
    // Start the heap profiler first so it observes allocations for the whole
    // process lifetime. The guard lives until `main` returns; on graceful
    // shutdown (ctrl_c / SIGTERM handled below) it drops and writes the profile
    // (view at https://nnethercote.github.io/dh_view/dh_view.html). A SIGKILL
    // skips the dump — drain the syncer gracefully (and allow a generous
    // stop-grace period, the dump can take several seconds) to get a profile.
    // Output path is `ZERO_DHAT_OUT` (default `dhat-heap.json` in CWD); point it
    // at a mounted volume so it survives container teardown.
    #[cfg(feature = "dhat-heap")]
    let _dhat_profiler = {
        // Output path precedence: ZERO_DHAT_OUT, else next to the replica file
        // (that dir is the mounted /var/zero volume in the container, so the
        // dump survives teardown and is trivially extractable), else dhat's
        // default (dhat-heap.json in CWD).
        let out = env::var("ZERO_DHAT_OUT")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| {
                env::var("REPLICA_FILE")
                    .ok()
                    .filter(|s| !s.is_empty())
                    .map(|rf| {
                        std::path::Path::new(&rf)
                            .parent()
                            .map(|d| d.join("dhat-heap.json"))
                            .unwrap_or_else(|| std::path::PathBuf::from("dhat-heap.json"))
                            .to_string_lossy()
                            .into_owned()
                    })
            });
        let mut builder = dhat::Profiler::builder();
        if let Some(path) = out {
            builder = builder.file_name(path);
        }
        builder.build()
    };

    // Initialize tracing. Filter precedence: RUST_LOG (rust-native, full
    // targeting syntax) else ZERO_LOG_LEVEL (the zero-cache config's level,
    // forwarded by rust-syncer-bridge) else info. ZERO_LOG_FORMAT=json emits
    // one JSON object per line — REQUIRED in deployments whose log pipeline
    // parses the container stream as JSON (the parent zero-cache forwards
    // this binary's stdout verbatim; a plaintext tracing line there is
    // unparseable and drops the very error lines operators alert on). ANSI
    // is always off: stdout is a pipe to the parent, never a tty.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| {
            env::var("ZERO_LOG_LEVEL").map(|l| tracing_subscriber::EnvFilter::new(l.trim()))
        })
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let json_logs =
        env::var("ZERO_LOG_FORMAT").is_ok_and(|f| f.trim().eq_ignore_ascii_case("json"));
    if json_logs {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_ansi(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .init();
    }

    #[cfg(feature = "dhat-heap")]
    tracing::warn!("dhat-heap profiling active: dhat-heap.json written on graceful shutdown");

    let config = Arc::new(SyncerConfig::from_env());

    tracing::info!(
        "Starting rust-syncer: ws_port={}, http_port={}, shard={}, task_id={}",
        config.ws_port,
        config.http_port,
        config.shard,
        config.task_id
    );

    // Create the tokio runtime first — it owns the shared CVR pool, and its
    // handle is injected into each CG's ViewSyncerService so CVR I/O is offloaded
    // onto this runtime (`ViewSyncerService::offload`; the CG executors are
    // current_thread runtimes that must not poll another reactor's connections —
    // doc 91 §5.1).
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Install OTLP metrics export (TS parity — `server/otel-start.ts` pushes
    // OTLP to a collector) BEFORE creating any instruments, so `Metrics` binds to
    // the SDK meter provider. Enter the runtime so the tonic exporter can build
    // its channel. `_otel_provider` is held for the process lifetime; dropping it
    // on a clean exit flushes a final batch. No-op unless an OTEL_* endpoint/
    // exporter is configured.
    let _otel_provider = {
        let _enter = runtime.enter();
        rust_syncer::otel::init_metrics(
            &std::env::var("ZERO_SERVER_VERSION").unwrap_or_else(|_| "unknown".to_string()),
        )
    };

    // Shared process metrics — the same Arc is handed to the router (read by
    // `/statz`) and to every CG's SyncEngineConfig (written on the hot path).
    let metrics = Arc::new(rust_syncer::metrics::Metrics::default());

    // Build the ONE shared CVR Postgres pool for the whole process, on THIS main
    // multi-thread runtime (doc 91). The `K` executors host client groups on
    // their own single-threaded runtimes but offload every CVR I/O future back
    // onto this runtime via `SyncEngine::offload` — so the pool's connections are
    // always polled by the reactor that created them (avoiding the §5.1
    // cross-runtime starvation) AND the whole `cvr_max_conns` budget is a single
    // shared pool rather than fragmented per executor. That de-fragmentation is
    // what lets any of the (up to `cvr_max_conns`) connections serve any group,
    // matching TS's one-`cvrDB`-pool-per-worker behavior. `acquire_timeout`
    // bounds a stalled acquire; a connection is warmed so the first
    // `initConnection` doesn't pay connect latency. Since I/O no longer runs on
    // the executor runtimes, the executor count is NOT capped by the budget —
    // `K ≈ cores` compute lanes draw from the one shared pool.
    // Boot guard — port of TS main.ts "Insufficient cvr connections" throw:
    // TS fails boot when `cvr.maxConns < numSyncers` (the rust path runs ONE
    // syncer with the whole budget, main.ts, so the strict-parity bound is 1).
    if config.cvr_max_conns < 1 {
        panic!(
            "Insufficient cvr connections ({}) for 1 syncer",
            config.cvr_max_conns
        );
    }
    let budget = config.cvr_max_conns.max(1);
    let num_shards = config.num_shards.max(1);
    // Rust-only observability (no TS twin: TS has one pool per worker, not a
    // shared-pool-vs-shards ratio): a budget far below the executor fan-out
    // guarantees acquire convoys at full load — surface the ratio at boot.
    if budget < num_shards as u32 {
        tracing::warn!(
            "CVR pool budget ({budget}) is below the executor shard count \
             ({num_shards}); cold-start convoys will queue on the pool \
             (tune CVR_MAX_CONNS / ZERO_SYNCER_SHARDS)"
        );
    }
    // Acquire timeout: TS's postgres.js has NO acquire timeout — contention
    // QUEUES (unboundedly) and degrades to latency, never to an error. The
    // previous 10s timeout turned a cold-start convoy into `PoolTimedOut` →
    // fail_group → clients reconnect + cold-rehydrate → MORE pool demand — a
    // self-amplifying storm (ART G25: 548 pool timeouts, 314 CG kills). A
    // large-but-finite bound rides out convoys like TS while keeping wedge
    // safety TS lacks. Env-overridable for load experiments.
    let acquire_timeout_s: u64 = env::var("CVR_ACQUIRE_TIMEOUT_SECONDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    let cvr_pool = runtime.block_on(async {
        let opts = || {
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(budget)
                .acquire_timeout(std::time::Duration::from_secs(acquire_timeout_s))
        };
        match opts().connect(&config.cvr_pg_uri).await {
            Ok(pool) => pool,
            Err(e) => {
                // Best-effort eager connect, then fall back to a lazy pool and
                // report ready anyway — DELIBERATE TS parity, do NOT "harden"
                // this into a hard readiness gate. TS's sync worker fires
                // `['ready',{ready:true}]` after a best-effort `warmupConnections`
                // wrapped in `Promise.allSettled` (syncer.ts), which TOLERATES a
                // CVR-down warmup failure — so TS also comes up ready with the CVR
                // Postgres unreachable and connects lazily on first client.
                // Gating our stdout ready signal on a CVR `SELECT 1` would make
                // this syncer stricter than TS (the orchestrator would restart-
                // loop where TS would serve /readyz=503 until PG returns). `/readyz`
                // still reports the true CVR+replica health for the LB to consult.
                tracing::error!("CVR pool eager connect failed ({e}); using lazy pool");
                opts()
                    .connect_lazy(&config.cvr_pg_uri)
                    .expect("build lazy CVR pool")
            }
        }
    });
    tracing::info!(
        "CVR pool: 1 shared pool × {budget} conns, {num_shards} executor(s) offloading I/O onto it",
    );
    // Pool size/idle gauges — the pool is the prime capacity-cliff suspect;
    // without these an acquire convoy is invisible until it becomes
    // 10s-timeout fail_groups.
    rust_syncer::metrics::register_cvr_pool_gauges(cvr_pool.clone());

    // Create the connection router with the real per-CG services factory and a
    // real JWT auth validator (secret/jwk/jwksUrl from config). Spawns the
    // `num_shards` executor threads; each receives a clone of the shared pool.
    // One registry shared between the router (populates it as connections are
    // admitted) and the services factory (hands it to each CG's push relay so a
    // drainer POST failure can be surfaced to the originating socket).
    let connection_sinks = rust_syncer::ConnectionSinks::new();
    let router = Arc::new(Syncer::new_sharded(
        Arc::new(RealServicesFactory {
            config: config.clone(),
            tokio_handle: runtime.handle().clone(),
            metrics: metrics.clone(),
            connection_sinks: connection_sinks.clone(),
        }),
        Arc::new(rust_syncer::JwtAuthValidator {
            jwk: config.auth_jwk.clone(),
            secret: config.auth_secret.clone(),
            jwks_url: config.auth_jwks_url.clone(),
            issuer: config.auth_issuer.clone(),
            audience: config.auth_audience.clone(),
        }),
        metrics.clone(),
        config.max_client_groups,
        num_shards,
        Some(cvr_pool.clone()),
        connection_sinks,
        // Server shard identity for the `connected` message body, emitted on the
        // accept task (router `handle_connection`).
        rust_cvr::shards::ShardID {
            app_id: config.app_id.clone(),
            shard_num: config.shard.parse::<u32>().unwrap_or(0),
        },
    ));

    // Cross-CG serving-lag observability (TS `Syncer` serving-lag gauges + the
    // 60s `#viewSyncerLag` sampler). Register the observable gauges, then spawn a
    // 60s sampler that records `view_syncer_lag` histogram observations for every
    // eligible CG's earliest-unserved change.
    let serving_lag_registry = router.serving_lag_registry();
    rust_syncer::metrics::register_serving_lag_gauges(serving_lag_registry.clone());
    runtime.spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
            rust_syncer::workers::syncer::VIEW_SYNCER_LAG_SAMPLE_INTERVAL_MS,
        ));
        // Skip the immediate first tick so the first sample is a real 60s later.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let dist = serving_lag_registry.compute_serving_lag_distribution(now);
            for lag_ms in dist.lags_ms {
                rust_syncer::metrics::record_view_syncer_lag_ms(lag_ms as f64);
            }
        }
    });

    // Bind BOTH listeners eagerly so the process is genuinely accepting on its
    // WS + HTTP ports before we announce readiness. The TS dispatcher reverse-
    // proxies client upgrades to the WS port and POSTs commit notifications to
    // the HTTP port, and `ProcessManager.allWorkersReady()` gates the dispatcher
    // on the ready signal — so readiness MUST come after the binds, not (as
    // before) after the accept loop exits at shutdown.
    let http_router = router.clone();
    let http_addr: SocketAddr = format!("0.0.0.0:{}", config.http_port).parse().unwrap();
    let ws_router = router.clone();
    // TS parity: `--websocket-max-payload-bytes` defaults to 10MB
    // (zero-config.ts websocketMaxPayloadBytes); oversized messages must be
    // rejected identically on both syncers. The env override is the same one
    // the TS config layer reads, so one knob configures both.
    let max_payload_bytes = std::env::var("ZERO_WEBSOCKET_MAX_PAYLOAD_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(10 * 1024 * 1024);
    let ws_config = WsServerConfig {
        port: config.ws_port,
        max_payload_bytes,
        compression: false,
    };

    let (http_listener, ws_listener) = runtime.block_on(async {
        let http_listener = bind_http_listener(http_addr).await;
        let ws_listener = bind_ws_listener(ws_config.port)
            .await
            .expect("bind WebSocket port");
        (http_listener, ws_listener)
    });

    // Serve HTTP in the background now that its listener is bound. The pool +
    // replica path power /readyz (a lazy, never-connected pool fails the PG
    // probe there — the stdout "ready" handshake alone can lie about PG).
    let readyz_pool = cvr_pool.clone();
    let readyz_replica = Some(config.replica_file.clone());
    runtime.spawn(async move {
        serve_http(
            http_listener,
            http_router,
            Some(readyz_pool),
            readyz_replica,
        )
        .await;
    });

    // Periodic allocator trim: pipeline/row memory freed on query-TTL expiry
    // and CG teardown stays in the allocator's free lists (RSS never falls),
    // which the ART leak gate (G6) — and any operator watching the pod — reads
    // as an unbounded leak. Return free memory to the OS on a slow cadence,
    // well off the hot path. Rust and SQLite allocations both live in mimalloc
    // (INVENTIONS.md I-13, `alloc.rs`; `mi_collect(true)` releases its retained
    // segments); `malloc_trim` still covers whatever else in the process uses
    // glibc malloc (glibc-only).
    std::thread::Builder::new()
        .name("malloc-trim".into())
        .spawn(|| {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(30));
                #[cfg(not(feature = "dhat-heap"))]
                // SAFETY: mi_collect is thread-safe; `force` also frees the
                // segments mimalloc would otherwise retain for reuse.
                unsafe {
                    libmimalloc_sys::mi_collect(true);
                }
                // SAFETY: malloc_trim is async-signal-unsafe but thread-safe;
                // calling it from a dedicated thread is the documented use.
                #[cfg(all(target_os = "linux", target_env = "gnu"))]
                unsafe {
                    libc::malloc_trim(0);
                }
            }
        })
        .expect("spawn malloc-trim thread");

    // Both ports are bound → announce readiness to the parent ProcessManager.
    // Format matches TS: JSON array ["ready", {"ready": true}]. Flush stdout
    // (it is piped, so line-buffered) so the parent sees it immediately.
    println!("[\"ready\", {{\"ready\": true}}]");
    use std::io::Write as _;
    let _ = std::io::stdout().flush();

    let _ = runtime.block_on(async move {
        let ws_router2 = ws_router.clone();
        let server = serve_ws_with_config(ws_listener, ws_config, move |ctx| {
            // Route the connection to the appropriate CG thread
            let router = ws_router2.clone();
            tokio::spawn(async move {
                router.create_connection(ctx).await;
            });
        });
        tokio::pin!(server);
        // Serve until the accept loop ends OR a shutdown signal arrives.
        // SIGTERM (a deploy: the ProcessManager signals and waits for exit)
        // takes the staggered drain — one client group Rehomed per drain
        // interval (TS `Syncer.drain`) — so the receiving servers absorb the
        // reconnects gradually. SIGINT keeps dev ctrl-C fast: an immediate
        // `router.shutdown()` fails every connection with a Rehome error and
        // joins the CG threads. A second signal (either kind) DURING the
        // SIGTERM drain expedites to an immediate shutdown.
        let result = tokio::select! {
            res = &mut server => res,
            sig = shutdown_signal() => {
                match sig {
                    ShutdownSignal::Terminate => {
                        tracing::info!("SIGTERM received; starting staggered drain");
                        // Run the drain as a task instead of awaiting it in
                        // this select! arm: awaiting here would stop polling
                        // `&mut server` for the whole drain (up to ~25s +
                        // final sweep), so in-flight WS handshakes would hang
                        // in the accept queue instead of promptly receiving
                        // the router's shutting-down rejection. Keep the
                        // accept loop polled while the drain runs.
                        let drain_router = ws_router.clone();
                        let mut drain = tokio::spawn(async move {
                            drain_router.drain().await;
                        });
                        tokio::select! {
                            // Drain finished (it ends with a full
                            // `shutdown()` sweep + executor join) — the
                            // normal SIGTERM exit.
                            _ = &mut drain => {}
                            // The accept loop ended on its own mid-drain
                            // (listener error): nothing left to accept, but
                            // let the drain finish so CG teardown + CVR pool
                            // close stay graceful.
                            _ = &mut server => {
                                let _ = drain.await;
                            }
                            // A second signal (SIGINT or SIGTERM) during the
                            // drain expedites shutdown: stop the staggered
                            // pacing and Rehome everything at once. The abort
                            // is awaited (reaped) BEFORE calling shutdown(),
                            // so drain/shutdown never truly run concurrently;
                            // a cancelled drain leaves only idempotent state
                            // behind (CG handles are removed atomically from
                            // the DashMap, executor Shutdown sends are
                            // ignorable re-sends, join handles are take()n at
                            // most once). See patch README for the one
                            // residual hazard (abort landing inside drain's
                            // own final shutdown() sweep).
                            _ = shutdown_signal() => {
                                tracing::info!(
                                    "second signal during drain; shutting down immediately"
                                );
                                drain.abort();
                                let _ = drain.await;
                                ws_router.shutdown().await;
                            }
                        }
                        Ok(())
                    }
                    ShutdownSignal::Interrupt => {
                        tracing::info!("SIGINT received; shutting down immediately");
                        ws_router.shutdown().await;
                        Ok(())
                    }
                }
            }
        };
        result
    });
}
