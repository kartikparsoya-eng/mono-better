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

use rust_syncer::http_server::{bind_http_listener, serve_http};
use rust_syncer::router::{CGServicesFactory, ConnectionRouter};
use rust_syncer::ws_server::{WsServerConfig, bind_ws_listener, serve_ws_with_config};

/// Configuration parsed from environment variables.
pub struct SyncerConfig {
    pub ws_port: u16,
    pub http_port: u16,
    pub replica_file: String,
    pub cvr_pg_uri: String,
    pub task_id: String,
    pub shard: String,
    pub app_id: String,
    pub auth_jwk: Option<String>,
    pub auth_jwks_url: Option<String>,
    pub auth_secret: Option<String>,
    pub auth_issuer: Option<String>,
    pub auth_audience: Option<String>,
    pub mutagen_url: Option<String>,
    pub pusher_url: Option<String>,
    /// Shared secret gating the TS push endpoint (`PUSHER_AUTH_TOKEN`), attached
    /// as `x-relay-auth` on every relayed push.
    pub pusher_auth_token: Option<String>,
    /// Normalized custom-query fetch configuration supplied by the TS
    /// dispatcher. This is the server-side default used when the client does
    /// not send a `userQueryURL` override.
    pub query_config: Option<rust_syncer::FetchConfig>,
    pub max_client_groups: usize,
    pub admin_password: Option<String>,
    pub server_version: String,
    /// Max CVR Postgres connections for this worker (parity with the TS
    /// `--cvr-max-conns-per-worker` flag: whole budget divided across syncers).
    /// The whole budget is ONE shared pool on the main runtime (doc 91
    /// Iteration C); executors offload CVR I/O onto it via `SyncEngine::offload`.
    pub cvr_max_conns: u32,
    /// Number of executor threads (doc 91). Client groups are least-loaded
    /// placed across them; each runs a `current_thread` runtime + `LocalSet`
    /// and draws CVR I/O from the shared pool. Defaults to the HOST core
    /// count via the affinity mask (`host_parallelism`), deliberately
    /// ignoring any cgroup cpu quota — quota-sized shard pools serialize
    /// whole client groups (see `num_shards` default). `ZERO_SYNCER_SHARDS`
    /// overrides.
    pub num_shards: usize,
    /// Interval (ms) between periodic JWT re-validation + query re-transform for
    /// live connections (TS `--auth-revalidate-interval-seconds`, default 300s).
    /// `0` disables periodic auth maintenance.
    pub revalidate_interval_ms: Option<i64>,
    /// Shadow-mode query-covering detection during hydration. Port of TS
    /// `zeroConfig.enableQueryCovering` (default true); log-only.
    pub enable_query_covering: bool,
}

impl SyncerConfig {
    pub fn from_env() -> Self {
        Self {
            ws_port: env::var("PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8080),
            http_port: env::var("HTTP_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8081),
            replica_file: env::var("REPLICA_FILE").unwrap_or_else(|_| "replica.db".to_string()),
            cvr_pg_uri: env::var("CVR_PG_URI")
                .unwrap_or_else(|_| "postgres://localhost/zero".to_string()),
            task_id: env::var("TASK_ID").unwrap_or_else(|_| "task-0".to_string()),
            shard: env::var("SHARD").unwrap_or_else(|_| "0".to_string()),
            app_id: env::var("ZERO_APP_ID")
                .or_else(|_| env::var("APP_ID"))
                .unwrap_or_else(|_| "zero".to_string()),
            auth_jwk: env::var("AUTH_JWK").ok(),
            auth_jwks_url: env::var("AUTH_JWKS_URL").ok(),
            auth_secret: env::var("AUTH_SECRET").ok(),
            auth_issuer: env::var("AUTH_ISSUER").ok(),
            auth_audience: env::var("AUTH_AUDIENCE").ok(),
            mutagen_url: env::var("MUTAGEN_URL").ok(),
            pusher_url: env::var("PUSHER_URL").ok().filter(|s| !s.is_empty()),
            pusher_auth_token: env::var("PUSHER_AUTH_TOKEN").ok().filter(|s| !s.is_empty()),
            query_config: parse_query_config(),
            // Memory backstop, NOT a normal-operation limit. TS has no
            // per-worker client-group reject cap (its only bound is the
            // dispatcher's 100k routing-map, which just forgets an old CG→worker
            // mapping, never rejects a connection). A default of 100 produced an
            // artificial capacity cliff far below the engine's real limit — a
            // reconnect blip near saturation tripped it and stormed. Default high
            // and let overflow REHOME (see handle_connection); operators tune
            // this to their per-instance memory budget via MAX_CLIENT_GROUPS.
            max_client_groups: env::var("MAX_CLIENT_GROUPS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1000),
            admin_password: env::var("ZERO_ADMIN_PASSWORD")
                .ok()
                .filter(|s| !s.is_empty()),
            server_version: env::var("ZERO_SERVER_VERSION")
                .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string()),
            cvr_max_conns: env::var("CVR_MAX_CONNS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
            // Shards bound TAIL FAIRNESS, not throughput: each is a
            // `current_thread` executor that SERIALIZES its client groups, so
            // any CG sharing a shard eats the full latency of its neighbor's
            // hydrations (a single 12k-row hydrate + poke serialization holds
            // the thread for ~200ms). Threads beyond the CPU count are cheap —
            // idle shards are parked; busy ones get OS time-slices — so the
            // default is sized for CG-per-shard isolation at realistic
            // concurrency, NOT for the core count. Measured A/B on a
            // 4-cpu-capped container (ART G25 25-conn drive, 2026-08-19):
            // 4 shards → 41+ of 51 queries breach 2x-of-TS parity (p95 to
            // multi-second); 14 shards (2 CGs/shard on ~11 shards) → 10-17
            // violations, p95 to 1.6s; 28 shards (1 CG/shard) → 0 violations.
            // 56 shards regressed slightly (4 violations + a slow-client-shed
            // rehome): more shards also means more CONCURRENT large pokes per
            // client socket, so 2x host is the measured sweet spot — enough
            // for CG isolation at gate concurrency without burstier egress.
            //
            // NOTE `std::thread::available_parallelism` is cgroup-quota-AWARE
            // on Linux (it returns 4 in a `--cpus 4` container regardless of
            // host cores), which silently re-created the quota-sized pool this
            // default was meant to avoid. `host_parallelism()` reads the CPU
            // affinity mask instead (quota-independent — `nproc` semantics).
            // `warn_if_quota_capped` still flags the mismatch so operators can
            // tune ZERO_SYNCER_SHARDS deliberately.
            num_shards: env::var("ZERO_SYNCER_SHARDS")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .filter(|n| *n > 0)
                .unwrap_or_else(|| {
                    warn_if_quota_capped();
                    (host_parallelism() * 2).clamp(16, 64)
                }),
            // TS default: 300s. `0` (or a negative) disables it.
            revalidate_interval_ms: {
                let secs = env::var("AUTH_REVALIDATE_INTERVAL_SECONDS")
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(300);
                (secs > 0).then_some(secs * 1000)
            },
            // TS default: true. An explicit false/0 (case-insensitive) disables.
            enable_query_covering: !env::var("ENABLE_QUERY_COVERING")
                .map(|v| {
                    let v = v.trim().to_ascii_lowercase();
                    v == "false" || v == "0"
                })
                .unwrap_or(false),
        }
    }
}

/// The HOST-side logical CPU count, independent of any cgroup cpu quota.
///
/// `std::thread::available_parallelism` is quota-aware on Linux, so in a
/// `--cpus N` container it returns N — exactly the quota-shrunk number the
/// shard pool must NOT use (see `num_shards`). The sched affinity mask is
/// quota-independent (`nproc` reports it), so count that instead; fall back
/// to `available_parallelism` off-Linux or if the syscall fails.
fn host_parallelism() -> usize {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) == 0 {
            let n = libc::CPU_COUNT(&set);
            if n > 0 {
                return n as usize;
            }
        }
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

/// Log when the cgroup cpu quota is far below the host core count the shard
/// default is derived from. We deliberately do NOT auto-shrink the shard pool
/// to the quota — an A/B (ART G25) showed quota-sized `current_thread` shards
/// serialize whole client groups behind each other and destroy tail latency —
/// but a 3x+ mismatch is worth an operator's attention (ZERO_SYNCER_SHARDS).
fn warn_if_quota_capped() {
    let host = host_parallelism();
    if let Some(cores) = cgroup_cpu_quota_cores()
        && cores.saturating_mul(3) <= host
    {
        tracing::warn!(
            quota_cores = cores,
            host_cores = host,
            "cgroup cpu quota is far below the host core count; the {host}-shard \
             default may oversubscribe — consider tuning ZERO_SYNCER_SHARDS"
        );
    }
}

/// The container's cpu quota in whole cores (cgroup v2 `cpu.max`, then v1 cfs
/// quota); `None` when unlimited or undetectable.
fn cgroup_cpu_quota_cores() -> Option<usize> {
    std::fs::read_to_string("/sys/fs/cgroup/cpu.max")
        .ok()
        .and_then(|s| parse_cpu_max(&s))
        .or_else(|| {
            let quota = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
                .ok()?
                .trim()
                .parse::<f64>()
                .ok()?;
            let period = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
                .ok()?
                .trim()
                .parse::<f64>()
                .ok()?;
            (quota > 0.0 && period > 0.0).then(|| (quota / period).ceil() as usize)
        })
        .filter(|c| *c >= 1)
}

/// Parse cgroup v2 `cpu.max` ("<quota> <period>" or "max <period>") into a
/// whole-core count. Returns None for unlimited ("max") or malformed content.
fn parse_cpu_max(s: &str) -> Option<usize> {
    let mut it = s.split_whitespace();
    let quota = it.next()?;
    let period = it.next()?;
    if quota == "max" {
        return None;
    }
    let (q, p) = (quota.parse::<f64>().ok()?, period.parse::<f64>().ok()?);
    (q > 0.0 && p > 0.0).then(|| (q / p).ceil() as usize)
}

fn parse_query_config() -> Option<rust_syncer::FetchConfig> {
    let urls = env::var("QUERY_URLS_JSON")
        .ok()
        .and_then(|value| serde_json::from_str::<Vec<String>>(&value).ok())
        .filter(|urls| !urls.is_empty())?;
    let allowed_client_headers = env::var("QUERY_ALLOWED_CLIENT_HEADERS_JSON")
        .ok()
        .and_then(|value| serde_json::from_str::<Vec<String>>(&value).ok());
    let allowed_request_headers = env::var("QUERY_ALLOWED_REQUEST_HEADERS_JSON")
        .ok()
        .and_then(|value| serde_json::from_str::<Vec<String>>(&value).ok());
    Some(rust_syncer::FetchConfig {
        url: Some(urls),
        api_key: env::var("QUERY_API_KEY")
            .ok()
            .filter(|value| !value.is_empty()),
        allowed_client_headers,
        allowed_request_headers,
        forward_cookies: env::var("QUERY_FORWARD_COOKIES").as_deref() == Ok("true"),
    })
}

/// Resolve when the process receives a shutdown signal (Ctrl-C / SIGINT, or
/// SIGTERM from the ProcessManager). Used to trigger a graceful drain.
async fn shutdown_signal() {
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
        () = ctrl_c => {},
        () = term => {},
    }
}

/// dhat's heap-profiling allocator, installed only under `--features dhat-heap`.
/// It intercepts every Rust allocation so `dhat::Profiler` can attribute retained
/// blocks. Left uninstalled by default so production runs use the system allocator.
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
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

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

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
    // handle is injected into each CG's SyncEngine so CVR I/O is offloaded onto
    // this runtime (`SyncEngine::offload`; the CG executors are current_thread
    // runtimes that must not poll another reactor's connections — doc 91 §5.1).
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
    let budget = config.cvr_max_conns.max(1);
    let num_shards = config.num_shards.max(1);
    let cvr_pool = runtime.block_on(async {
        let opts = || {
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(budget)
                .acquire_timeout(std::time::Duration::from_secs(10))
        };
        match opts().connect(&config.cvr_pg_uri).await {
            Ok(pool) => pool,
            Err(e) => {
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
    let router = Arc::new(ConnectionRouter::new_sharded(
        Arc::new(RealServicesFactory {
            config: config.clone(),
            tokio_handle: runtime.handle().clone(),
            metrics: metrics.clone(),
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
    ));

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

    // Periodic glibc malloc_trim: pipeline/row memory freed on query-TTL
    // expiry and CG teardown stays in malloc's arenas (RSS never falls), which
    // the ART leak gate (G6) — and any operator watching the pod — reads as an
    // unbounded leak. Return free arena memory to the OS on a slow cadence;
    // trim walks the arenas so keep it well off the hot path. glibc-only.
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    std::thread::Builder::new()
        .name("malloc-trim".into())
        .spawn(|| {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(30));
                // SAFETY: malloc_trim is async-signal-unsafe but thread-safe;
                // calling it from a dedicated thread is the documented use.
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
                router.handle_connection(ctx).await;
            });
        });
        tokio::pin!(server);
        // Serve until the accept loop ends OR a shutdown signal arrives, then
        // drain: `router.shutdown()` fails every connection with a Rehome error
        // so clients reconnect elsewhere (TS `Syncer.drain` / view-syncer
        // `#cleanup`), and joins the CG threads.
        let result = tokio::select! {
            res = &mut server => res,
            () = shutdown_signal() => {
                tracing::info!("shutdown signal received; draining connections");
                ws_router.shutdown().await;
                Ok(())
            }
        };
        result
    });
}

/// Per-CG services factory. Builds a real `SyncEngine` config from the process
/// config (replica path, CVR Postgres, shard). Mutagen/pusher are intentionally
/// absent (mutations are HTTP-direct — see `create_mutagen`); the
/// connection-context dispatch is a light placeholder (the CG-thread path owns
/// auth state directly, see `router.rs`).
struct RealServicesFactory {
    config: Arc<SyncerConfig>,
    tokio_handle: tokio::runtime::Handle,
    metrics: Arc<rust_syncer::metrics::Metrics>,
}

impl CGServicesFactory for RealServicesFactory {
    fn create_view_syncer(&self, _cg_id: &str) -> Arc<dyn rust_syncer::ViewSyncerDispatch> {
        Arc::new(PlaceholderViewSyncer)
    }

    fn create_conn_context_manager(
        &self,
        _cg_id: &str,
    ) -> Arc<dyn rust_syncer::ConnContextManagerDispatch> {
        Arc::new(PlaceholderConnContextManager)
    }

    // The Rust syncer runs ZERO mutation logic. CRUD mutations (mutagen → PG)
    // genuinely require mutation processing and stay unsupported here (no app
    // uses them on this path); a CRUD push still hits the "legacy CRUD disabled"
    // rejection.
    fn create_mutagen(&self, _cg_id: &str) -> Option<Arc<dyn rust_syncer::MutagenDispatch>> {
        None
    }

    // Custom mutations are RELAYED, not processed. When `PUSHER_URL` is set, a
    // custom WS push is forwarded (with this connection's auth/header material)
    // to the TS push endpoint, which runs the real pusher → `userPushURL`. The
    // result flows back through the CVR's `lmids`/`mutationResults` queries this
    // syncer already hydrates and pokes — so the relay is one-directional and
    // adds no mutation logic here. With `PUSHER_URL` unset, a custom push hits
    // the read-only rejection (the prior behavior).
    fn create_pusher(&self, _cg_id: &str) -> Option<Arc<dyn rust_syncer::PusherDispatch>> {
        let url = self.config.pusher_url.clone()?;
        Some(Arc::new(rust_syncer::HttpRelayPusher::new(
            url,
            self.config.pusher_auth_token.clone(),
            self.tokio_handle.clone(),
        )))
    }

    fn create_sync_engine_config(&self, cg_id: &str) -> rust_syncer::SyncEngineConfig {
        let shard_num = self.config.shard.parse::<u32>().unwrap_or(0);
        let mut initialization_errors = Vec::new();
        let replica_version =
            match rust_syncer::read_replica_versions_from_path(&self.config.replica_file) {
                Ok(versions) => versions.replica_version,
                Err(error) => {
                    initialization_errors.push(format!(
                        "failed to read replica versions from {}: {error}",
                        self.config.replica_file
                    ));
                    String::new()
                }
            };
        // Read the syncable table specs + read-permissions from the replica.
        let tables = match rust_syncer::compute_table_specs_from_path(&self.config.replica_file) {
            Ok(tables) => tables,
            Err(error) => {
                initialization_errors.push(format!(
                    "failed to read replica table specs from {}: {error}",
                    self.config.replica_file
                ));
                Vec::new()
            }
        };
        tracing::info!(
            "CG {cg_id}: loaded {} table specs from replica",
            tables.len()
        );
        let load_result: Result<Option<serde_json::Value>, String> =
            rusqlite::Connection::open(&self.config.replica_file)
                .map_err(|e| e.to_string())
                .and_then(|conn| rust_syncer::load_permissions(&conn, &self.config.app_id))
                .map(|loaded| loaded.permissions);
        match &load_result {
            Ok(Some(_)) => tracing::info!("CG {cg_id}: loaded read-permissions from replica"),
            Ok(None) => {
                tracing::warn!("CG {cg_id}: no read-permissions deployed — queries pass through")
            }
            // Fail CLOSED: an existing-but-unloadable permissions doc must not
            // silently disable authorization. `resolve_permissions` substitutes a
            // deny-all config so no unauthorized row is served.
            Err(e) => tracing::error!(
                "CG {cg_id}: failed to load permissions ({e}); denying all client queries (fail-closed)"
            ),
        }
        // Re-read the hash separately for hot-reload detection. It shares the
        // load path but we keep it even when `resolve_permissions` substitutes
        // deny-all on error: a later successful read with a real hash then
        // differs from this seed and triggers a self-healing reload.
        let permissions_hash: Option<String> =
            rusqlite::Connection::open(&self.config.replica_file)
                .ok()
                .and_then(|conn| rust_syncer::load_permissions(&conn, &self.config.app_id).ok())
                .and_then(|loaded| loaded.hash);
        let permissions = rust_syncer::resolve_permissions(load_result);
        let app_id = self.config.app_id.clone();
        rust_syncer::SyncEngineConfig {
            initialization_error: (!initialization_errors.is_empty())
                .then(|| initialization_errors.join("; ")),
            tables,
            replica_path: Some(self.config.replica_file.clone()),
            app_id: app_id.clone(),
            replica_version,
            shard: rust_cvr::types::ShardID {
                app_id: app_id.clone(),
                shard_num,
            },
            cvr_pg: Some(rust_syncer::CvrPgConfig {
                // Identity only; the pool is supplied by the hosting executor
                // (doc 91, §5.1). Every CG on a given executor draws from that
                // executor's bounded pool.
                schema: format!("{}_{}/cvr", app_id, self.config.shard),
                cvr_id: cg_id.to_string(),
                task_id: self.config.task_id.clone(),
            }),
            permissions,
            permissions_hash,
            revalidate_interval_ms: self.config.revalidate_interval_ms,
            query_config: self.config.query_config.clone(),
            enable_query_covering: self.config.enable_query_covering,
            tokio_handle: self.tokio_handle.clone(),
            admin_password: self.config.admin_password.clone(),
            server_version: self.config.server_version.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

struct PlaceholderViewSyncer;

impl rust_syncer::ViewSyncerDispatch for PlaceholderViewSyncer {
    fn change_desired_queries(&self, _selector: &rust_syncer::ConnectionSelector, _msg: &str) {}
    fn update_auth(&self, _selector: &rust_syncer::ConnectionSelector, _msg: &str, _changed: bool) {
    }
    fn delete_clients(
        &self,
        _selector: &rust_syncer::ConnectionSelector,
        _msg: &str,
    ) -> Vec<String> {
        Vec::new()
    }
    fn init_connection(&self, _selector: &rust_syncer::ConnectionSelector, _msg: &str) -> bool {
        true
    }
    fn inspect(&self, _selector: &rust_syncer::ConnectionSelector, _msg: &str) {}
}

struct PlaceholderConnContextManager;

impl rust_syncer::ConnContextManagerDispatch for PlaceholderConnContextManager {
    fn must_get_connection_context(
        &self,
        _selector: &rust_syncer::ConnectionSelector,
    ) -> rust_syncer::ConnContextInfo {
        rust_syncer::ConnContextInfo {
            auth: None,
            revision: 0,
        }
    }

    fn init_connection(
        &self,
        _selector: &rust_syncer::ConnectionSelector,
        _body: &serde_json::Value,
    ) {
    }

    fn update_auth(
        &self,
        _selector: &rust_syncer::ConnectionSelector,
        _body: &serde_json::Value,
    ) -> bool {
        true
    }
}

#[cfg(test)]
mod cpu_quota_tests {
    use super::parse_cpu_max;

    /// cgroup v2 cpu.max parsing: quota/period rounds UP to whole cores,
    /// "max" (unlimited) and malformed content defer to available_parallelism.
    #[test]
    fn parse_cpu_max_quota_shapes() {
        assert_eq!(parse_cpu_max("400000 100000\n"), Some(4));
        assert_eq!(parse_cpu_max("150000 100000"), Some(2)); // 1.5 cpus -> 2
        assert_eq!(parse_cpu_max("50000 100000"), Some(1)); // 0.5 cpus -> 1
        assert_eq!(parse_cpu_max("max 100000"), None);
        assert_eq!(parse_cpu_max(""), None);
        assert_eq!(parse_cpu_max("garbage here"), None);
    }
}
