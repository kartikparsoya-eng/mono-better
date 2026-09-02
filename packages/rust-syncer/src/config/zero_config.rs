//! Port of `zero-cache/src/config/zero-config.ts` — the syncer worker's
//! env-derived configuration (L9 Stage 5c move out of `main.rs`). Rust reads
//! the same ZERO_* environment the TS zero-config parser normalizes; option
//! names mirror the TS config fields.

use std::env;

// ── Rust-only runtime-sizing helpers (no TS twin: node sizes its own
// worker pool; these feed the same worker-count decisions TS's config
// normalization makes). Moved with `SyncerConfig` (their only consumer
// besides the bin's runtime builder).
/// The HOST-side logical CPU count, independent of any cgroup cpu quota.
///
/// `std::thread::available_parallelism` is quota-aware on Linux, so in a
/// `--cpus N` container it returns N — exactly the quota-shrunk number the
/// shard pool must NOT use (see `num_shards`). The sched affinity mask is
/// quota-independent (`nproc` reports it), so count that instead; fall back
/// to `available_parallelism` off-Linux or if the syscall fails.
pub fn host_parallelism() -> usize {
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
pub fn warn_if_quota_capped() {
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
pub(crate) fn parse_cpu_max(s: &str) -> Option<usize> {
    let mut it = s.split_whitespace();
    let quota = it.next()?;
    let period = it.next()?;
    if quota == "max" {
        return None;
    }
    let (q, p) = (quota.parse::<f64>().ok()?, period.parse::<f64>().ok()?);
    (q > 0.0 && p > 0.0).then(|| (q / p).ceil() as usize)
}

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
    pub query_config: Option<crate::FetchConfig>,
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
    /// Cost-based query-flip planning during hydration. Port of TS
    /// `zeroConfig.enableQueryPlanner` (zero-config.ts:510, default true) —
    /// "You can disable the planner if it is picking bad strategies."
    pub enable_query_planner: bool,
    /// Enable the per-hydrate VENDED row-count diagnostic. Port of TS
    /// `queryHydrationStats` (zero-config.ts:1213 sets
    /// `runtimeDebugFlags.trackRowCountsVended = true`). Off by default (prod);
    /// [`apply_runtime_debug_flags`](SyncerConfig::apply_runtime_debug_flags)
    /// flips the process-global flag the VENDED log gate reads.
    pub query_hydration_stats: bool,
    /// The maximum amount of time in milliseconds that a sync worker will
    /// spend in IVM (processing query hydration and advancement) before
    /// yielding to the event loop. Lower values increase responsiveness and
    /// fairness at the cost of reduced throughput. Port of TS
    /// `yieldThresholdMs` (zero-config.ts:534, default 10). Env
    /// `ZERO_YIELD_THRESHOLD_MS`. `server/syncer.rs` derives the two
    /// per-driver thresholds from it (syncer.ts:209-213).
    pub yield_threshold_ms: f64,
}

impl SyncerConfig {
    pub fn from_env() -> Self {
        let config = Self {
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
            task_id: env::var("TASK_ID")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| {
                    // TS asserts TASK_ID is present (the dispatcher always passes
                    // a unique ECS-ARN / nanoid). A SHARED constant like "task-0"
                    // would collapse the CVR ownership lease: two standalone
                    // instances would each satisfy `owner == task_id` and never
                    // see the other as a competing owner, permitting interleaved
                    // lost updates. Fall back to a per-process-UNIQUE id instead
                    // (in a real deployment the env is always set, so this only
                    // guards misconfigured/standalone launches).
                    let auto = format!(
                        "task-auto-{}-{}",
                        std::process::id(),
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_nanos())
                            .unwrap_or(0)
                    );
                    eprintln!(
                        "WARNING: TASK_ID unset; using unique fallback owner id \
                         '{auto}'. Set TASK_ID in production."
                    );
                    auto
                }),
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
            // The bridge only forwards an explicit opt-out (rust-syncer-bridge
            // rustSyncerEnv), same contract as ENABLE_QUERY_COVERING above.
            enable_query_planner: !env::var("ENABLE_QUERY_PLANNER")
                .map(|v| {
                    let v = v.trim().to_ascii_lowercase();
                    v == "false" || v == "0"
                })
                .unwrap_or(false),
            // TS default: false. An explicit true/1 (case-insensitive) enables
            // the VENDED diagnostic. (TS env: `ZERO_QUERY_HYDRATION_STATS`.)
            query_hydration_stats: env::var("ZERO_QUERY_HYDRATION_STATS")
                .map(|v| {
                    let v = v.trim().to_ascii_lowercase();
                    v == "true" || v == "1"
                })
                .unwrap_or(false),
            // TS zero-config.ts:534 `v.number().default(10)`.
            yield_threshold_ms: env::var("ZERO_YIELD_THRESHOLD_MS")
                .ok()
                .and_then(|s| s.trim().parse::<f64>().ok())
                .unwrap_or(10.0),
        };
        // TS getZeroConfig applies this flag as a side effect of config load.
        config.apply_runtime_debug_flags();
        config
    }

    /// Apply the config's runtime debug toggles to the process-global
    /// `runtimeDebugFlags`. Port of the side effect in TS `getZeroConfig`
    /// (zero-config.ts:1213): `if (queryHydrationStats)
    /// runtimeDebugFlags.trackRowCountsVended = true`. Called once at startup
    /// from `from_env`; separated out so it is testable without env mutation.
    pub fn apply_runtime_debug_flags(&self) {
        if self.query_hydration_stats {
            rust_ivm::builder::debug_delegate::runtime_debug_flags()
                .set_track_row_counts_vended(true);
        }
    }
}

fn parse_query_config() -> Option<crate::FetchConfig> {
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
    Some(crate::FetchConfig {
        url: Some(urls),
        api_key: env::var("QUERY_API_KEY")
            .ok()
            .filter(|value| !value.is_empty()),
        allowed_client_headers,
        allowed_request_headers,
        forward_cookies: env::var("QUERY_FORWARD_COOKIES").as_deref() == Ok("true"),
    })
}

/// Port of TS `isAdminPasswordValid` (config/zero-config.ts:1242). In
/// DEVELOPMENT mode (`NODE_ENV=development`) with no admin password configured
/// and none provided, access is allowed (open inspector). Otherwise a
/// configured admin password must be non-empty and match. rust previously
/// omitted the dev-mode branch (`admin_password.is_some_and(...)` alone), so a
/// dev sandbox with no `ZERO_ADMIN_PASSWORD` LOCKED the inspector where TS
/// OPENED it — caught by the G49/E inspect-auth differential (2026-08-28: rust
/// authenticated:false, TS answered `queries` as an authenticated CG).
pub fn is_admin_password_valid(
    password: &str,
    admin_password: Option<&str>,
    dev_mode: bool,
) -> bool {
    if password.is_empty() && admin_password.is_none() && dev_mode {
        return true;
    }
    admin_password.is_some_and(|p| !p.is_empty() && p == password)
}

#[cfg(test)]
mod admin_password_tests {
    use super::is_admin_password_valid;

    /// Port fidelity for TS `isAdminPasswordValid` (config/zero-config.ts). The
    /// dev-mode-no-password branch is the one rust omitted (G49/E finding).
    /// Non-vacuous: dropping that branch (the pre-fix `admin_password.is_some_and`
    /// alone) makes the first assertion fail — dev sandbox would lock the inspector.
    #[test]
    fn is_admin_password_valid_matches_ts() {
        // dev mode + no admin password + no password provided → OPEN (the fix).
        assert!(is_admin_password_valid("", None, true));
        // production (not dev) + no admin password → LOCKED.
        assert!(!is_admin_password_valid("", None, false));
        // admin password configured: must match exactly.
        assert!(is_admin_password_valid("secret", Some("secret"), true));
        assert!(!is_admin_password_valid("wrong", Some("secret"), true));
        // empty configured password never authenticates.
        assert!(!is_admin_password_valid("", Some(""), true));
        // dev mode does NOT bypass a configured password.
        assert!(!is_admin_password_valid("", Some("secret"), true));
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
