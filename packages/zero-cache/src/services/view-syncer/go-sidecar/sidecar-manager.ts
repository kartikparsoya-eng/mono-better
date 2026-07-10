// Owns the in-process Go engine lifecycle: dlopen via the goivm_napi addon,
// health-ping + protocol handshake, and terminal-failure handling. One shared
// GoIVMClient per zero-cache worker. The Go runtime cannot be unloaded or
// restarted in-process, so there is no restart machinery: any start failure
// is terminal ('failed' → callers fall back to TS), and a post-start fatal
// crashes the worker so the supervisor restores a working state.

import {getOrCreateCounter} from '../../../observability/metrics.ts';
import {GoIVMClient} from './go-ivm-client.ts';
import {loadGoNapiAddon, type GoNapiAddon} from './napi/index.ts';

/**
 * D10: Sidecar init-failure counter with {reason} label. Each failure mode
 * (dlopen failure, ping fail, version mismatch) increments with its own
 * label so dashboards can attribute initial-start failures vs mid-flight
 * crashes.
 */
const initFailureCounter = getOrCreateCounter(
  'sync',
  'ivm.sidecar-init-failure',
  'Sidecar initial-start attempt failed (label: reason)',
);

/**
 * Wire protocol revision this client expects. Bumped in lockstep with
 * `sidecarProtocolRev` in `go-ivm/cmd/sidecar/main.go`. A mismatch refuses
 * to start the manager (REVIEW-final MED-CROSS-5).
 */
const EXPECTED_PROTOCOL_REV = 12;

/**
 * Cold-start init concurrency cap. When N ViewSyncers start simultaneously,
 * they all call `initEngine` against the same engine. Without a cap, the
 * init reads stampede. This semaphore lets only a few proceed at once
 * (REVIEW-final MED-CROSS-1).
 */
const INIT_CONCURRENCY = 4;

/**
 * Pattern-match common init-failure error messages to a stable {reason}
 * label for the initFailureCounter. Unknown messages get "other" — a
 * non-zero rate of "other" on dashboards means a new failure mode appeared
 * and operators should add a bucket here.
 */
function classifyInitFailure(err: unknown): string {
  const msg = err instanceof Error ? err.message : String(err);
  if (msg.includes('protocol revision mismatch')) return 'protocol-mismatch';
  if (msg.includes('health check failed')) return 'health-check-fail';
  if (msg.includes('terminal state: failed')) return 'sliding-window-exceeded';
  return 'other';
}

export type SidecarLogger = (
  level: 'info' | 'warn' | 'error',
  msg: string,
  err?: unknown,
) => void;

export type SidecarConfig = {
  /** Path to libgoivm (default: 'libgoivm.so', resolved by the dynamic
   *  linker's search path). */
  napiLibPath?: string;
  /**
   * Number of syncer workers in this container. Each worker loads its OWN
   * Go runtime, and GO_IVM_GOMEMLIMIT_PERCENT is a CONTAINER-wide budget
   * share written for the one-shared-sidecar topology — so the manager
   * divides it across workers before dlopen (see #startNapi). Default 1
   * (no division).
   */
  numSyncWorkers?: number;
  /**
   * Invoked when the in-process (napi) transport fails POST-START — a
   * terminal state (the Go runtime cannot re-initialize) where "fall back
   * to TS" is unsound: the user pipelines are STUBS (Go-owned), so a
   * worker that keeps running serves nothing for its Go-owned client
   * groups and nothing ever heals it. Default crashes the worker
   * (process.exit(1)) so the supervisor restores a working state.
   * Injectable for tests. Startup failures do NOT route here — they keep
   * the graceful 'failed' → TS-fallback path (nothing was ever Go-owned).
   */
  fatalExit?: () => void;
  /** Structured logger; defaults to silence. */
  logger?: SidecarLogger;
  /**
   * Extra environment variables to set BEFORE the dlopen (the Go runtime
   * snapshots environ at load). Used to derive the engine-side `GO_IVM_*`
   * flags (e.g. app-id) from the SAME zero-cache config that drives the TS
   * dispatch decision, so the two sides can't silently disagree on mode
   * (O1).
   */
  spawnEnv?: Record<string, string>;
};

export type SidecarStatus = 'stopped' | 'starting' | 'running' | 'failed';

export type RestartListener = (epoch: number) => void | Promise<void>;

/**
 * Divides the container-wide SQLite connection ceilings across the napi
 * workers (napi review M8, companion to the GOMEMLIMIT_PERCENT division in
 * #startNapi): GO_IVM_MAX_OPEN_CONNS / GO_IVM_MAX_IDLE_CONNS are written
 * for the one-shared-sidecar topology (image defaults 1024/128). In napi
 * mode every syncer worker opens its OWN replica pools, so N workers ×
 * 1024 conns × ~GO_IVM_CONN_CACHE_KB of C-side page cache overcommits the
 * container by N× — memory no Go-side limiter can see. Divide when set
 * (ceil, with floors so one worker's CGs aren't starved); warn when unset
 * with W>1 (the Go package default, 256, then applies PER WORKER).
 * Exported for tests.
 */
export function divideGoConnCeilingsForWorkers(
  env: Record<string, string | undefined>,
  workers: number,
  logger: SidecarLogger,
): void {
  if (workers <= 1) {
    return;
  }
  const divide = (key: string, floor: number): void => {
    const raw = env[key];
    if (raw === undefined || raw === '') {
      logger(
        'warn',
        `${key} is unset with ${workers} napi workers — the Go package ` +
          `default applies PER WORKER (C-side page cache multiplies by ` +
          `worker count); set it to the container budget to enable division`,
      );
      return;
    }
    const n = Number(raw);
    if (!Number.isFinite(n) || n <= 0) {
      logger('warn', `invalid ${key}=${JSON.stringify(raw)} — leaving as-is`);
      return;
    }
    const per = Math.max(floor, Math.ceil(n / workers));
    env[key] = String(per);
    logger(
      'info',
      `${key}: ${per} conns for this worker's pools (container budget ` +
        `${n} ÷ ${workers} workers)`,
    );
  };
  divide('GO_IVM_MAX_OPEN_CONNS', 8);
  divide('GO_IVM_MAX_IDLE_CONNS', 2);
}

/**
 * Validates the two absolute Go memory-limit env vars before the napi
 * dlopen (scale review). Syntax mirrors the Go runtime's GOMEMLIMIT: an
 * integer byte count with an optional B / KiB / MiB / GiB / TiB suffix
 * ("off" is also legal for GOMEMLIMIT). Invalid values are DELETED with a
 * loud log:
 *
 *   - a malformed GO_IVM_GOMEMLIMIT would skip the per-worker percent
 *     fallback in #startNapi while ALSO failing Go-side parsing — the
 *     engine then ran with NO memory ceiling at GOGC=200 (container OOM);
 *   - a malformed GOMEMLIMIT makes the Go RUNTIME fatal during rt0 env
 *     parsing — with the in-process transport that's an unbootable,
 *     crash-looping worker.
 *
 * Exported for tests.
 */
const VALID_GO_MEM_LIMIT = /^\d+((K|M|G|T)i?B|B)?$/;

export function sanitizeGoMemLimitEnv(
  env: Record<string, string | undefined>,
  logger: SidecarLogger,
): void {
  const gi = env.GO_IVM_GOMEMLIMIT;
  if (gi !== undefined && !VALID_GO_MEM_LIMIT.test(gi)) {
    logger(
      'error',
      `invalid GO_IVM_GOMEMLIMIT ${JSON.stringify(gi)} — ignoring it ` +
        `(want bytes or a KiB/MiB/GiB/TiB suffix); the per-worker percent ` +
        `budget applies instead`,
    );
    delete env.GO_IVM_GOMEMLIMIT;
  }
  const gml = env.GOMEMLIMIT;
  if (gml !== undefined && gml !== 'off' && !VALID_GO_MEM_LIMIT.test(gml)) {
    logger(
      'error',
      `invalid GOMEMLIMIT ${JSON.stringify(gml)} — deleting it (the Go ` +
        `runtime FATALS on a malformed GOMEMLIMIT at load, which would ` +
        `crash-loop this worker); the per-worker percent budget applies ` +
        `instead`,
    );
    delete env.GOMEMLIMIT;
  }
}

/**
 * True iff `err` is the wire-protocol-revision mismatch raised during the
 * version handshake (#start). Startup fails on every version() error; this
 * predicate only keeps the mismatch classifier stable for metrics/tests.
 */
export function isProtocolMismatchError(err: unknown): boolean {
  return (
    err instanceof Error && err.message.includes('protocol revision mismatch')
  );
}

export class SidecarManager {
  readonly #config: Required<Omit<SidecarConfig, 'logger'>> & {
    logger: SidecarLogger;
  };
  #client: GoIVMClient | null = null;
  #status: SidecarStatus = 'stopped';
  #epoch = 0;
  #firstStartComplete = false;
  #listeners = new Set<RestartListener>();
  // Resolves the next time status transitions to 'running'. Replaced with a
  // fresh pending promise whenever status leaves 'running'. Rejected when
  // status hits the terminal 'failed' or 'stopped' state so awaiting callers
  // fall through instead of hanging.
  #runningResolve: () => void = () => {};
  #runningReject: (err: Error) => void = () => {};
  #runningPromise: Promise<void> = Promise.resolve();
  /** Init concurrency cap state (REVIEW-final MED-CROSS-1). */
  #initInFlight = 0;
  #initWaiters: Array<() => void> = [];
  /**
   * The loaded goivm_napi addon. Set on the first successful #startNapi;
   * never cleared — the Go host it fronts can only be started ONCE per
   * process (a Go runtime cannot be re-initialized), so its presence
   * doubles as the "no second start" guard.
   */
  #napiAddon: GoNapiAddon | null = null;

  constructor(config: SidecarConfig) {
    // No console fallback: callers without a logger get silence. The syncer
    // wires a LogContext-backed logger through to here.
    const noop: SidecarLogger = () => {};
    const logger: SidecarLogger = config.logger ?? noop;
    this.#config = {
      napiLibPath: config.napiLibPath ?? 'libgoivm.so',
      numSyncWorkers: config.numSyncWorkers ?? 1,
      fatalExit:
        config.fatalExit ??
        (() => {
          process.exit(1);
        }),
      spawnEnv: config.spawnEnv ?? {},
      logger,
    };
  }

  get status(): SidecarStatus {
    return this.#status;
  }

  // Resolves the next time the sidecar is running. Rejects if the manager
  // has reached a terminal state ('failed' / 'stopped'). Used by clients to
  // queue calls across the start window instead of failing fast.
  waitForRunning(): Promise<void> {
    if (this.#status === 'running') return Promise.resolve();
    if (this.#status === 'failed' || this.#status === 'stopped') {
      return Promise.reject(
        new Error(`Sidecar reached terminal state: ${this.#status}`),
      );
    }
    return this.#runningPromise;
  }

  /** Monotonic counter incremented when the engine starts. */
  get epoch(): number {
    return this.#epoch;
  }

  /**
   * Run `fn` while holding one of the manager's init slots. When the slot
   * cap is reached, the call awaits until another init completes. Prevents
   * cold-start stampedes across many ViewSyncers (REVIEW-final MED-CROSS-1).
   */
  async withInitSlot<T>(fn: () => Promise<T>): Promise<T> {
    if (this.#initInFlight >= INIT_CONCURRENCY) {
      await new Promise<void>(resolve => this.#initWaiters.push(resolve));
    }
    this.#initInFlight++;
    try {
      return await fn();
    } finally {
      this.#initInFlight--;
      const next = this.#initWaiters.shift();
      if (next) next();
    }
  }

  /**
   * Register a listener invoked AFTER each successful engine restart
   * (not the initial start). Listener receives the new epoch. On the
   * in-process transport restarts never happen (a dead host is terminal),
   * so listeners are registered but never fire post-start; the
   * subscription API stays for the GoComputeBackend wiring.
   * Returns an unsubscribe function.
   */
  onRestart(listener: RestartListener): () => void {
    this.#listeners.add(listener);
    return () => {
      this.#listeners.delete(listener);
    };
  }

  /**
   * Start the in-process engine and wait until it's ready.
   * Throws if the dlopen/handshake fails.
   */
  async start(): Promise<void> {
    if (this.#status === 'running') return;
    // Idempotent during 'starting': a second concurrent start() must NOT
    // start a second host or replace the pending #runningPromise (which
    // would orphan the first caller's await). Hand back the in-flight
    // promise so both callers resolve/reject together.
    if (this.#status === 'starting') return this.#runningPromise;
    // Terminal state: starting again would sidestep the one-shot dlopen
    // guard. Reject explicitly so callers know to stay on TS-only.
    if (this.#status === 'failed') {
      throw new Error('Sidecar reached terminal state: failed');
    }

    this.#status = 'starting';

    // Initialize #runningPromise to a fresh pending promise BEFORE
    // calling #spawn. The constructor defaults it to Promise.resolve()
    // (sentinel), and pre-fix start() never replaced it — so any caller
    // who invoked waitForRunning() between start()-set-status-to-starting
    // and #spawn-completes-and-replaces-the-promise would receive the
    // resolved sentinel and proceed as if the sidecar were ready.
    this.#runningPromise = new Promise<void>((res, rej) => {
      this.#runningResolve = res;
      this.#runningReject = rej;
    });
    // A rejection with no waitForRunning() caller attached must not become
    // an unhandledRejection (Node's default crashes the process — e.g. a
    // stop() rejecting this while nobody awaits it). The derived catch
    // marks it handled; waitForRunning() still hands out the raw promise.
    this.#runningPromise.catch(() => {});

    try {
      await this.#spawn();
    } catch (err) {
      // Transition to 'failed' and surface the error so callers can fall
      // back to TS (nothing was ever Go-owned on a startup failure).
      this.#status = 'failed';
      const reason = classifyInitFailure(err);
      initFailureCounter.add(1, {reason});
      this.#runningReject(
        err instanceof Error
          ? err
          : new Error(`Initial sidecar start failed: ${String(err)}`),
      );
      throw err;
    }
  }

  /**
   * Get the shared GoIVMClient instance (connected to the running engine).
   * Throws if the engine is not running.
   */
  getClient(): GoIVMClient {
    if (!this.#client || this.#status !== 'running') {
      throw new Error(`Sidecar is not running (status: ${this.#status})`);
    }
    return this.#client;
  }

  /**
   * Stop the manager. The in-process host has NO shutdown export (removed
   * in the scale review: calling goivm_shutdown on the JS thread deadlocks
   * against TSFN backpressure and racing deliveries are a use-after-free).
   * The host can never be restarted in-process anyway, so the only stop()
   * caller that matters is worker shutdown — where process exit reclaims
   * everything. The idle host holds no client groups after close()
   * rejected the pendings.
   */
  stop(): Promise<void> {
    this.#status = 'stopped';
    this.#runningReject(new Error('Sidecar reached terminal state: stopped'));

    if (this.#client) {
      this.#client.close();
      this.#client = null;
    }
    return Promise.resolve();
  }

  // --- Private ---

  async #spawn(): Promise<void> {
    // In-process transport: no child process, no socket. #startNapi wires
    // this.#client to the addon; the handshake below (ping + version) then
    // verifies the engine.
    this.#startNapi();

    const client = this.#client;
    if (!client) {
      throw new Error('transport establishment did not produce a client');
    }
    const pong = await client.ping();
    if (pong !== 'pong') {
      throw new Error(
        `Sidecar health check failed: expected 'pong', got '${pong}'`,
      );
    }
    // Verify wire protocol version (REVIEW-final MED-CROSS-5).
    const v = await client.version();
    if (v.protocolRev !== EXPECTED_PROTOCOL_REV) {
      const msg =
        `Sidecar protocol revision mismatch: client expects ${EXPECTED_PROTOCOL_REV}, ` +
        `sidecar (v${v.version}) is at ${v.protocolRev}. Refusing to use this sidecar.`;
      this.#config.logger('error', msg);
      throw new Error(msg);
    }
    this.#config.logger(
      'info',
      `Sidecar version ${v.version} (protocol rev ${v.protocolRev})`,
    );

    this.#status = 'running';
    this.#epoch++;
    // Explicit startup line: operators grep for this to confirm the Go path
    // is engaged on a fresh deploy.
    this.#config.logger(
      'info',
      `Go sidecar manager running in-process via napi (epoch ${this.#epoch})`,
    );
    this.#runningResolve();

    if (this.#firstStartComplete) {
      // This was a restart — notify dependents. Run listeners best-effort;
      // failures don't roll back the restart. (Unreachable on the one-shot
      // in-process transport; kept for the listener contract.)
      const epoch = this.#epoch;
      for (const listener of this.#listeners) {
        Promise.resolve()
          .then(() => listener(epoch))
          .catch(err =>
            this.#config.logger('error', 'restart listener failed', err),
          );
      }
    } else {
      this.#firstStartComplete = true;
    }
  }

  /**
   * In-process (napi) transport establishment: load the goivm_napi addon,
   * dlopen libgoivm, start the Go host with this client's delivery
   * callback. Synchronous — goivm_start returns only after the host's pump
   * goroutines are up.
   *
   * ONE-SHOT: a Go runtime cannot be re-initialized in-process, so a second
   * establishment attempt throws. The client created here is never replaced
   * — the TSFN registered with goivm_start closes over it.
   */
  #startNapi(): void {
    if (this.#napiAddon) {
      throw new Error(
        'in-process Go host cannot be restarted (Go runtimes cannot ' +
          're-initialize in a loaded library); worker restart required',
      );
    }
    // Env BEFORE dlopen: the Go runtime snapshots environ when the library
    // loads (rt0 init), so os.Getenv inside newServerFromEnv sees only what
    // exists at that moment. Same GO_IVM_* keys, same values as the config
    // that drives the TS dispatch (O1: worker and engine can't silently
    // disagree on mode).
    for (const [k, val] of Object.entries(this.#config.spawnEnv)) {
      process.env[k] = val;
    }
    // Go memory budget (REVIEW-napi-transport MED): GO_IVM_GOMEMLIMIT_PERCENT
    // is a CONTAINER-wide budget share, written for the one-shared-sidecar
    // topology (image default 40). In napi mode every syncer worker loads
    // its own Go runtime, so N workers × 40% = 40N% overcommit — the
    // container OOMs under load with every Go heap individually "under
    // budget". Divide the container share across workers (floor 3%) unless
    // the operator pinned an absolute limit (GO_IVM_GOMEMLIMIT / GOMEMLIMIT)
    // or an explicit per-worker share (GO_IVM_NAPI_GOMEMLIMIT_PERCENT).
    //
    // Scale review: validate the pinned limits FIRST. A malformed
    // GO_IVM_GOMEMLIMIT used to skip this fallback while ALSO failing to
    // parse on the Go side (no limit at all, GOGC=200 → container OOM);
    // a malformed GOMEMLIMIT FATALS the Go runtime at dlopen (an
    // unbootable, crash-looping worker). sanitizeGoMemLimitEnv strips
    // invalid values with a loud log so the percent fallback applies.
    sanitizeGoMemLimitEnv(process.env, this.#config.logger);
    if (!process.env.GO_IVM_GOMEMLIMIT && !process.env.GOMEMLIMIT) {
      const workers = Math.max(1, this.#config.numSyncWorkers);
      const explicit = Number(process.env.GO_IVM_NAPI_GOMEMLIMIT_PERCENT);
      const base = Number(process.env.GO_IVM_GOMEMLIMIT_PERCENT) || 40;
      const per =
        Number.isFinite(explicit) && explicit > 0
          ? Math.floor(explicit)
          : Math.max(3, Math.floor(base / workers));
      process.env.GO_IVM_GOMEMLIMIT_PERCENT = String(per);
      this.#config.logger(
        'info',
        `Go memory budget: ${per}% of cgroup for this worker's runtime ` +
          `(container share ${base}% ÷ ${workers} workers)`,
      );
    }
    // C-side SQLite ceilings are per-ENGINE, and napi runs one engine per
    // worker — divide the container-wide conn budgets the same way as the
    // memory share above (napi review M8).
    divideGoConnCeilingsForWorkers(
      process.env,
      Math.max(1, this.#config.numSyncWorkers),
      this.#config.logger,
    );
    const addon = loadGoNapiAddon(); // throws if the .node isn't built
    const client = new GoIVMClient({
      onLog: (level, msg, err) =>
        this.#config.logger(level, `client: ${msg}`, err),
      // A2 (scale review): the client detects in-process host death
      // (goivm_send rc != 0) and latches its transport dead. Route that to
      // the terminal-failure path (status 'failed' → fatalExit). Without
      // this wiring the branch was dead code: the manager stayed 'running'
      // with a dead engine and every Go-owned CG spun in per-CG reset loops
      // the supervisor never saw. Skip when already terminal — a send
      // racing a deliberate stop() (status 'stopped') must not crash a
      // cleanly-shutting-down worker.
      onFatal: err => {
        if (this.#status === 'stopped' || this.#status === 'failed') return;
        this.#config.logger(
          'error',
          `in-process Go engine fatal: ${err.message}`,
          err,
        );
        this.#handleFatal();
      },
    });
    // Throws on dlopen failure, missing symbols, ABI mismatch, or
    // goivm_start rc != 0 — all routed to start()'s catch → 'failed'.
    addon.start(this.#config.napiLibPath, client.handleNapiDelivery);
    client.connectNapi(addon);
    this.#napiAddon = addon;
    this.#client = client;
    this.#config.logger(
      'info',
      `in-process Go engine loaded (lib ${this.#config.napiLibPath}, ` +
        `abi v${addon.abiVersion()})`,
    );
  }

  /**
   * Terminal post-start failure of the in-process engine. There is nothing
   * to restart — the Go host lives (and dies) with this worker and cannot
   * be re-initialized.
   *
   * CRASH, don't degrade (REVIEW-napi-transport B3): "fall back to TS"
   * does not exist once client groups are Go-owned — the user pipelines
   * are STUBS, so a worker that keeps running serves nothing for those CGs
   * and nothing ever heals it. Startup failures never reach here (they
   * route through start()'s catch → graceful 'failed' → TS fallback, sound
   * because nothing was ever Go-owned); this is the POST-START path —
   * wired live (A2) from GoIVMClient's onFatal, which fires when
   * goivm_send returns rc != 0 (host receive loop gone). It MUST crash so
   * the supervisor restores a working state (fresh worker, fresh dlopen).
   */
  #handleFatal(): void {
    this.#status = 'failed';
    initFailureCounter.add(1, {reason: 'napi-no-restart'});
    this.#runningReject(new Error('Sidecar reached terminal state: failed'));
    this.#config.logger(
      'error',
      'In-process Go engine failed post-start; it cannot be restarted ' +
        'in-process and TS fallback is unsound for Go-owned client groups ' +
        '(Go-owned stubs). Crashing the worker so the supervisor ' +
        'restores a working state.',
    );
    this.#config.fatalExit();
  }
}
