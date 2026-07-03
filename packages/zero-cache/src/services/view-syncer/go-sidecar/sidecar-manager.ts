// Owns the Go sidecar process lifecycle: spawn, health-ping, automatic
// restart under a sliding-window failure cap, and restart notifications so
// dependents (GoComputeBackend) can re-initialize. One shared GoIVMClient
// per zero-cache worker.

import {spawn, type ChildProcess} from 'child_process';
import {existsSync, unlinkSync} from 'fs';
import {createConnection} from 'net';
import {tmpdir} from 'os';
import {join} from 'path';
import {getOrCreateCounter} from '../../../observability/metrics.ts';
import {GoIVMClient} from './go-ivm-client.ts';
import {loadGoNapiAddon, type GoNapiAddon} from './napi/index.ts';

/**
 * D5: Protocol-version fallback counter. Pre-fix the catch-and-warn path
 * for "version RPC not implemented" left no metric — operators couldn't
 * tell whether a deployed sidecar was actually using the new wire protocol
 * or silently running in compatibility mode. Now every fallback bumps
 * this counter; non-zero on a dashboard means an older sidecar is in
 * the field and the next protocol-breaking change will need migration.
 *
 * D10: Sidecar init-failure counter with {reason} label. Each failure mode
 * (binary-missing, waitForReady timeout, ping fail, version mismatch,
 * spawn exit during startup) increments with its own label so dashboards
 * can attribute initial-spawn failures vs mid-flight crashes.
 */
const protocolFallbackCounter = getOrCreateCounter(
  'sync',
  'ivm.sidecar-protocol-fallback',
  'Sidecar version RPC failed; client fell back to assumed-compatible mode',
);
const initFailureCounter = getOrCreateCounter(
  'sync',
  'ivm.sidecar-init-failure',
  'Sidecar initial-start or restart attempt failed (label: reason)',
);

/**
 * Wire protocol revision this client expects. Bumped in lockstep with
 * `sidecarProtocolRev` in `go-ivm/cmd/sidecar/main.go`. A mismatch refuses
 * to start the manager (REVIEW-final MED-CROSS-5).
 */
const EXPECTED_PROTOCOL_REV = 9;

/**
 * Cold-start init concurrency cap. When N ViewSyncers start simultaneously,
 * they all call `initEngine` → `init` + `loadRows` against the same socket.
 * Without a cap, the SQLite `SELECT *` reads stampede and memory peaks at
 * N × largest_table_bytes. This semaphore lets only a few proceed at once
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
  if (msg.includes('binary not found')) return 'binary-missing';
  if (msg.includes('did not accept connections')) return 'waitForReady-timeout';
  if (msg.includes('socket')) return 'socket-error';
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
  /** Path to the compiled go-ivm-sidecar binary. Ignored when
   *  `externallyManaged` is true (the manager does not spawn) or when
   *  `transport` is 'napi' (no process at all). */
  binaryPath: string;
  /**
   * Transport to the Go engine (default: 'socket').
   *
   * 'socket': spawn/connect to a sidecar process over a Unix socket.
   * 'napi': load the engine IN-PROCESS via the goivm_napi addon +
   * libgoivm c-shared library. No child process, no socket, no syscalls
   * per frame. The Go runtime cannot be unloaded or restarted in-process,
   * so restart recovery does not exist: any start failure is terminal
   * ('failed' → callers fall back to TS), and a Go FATAL error (not a
   * panic — those are recovered at the handler boundary) takes down the
   * whole worker. Mutually exclusive with `externallyManaged`.
   */
  transport?: 'socket' | 'napi';
  /** Path to libgoivm for transport='napi' (default: 'libgoivm.so',
   *  resolved by the dynamic linker's search path). */
  napiLibPath?: string;
  /**
   * Number of syncer workers in this container (transport='napi' only).
   * Each worker loads its OWN Go runtime, and GO_IVM_GOMEMLIMIT_PERCENT is
   * a CONTAINER-wide budget share written for the one-shared-sidecar
   * topology — so the manager divides it across workers before dlopen
   * (see #startNapi). Default 1 (no division).
   */
  numSyncWorkers?: number;
  /**
   * Invoked when the in-process (napi) transport fails POST-START — a
   * terminal state (the Go runtime cannot re-initialize) where "fall back
   * to TS" is unsound: under leanPrimary the user pipelines are STUBS, so
   * a worker that keeps running serves nothing for its Go-owned client
   * groups and nothing ever heals it. Default crashes the worker
   * (process.exit(1)) so the supervisor restores a working state.
   * Injectable for tests. Startup failures do NOT route here — they keep
   * the graceful 'failed' → TS-fallback path (nothing was ever Go-owned).
   */
  fatalExit?: () => void;
  /** Unix socket path (default: /tmp/go-ivm-<pid>.sock) */
  socketPath?: string;
  /** Max restart attempts within restartWindowMs before giving up (default: 5) */
  maxRestartsInWindow?: number;
  /** Sliding window in ms for restart counting (default: 60000) */
  restartWindowMs?: number;
  /** Delay between restart attempts in ms (default: 1000) */
  restartDelayMs?: number;
  /** Timeout for socket readiness in ms (default: 5000) */
  healthCheckTimeoutMs?: number;
  /** Whether to write sidecar stdout/stderr to console (default: true).
   *  Routed through `logger` when provided. */
  verbose?: boolean;
  /** Structured logger; defaults to console. */
  logger?: SidecarLogger;
  /**
   * When true, the sidecar process is owned by something outside this
   * worker (e.g., the container's entrypoint script spawning a shared
   * sidecar consumed by all workers). The manager skips `spawn`, won't
   * kill the process on `stop`, and treats client disconnects as
   * reconnect-with-restart-notification rather than respawn. The owner
   * is responsible for keeping the sidecar process alive; this manager
   * just connects and reconnects as needed.
   *
   * Use case: addresses the "more zero-cache workers = more sidecars =
   * smaller cg shards = lower inter-cg parallelism" footgun. With one
   * shared sidecar, all client groups colocate and parallel work scales
   * with the number of active cgs, not workers.
   */
  externallyManaged?: boolean;
  /**
   * Extra environment variables to set on the spawned sidecar process (merged
   * over the inherited env). Used to derive the sidecar-side `GO_IVM_*` flags
   * (advance-to-head / advance-drive / app-id) from the SAME zero-cache config
   * that drives the TS dispatch decision, so the two processes can't silently
   * disagree on mode (O1). Ignored when `externallyManaged` is true — the owner
   * sets the env on the shared process it spawns.
   */
  spawnEnv?: Record<string, string>;
};

export type SidecarStatus = 'stopped' | 'starting' | 'running' | 'restarting' | 'failed';

export type RestartListener = (epoch: number) => void | Promise<void>;

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
 * version handshake (#start). That handshake's catch intentionally SWALLOWS a
 * "method not found" error so a pre-version-RPC sidecar stays usable, but it
 * MUST re-throw a genuine revision mismatch — silently accepting an
 * incompatible wire protocol corrupts every subsequent RPC. This predicate is
 * the seam that splits "swallow" from "escalate"; pinned by sidecar-manager.test.ts.
 */
export function isProtocolMismatchError(err: unknown): boolean {
  return (
    err instanceof Error &&
    err.message.includes('protocol revision mismatch')
  );
}

export class SidecarManager {
  readonly #config: Required<Omit<SidecarConfig, 'logger'>> & {logger: SidecarLogger};
  #proc: ChildProcess | null = null;
  #client: GoIVMClient | null = null;
  #status: SidecarStatus = 'stopped';
  #restartTimestamps: number[] = []; // for sliding-window cap
  // M2: retriggers (a follow-up to an already-counted failure) deliberately do
  // NOT push a sliding-window timestamp. But a PERSISTENT mid-handshake failure
  // — a sidecar that spawns then hangs (or a permanently-down externally-managed
  // one) — manifests ONLY as retriggers, so the timestamp-based cap would never
  // trip and the manager would retry forever, never reaching 'failed'. This
  // separate counter bounds consecutive retriggers; it resets to 0 on a
  // successful spawn (status → 'running').
  #consecutiveRetriggers = 0;
  #shutdownRequested = false;
  // Set true immediately before the manager issues a deliberate SIGKILL
  // (post-spawn-failure cleanup in #handleRestartTrigger's catch path).
  // The exit handler reads this to pass isRetrigger=true and avoid
  // counting the manager-induced exit as a fresh failure. Reset after
  // consumption so a subsequent natural process death is counted
  // normally.
  #suppressNextExitCount = false;
  #epoch = 0;
  // Source mode reported by the sidecar's version RPC at handshake:
  // 'table' means its leaves read SQLite directly and loadRows is a no-op,
  // so the init path can skip materializing + shipping row contents
  // (a full-DB `SELECT *` per table — OOMs the syncer on real datasets).
  // undefined = older sidecar without the field, or version RPC failed →
  // callers must assume memory mode and ship rows.
  #sidecarSourceMode: string | undefined = undefined;
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
   * Periodic isConnected() poll used in externallyManaged mode to detect
   * sidecar restarts (where we lose the socket connection but the manager
   * has no proc.on('exit') hook because it didn't spawn).
   */
  #healthTimer: ReturnType<typeof setInterval> | null = null;
  // Pending delayed respawn armed by #handleRestartTrigger. Stored so stop()
  // can cancel it — pre-fix (scale review) the timer was anonymous, so a
  // stop() landing during 'restarting' let the timer fire afterwards and
  // #spawn a fresh sidecar NOBODY owned: a zombie process holding the socket
  // (and a manager resurrected to 'running' after its owner stopped it).
  #restartTimer: ReturnType<typeof setTimeout> | null = null;
  /** How often the health check runs in externallyManaged mode (ms). */
  static readonly #HEALTH_CHECK_MS = 2000;
  /**
   * The loaded goivm_napi addon (transport='napi' only). Set on the first
   * successful #startNapi; never cleared — the Go host it fronts can only
   * be started ONCE per process (a Go runtime cannot be re-initialized),
   * so its presence doubles as the "no second start" guard.
   */
  #napiAddon: GoNapiAddon | null = null;

  constructor(config: SidecarConfig) {
    // No console fallback: callers without a logger get silence. The syncer
    // wires a LogContext-backed logger through to here.
    const noop: SidecarLogger = () => {};
    const logger: SidecarLogger = config.logger ?? noop;
    this.#config = {
      binaryPath: config.binaryPath,
      transport: config.transport ?? 'socket',
      napiLibPath: config.napiLibPath ?? 'libgoivm.so',
      numSyncWorkers: config.numSyncWorkers ?? 1,
      fatalExit:
        config.fatalExit ??
        (() => {
          process.exit(1);
        }),
      socketPath: config.socketPath ?? join(tmpdir(), `go-ivm-${process.pid}.sock`),
      maxRestartsInWindow: config.maxRestartsInWindow ?? 5,
      restartWindowMs: config.restartWindowMs ?? 60_000,
      restartDelayMs: config.restartDelayMs ?? 1000,
      healthCheckTimeoutMs: config.healthCheckTimeoutMs ?? 5000,
      verbose: config.verbose ?? true,
      externallyManaged: config.externallyManaged ?? false,
      spawnEnv: config.spawnEnv ?? {},
      logger,
    };
  }

  get status(): SidecarStatus {
    return this.#status;
  }

  // Resolves the next time the sidecar is running. Rejects if the manager
  // has reached a terminal state ('failed' / 'stopped'). Used by clients to
  // queue calls across a restart window instead of failing fast.
  waitForRunning(): Promise<void> {
    if (this.#status === 'running') return Promise.resolve();
    if (this.#status === 'failed' || this.#status === 'stopped') {
      return Promise.reject(
        new Error(`Sidecar reached terminal state: ${this.#status}`),
      );
    }
    return this.#runningPromise;
  }

  get socketPath(): string {
    return this.#config.socketPath;
  }

  /** Monotonic counter incremented each time the sidecar process is (re)started. */
  /**
   * Source mode the sidecar reported at handshake ('memory' | 'table'),
   * or undefined for older sidecars / failed version RPC (treat as memory).
   */
  get sidecarSourceMode(): string | undefined {
    return this.#sidecarSourceMode;
  }

  get epoch(): number {
    return this.#epoch;
  }

  /**
   * Register a listener invoked AFTER each successful sidecar restart
   * (not the initial start). Listener receives the new epoch.
   * Returns an unsubscribe function.
   */
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

  onRestart(listener: RestartListener): () => void {
    this.#listeners.add(listener);
    return () => {
      this.#listeners.delete(listener);
    };
  }

  /**
   * Start the sidecar process and wait until it's ready.
   * Throws if the binary doesn't exist or the process fails to start.
   */
  async start(): Promise<void> {
    if (this.#status === 'running') return;
    // Idempotent during 'starting': a second concurrent start() must NOT
    // spawn a second process or replace the pending #runningPromise (which
    // would orphan the first caller's await). Hand back the in-flight
    // promise so both callers resolve/reject together. This matters during
    // worker bootstrap where multiple components call start() near-
    // simultaneously, and on the restart path if external code retries
    // start() while #handleRestartTrigger is still respawning.
    if (this.#status === 'starting') return this.#runningPromise;
    // Same idempotency during 'restarting': #handleRestartTrigger is already
    // driving a respawn and has installed a fresh #runningPromise that
    // waitForRunning() callers are blocked on. A concurrent start() must hand
    // that promise back rather than spawn a second process and orphan those
    // awaiters (LOW-2).
    if (this.#status === 'restarting') return this.#runningPromise;
    // Terminal states: 'failed' is reached after the sliding-window cap
    // exceeds; starting() again would skip the cap check and silently
    // restart a sidecar the manager has already declared dead. Reject
    // explicitly so callers know to stay on TS-only.
    if (this.#status === 'failed') {
      throw new Error('Sidecar reached terminal state: failed');
    }

    // In externally-managed mode the binary path is not used by this
    // manager (some other process owns the sidecar), and in napi mode
    // there is no process at all (the engine loads in-process from
    // napiLibPath — dlopen failures surface from #startNapi). Skip the
    // existence check so an unused binaryPath doesn't block startup.
    if (!this.#config.externallyManaged && this.#config.transport !== 'napi') {
      if (!existsSync(this.#config.binaryPath)) {
        initFailureCounter.add(1, {reason: 'binary-missing'});
        throw new Error(
          `Go IVM sidecar binary not found at: ${this.#config.binaryPath}. ` +
            `Build it with: cd go-ivm && go build -o ${this.#config.binaryPath} ./cmd/sidecar/`,
        );
      }
    }

    this.#shutdownRequested = false;
    this.#status = 'starting';

    // Initialize #runningPromise to a fresh pending promise BEFORE
    // calling #spawn. The constructor defaults it to Promise.resolve()
    // (sentinel), and pre-fix start() never replaced it — so any caller
    // who invoked waitForRunning() between start()-set-status-to-starting
    // and #spawn-completes-and-replaces-the-promise would receive the
    // resolved sentinel and proceed as if the sidecar were ready. The
    // resolve/reject hooks let #spawn fire them when status transitions
    // to running (line ~373) or, on initial-start failure, here in the
    // catch block below.
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
      // Pre-fix the throw propagated to the caller without transitioning
      // the manager state machine. #status stayed at 'starting' forever
      // because terminal-state checks in waitForRunning() look for
      // 'failed' or 'stopped', and the post-spawn-failure restart
      // machinery is wired to proc.on('exit') or the catch in
      // #handleRestartTrigger — neither fires for the initial start()
      // call. Result: every subsequent caller hung on a resolved-sentinel
      // #runningPromise. Now we transition to 'failed' and surface the
      // error so callers can fall back to TS.
      this.#status = 'failed';
      // D10: classify the spawn failure. Most paths in #spawn end up here
      // via #waitForReady-timeout, version-mismatch throw, or ping-fail
      // throw — those throw their own Error with a recognizable message;
      // we bucket here by inspecting it. Unknown error texts get "other".
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
   * Get the shared GoIVMClient instance (connected to the running sidecar).
   * Throws if the sidecar is not running.
   */
  getClient(): GoIVMClient {
    if (!this.#client || this.#status !== 'running') {
      throw new Error(`Sidecar is not running (status: ${this.#status})`);
    }
    return this.#client;
  }

  /**
   * Stop the sidecar process gracefully.
   * In externallyManaged mode this only closes the client and stops the
   * health-check ticker — it does NOT kill the sidecar process or unlink
   * the socket (the owner is responsible for that).
   */
  async stop(): Promise<void> {
    this.#shutdownRequested = true;
    this.#status = 'stopped';
    this.#runningReject(new Error('Sidecar reached terminal state: stopped'));

    if (this.#healthTimer) {
      clearInterval(this.#healthTimer);
      this.#healthTimer = null;
    }
    // Zombie guard (scale review): a restart scheduled before stop() must
    // never spawn after it.
    if (this.#restartTimer) {
      clearTimeout(this.#restartTimer);
      this.#restartTimer = null;
    }

    if (this.#client) {
      this.#client.close();
      this.#client = null;
    }

    if (this.#proc) {
      this.#proc.kill('SIGTERM');
      // Wait for exit (max 5s)
      await new Promise<void>(resolve => {
        const timeout = setTimeout(() => {
          this.#proc?.kill('SIGKILL');
          resolve();
        }, 5000);
        this.#proc?.on('exit', () => {
          clearTimeout(timeout);
          resolve();
        });
      });
      this.#proc = null;
    }

    // In-process transport: nothing to tear down — the addon has NO
    // shutdown export (removed in the scale review: calling goivm_shutdown
    // on the JS thread deadlocks against TSFN backpressure and racing
    // deliveries are a use-after-free). The host can never be restarted
    // in-process anyway, so the only stop() caller that matters is worker
    // shutdown — where process exit reclaims everything. The idle host
    // holds no client groups after close() rejected the pendings.

    if (!this.#config.externallyManaged && this.#config.transport !== 'napi') {
      // Clean up socket file (only when we own it)
      try {
        unlinkSync(this.#config.socketPath);
      } catch {
        // ignore
      }
    }
  }

  // --- Private ---

  async #spawn(): Promise<void> {
    // In-process transport: no child process, no socket. #startNapi wires
    // this.#client to the addon; the shared handshake below (ping + version)
    // then runs identically to the socket path.
    if (this.#config.transport === 'napi') {
      this.#startNapi();
    } else {
      await this.#spawnSocketTransport();
    }

    // Verify with ping. #client was just wired by the transport helper;
    // the local pins non-null for the handshake.
    const client = this.#client;
    if (!client) {
      throw new Error('transport establishment did not produce a client');
    }
    const pong = await client.ping();
    if (pong !== 'pong') {
      throw new Error(`Sidecar health check failed: expected 'pong', got '${pong}'`);
    }
    // Verify wire protocol version (REVIEW-final MED-CROSS-5).
    try {
      const v = await client.version();
      if (v.protocolRev !== EXPECTED_PROTOCOL_REV) {
        const msg =
          `Sidecar protocol revision mismatch: client expects ${EXPECTED_PROTOCOL_REV}, ` +
          `sidecar (v${v.version}) is at ${v.protocolRev}. Refusing to use this sidecar.`;
        this.#config.logger('error', msg);
        throw new Error(msg);
      }
      this.#sidecarSourceMode = v.sourceMode;
      this.#config.logger(
        'info',
        `Sidecar version ${v.version} (protocol rev ${v.protocolRev}, ` +
          `source mode ${v.sourceMode ?? 'unreported'})`,
      );
    } catch (err) {
      // Re-throw protocol mismatches — an incompatible sidecar MUST NOT be
      // accepted. Only swallow "method not found" errors from older sidecars
      // that don't implement the version RPC at all.
      if (isProtocolMismatchError(err)) {
        throw err;
      }
      // If the version RPC isn't implemented (older sidecar), warn loudly
      // but don't refuse — operators can roll out a new client first.
      // Source mode stays undefined → init ships rows (memory-mode behavior).
      this.#sidecarSourceMode = undefined;
      // D5: also bump the fallback counter so dashboards can detect this
      // (operators kept missing the warn log in busy stdout).
      protocolFallbackCounter.add(1);
      this.#config.logger(
        'warn',
        'Sidecar does not implement version RPC; assuming compatibility (consider upgrading)',
        err,
      );
    }

    this.#status = 'running';
    this.#epoch++;
    // M2: a clean handshake clears the consecutive-retrigger streak so a later
    // transient restart starts from a full budget again.
    this.#consecutiveRetriggers = 0;
    // Explicit startup line: operators grep for this to confirm the Go path
    // is engaged on a fresh deploy. Pre-fix only the version banner logged
    // here, leaving "is the manager actually ready?" ambiguous when version
    // RPC silently failed.
    this.#config.logger(
      'info',
      this.#config.transport === 'napi'
        ? `Go sidecar manager running in-process via napi (epoch ${this.#epoch})`
        : `Go sidecar manager running at ${this.#config.socketPath} ` +
            `(epoch ${this.#epoch}${this.#config.externallyManaged ? ', externally-managed' : ''})`,
    );
    this.#runningResolve();

    // In externallyManaged mode there is no proc.on('exit') hook to detect
    // a sidecar crash. Install a health-check ticker that polls the client's
    // socket state; on disconnect, route through the same restart pipeline
    // that the spawned-process path uses.
    if (this.#config.externallyManaged) {
      if (this.#healthTimer) clearInterval(this.#healthTimer);
      this.#healthTimer = setInterval(() => {
        if (this.#shutdownRequested) return;
        if (this.#status !== 'running') return;
        if (this.#client && !this.#client.isConnected()) {
          this.#config.logger(
            'error',
            'External sidecar connection lost — attempting to reconnect',
          );
          // Stop polling while restart is in flight; #spawn will reinstall.
          if (this.#healthTimer) clearInterval(this.#healthTimer);
          this.#healthTimer = null;
          this.#handleRestartTrigger();
        }
      }, SidecarManager.#HEALTH_CHECK_MS);
      this.#healthTimer?.unref();
    }

    if (this.#firstStartComplete) {
      // This was a restart — notify dependents. Run listeners best-effort;
      // failures don't roll back the restart.
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
   * Socket-transport establishment: spawn the sidecar process (unless
   * externally managed), wait for its socket, and connect this.#client.
   * Extracted verbatim from the pre-napi #spawn body.
   */
  async #spawnSocketTransport(): Promise<void> {
    // Close any prior client BEFORE replacing — without this, restart leaks
    // socket listeners and pending Promises (REVIEW-ts-integration MEDIUM-4).
    if (this.#client) {
      this.#client.close();
      this.#client = null;
    }

    if (!this.#config.externallyManaged) {
      // Clean up stale socket
      try {
        unlinkSync(this.#config.socketPath);
      } catch {
        // ignore
      }

      const proc = spawn(this.#config.binaryPath, [this.#config.socketPath], {
        stdio: ['ignore', 'pipe', 'pipe'],
        // O1: derive the sidecar's GO_IVM_* mode flags from the same config the
        // TS dispatch uses, so a spawned sidecar can't be armed differently than
        // the worker expects. Merged over the inherited env.
        env: {...process.env, ...this.#config.spawnEnv},
      });

      this.#proc = proc;

      if (this.#config.verbose) {
        proc.stdout?.on('data', (data: Buffer) => {
          this.#config.logger('info', `stdout: ${data.toString().trimEnd()}`);
        });
        proc.stderr?.on('data', (data: Buffer) => {
          this.#config.logger('warn', `stderr: ${data.toString().trimEnd()}`);
        });
      }

      // Handle unexpected exit
      proc.on('exit', (code, signal) => {
        if (this.#shutdownRequested) return;

        this.#config.logger(
          'error',
          `Sidecar exited unexpectedly (code=${code}, signal=${signal})`,
        );

        // If the manager itself issued the SIGKILL (cleanup after a
        // spawn-attempt failure), pass isRetrigger so the failure
        // counter doesn't double-count the same logical failure.
        const isRetrigger = this.#suppressNextExitCount;
        this.#suppressNextExitCount = false;
        this.#handleRestartTrigger(isRetrigger);
      });

      // ChildProcess 'error' fires when spawn itself fails (ENOENT, EACCES,
      // wrong arch, etc.). Without an explicit handler, EventEmitter
      // semantics escalate to an uncaught exception that can crash the
      // worker. 'exit' typically pairs with 'error' and routes the restart;
      // logging here is the defensive net.
      proc.on('error', err => {
        if (this.#shutdownRequested) return;
        this.#config.logger('error', 'Sidecar process error event', err);
      });
    }
    // In externallyManaged mode we don't spawn — some other process owns
    // the sidecar. We trust the socket is already there (or appearing
    // shortly). Connection loss is detected by the periodic health check
    // installed by #spawn and routed through the same #handleRestartTrigger
    // path so dependents see a consistent restart-event model.

    // Wait for the socket to appear and become connectable
    await this.#waitForReady();

    // Connect a fresh client
    this.#client = new GoIVMClient(this.#config.socketPath, {
      onLog: (level, msg, err) =>
        this.#config.logger(level, `client: ${msg}`, err),
    });
    await this.#client.connect();
  }

  /**
   * In-process (napi) transport establishment: load the goivm_napi addon,
   * dlopen libgoivm, start the Go host with this client's delivery
   * callback. Synchronous — goivm_start returns only after the host's pump
   * goroutines are up, so there is no #waitForReady equivalent.
   *
   * ONE-SHOT: a Go runtime cannot be re-initialized in-process, so a second
   * establishment attempt (restart path) throws. The client created here is
   * never replaced — the TSFN registered with goivm_start closes over it.
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
    // exists at that moment. This mirrors the spawnEnv merge the socket
    // path applies to the child process — same GO_IVM_* keys, same values
    // (O1: worker and engine can't silently disagree on mode).
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
    const addon = loadGoNapiAddon(); // throws if the .node isn't built
    const client = new GoIVMClient('napi:in-process', {
      onLog: (level, msg, err) =>
        this.#config.logger(level, `client: ${msg}`, err),
      // A2 (scale review): the client detects in-process host death
      // (goivm_send rc != 0) and latches its transport dead. Route that to
      // the restart trigger, whose napi branch is the crash-don't-degrade
      // path (status 'failed' → fatalExit). Without this wiring the branch
      // was dead code: the manager stayed 'running' with a dead engine and
      // every Go-owned CG spun in per-CG reset loops the supervisor never
      // saw. Skip when already terminal — a send racing a deliberate stop()
      // (status 'stopped') must not crash a cleanly-shutting-down worker.
      onFatal: err => {
        if (this.#status === 'stopped' || this.#status === 'failed') return;
        this.#config.logger(
          'error',
          `in-process Go engine fatal: ${err.message}`,
          err,
        );
        this.#handleRestartTrigger();
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
   * Shared restart pipeline used by both:
   *   - the spawned process's `exit` handler, and
   *   - the externally-managed mode's health-check ticker on disconnect.
   *
   * Enforces the sliding-window failure cap and either schedules a retry
   * (`#spawn`) or marks the manager `failed` so callers can fall back to
   * the TS-native IVM path.
   */
  // isRetrigger: true when invoked from within #handleRestartTrigger's
  // own catch path (after a spawn-attempt failure routed through
  // SIGKILL/exit or explicit re-entry). Pre-fix every such re-entry
  // pushed a fresh timestamp, so one logical spawn failure consumed
  // TWO budget slots — operators who set maxRestartsInWindow=5 saw
  // the cap trip at 2-3 real failures instead of the expected 5.
  // The retrigger represents the FOLLOW-UP attempt to the same
  // already-counted failure; only natural process-exit events and
  // external triggers (initial start, externally-managed reconnect)
  // count as a new failure.
  #handleRestartTrigger(isRetrigger = false): void {
    // In-process transport: there is nothing to restart — the Go host
    // lives (and dies) with this worker and cannot be re-initialized.
    if (this.#config.transport === 'napi') {
      this.#status = 'failed';
      initFailureCounter.add(1, {reason: 'napi-no-restart'});
      this.#runningReject(new Error('Sidecar reached terminal state: failed'));
      // CRASH, don't degrade (REVIEW-napi-transport B3): "fall back to TS"
      // does not exist once client groups are Go-owned — under leanPrimary
      // the user pipelines are STUBS, so a worker that keeps running serves
      // nothing for those CGs and nothing ever heals it (the Go runtime
      // cannot restart in-process). Startup failures never reach here (they
      // route through start()'s catch → graceful 'failed' → TS fallback,
      // sound because nothing was ever Go-owned); this branch is the
      // POST-START path — wired live (A2) from GoIVMClient's onFatal, which
      // fires when goivm_send returns rc != 0 (host receive loop gone). It
      // MUST crash so the supervisor restores a working state (fresh
      // worker, fresh dlopen).
      this.#config.logger(
        'error',
        'In-process Go engine failed post-start; it cannot be restarted ' +
          'in-process and TS fallback is unsound for Go-owned client groups ' +
          '(lean-primary stubs). Crashing the worker so the supervisor ' +
          'restores a working state.',
      );
      this.#config.fatalExit();
      return;
    }
    // Sliding-window restart cap: count failures in the last
    // restartWindowMs and bail if we exceed the cap.
    const now = Date.now();
    const cutoff = now - this.#config.restartWindowMs;
    this.#restartTimestamps = this.#restartTimestamps.filter(t => t > cutoff);
    if (!isRetrigger) {
      this.#restartTimestamps.push(now);
      // A genuinely new failure breaks any consecutive-retrigger streak.
      this.#consecutiveRetriggers = 0;
    } else {
      this.#consecutiveRetriggers++;
    }

    // M2: trip the cap on EITHER too many windowed failures OR too many
    // consecutive retriggers (a persistent mid-handshake failure that never
    // surfaces as a fresh windowed failure). Either way we reach 'failed' and
    // dispatch falls through to TS instead of looping forever.
    if (
      this.#restartTimestamps.length <= this.#config.maxRestartsInWindow &&
      this.#consecutiveRetriggers <= this.#config.maxRestartsInWindow
    ) {
      this.#status = 'restarting';
      // Future waitForRunning() calls block on this fresh promise until the
      // restart settles. Without this, callers throw immediately and the
      // documented TS-fallback path becomes a client-visible Internal error.
      this.#runningPromise = new Promise<void>((res, rej) => {
        this.#runningResolve = res;
        this.#runningReject = rej;
      });
      // Same unhandledRejection guard as start(): a stop() during
      // 'restarting' rejects this promise, and there may be no
      // waitForRunning() caller left to observe it.
      this.#runningPromise.catch(() => {});
      this.#config.logger(
        'warn',
        `Restarting (failures in last ${this.#config.restartWindowMs}ms: ` +
          `${this.#restartTimestamps.length}/${this.#config.maxRestartsInWindow})`,
      );
      this.#restartTimer = setTimeout(
        () => {
          this.#restartTimer = null;
          // Zombie guard (scale review): stop() clears this timer, but
          // re-check at fire time anyway — a stop() immediately followed by
          // a fresh start() resets #shutdownRequested, so the only state a
          // scheduled respawn may legally fire in is a live 'restarting'.
          if (this.#shutdownRequested || this.#status !== 'restarting') {
            return;
          }
          this.#spawn().catch(err => {
            this.#config.logger('error', 'spawn failed', err);
            if (this.#shutdownRequested) return;
            // #spawn rejected mid-restart (e.g., #waitForReady timed out,
            // ping/version failed). Status is still 'restarting' and
            // #runningPromise is unresolved; without re-entry, callers
            // awaiting waitForRunning() would hang forever.
            //
            // In spawned mode the existing proc.on('exit') handler re-enters
            // automatically when the process dies — but if the process is
            // still alive (failed mid-handshake), nothing fires. Killing it
            // forces 'exit' and re-routes through the restart pipeline.
            //
            // In externallyManaged mode there is no proc.on('exit') at all,
            // so we re-trigger explicitly to advance the state machine.
            if (this.#config.externallyManaged) {
              // Retrigger: don't bump the failure counter for the same
              // spawn attempt that just failed; we already counted it
              // on entry to this trigger.
              this.#handleRestartTrigger(true);
            } else if (this.#proc && this.#proc.exitCode === null) {
              try {
                // Signal the exit handler that the upcoming death is
                // our doing, not a natural process exit, so it doesn't
                // double-count the same logical spawn failure.
                this.#suppressNextExitCount = true;
                this.#proc.kill('SIGKILL');
              } catch (killErr) {
                this.#suppressNextExitCount = false;
                this.#config.logger(
                  'error',
                  'failed to kill wedged sidecar process',
                  killErr,
                );
                this.#handleRestartTrigger(true);
              }
              // Note: when SIGKILL succeeds, proc.on('exit') will fire
              // and call #handleRestartTrigger again. That path used
              // to double-count too — see #onProcExit's isRetrigger
              // wiring below.
            } else {
              // Spawned mode, process already dead — proc.on('exit') will
              // have queued #handleRestartTrigger; nothing to do here.
            }
          });
        },
        this.#config.restartDelayMs,
      );
    } else {
      this.#status = 'failed';
      // D10: bump init-failure with sliding-window-exceeded so the
      // terminal-state transition is visible in metrics, not just logs.
      initFailureCounter.add(1, {reason: 'sliding-window-exceeded'});
      this.#runningReject(new Error('Sidecar reached terminal state: failed'));
      this.#config.logger(
        'error',
        'Max restart failures exceeded in window. Sidecar is DOWN. Falling back to TS IVM.',
      );
    }
  }

  async #waitForReady(): Promise<void> {
    // In externallyManaged mode the owner (e.g., container entrypoint) may
    // need longer to bring up the shared sidecar than the per-worker spawn
    // path's default. Allow a generous timeout to absorb container startup
    // ordering races; the worker will block on this until the socket
    // accepts connections or we give up.
    const timeoutMs = this.#config.externallyManaged
      ? Math.max(this.#config.healthCheckTimeoutMs, 30_000)
      : this.#config.healthCheckTimeoutMs;
    const deadline = Date.now() + timeoutMs;
    const pollMs = 50;

    while (Date.now() < deadline) {
      // existsSync is a fast precheck — the path must at minimum exist
      // before we burn a connect() attempt on it. But existence alone
      // doesn't mean the sidecar is accepting: the file may have been
      // created (bind()) before listen()/accept() are ready, and in
      // externally-managed mode it may also be a stale socket left over
      // from a prior process. A connect probe is the only thing that
      // proves the server is actually serving.
      if (existsSync(this.#config.socketPath) && (await this.#probeSocket())) {
        return;
      }
      await new Promise(resolve => setTimeout(resolve, pollMs));
    }

    throw new Error(
      `Sidecar did not accept connections at ${this.#config.socketPath} within ${timeoutMs}ms`,
    );
  }

  /**
   * Attempt a connect+immediate-close on the sidecar's Unix socket. Resolves
   * true if the connection succeeded, false on any error (ECONNREFUSED, ENOENT,
   * timeout, etc.). The socket is closed immediately — we're only probing
   * acceptability, not opening a session. The real GoIVMClient connection
   * happens in start() right after #waitForReady returns.
   */
  #probeSocket(): Promise<boolean> {
    return new Promise<boolean>(resolve => {
      let settled = false;
      const settle = (ok: boolean) => {
        if (settled) return;
        settled = true;
        resolve(ok);
      };
      const sock = createConnection(this.#config.socketPath);
      // Bounded probe: even a hung connect shouldn't stall the readiness
      // loop. 1s is generous — a healthy Unix-socket connect on the same
      // host is sub-millisecond.
      const timer = setTimeout(() => {
        sock.destroy();
        settle(false);
      }, 1_000);
      sock.once('connect', () => {
        clearTimeout(timer);
        sock.end();
        settle(true);
      });
      sock.once('error', () => {
        clearTimeout(timer);
        sock.destroy();
        settle(false);
      });
    });
  }
}
