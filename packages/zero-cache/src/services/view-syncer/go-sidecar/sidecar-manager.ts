// Owns the Go sidecar process lifecycle: spawn, health-ping, automatic
// restart under a sliding-window failure cap, and restart notifications so
// dependents (GoComputeBackend) can re-initialize. One shared GoIVMClient
// per zero-cache worker.

import {spawn, type ChildProcess} from 'child_process';
import {existsSync, unlinkSync} from 'fs';
import {tmpdir} from 'os';
import {join} from 'path';
import {GoIVMClient} from './go-ivm-client.ts';

/**
 * Wire protocol revision this client expects. Bumped in lockstep with
 * `sidecarProtocolRev` in `go-ivm/cmd/sidecar/main.go`. A mismatch refuses
 * to start the manager (REVIEW-final MED-CROSS-5).
 */
const EXPECTED_PROTOCOL_REV = 3;

/**
 * Cold-start init concurrency cap. When N ViewSyncers start simultaneously,
 * they all call `initEngine` → `init` + `loadRows` against the same socket.
 * Without a cap, the SQLite `SELECT *` reads stampede and memory peaks at
 * N × largest_table_bytes. This semaphore lets only a few proceed at once
 * (REVIEW-final MED-CROSS-1).
 */
const INIT_CONCURRENCY = 4;

export type SidecarLogger = (
  level: 'info' | 'warn' | 'error',
  msg: string,
  err?: unknown,
) => void;

export type SidecarConfig = {
  /** Path to the compiled go-ivm-sidecar binary. Ignored when
   *  `externallyManaged` is true (the manager does not spawn). */
  binaryPath: string;
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
};

export type SidecarStatus = 'stopped' | 'starting' | 'running' | 'restarting' | 'failed';

export type RestartListener = (epoch: number) => void | Promise<void>;

export class SidecarManager {
  readonly #config: Required<Omit<SidecarConfig, 'logger'>> & {logger: SidecarLogger};
  #proc: ChildProcess | null = null;
  #client: GoIVMClient | null = null;
  #status: SidecarStatus = 'stopped';
  #restartTimestamps: number[] = []; // for sliding-window cap
  #shutdownRequested = false;
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
   * Periodic isConnected() poll used in externallyManaged mode to detect
   * sidecar restarts (where we lose the socket connection but the manager
   * has no proc.on('exit') hook because it didn't spawn).
   */
  #healthTimer: ReturnType<typeof setInterval> | null = null;
  /** How often the health check runs in externallyManaged mode (ms). */
  static readonly #HEALTH_CHECK_MS = 2000;

  constructor(config: SidecarConfig) {
    // No console fallback: callers without a logger get silence. The syncer
    // wires a LogContext-backed logger through to here.
    const noop: SidecarLogger = () => {};
    const logger: SidecarLogger = config.logger ?? noop;
    this.#config = {
      binaryPath: config.binaryPath,
      socketPath: config.socketPath ?? join(tmpdir(), `go-ivm-${process.pid}.sock`),
      maxRestartsInWindow: config.maxRestartsInWindow ?? 5,
      restartWindowMs: config.restartWindowMs ?? 60_000,
      restartDelayMs: config.restartDelayMs ?? 1000,
      healthCheckTimeoutMs: config.healthCheckTimeoutMs ?? 5000,
      verbose: config.verbose ?? true,
      externallyManaged: config.externallyManaged ?? false,
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

    // In externally-managed mode the binary path is not used by this
    // manager (some other process owns the sidecar). Skip the existence
    // check so a typo'd binaryPath doesn't block startup in the shared
    // deployment.
    if (!this.#config.externallyManaged) {
      if (!existsSync(this.#config.binaryPath)) {
        throw new Error(
          `Go IVM sidecar binary not found at: ${this.#config.binaryPath}. ` +
            `Build it with: cd go-ivm && go build -o ${this.#config.binaryPath} ./cmd/sidecar/`,
        );
      }
    }

    this.#shutdownRequested = false;
    this.#status = 'starting';
    await this.#spawn();
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

    if (!this.#config.externallyManaged) {
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

        this.#handleRestartTrigger();
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
    // installed below and routed through the same #handleRestartTrigger
    // path so dependents see a consistent restart-event model.

    // Wait for the socket to appear and become connectable
    await this.#waitForReady();

    // Connect a fresh client
    this.#client = new GoIVMClient(this.#config.socketPath, {
      onLog: (level, msg, err) =>
        this.#config.logger(level, `client: ${msg}`, err),
    });
    await this.#client.connect();

    // Verify with ping
    const pong = await this.#client.ping();
    if (pong !== 'pong') {
      throw new Error(`Sidecar health check failed: expected 'pong', got '${pong}'`);
    }
    // Verify wire protocol version (REVIEW-final MED-CROSS-5).
    try {
      const v = await this.#client.version();
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
    } catch (err) {
      // If the version RPC isn't implemented (older sidecar), warn loudly
      // but don't refuse — operators can roll out a new client first.
      this.#config.logger(
        'warn',
        'Sidecar does not implement version RPC; assuming compatibility (consider upgrading)',
        err,
      );
    }

    this.#status = 'running';
    this.#epoch++;
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
   * Shared restart pipeline used by both:
   *   - the spawned process's `exit` handler, and
   *   - the externally-managed mode's health-check ticker on disconnect.
   *
   * Enforces the sliding-window failure cap and either schedules a retry
   * (`#spawn`) or marks the manager `failed` so callers can fall back to
   * the TS-native IVM path.
   */
  #handleRestartTrigger(): void {
    // Sliding-window restart cap: count failures in the last
    // restartWindowMs and bail if we exceed the cap.
    const now = Date.now();
    const cutoff = now - this.#config.restartWindowMs;
    this.#restartTimestamps = this.#restartTimestamps.filter(t => t > cutoff);
    this.#restartTimestamps.push(now);

    if (this.#restartTimestamps.length <= this.#config.maxRestartsInWindow) {
      this.#status = 'restarting';
      // Future waitForRunning() calls block on this fresh promise until the
      // restart settles. Without this, callers throw immediately and the
      // documented TS-fallback path becomes a client-visible Internal error.
      this.#runningPromise = new Promise<void>((res, rej) => {
        this.#runningResolve = res;
        this.#runningReject = rej;
      });
      this.#config.logger(
        'warn',
        `Restarting (failures in last ${this.#config.restartWindowMs}ms: ` +
          `${this.#restartTimestamps.length}/${this.#config.maxRestartsInWindow})`,
      );
      setTimeout(
        () =>
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
              this.#handleRestartTrigger();
            } else if (this.#proc && this.#proc.exitCode === null) {
              try {
                this.#proc.kill('SIGKILL');
              } catch (killErr) {
                this.#config.logger(
                  'error',
                  'failed to kill wedged sidecar process',
                  killErr,
                );
                this.#handleRestartTrigger();
              }
            } else {
              // Spawned mode, process already dead — proc.on('exit') will
              // have queued #handleRestartTrigger; nothing to do here.
            }
          }),
        this.#config.restartDelayMs,
      );
    } else {
      this.#status = 'failed';
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
    // appears or we give up.
    const timeoutMs = this.#config.externallyManaged
      ? Math.max(this.#config.healthCheckTimeoutMs, 30_000)
      : this.#config.healthCheckTimeoutMs;
    const deadline = Date.now() + timeoutMs;
    const pollMs = 50;

    while (Date.now() < deadline) {
      if (existsSync(this.#config.socketPath)) {
        return;
      }
      await new Promise(resolve => setTimeout(resolve, pollMs));
    }

    throw new Error(
      `Sidecar did not create socket at ${this.#config.socketPath} within ${timeoutMs}ms`,
    );
  }
}
