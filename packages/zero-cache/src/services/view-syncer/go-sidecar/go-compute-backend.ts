// Compute backend that PipelineDriver delegates to for the two CPU-bound
// operations: advance (genPush loop) and hydrate (initial query fetch).
// PipelineDriver remains the single source of truth for state, snapshots,
// permissions, and the row-set signature; this class only sees ASTs and diffs.
//
// Restart safety: the sidecar client is resolved through SidecarManager on
// every call, and a restart subscription invalidates #initialized so the
// next advance/hydrate re-inits via getCurrentTables before proceeding.
// Init chunking: schema-only `init`, then `loadRows` in batches so no frame
// approaches the 64MB cap.
//
// Feature flag: ZERO_GO_SIDECAR_ENABLED=true.

import type {ZeroConfig} from '../../../config/zero-config.ts';
import {DriftError} from './go-ivm-client.ts';
import type {
  AdvanceResult as GoAdvanceResult,
  HydrateResult as GoHydrateResult,
  SnapshotChange,
  TableData,
} from './go-ivm-client.ts';
import type {SidecarManager} from './sidecar-manager.ts';

export type QueryAST = unknown;

export interface TableSchemaSpec {
  columns: Record<string, {type: 'boolean' | 'number' | 'string' | 'null' | 'json'; optional?: boolean | undefined}>;
  primaryKey: string[];
}

/** Max rows per loadRows RPC. Picked so each batch stays well under 64MB. */
const DEFAULT_LOAD_BATCH_ROWS = 5_000;

export type GoBackendLogger = (
  level: 'info' | 'warn' | 'error',
  msg: string,
  err?: unknown,
) => void;

/** Callback returning the queries this backend should re-register after a restart-driven re-init. */
export type GetCurrentQueries = () => {queryID: string; ast: unknown}[];

export class GoComputeBackend {
  readonly #manager: SidecarManager;
  readonly #clientGroupID: string;
  readonly #getCurrentTables: () => Record<string, TableData>;
  readonly #getCurrentQueries: GetCurrentQueries;
  readonly #log: GoBackendLogger;
  readonly #loadBatchRows: number;
  #initialized = false;
  #initEpoch = -1;
  /**
   * Per-cgID init epoch the sidecar issues on every {@link client.init}.
   * Threaded through every subsequent mutating RPC (loadRows / addQuery* /
   * advance*) so the sidecar rejects calls from a torn-down instance
   * whose RPC raced past a fresh init for the same cgID.
   * `-1` = not yet init'd (any call would fail validation anyway).
   */
  #sidecarInitEpoch = -1;
  /** Promise resolving when the *current* epoch's init finishes (resolve OR reject). */
  #currentInitPromise: Promise<void> | null = null;
  /**
   * In-flight restart promise. While set, advance/hydrate calls await it
   * before issuing the RPC — prevents advance-against-empty-engine race
   * during the window between source re-init and pipeline re-attach
   * (REVIEW-final MED-CROSS-6 + restart correctness gap).
   */
  #restartGate: Promise<void> | null = null;
  #unsubscribe: (() => void) | null = null;
  #destroyed = false;

  constructor(
    manager: SidecarManager,
    clientGroupID: string,
    getCurrentTables: () => Record<string, TableData>,
    getCurrentQueries: GetCurrentQueries,
    options?: {logger?: GoBackendLogger; loadBatchRows?: number},
  ) {
    this.#manager = manager;
    this.#clientGroupID = clientGroupID;
    this.#getCurrentTables = getCurrentTables;
    this.#getCurrentQueries = getCurrentQueries;
    // No console fallback: callers without a logger get silence by design.
    const noop: GoBackendLogger = () => {};
    this.#log = options?.logger ?? noop;
    this.#loadBatchRows = options?.loadBatchRows ?? DEFAULT_LOAD_BATCH_ROWS;

    this.#unsubscribe = manager.onRestart(epoch => this.#onSidecarRestart(epoch));
  }

  get initialized(): boolean {
    return this.#initialized && this.#initEpoch === this.#manager.epoch;
  }

  // Manager epoch; increments on every sidecar restart. Surfaced for callers
  // (e.g. the drift audit) that need to detect a restart that happened mid-
  // operation even when their Snapshotter-version check would otherwise pass.
  get epoch(): number {
    return this.#manager.epoch;
  }

  // Restart-aware: a sidecar restart invalidates the prior init promise and
  // starts a new one — this returns the in-flight one so callers don't get
  // a stale "ready" mid-restart (REVIEW-final HIGH-TS-1).
  whenInitialized(): Promise<void> {
    if (this.initialized) return Promise.resolve();
    return this.#currentInitPromise ?? Promise.resolve();
  }

  async initEngine(tables: Record<string, TableData>): Promise<void> {
    await this.#doInit(tables);
  }

  async resetEngine(tables: Record<string, TableData>): Promise<void> {
    if (this.#initialized) {
      try {
        await this.#client().destroy(this.#clientGroupID, this.#cgOpts());
      } catch (err) {
        this.#log('warn', 'destroy before reset failed (continuing)', err);
      }
    }
    this.#initialized = false;
    await this.#doInit(tables);
  }

  async hydrate(queryID: string, ast: QueryAST): Promise<GoHydrateResult> {
    if (this.#restartGate) await this.#restartGate;
    return this.#withReinitRetry(() =>
      this.#client().addQuery(this.#clientGroupID, queryID, ast, this.#sidecarInitEpoch, this.#cgOpts()),
    );
  }

  async hydrateMany(queries: {queryID: string; ast: QueryAST}[]): Promise<GoHydrateResult[]> {
    if (this.#restartGate) await this.#restartGate;
    return this.#withReinitRetry(() =>
      this.#client().addQueries(this.#clientGroupID, queries, this.#sidecarInitEpoch, this.#cgOpts()),
    );
  }

  // Like {@link hydrateMany} but `onResult` fires per query as each Go
  // goroutine finishes. Resolves on the terminal "done" frame.
  async hydrateManyStream(
    queries: {queryID: string; ast: QueryAST}[],
    onResult: (r: {queryID: string; changes: unknown[]; timingMs: number | undefined}) => void,
  ): Promise<void> {
    if (this.#restartGate) await this.#restartGate;
    await this.#withReinitRetry(() =>
      this.#client().addQueriesStream(this.#clientGroupID, queries, this.#sidecarInitEpoch, onResult, this.#cgOpts()),
    );
  }

  advance(changes: SnapshotChange[]): Promise<GoAdvanceResult> {
    return this.#advanceWithRecovery(() =>
      this.#client().advance(this.#clientGroupID, changes, this.#sidecarInitEpoch, this.#cgOpts()),
    );
  }

  // Streaming variant of {@link advance}. Same on-the-wire semantics but
  // Go ships partial frames during the call; the underlying client
  // reassembles into the same {@link GoAdvanceResult} shape so callers
  // (PipelineDriver#goAdvance) see no behavioral difference.
  //
  // Wins: Go-side memory pressure relieved on large diffs; first wire
  // bytes flow as soon as `advanceChunkSize` rows accumulate on Go.
  advanceStream(changes: SnapshotChange[]): Promise<GoAdvanceResult> {
    return this.#advanceWithRecovery(() =>
      this.#client().advanceStream(this.#clientGroupID, changes, this.#sidecarInitEpoch, this.#cgOpts()),
    );
  }

  // Shared recovery path for both advance and advanceStream. Two error
  // classes:
  //   1. Sidecar died/disconnected mid-call. Wait for re-init via the
  //      onRestart listener, then DROP this advance — Go's new state
  //      after init+loadRows already reflects the post-advance snapshot.
  //      Retrying the diff against the freshly-initialized engine would
  //      double-apply and panic with "Row not found" (sidecar-kill drill
  //      finding). Clients miss this delta from the IVM stream, but Go's
  //      state is consistent and the next advance produces correct
  //      deltas.
  //   2. "engine not initialized" — same race as #withReinitRetry but
  //      with the same "don't replay" rule.
  async #advanceWithRecovery(
    call: () => Promise<GoAdvanceResult>,
  ): Promise<GoAdvanceResult> {
    if (this.#restartGate) await this.#restartGate;
    try {
      return await call();
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);

      // Drift recovery (option 3 / self-heal on miss): Go detected a
      // missing row on Edit/Remove (or duplicate Add) and bailed out of
      // this advance BEFORE mutating any state. We treat it like a
      // sidecar restart — force a fresh init from current SQLite truth
      // and DROP this advance. Clients miss exactly one delta and
      // resync against the post-init state. No retry: replaying the
      // diff against the freshly-loaded Go state would either
      // double-apply (the missing row is now present from SQLite) or
      // re-trip the same drift.
      if (err instanceof DriftError) {
        this.#log(
          'warn',
          `Go drift detected (${err.op} on ${err.table} pk=${JSON.stringify(err.pk)} has=${err.hasCount}); ` +
            `re-initing engine from current snapshot, dropping this advance`,
        );
        this.#initialized = false;
        try {
          await this.#doInit(this.#getCurrentTables());
        } catch (initErr) {
          this.#log('error', 're-init after drift failed', initErr);
          throw err; // surface original drift so caller's fallback engages
        }
        return {changes: [], timings: []};
      }

      const sidecarUnavailable =
        this.#manager.status !== 'running' ||
        msg.includes('Sidecar is not running') ||
        msg.includes('Connection closed');
      if (sidecarUnavailable) {
        try {
          await this.#manager.waitForRunning();
        } catch {
          throw err;
        }
        await this.whenInitialized();
        if (!this.initialized) {
          try {
            await this.#doInit(this.#getCurrentTables());
          } catch (initErr) {
            this.#log('error', 're-init after sidecar-restart failed', initErr);
            throw err;
          }
        }
        this.#log(
          'warn',
          'advance dropped after sidecar restart; Go state re-init from current snapshot',
        );
        return {changes: [], timings: []};
      }
      if (msg.includes('engine not initialized')) {
        this.#initialized = false;
        try {
          await this.#doInit(this.#getCurrentTables());
        } catch (initErr) {
          this.#log('error', 're-init after engine-not-initialized failed', initErr);
          throw err;
        }
        return {changes: [], timings: []};
      }
      throw err;
    }
  }

  // Pinned cgID for the client's per-group fairness semaphore.
  #cgOpts(): {clientGroupID: string} {
    return {clientGroupID: this.#clientGroupID};
  }

  // Two retry-worthy error classes:
  //   1. "engine not initialized" — sidecar restarted after our onRestart
  //      listener fired the re-init RPC; race window between the two. Force
  //      re-init then retry (REVIEW-final MED-TS-4).
  //   2. "Sidecar is not running" — getClient() threw because the manager is
  //      mid-restart. Without this branch, in-flight advance/hydrate RPCs
  //      propagate as Internal WebSocket errors to clients (sidecar-kill
  //      drill finding). Await the manager's running state and retry once.
  async #withReinitRetry<T>(fn: () => Promise<T>): Promise<T> {
    try {
      return await fn();
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);

      // Restart-window detection: when the sidecar dies, RPCs surface as
      // one of "Sidecar is not running" (getClient threw) or "Connection
      // closed" (socket dropped mid-RPC). Don't string-match every variant
      // — if the manager isn't `running` right now, treat any error as
      // restart-related and retry once the engine is rebuilt.
      const sidecarUnavailable =
        this.#manager.status !== 'running' ||
        msg.includes('Sidecar is not running') ||
        msg.includes('Connection closed');
      if (sidecarUnavailable) {
        try {
          await this.#manager.waitForRunning();
        } catch {
          // Manager hit terminal state — surface the original so the
          // caller can decide whether to fall back.
          throw err;
        }
        // Sidecar process is back, but THIS backend's per-clientGroup
        // engine isn't initialized until the onRestart listener finishes
        // init+loadRows. Retrying now would hit a freshly-spawned Go with
        // no rows for this group and silently return empty results.
        //   (a) listener already started: whenInitialized() awaits it.
        //   (b) listener microtask hasn't fired yet: whenInitialized()
        //       returns Promise.resolve() and `initialized` stays false,
        //       so we force init ourselves (idempotent via withInitSlot).
        await this.whenInitialized();
        if (!this.initialized) {
          try {
            await this.#doInit(this.#getCurrentTables());
          } catch (initErr) {
            this.#log('error', 're-init after sidecar-restart failed', initErr);
            throw err;
          }
        }
        return await fn();
      }

      if (!msg.includes('engine not initialized')) throw err;
      this.#log(
        'warn',
        'caught "engine not initialized" from Go; forcing re-init and retrying',
      );
      this.#initialized = false;
      try {
        await this.#doInit(this.#getCurrentTables());
      } catch (initErr) {
        this.#log('error', 're-init after engine-not-initialized failed', initErr);
        throw err; // surface original
      }
      return await fn();
    }
  }

  // Rejects on failure — callers either ignore or surface depending on context.
  async removeQuery(queryID: string): Promise<void> {
    await this.#client().removeQuery(this.#clientGroupID, queryID, this.#sidecarInitEpoch, this.#cgOpts());
  }

  // Roll the leaf source's pinned read tx so subsequent Fetches see the
  // current WAL frame instead of the snapshot pinned since the last
  // Push. Called by the drift audit before its comparison reads. No-op
  // on MemorySource (sidecar handles the type switch).
  async refreshSnapshot(): Promise<void> {
    await this.#client().refreshSnapshot(
      this.#clientGroupID,
      this.#sidecarInitEpoch,
      this.#cgOpts(),
    );
  }

  async destroy(): Promise<void> {
    this.#destroyed = true;
    if (this.#unsubscribe) {
      this.#unsubscribe();
      this.#unsubscribe = null;
    }
    if (this.#initialized) {
      this.#initialized = false;
      try {
        await this.#client().destroy(this.#clientGroupID, this.#cgOpts());
      } catch (err) {
        // Best-effort: the sidecar may already be gone.
        this.#log('warn', 'destroy failed (ignoring)', err);
      }
    }
  }

  // --- Private ---

  #client() {
    // Lazy resolution: always go through the manager so we pick up the
    // post-restart client, not a stale reference captured at construction.
    return this.#manager.getClient();
  }

  async #doInit(tables: Record<string, TableData>): Promise<void> {
    // Dedup: if an init is already in flight for this backend, reuse it.
    // Without this, the onRestart listener + the #withReinitRetry path can
    // BOTH call #doInit concurrently after a sidecar restart. Two inits
    // interleave their init+loadRows+addQueries RPCs at the wire level,
    // each fresh init landing between the other's loadRows chunks → engine
    // ends up with duplicate row sets (sidecar-kill drill caught this:
    // audit saw ts_count=N, go_count=2N exactly).
    if (this.#currentInitPromise) {
      return this.#currentInitPromise;
    }
    // Track the in-flight init so whenInitialized() callers wait for the
    // CURRENT epoch's init, not a stale resolved promise from a prior
    // epoch (REVIEW-final HIGH-TS-1).
    // Concurrency-cap across backends via the SidecarManager's slot
    // semaphore so a wave of view-syncer startups doesn't stampede SQLite
    // reads (REVIEW-final MED-CROSS-1).
    const initPromise = this.#manager.withInitSlot(() => this.#runInit(tables));
    this.#currentInitPromise = initPromise;
    try {
      await initPromise;
    } finally {
      if (this.#currentInitPromise === initPromise) {
        this.#currentInitPromise = null;
      }
    }
  }

  async #runInit(tables: Record<string, TableData>): Promise<void> {
    const client = this.#client();
    // Capture epoch BEFORE the RPC so we can detect a restart that races
    // with init (the post-restart init will re-run with a fresh epoch).
    const epoch = this.#manager.epoch;

    // Step 1: send schema only — rows go through loadRows so we never blow
    // past the 64MB frame cap on large tables (REVIEW-ts-integration CRITICAL-2).
    const tablesNoRows: Record<string, TableData> = {};
    for (const [name, t] of Object.entries(tables)) {
      tablesNoRows[name] = {
        columns: t.columns,
        primaryKey: t.primaryKey,
        uniqueKeys: t.uniqueKeys,
        rows: [],
      };
    }
    const initResult = await client.init(
      this.#clientGroupID,
      {tables: tablesNoRows},
      this.#cgOpts(),
    );
    // Stash the sidecar's per-cgID epoch BEFORE any loadRows so all
    // subsequent mutating RPCs from THIS instance carry the matching
    // epoch. A torn-down predecessor instance for the same cgID still
    // holds an older epoch; its in-flight RPCs land on the sidecar
    // AFTER this init and get rejected with rpcCodeStaleInitEpoch
    // instead of corrupting state.
    this.#sidecarInitEpoch = initResult.initEpoch;

    // Step 2: stream rows for each table in bounded batches. If any batch
    // fails (timeout, decode error, process didn't die but the RPC tripped),
    // the Go side is left with a half-loaded engine. Destroy it so the next
    // init attempt starts clean instead of stacking onto inconsistent state
    // (REVIEW-final MED-TS-3).
    try {
      for (const [name, t] of Object.entries(tables)) {
        if (t.rows.length === 0) continue;
        for (let i = 0; i < t.rows.length; i += this.#loadBatchRows) {
          const batch = t.rows.slice(i, i + this.#loadBatchRows);
          await client.loadRows(
            this.#clientGroupID,
            name,
            batch,
            this.#sidecarInitEpoch,
            this.#cgOpts(),
          );
        }
      }
    } catch (err) {
      this.#log('warn', 'loadRows failed mid-init; destroying partial engine to clear state', err);
      try {
        await client.destroy(this.#clientGroupID, this.#cgOpts());
      } catch (destroyErr) {
        // Best-effort — surface the original error regardless.
        this.#log('warn', 'destroy after partial init also failed', destroyErr);
      }
      throw err;
    }

    this.#initialized = true;
    this.#initEpoch = epoch;
  }

  async #onSidecarRestart(epoch: number): Promise<void> {
    if (this.#destroyed) return;
    this.#initialized = false;
    this.#log('info', `sidecar restarted to epoch ${epoch}; re-initializing`);
    // Block all advance / hydrate RPCs until reinit + query re-registration
    // completes. Without this gate, an in-flight advance from PipelineDriver
    // races against the empty-engine window and silently returns empty
    // (the bug behind MED-CROSS-6 + the post-restart query-loss correctness gap).
    let resolveGate!: () => void;
    this.#restartGate = new Promise<void>(resolve => {
      resolveGate = resolve;
    });
    try {
      const tables = this.#getCurrentTables();
      await this.#doInit(tables);
      // Re-register the queries this client group had before the restart.
      // Without this, Go has zero pipelines after reinit while TS thinks
      // they're still registered → all advances silently return empty.
      const queries = this.#getCurrentQueries();
      if (queries.length > 0) {
        try {
          await this.#client().addQueries(
            this.#clientGroupID,
            queries,
            this.#sidecarInitEpoch,
            this.#cgOpts(),
          );
          this.#log(
            'info',
            `re-registered ${queries.length} queries after restart (epoch ${epoch})`,
          );
        } catch (qerr) {
          // Pipeline rebuild failure leaves Go inconsistent with TS state.
          // Mark uninitialized so dispatch falls back to TS until next restart.
          this.#initialized = false;
          this.#log('error', 're-register queries after restart failed', qerr);
          throw qerr;
        }
      }
      this.#log('info', `re-initialized after restart (epoch ${epoch})`);
    } catch (err) {
      // Leave #initialized=false so PipelineDriver falls back to TS until
      // the next restart attempt or operational intervention.
      this.#log('error', `re-init after restart failed (epoch ${epoch})`, err);
    } finally {
      // Release the gate so queued advance/hydrate calls can proceed
      // (or fall back to TS based on #initialized state).
      this.#restartGate = null;
      resolveGate();
    }
  }
}

// --- Config accessors ---
//
// These mirror fields under `config.goSidecar` and accept a partial config so
// callers don't have to thread the full ZeroConfig. Undefined config → off,
// which is what tests want.

export function isGoSidecarEnabled(
  config: Pick<ZeroConfig, 'goSidecar'> | undefined,
): boolean {
  return config?.goSidecar?.enabled === true;
}

export function isGoShadowMode(
  config: Pick<ZeroConfig, 'goSidecar'> | undefined,
): boolean {
  return config?.goSidecar?.shadowMode === true;
}

export function isGoShadowVerbose(
  config: Pick<ZeroConfig, 'goSidecar'> | undefined,
): boolean {
  return config?.goSidecar?.shadowVerbose === true;
}

// Returns 0 when the audit should be off — either Go is disabled, shadow mode
// already covers it, or the interval is explicitly zeroed in config.
export function goDriftAuditIntervalMs(
  config: Pick<ZeroConfig, 'goSidecar'> | undefined,
): number {
  if (!isGoSidecarEnabled(config) || isGoShadowMode(config)) return 0;
  const ms = config?.goSidecar?.driftAuditIntervalMs ?? 0;
  return ms > 0 ? ms : 0;
}

// Whether the drift audit should run a raw SQL query on the replica as a
// third opinion. Defaults to true (matches the schema default); pass false
// to skip the SQL re-query and fall back to TS-vs-Go set comparison only.
export function goDriftAuditSqlGroundTruth(
  config: Pick<ZeroConfig, 'goSidecar'> | undefined,
): boolean {
  return config?.goSidecar?.driftAuditSqlGroundTruth !== false;
}

// Returns null when the manager isn't `running` so the caller falls back
// to the TS path; `getCurrentTables` is invoked both on initial init and
// after each sidecar restart.
export function createGoComputeBackend(
  sidecarManager: SidecarManager,
  clientGroupID: string,
  getCurrentTables: () => Record<string, TableData>,
  getCurrentQueries: GetCurrentQueries,
  options?: {logger?: GoBackendLogger; loadBatchRows?: number},
): GoComputeBackend | null {
  try {
    if (sidecarManager.status !== 'running') return null;
    // Touch getClient to fail-fast if the manager refuses (status race).
    sidecarManager.getClient();
    return new GoComputeBackend(
      sidecarManager,
      clientGroupID,
      getCurrentTables,
      getCurrentQueries,
      options,
    );
  } catch {
    return null;
  }
}
