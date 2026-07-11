// Compute backend that PipelineDriver delegates to for the two CPU-bound
// operations: advance (genPush loop) and hydrate (initial query fetch).
// PipelineDriver remains the single source of truth for state, snapshots,
// permissions, and the row-set signature; this class only sees ASTs and diffs.
//
// Restart safety: the sidecar client is resolved through SidecarManager on
// every call, and a restart subscription invalidates #initialized so the
// next advance/hydrate re-inits via getCurrentTables before proceeding.
// Init is schema-only: the table-mode sidecar reads rows from SQLite
// directly.
//
// Feature flag: ZERO_GO_SIDECAR_ENABLED=true.

import type {ZeroConfig} from '../../../config/zero-config.ts';
import {RetryableAdvanceError} from './go-ivm-client.ts';
import type {AdvanceToHeadStreamChunk, TableData} from './go-ivm-client.ts';
import type {SidecarManager} from './sidecar-manager.ts';

export type QueryAST = unknown;

export interface TableSchemaSpec {
  columns: Record<
    string,
    {
      type: 'boolean' | 'number' | 'string' | 'null' | 'json';
      optional?: boolean | undefined;
    }
  >;
  primaryKey: string[];
}

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
  readonly #appID: string;
  /** Pull lookahead window W (see zero-config goSidecar.pullWindow). */
  readonly #pullWindow: number;
  #initialized = false;
  #initEpoch = -1;
  /**
   * Per-cgID init epoch the sidecar issues on every {@link client.init}.
   * Threaded through every subsequent mutating RPC (hydrate / advance) so the
   * sidecar rejects calls from a torn-down instance
   * whose RPC raced past a fresh init for the same cgID.
   * `-1` = not yet init'd (any call would fail validation anyway).
   */
  #sidecarInitEpoch = -1;
  /**
   * The stateVersion Go's per-CG snapshotter pinned at on the LAST
   * successful init — the frame the first hydrate reads (gen-6). The
   * PipelineDriver maxes this into the CVR hydrate-updater stateVersion so
   * rows Go hydrates from a frame ahead of TS's own snapshot pin are never
   * received under an unbumped CVR version (cvr.ts:778 teardown).
   * Undefined until init succeeds or against a pre-gen-6 sidecar.
   */
  #pinnedVersion: string | undefined = undefined;
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
    options?: {
      logger?: GoBackendLogger;
      appID?: string;
      pullWindow?: number;
    },
  ) {
    this.#manager = manager;
    this.#clientGroupID = clientGroupID;
    this.#getCurrentTables = getCurrentTables;
    this.#getCurrentQueries = getCurrentQueries;
    this.#pullWindow = Math.max(1, Math.floor(options?.pullWindow ?? 64));
    // O2: send the appID on the advanceToHead wire so the sidecar uses the
    // SHARD's appID for its snapshotter's permissions-table watch instead of
    // relying solely on the GO_IVM_APP_ID env (which an externally-managed
    // owner could set inconsistently). Empty string ⇒ Go falls back to its env.
    this.#appID = options?.appID ?? '';
    // No console fallback: callers without a logger get silence by design.
    const noop: GoBackendLogger = () => {};
    const raw = options?.logger ?? noop;
    // Wrap caller's logger to prepend cgID on every line. Without this, drift
    // recovery / restart / breaker logs from N concurrent backends look
    // identical in the syncer's stdout — operators have no way to tell which
    // CG is misbehaving. Pre-fix the only cgID hint came from the message
    // bodies where authors happened to include it; many lines didn't.
    const cgTag = `[cg=${clientGroupID}]`;
    this.#log = (level, msg, err) => raw(level, `${cgTag} ${msg}`, err);

    this.#unsubscribe = manager.onRestart(epoch =>
      this.#onSidecarRestart(epoch),
    );
  }

  get initialized(): boolean {
    return this.#initialized && this.#initEpoch === this.#manager.epoch;
  }

  /**
   * Go's snapshotter pin from the last successful init (gen-6). Only
   * meaningful while {@link initialized}; monotone across re-inits (a
   * re-init pins at the then-current head, which only grows).
   */
  get pinnedVersion(): string | undefined {
    return this.#pinnedVersion;
  }

  // Manager epoch; increments on every sidecar restart. Surfaced for callers
  // that need to detect a restart that happened mid-operation.
  get epoch(): number {
    return this.#manager.epoch;
  }

  /**
   * Resolves when this backend is in a stable state for the current
   * manager epoch — either initialized, or an init attempt has completed
   * (even unsuccessfully).
   *
   * Contract callers MUST honor: after awaiting, check `this.initialized`
   * before issuing RPCs. The resolution does NOT promise readiness; only
   * that the in-flight init (if any) has settled. The four states this
   * collapses to:
   *
   *   1. Currently initialized at the live epoch → resolve immediately.
   *   2. An init is in flight → await the in-flight init's settlement
   *      (which may resolve OR reject; we swallow rejection here so
   *      callers don't have to wrap in try/catch — they self-check
   *      `initialized` afterward).
   *   3. Destroyed → resolve immediately; callers' `initialized` check
   *      will see false and they'll fall through to TS.
   *   4. Never inited / init failed / between-epoch quiescence
   *      (#currentInitPromise null, #initialized false) → resolve
   *      immediately. The caller's `initialized` check covers this.
   *
   * Restart awareness: a sidecar restart's onRestart listener replaces
   * #currentInitPromise with the new epoch's init, so case 2 here waits
   * for the NEW one — preventing a stale "ready" mid-restart.
   * (REVIEW-final HIGH-TS-1.)
   */
  whenInitialized(): Promise<void> {
    // If a restart reinit is in progress, wait for the FULL reinit
    // (init + query re-registration) before resolving. #doInit sets
    // #initialized=true before re-registration drains, so checking
    // this.initialized alone would let batch hydrate start while Go
    // still has zero pipelines — every advance returns empty changes
    // and the client view freezes silently.
    if (this.#restartGate) {
      return this.#restartGate.catch(() => undefined);
    }
    if (this.initialized) return Promise.resolve();
    if (this.#currentInitPromise) {
      // Wrap to swallow rejection — callers self-check `initialized`
      // afterward; we don't want them to have to try/catch.
      return this.#currentInitPromise.catch(() => undefined);
    }
    return Promise.resolve();
  }

  async initEngine(tables: Record<string, TableData>): Promise<void> {
    await this.#doInit(tables);
  }

  async resetEngine(): Promise<void> {
    if (this.#initialized) {
      try {
        await this.#client().destroy(
          this.#clientGroupID,
          this.#sidecarInitEpoch,
          this.#cgOpts(),
        );
      } catch (err) {
        this.#log('warn', 'destroy before reset failed (continuing)', err);
      }
    }
    // CRIT-5: read the snapshot HERE — after the destroy round-trip and
    // immediately before reinit — not at schedule time. Capturing it early
    // (in #scheduleGoReset / #maybeResetGoBackend) meant mutations that
    // landed during the destroy + network window were missing from the
    // loaded rows, so the next #snapshotter.advance() diff applied against
    // state that didn't match the just-loaded data → drift → another reset
    // → loop. The other #reinitPerCGAndRegisterQueries call sites already
    // capture via #getCurrentTables() at reinit time; this matches them.
    const tables = this.#getCurrentTables();
    // Full rebuild: init engine + re-register queries. The bare #doInit
    // path that lived here used to leave Go with zero pipelines while TS
    // still had registered queries — every subsequent advance returned
    // empty changes and the client view froze silently. See
    // #reinitPerCGAndRegisterQueries.
    const ok = await this.#reinitPerCGAndRegisterQueries('reset', tables);
    if (!ok) {
      throw new Error('resetEngine: reinit + query re-registration failed');
    }
  }

  hydrateStreamPull(
    queryID: string,
    ast: QueryAST,
  ): AsyncIterableIterator<{
    queryID: string;
    changes: unknown[];
    timingMs: number | undefined;
    sigDelta?: string | undefined;
    final: boolean;
    chunkIndex?: number | undefined;
  }> {
    return this.hydrateManyStreamPull([{queryID, ast}]);
  }

  /**
   * Pull-mode batch hydrate (ABI v3): Go produces rows only as the returned
   * iterator is consumed; return()/throw() cancels the Go producer.
   *
   * No transparent replay here — deliberately. The consumer processes entries
   * as they arrive, so a mid-stream sidecar restart is never replay-safe
   * (a replay would double-count the delivered entries: double-XORed
   * row-set signatures and duplicate CVR rows). A restart rejects the
   * in-flight RPC → the iterator throws → the caller's hydrate-failure
   * path re-hydrates from a clean slate.
   */
  hydrateManyStreamPull(
    queries: {queryID: string; ast: QueryAST}[],
  ): AsyncIterableIterator<{
    queryID: string;
    changes: unknown[];
    timingMs: number | undefined;
    sigDelta?: string | undefined;
    final: boolean;
    chunkIndex?: number | undefined;
  }> {
    return this.#client().addQueriesStreamPull(
      this.#clientGroupID,
      queries,
      this.#sidecarInitEpoch,
      {...this.#cgOpts(), window: this.#pullWindow},
    );
  }

  // Streaming push-based advance (the production advance path). Go
  // independently leapfrogs its Snapshotter, derives its own diff, drives its
  // engine, and streams RowChanges per row over the in-process boundary.
  //
  // Clean-retryable failures (RetryableAdvanceError — Go failed BEFORE any
  // state moved; the call is idempotent) are retried IN PLACE only before this
  // iterator yields anything. Once any header/row/final is observable, replay
  // would duplicate streamed effects, so the error surfaces to the caller.
  async *advanceToHeadStreamChunks(abortBudget?: {
    totalHydrationTimeMs: number;
    suppressAbort?: boolean;
  }): AsyncIterableIterator<AdvanceToHeadStreamChunk> {
    const delaysMs = [100, 500, 2000];
    for (let attempt = 0; ; attempt++) {
      let yielded = false;
      try {
        if (this.#restartGate) await this.#restartGate;
        const stream = this.#client().advanceToHeadStreamChunks(
          this.#clientGroupID,
          this.#sidecarInitEpoch,
          this.#appID,
          {
            ...this.#cgOpts(),
            window: this.#pullWindow,
            ...(abortBudget ? {abortBudget} : {}),
          },
        );
        for await (const chunk of stream) {
          yielded = true;
          yield chunk;
        }
        return;
      } catch (err) {
        if (
          yielded ||
          !(err instanceof RetryableAdvanceError) ||
          attempt >= delaysMs.length
        ) {
          throw err;
        }
        const delay = Math.round(delaysMs[attempt] * (0.5 + Math.random()));
        this.#log(
          'warn',
          `advanceToHeadStream failed clean before streaming; retrying in ` +
            `place (attempt ${attempt + 1}/${delaysMs.length}, ${delay}ms): ` +
            err.message,
        );
        await new Promise<void>(resolve => setTimeout(resolve, delay));
      }
    }
  }

  // Pinned cgID for the client's per-group fairness semaphore.
  #cgOpts(): {clientGroupID: string} {
    return {clientGroupID: this.#clientGroupID};
  }

  // Rejects on failure — callers either ignore or surface depending on context.
  async removeQuery(queryID: string): Promise<void> {
    await this.#client().removeQuery(
      this.#clientGroupID,
      queryID,
      this.#sidecarInitEpoch,
      this.#cgOpts(),
    );
  }

  // Resolves when no per-CG recovery is in flight. Callers that dispatch
  // a new query/advance can await this BEFORE deciding TS vs Go, so a
  // brief recovery window doesn't cause a fall-through to TS that
  // creates a phantom TS pipeline (audit H7). Resolves immediately when
  // no recovery is active.
  async whenRecovered(): Promise<void> {
    if (this.#restartGate) {
      await this.#restartGate;
    }
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
        await this.#client().destroy(
          this.#clientGroupID,
          this.#sidecarInitEpoch,
          this.#cgOpts(),
        );
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
    // Without this, restart/reset paths can both call #doInit concurrently
    // after a sidecar restart. Two inits
    // interleave their init + hydrate re-register RPCs at the wire level,
    // each fresh init landing between the other's table-source bindings → engine
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

    // Schema-only init: the table-mode sidecar reads rows from SQLite
    // directly, so no row contents ever ship (strip defensively — the
    // tables callback already produces empty row arrays).
    const tablesNoRows: Record<string, TableData> = {};
    for (const [name, t] of Object.entries(tables)) {
      tablesNoRows[name] = {
        columns: t.columns,
        primaryKey: t.primaryKey,
        uniqueKeys: t.uniqueKeys,
        minRowVersion: t.minRowVersion,
        rows: [],
      };
    }
    const initResult = await client.init(
      this.#clientGroupID,
      {tables: tablesNoRows},
      this.#cgOpts(),
    );
    // Stash the sidecar's per-cgID epoch so all subsequent mutating RPCs
    // from THIS instance carry the matching epoch. A torn-down predecessor
    // instance for the same cgID still holds an older epoch; its in-flight
    // RPCs land on the sidecar AFTER this init and get rejected with
    // rpcCodeStaleInitEpoch instead of corrupting state.
    this.#sidecarInitEpoch = initResult.initEpoch;
    if (initResult.version !== undefined) {
      this.#pinnedVersion = initResult.version;
    } else {
      // Pre-gen-6 sidecar: the CVR hydrate stamp falls back to TS's own
      // version and the version-skew window stays open. Loud, once per init.
      this.#log(
        'warn',
        'init returned no snapshotter pin version (old sidecar?); ' +
          'CVR hydrate stamps fall back to the TS snapshot version',
      );
    }

    this.#initialized = true;
    this.#initEpoch = epoch;
  }

  async #onSidecarRestart(epoch: number): Promise<void> {
    if (this.#destroyed) return;
    this.#log('info', `sidecar restarted to epoch ${epoch}; re-initializing`);
    this.#currentInitPromise = null;
    const ok = await this.#reinitPerCGAndRegisterQueries(
      `sidecar-restart-epoch-${epoch}`,
      this.#getCurrentTables(),
    );
    if (ok) {
      this.#log('info', `re-initialized after restart (epoch ${epoch})`);
    }
    // On failure, the helper already left #initialized=false so
    // PipelineDriver falls back to TS.
  }

  // Shared per-CG reinit path. Used by:
  //   1. #onSidecarRestart  (manager-level restart)
  //   2. resetEngine        (intentional reset from PipelineDriver)
  //
  // Both paths used to call only #doInit, which loads sources but
  // does NOT re-register queries — leaving Go with zero pipelines while
  // TS still had registered queries → every subsequent advance returned
  // {changes:[], timings:[]} and the client view froze silently with
  // no error logged anywhere. Only #onSidecarRestart got it right; the
  // per-CG recovery paths shared the same bug. Centralizing here so the
  // contract is unified.
  //
  // Concurrency: holds #restartGate for the duration so concurrent
  // advance/hydrate from PipelineDriver await instead of racing the
  // empty-engine window. If another reinit is already in flight, we
  // await its gate and trust its outcome rather than starting a second
  // reinit — only one rebuild is ever in progress per backend.
  //
  // Returns: true if reinit completed and queries are registered (or
  // there were no queries); false if any step failed (caller decides
  // whether to surface the original error and fall back to TS).
  async #reinitPerCGAndRegisterQueries(
    reason: string,
    tables: Record<string, TableData>,
  ): Promise<boolean> {
    if (this.#destroyed) return false;

    // Coalesce concurrent reinit requests: if another reinit set the
    // gate, await it and report its outcome via #initialized.
    if (this.#restartGate) {
      await this.#restartGate;
      return this.initialized;
    }

    this.#initialized = false;
    let resolveGate!: () => void;
    this.#restartGate = new Promise<void>(resolve => {
      resolveGate = resolve;
    });

    try {
      await this.#doInit(tables);

      // Re-register the queries this client group had before the
      // recovery event. Without this, Go has zero pipelines after
      // reinit while TS thinks they're still registered.
      //
      // Snapshot getCurrentQueries() AFTER #doInit so any queries that
      // were destroyed during the recovery window aren't carried
      // forward. The hydrate result is discarded — we only need Go's
      // internal pipeline state rebuilt; TS already owns the client
      // view and the next advance will produce correct deltas against
      // both sides.
      const queries = this.#getCurrentQueries();
      if (queries.length > 0) {
        try {
          for await (const _ of this.hydrateManyStreamPull(queries)) {
            // Re-register only: drain the prod pull stream so Go rebuilds
            // pipeline state, but discard rows because TS already owns CVR.
          }
          this.#log(
            'info',
            `[${reason}] re-registered ${queries.length} queries`,
          );
        } catch (qerr) {
          this.#initialized = false;
          this.#log('error', `[${reason}] re-register queries failed`, qerr);
          return false;
        }
      } else {
        this.#log(
          'info',
          `[${reason}] re-init complete (no queries to register)`,
        );
      }
      return true;
    } catch (err) {
      this.#initialized = false;
      this.#log('error', `[${reason}] re-init failed`, err);
      return false;
    } finally {
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

// Pull lookahead window W (see zero-config goSidecar.pullWindow). Clamped
// to ≥ 1: W=0 would strand the stream (zero opening credit and no consumer
// deliveries to trigger top-ups).
export function goPullWindow(
  config: Pick<ZeroConfig, 'goSidecar'> | undefined,
): number {
  const w = config?.goSidecar?.pullWindow ?? 64;
  return Math.max(1, Math.floor(w));
}

// Returns null when the manager isn't `running` so the caller falls back
// to the TS path; `getCurrentTables` is invoked both on initial init and
// after each sidecar restart.
export function createGoComputeBackend(
  sidecarManager: SidecarManager,
  clientGroupID: string,
  getCurrentTables: () => Record<string, TableData>,
  getCurrentQueries: GetCurrentQueries,
  options?: {logger?: GoBackendLogger; appID?: string; pullWindow?: number},
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
