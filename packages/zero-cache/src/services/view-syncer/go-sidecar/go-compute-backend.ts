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
  AdvanceToHeadResult,
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

/**
 * Drift-loop circuit breaker. Without this, a pathological steady-state where
 * every advance trips a Go DriftError → full reinit (loadRows + readdQueries
 * for the whole CG state) → next advance trips again would peg the sidecar's
 * IO/CPU indefinitely. Each reinit is seconds-long for large CGs so even a
 * modest rate is destructive.
 *
 * Breaker semantics: count drift-driven reinit events in a sliding
 * `DRIFT_BREAKER_WINDOW_MS` window. If the count crosses the threshold, mark
 * the backend uninitialized for `DRIFT_BREAKER_COOLDOWN_MS` so the
 * PipelineDriver's dispatch falls through to the TS-native path entirely.
 * Cooldown auto-clears; legitimate Go path is restored without manual
 * intervention. A second drift loop after cooldown trips the breaker again,
 * giving operators a clean metric to alert on (sidecar_drift_breaker_open).
 */
const DRIFT_BREAKER_WINDOW_MS = 60_000;
const DRIFT_BREAKER_THRESHOLD = 5;
const DRIFT_BREAKER_COOLDOWN_MS = 300_000;

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
  readonly #appID: string;
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
  /** Sliding-window timestamps of drift-driven reinits. See breaker constants. */
  #driftReinitTimestamps: number[] = [];
  /** When non-zero, breaker is open until Date.now() >= this value. */
  #driftBreakerOpenUntil = 0;

  constructor(
    manager: SidecarManager,
    clientGroupID: string,
    getCurrentTables: () => Record<string, TableData>,
    getCurrentQueries: GetCurrentQueries,
    options?: {
      logger?: GoBackendLogger;
      loadBatchRows?: number;
      appID?: string;
    },
  ) {
    this.#manager = manager;
    this.#clientGroupID = clientGroupID;
    this.#getCurrentTables = getCurrentTables;
    this.#getCurrentQueries = getCurrentQueries;
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
    this.#loadBatchRows = options?.loadBatchRows ?? DEFAULT_LOAD_BATCH_ROWS;

    this.#unsubscribe = manager.onRestart(epoch => this.#onSidecarRestart(epoch));
  }

  get initialized(): boolean {
    // Drift-loop breaker overrides ready-state: while open, callers see Go as
    // not-initialized and the PipelineDriver dispatch falls through to TS
    // until cooldown clears. Reading Date.now() per dispatch is cheap; the
    // alternative (timer that flips a flag) adds setTimeout pressure.
    if (this.#driftBreakerOpenUntil !== 0) {
      if (Date.now() < this.#driftBreakerOpenUntil) return false;
      this.#driftBreakerOpenUntil = 0;
      this.#driftReinitTimestamps.length = 0;
      this.#log('info', 'drift-loop breaker cooldown elapsed; Go path re-engaged');
    }
    return this.#initialized && this.#initEpoch === this.#manager.epoch;
  }

  // Manager epoch; increments on every sidecar restart. Surfaced for callers
  // (e.g. the drift audit) that need to detect a restart that happened mid-
  // operation even when their Snapshotter-version check would otherwise pass.
  get epoch(): number {
    return this.#manager.epoch;
  }

  // Source mode the sidecar reported at handshake ('memory' | 'table';
  // undefined = older sidecar, treat as memory). In table mode the init
  // path must NOT materialize + ship row contents — the sidecar reads
  // SQLite directly and discards them (loadRows is a no-op).
  get sidecarSourceMode(): string | undefined {
    return this.#manager.sidecarSourceMode;
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
        await this.#client().destroy(this.#clientGroupID, this.#cgOpts());
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

  async hydrate(queryID: string, ast: QueryAST): Promise<GoHydrateResult> {
    if (this.#restartGate) await this.#restartGate;
    // Route through the STREAMING single-query path: a wide-result query
    // hydrated via non-streaming addQuery encodes to one unbounded frame that
    // can exceed the wire cap, get SKIPPED by the TS reader, and orphan the RPC
    // into a 60s timeout (the cold-hydrate freeze). addQueryStream chunks it.
    return this.#withReinitRetry(() =>
      this.#client().addQueryStream(this.#clientGroupID, queryID, ast, this.#sidecarInitEpoch, this.#cgOpts()),
    );
  }

  // Batch-hydrate `queries`; `onResult` fires per query as each Go
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

  // Push-mode advance (streaming): ship a SnapshotChange diff to Go.
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

  // advanceToHead: ask Go to derive its OWN snapshot diff (no changes payload).
  // Read-only on the Go side (it only leapfrogs the Snapshotter; it does NOT
  // drive the engine), so it does NOT go through #advanceWithRecovery's
  // drop-the-advance drift handling — a failure just surfaces to the caller,
  // which (in the P1 shadow compare) logs it and skips the comparison.
  async advanceToHead(): Promise<AdvanceToHeadResult> {
    try {
      return await this.#withReinitRetry(() =>
        this.#client().advanceToHead(
          this.#clientGroupID,
          this.#sidecarInitEpoch,
          this.#appID,
          this.#cgOpts(),
        ),
      );
    } catch (err) {
      // F4: drive-mode (advanceToHead) drift must feed the SAME drift-loop
      // circuit breaker as push-mode advance. #advanceWithRecovery records +
      // trips the breaker on DriftError, but advanceToHead routes through
      // #withReinitRetry — which only handles sidecar-restart / not-initialized
      // and re-throws everything else — so a drive-mode DriftError escaped
      // unaccounted and the breaker was DEAD in the deployed trigger mode (a
      // pathological drift loop would reset-loop forever with nothing
      // tripping). Record the drift + trip the breaker if the rate is
      // pathological, then re-throw so the caller's classify path escalates to
      // a full pipeline reset (F2) — which both reinits Go AND re-hydrates the
      // client view (a bare Go reinit would leave the F2 gap). No separate
      // reinit here: the reset path owns the rebuild, avoiding a double reinit.
      // When the breaker trips, `initialized` flips false and the next advance
      // degrades to TS via the F1 path.
      if (err instanceof DriftError) {
        this.#recordDriftReinit();
        this.#maybeTripDriftBreaker();
      }
      throw err;
    }
  }

  // advanceToHeadStream: streaming variant of {@link advanceToHead} for DRIVE
  // mode (finding F5). Identical contract — same #withReinitRetry wrapper and
  // the SAME F4 drift-breaker accounting on DriftError — but the client chunks
  // the engine RowChanges over the wire so a bulk backfill / mass UPDATE can't
  // blow the 64MB single-frame cap (which the TS receive loop SKIPS, orphaning
  // the RPC into a timeout). Reassembles into the same AdvanceToHeadResult, so
  // PipelineDriver#goPrimaryAdvance consumes it identically.
  async advanceToHeadStream(): Promise<AdvanceToHeadResult> {
    try {
      return await this.#withReinitRetry(() =>
        this.#client().advanceToHeadStream(
          this.#clientGroupID,
          this.#sidecarInitEpoch,
          this.#appID,
          this.#cgOpts(),
        ),
      );
    } catch (err) {
      // F4: drive-mode drift must feed the SAME drift-loop circuit breaker as
      // push-mode advance (see advanceToHead for the full rationale). Record +
      // maybe-trip, then re-throw so the caller's classify path escalates to a
      // full pipeline reset (F2).
      if (err instanceof DriftError) {
        this.#recordDriftReinit();
        this.#maybeTripDriftBreaker();
      }
      throw err;
    }
  }

  // Recovery path for advanceStream (push mode). Two error
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
        const partialCount = err.partialChanges?.length ?? 0;
        this.#log(
          'warn',
          `Go drift detected (${err.op} on ${err.table} pk=${JSON.stringify(
            err.pk,
            // HIGH-9: msgpackr decodes int64 PKs > 2^32 as BigInt, which
            // plain JSON.stringify throws on — crashing drift RECOVERY itself
            // with a misleading error instead of recovering. Stringify BigInt.
            (_k, v) => (typeof v === 'bigint' ? v.toString() : v),
          )} has=${err.hasCount}); ` +
            `re-initing engine from current snapshot, forwarding ${partialCount} partial RowChanges`,
        );
        // Record this drift in the sliding window BEFORE the reinit so
        // breaker arithmetic reflects the true rate even if reinit hangs.
        // Trip is checked after a successful reinit so the current advance
        // gets its full recovery — clients are no worse off than before;
        // the trip just suppresses Go for SUBSEQUENT advances.
        this.#recordDriftReinit();
        const ok = await this.#reinitPerCGAndRegisterQueries(
          'advance-drift',
          this.#getCurrentTables(),
        );
        if (!ok) {
          throw err; // surface original drift so caller's fallback engages
        }
        this.#maybeTripDriftBreaker();
        // Forward partial output produced by Pushes that completed before
        // the drift panic. Matches TS view-syncer's assert-and-throw path
        // where the pre-throw RowChanges already streamed to pokers; the
        // CVR is then rebuilt against the re-initialized state. Without
        // this, Go silently swallowed RowChanges that TS would have
        // emitted, producing the observable TS/Go output divergence seen
        // in 20-user 30-min shadow soaks.
        return {
          changes: err.partialChanges ?? [],
          timings: err.partialTimings ?? [],
        };
      }

      const sidecarUnavailable =
        this.#manager.status !== 'running' ||
        msg.includes('Sidecar is not running') ||
        msg.includes('Connection closed') ||
        msg.includes('Not connected');
      if (sidecarUnavailable) {
        try {
          await this.#manager.waitForRunning();
        } catch {
          throw err;
        }
        await this.whenInitialized();
        if (!this.initialized) {
          const ok = await this.#reinitPerCGAndRegisterQueries(
            'advance-sidecar-restart',
            this.#getCurrentTables(),
          );
          if (!ok) {
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
        const ok = await this.#reinitPerCGAndRegisterQueries(
          'advance-engine-not-initialized',
          this.#getCurrentTables(),
        );
        if (!ok) {
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
      // one of "Sidecar is not running" (getClient threw), "Connection
      // closed" (socket dropped mid-RPC), or "Not connected" (socket nulled
      // between slot acquire and write). Don't string-match every variant
      // — if the manager isn't `running` right now, treat any error as
      // restart-related and retry once the engine is rebuilt.
      const sidecarUnavailable =
        this.#manager.status !== 'running' ||
        msg.includes('Sidecar is not running') ||
        msg.includes('Connection closed') ||
        msg.includes('Not connected');
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
          const ok = await this.#reinitPerCGAndRegisterQueries(
            'reinit-retry-sidecar-restart',
            this.#getCurrentTables(),
          );
          if (!ok) {
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
      const ok = await this.#reinitPerCGAndRegisterQueries(
        'reinit-retry-engine-not-initialized',
        this.#getCurrentTables(),
      );
      if (!ok) {
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
  //
  // Long timeout because this RPC currently routes through the per-CG
  // worker FIFO on the sidecar, so it can queue behind in-flight
  // advance/hydrate work under heavy load. The handler itself is a
  // cheap mutex bump; the wait is the queue. 120s budget covers a
  // worst-case hydrate of a fat query before refresh gets its turn.
  // If we observe persistent timeouts, the next step is to bypass the
  // CG FIFO on the sidecar side (refreshSnapshot only touches per-source
  // mutexes — no engine-state mutation that the FIFO is protecting).
  // Health probe used by the drift audit. Returns the number of queries
  // currently registered on this CG's Go engine. If the value disagrees
  // with TS's #pipelines.size, per-CG recovery has dropped pipeline state
  // and the client view is silently frozen — audit treats that as a
  // freeze signal and self-heals via resetEngine.
  pipelineCount(): Promise<number> {
    return this.#client().pipelineCount(this.#clientGroupID);
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

  async refreshSnapshot(): Promise<void> {
    await this.#client().refreshSnapshot(
      this.#clientGroupID,
      this.#sidecarInitEpoch,
      {...this.#cgOpts(), timeoutMs: 120_000},
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
        minRowVersion: t.minRowVersion,
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

  /** Prune timestamps older than the window then append now. */
  #recordDriftReinit(): void {
    const now = Date.now();
    const cutoff = now - DRIFT_BREAKER_WINDOW_MS;
    // Most recent timestamps cluster at the tail, so scan from head and
    // slice once. Array is tiny (bounded by threshold), so O(n) is fine.
    let dropTo = 0;
    while (
      dropTo < this.#driftReinitTimestamps.length &&
      this.#driftReinitTimestamps[dropTo] < cutoff
    ) {
      dropTo++;
    }
    if (dropTo > 0) {
      this.#driftReinitTimestamps.splice(0, dropTo);
    }
    this.#driftReinitTimestamps.push(now);
  }

  /** Open the breaker if recent drift count crossed the threshold. */
  #maybeTripDriftBreaker(): void {
    if (this.#driftReinitTimestamps.length >= DRIFT_BREAKER_THRESHOLD) {
      this.#driftBreakerOpenUntil = Date.now() + DRIFT_BREAKER_COOLDOWN_MS;
      this.#log(
        'error',
        `drift-loop breaker OPEN: ${this.#driftReinitTimestamps.length} drift-driven reinits ` +
          `within ${DRIFT_BREAKER_WINDOW_MS}ms; suppressing Go path for ${DRIFT_BREAKER_COOLDOWN_MS}ms`,
      );
    }
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
  //   3. #advanceWithRecovery's three error branches (drift,
  //      sidecar-unavailable, engine-not-initialized)
  //   4. #withReinitRetry's two error branches (sidecar-unavailable,
  //      engine-not-initialized)
  //
  // All these paths used to call only #doInit, which loads sources but
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
          // STREAMING re-register: the result is discarded (we only need Go's
          // pipeline state rebuilt), but non-streaming addQueries would still
          // ship every query's full result as one unbounded frame just to throw
          // it away — a fat-frame orphan risk on a CG with large queries. The
          // chunked stream bounds the frame; the no-op onResult drops the rows.
          await this.#client().addQueriesStream(
            this.#clientGroupID,
            queries,
            this.#sidecarInitEpoch,
            () => {},
            this.#cgOpts(),
          );
          this.#log(
            'info',
            `[${reason}] re-registered ${queries.length} queries`,
          );
        } catch (qerr) {
          this.#initialized = false;
          this.#log(
            'error',
            `[${reason}] re-register queries failed`,
            qerr,
          );
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

// Whether to run the advanceToHead Go-derived-diff shadow: each shadow advance
// also asks Go to derive its OWN snapshot diff and compares it against TS's
// computed diff (design §7 P1). Only meaningful alongside shadowMode (the
// comparison runs in #shadowAdvance) and requires a protocolRev>=7 sidecar with
// GO_IVM_ADVANCE_TO_HEAD=true.
export function isGoDerivedDiff(
  config: Pick<ZeroConfig, 'goSidecar'> | undefined,
): boolean {
  return config?.goSidecar?.advanceToHead === true;
}

// Whether to DRIVE Go's engine from its own derived diff (P2): in shadow mode
// the Go advance is sourced via advanceToHead (frame-coordinated, no TS-shipped
// SnapshotChange[]) and its RowChanges are compared to TS's. Implies a sidecar
// launched with GO_IVM_ADVANCE_DRIVE=true.
export function isGoAdvanceDrive(
  config: Pick<ZeroConfig, 'goSidecar'> | undefined,
): boolean {
  return config?.goSidecar?.advanceDrive === true;
}

// Whether Go-PRIMARY serving should source the user-query advance via
// advanceToHead (P2c trigger) instead of advanceStream (push). Only meaningful
// in Go-primary mode (enabled && !shadowMode); makes Go self-consistent and
// moves the user-query CVR watermark to min(V_ts, V_go). Implies a sidecar
// launched with GO_IVM_ADVANCE_TO_HEAD=true + GO_IVM_ADVANCE_DRIVE=true.
export function isGoPrimaryTrigger(
  config: Pick<ZeroConfig, 'goSidecar'> | undefined,
): boolean {
  return config?.goSidecar?.goPrimaryTrigger === true;
}

// P3 lean primary: whether TS should skip walking USER-table changes during a
// Go-primary advance (TS holds only stub user pipelines and keeps user
// TableSources current via snapshot setDB, so the walk is redundant). Only
// meaningful in Go-primary mode (enabled && !shadowMode).
export function isGoLeanPrimary(
  config: Pick<ZeroConfig, 'goSidecar'> | undefined,
): boolean {
  return config?.goSidecar?.leanPrimary === true;
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

// Directory to write divergence capture repro bundles to (VACUUM INTO snapshot
// .db + .json metadata). Empty string / undefined = capture OFF (the default —
// VACUUM INTO copies the whole replica on the divergence hot path, so capture
// is opt-in and rate-capped). See goSidecar.divergenceCaptureDir in zero-config.
export function goDivergenceCaptureDir(
  config: Pick<ZeroConfig, 'goSidecar'> | undefined,
): string {
  return config?.goSidecar?.divergenceCaptureDir ?? '';
}

// Returns null when the manager isn't `running` so the caller falls back
// to the TS path; `getCurrentTables` is invoked both on initial init and
// after each sidecar restart.
export function createGoComputeBackend(
  sidecarManager: SidecarManager,
  clientGroupID: string,
  getCurrentTables: () => Record<string, TableData>,
  getCurrentQueries: GetCurrentQueries,
  options?: {logger?: GoBackendLogger; loadBatchRows?: number; appID?: string},
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
