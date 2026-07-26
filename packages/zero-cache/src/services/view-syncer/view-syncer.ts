import { Lock } from "@rocicorp/lock";
import type { LogContext } from "@rocicorp/logger";
import { resolver } from "@rocicorp/resolver";
import type { Row } from "postgres";
import {
  manualSpan,
  startAsyncSpan,
  startSpan,
} from "../../../../otel/src/span.ts";
import { assert, unreachable } from "../../../../shared/src/asserts.ts";
import { stringify } from "../../../../shared/src/bigint-json.ts";
import { CustomKeyMap } from "../../../../shared/src/custom-key-map.ts";
import { must } from "../../../../shared/src/must.ts";
import { randInt } from "../../../../shared/src/rand.ts";
import type { AST } from "../../../../zero-protocol/src/ast.ts";
import type { ChangeDesiredQueriesMessage } from "../../../../zero-protocol/src/change-desired-queries.ts";
import type {
  InitConnectionBody,
  InitConnectionMessage,
} from "../../../../zero-protocol/src/connect.ts";
import type { ErroredQuery } from "../../../../zero-protocol/src/custom-queries.ts";
import type { DeleteClientsMessage } from "../../../../zero-protocol/src/delete-clients.ts";
import type { Downstream } from "../../../../zero-protocol/src/down.ts";
import { ErrorKind } from "../../../../zero-protocol/src/error-kind.ts";
import { ErrorOrigin } from "../../../../zero-protocol/src/error-origin.ts";
import {
  isProtocolError,
  ProtocolError,
  type TransformFailedBody,
} from "../../../../zero-protocol/src/error.ts";
import type {
  InspectUpBody,
  InspectUpMessage,
} from "../../../../zero-protocol/src/inspect-up.ts";
import type { UpdateAuthMessage } from "../../../../zero-protocol/src/update-auth.ts";
import { ChangeType } from "../../../../zql/src/ivm/change-type.ts";
import { clampTTL, MAX_TTL_MS } from "../../../../zql/src/query/ttl.ts";
import { isAuthErrorBody, type Auth } from "../../auth/auth.ts";
import {
  transformAndHashQuery,
  type TransformedAndHashed,
} from "../../auth/read-authorizer.ts";
import type { NormalizedZeroConfig } from "../../config/normalize.ts";
import type {
  CustomQueryTransformer,
  HashedTransformResponse,
} from "../../custom-queries/transform-query.ts";
import {
  getOrCreateCounter,
  getOrCreateLatencyHistogram,
  getOrCreateUpDownCounter,
} from "../../observability/metrics.ts";
import type { InspectorDelegate } from "../../server/inspector-delegate.ts";
import {
  getLogLevel,
  ProtocolErrorWithLevel,
  wrapWithProtocolError,
} from "../../types/error-with-level.ts";
import type { PostgresDB } from "../../types/pg.ts";
import { rowIDString, type RowKey } from "../../types/row-key.ts";
import type { ShardID } from "../../types/shards.ts";
import type { Source } from "../../types/streams.ts";
import { Subscription } from "../../types/subscription.ts";
import type { ReplicaState } from "../replicator/replicator.ts";
import { ZERO_VERSION_COLUMN_NAME } from "../replicator/schema/replication-state.ts";
import type { ActivityBasedService } from "../service.ts";
import {
  ClientHandler,
  startPoke,
  type PatchToVersion,
  type PokeHandler,
  type RowPatch,
} from "./client-handler.ts";
import type {
  ConnectionContext,
  ConnectionContextManager,
  ConnectionSelector,
  ConnectionValidation,
} from "./connection-context-manager.ts";
import { ClientNotFoundError, CVRStore } from "./cvr-store.ts";
import type { CVRUpdater } from "./cvr.ts";
import {
  CVRConfigDrivenUpdater,
  CVRQueryDrivenUpdater,
  nextEvictionTime,
  type CVRSnapshot,
  type RowUpdate,
} from "./cvr.ts";
import type { DrainCoordinator } from "./drain-coordinator.ts";
import { handleInspect } from "./inspect-handler.ts";
import type { PipelineDriver } from "./pipeline-driver.ts";
import { type RowChange } from "./pipeline-driver.ts";
import { parseSignature } from "./row-set-signature.ts";
import {
  cmpVersions,
  EMPTY_CVR_VERSION,
  versionFromString,
  versionString,
  versionToCookie,
  type ClientQueryRecord,
  type CustomQueryRecord,
  type CVRVersion,
  type InternalQueryRecord,
  type NullableCVRVersion,
  type QueryRecord,
  type RowID,
} from "./schema/types.ts";
import { ResetPipelinesSignal } from "./snapshotter.ts";
import type { ResetPipelinesReason } from "./snapshotter.ts";
import { tracer } from "./tracer.ts";
import {
  ttlClockAsNumber,
  ttlClockFromNumber,
  type TTLClock,
} from "./ttl-clock.ts";

const PROTOCOL_VERSION_ATTR = "protocol.version";

export interface ViewSyncer {
  initConnection(
    selector: ConnectionSelector,
    initConnectionMessage: InitConnectionMessage,
  ): Source<Downstream>;

  changeDesiredQueries(
    selector: ConnectionSelector,
    msg: ChangeDesiredQueriesMessage,
  ): Promise<void>;

  deleteClients(
    selector: ConnectionSelector,
    msg: DeleteClientsMessage,
  ): Promise<string[]>;

  inspect(selector: ConnectionSelector, msg: InspectUpMessage): Promise<void>;
  updateAuth(
    selector: ConnectionSelector,
    msg: UpdateAuthMessage,
    authRevisionChanged: boolean,
  ): Promise<void>;

  // Connection context management is owned by the view syncer for disconnect cleanup.
  connContextManager: ConnectionContextManager;

  readonly queryCount: number;
  readonly rowCount: number;
}

export type SyncContext = ConnectionSelector & {
  readonly profileID: string | null;
  readonly baseCookie: string | null;
  readonly protocolVersion: number;
  readonly httpCookie: string | undefined;
  readonly origin: string | undefined;
  readonly userID: string | undefined;
  readonly auth: Auth | undefined;
};

const DEFAULT_KEEPALIVE_MS = 5_000;

function randomID() {
  return randInt(1, Number.MAX_SAFE_INTEGER).toString(36);
}

function shutdownBeforeInitializationError(): ProtocolErrorWithLevel {
  return new ProtocolErrorWithLevel(
    {
      kind: ErrorKind.Internal,
      message: "shut down before initialization completed",
      origin: ErrorOrigin.ZeroCache,
    },
    "warn",
  );
}

type SetTimeout = (
  fn: (...args: unknown[]) => void,
  delay?: number,
) => ReturnType<typeof setTimeout>;

/**
 * We update the ttlClock in flush that writes to the CVR but
 * some flushes do not write to the CVR and in those cases we
 * use a timer to update the ttlClock every minute.
 */
export const TTL_CLOCK_INTERVAL = 60_000;

/**
 * This is some extra time we delay the TTL timer to allow for some
 * slack in the timing of the timer. This is to allow multiple evictions
 * to happen in a short period of time without having to wait for the
 * next tick of the timer.
 */
export const TTL_TIMER_HYSTERESIS = 50; // ms

// Reset circuit breaker: at most RESET_CIRCUIT_LIMIT pipeline resets are
// allowed within RESET_CIRCUIT_WINDOW_MS before the CG is torn down instead of
// reset again. A deterministic, reset-proof error (e.g. an unclassified Go
// panic that survives a rebuild) otherwise loops reset→re-hydrate→re-panic
// forever, sustaining p99 at 5–8s and re-paying every CG's hydrate. Teardown
// breaks the loop loudly; the client reconnects into a fresh view-syncer.
export const RESET_CIRCUIT_LIMIT = 2;
export const RESET_CIRCUIT_WINDOW_MS = 20_000;

// Transient-class breaker: sidecar restarts / Go-unavailable windows produce a
// bounded burst of lawful resets while the outage lasts (reset-degrade fires
// once, reset-recovered fires once, drops during the restart each reset once).
// The deterministic 2/20s limit reads a normal restart cycle as a reset-proof
// loop and tears the CG down mid-recovery — making the outage WORSE (reconnect
// + cold re-hydrate on top of the restart). A genuine flapping loop still
// clears this looser bar within a minute.
export const TRANSIENT_RESET_CIRCUIT_LIMIT = 6;
export const TRANSIENT_RESET_CIRCUIT_WINDOW_MS = 60_000;

// Economic-reset watchdog: 'advancement-timeout' resets are exempt from the
// tiered breaker (they're the designed recovery), but a CG that aborts every
// advance — even after suppressAbort fires and the 60s wall backstop takes
// over — will loop reset→re-hydrate→abort forever with no escalation path.
// This watchdog counts consecutive economic resets with zero successful
// advances in between; when it exceeds the limit, the CG is torn down so the
// client reconnects into a fresh view-syncer instead of silently spinning.
export const ECONOMIC_RESET_WATCHDOG_LIMIT = 10;
export const ECONOMIC_RESET_WATCHDOG_WINDOW_MS = 5 * 60_000; // 5 min

/**
 * The breaker's premise — "N resets in a short window ⇒ the error is
 * reset-proof ⇒ teardown is the only way out" — is only sound for a subset of
 * {@link ResetPipelinesReason}s. Classify each reason by what a repeat
 * actually means:
 *
 * - `deterministic`: should-be-unreachable invariant recoveries. A repeat
 *   really does mean a reset-proof loop → keep the fast 2/20s trip.
 * - `transient`: Go backend outage/restart windows. Repeats are bounded by
 *   the outage, not by a loop → looser 6/60s trip catches genuine flapping.
 * - `economic`: the advancement-timeout is a STRATEGY (re-hydrating is priced
 *   cheaper than continuing the advance), not a failure — under sustained
 *   write load every large batch lawfully resets. Never tear down: the abort
 *   convergence is owned by the budget escalation + suppressAbort catch-up in
 *   pipeline-driver.ts (goAdvanceAbortBudgetMs / SUPPRESS_ABORT_AFTER_STREAK),
 *   which makes an unbounded abort→reset loop structurally impossible.
 * - `lawful`: externally-caused structural events (a migration touching three
 *   tables = three lawful resets). Resetting IS the designed handling; a
 *   teardown adds a client reconnect for zero benefit.
 *
 * (The original breaker motivation — unclassified Go panics — no longer flows
 * through resets at all: 'unclassified'/'data-error' advance failures re-throw
 * directly to CG teardown in pipeline-driver.ts.)
 */
export type ResetReasonClass =
  "deterministic" | "transient" | "economic" | "lawful";

export function classifyResetReason(
  reason: ResetPipelinesReason,
): ResetReasonClass {
  switch (reason) {
    case "watermark-regression":
      return "deterministic";
    case "go-primary-unavailable":
    case "go-primary-drop":
      return "transient";
    case "advancement-timeout":
      return "economic";
    case "schema-change":
    case "truncation":
    case "permissions-change":
    case "scalar-subquery":
      return "lawful";
  }
}

/**
 * Pure decision for the reset circuit breaker. Prunes reset timestamps older
 * than the window and reports whether a new reset would exceed the limit (i.e.
 * the caller should tear down instead of resetting again).
 */
export function resetCircuitDecision(
  recentResetTimes: readonly number[],
  nowMs: number,
  limit: number = RESET_CIRCUIT_LIMIT,
  windowMs: number = RESET_CIRCUIT_WINDOW_MS,
): { trip: boolean; pruned: number[] } {
  const pruned = recentResetTimes.filter((t) => nowMs - t < windowMs);
  return { trip: pruned.length >= limit, pruned };
}

/** Per-class sliding windows for the tiered breaker (only the classes that
 * can trip carry state). */
export type ResetCircuitBuckets = {
  deterministic: number[];
  transient: number[];
};

export function emptyResetCircuitBuckets(): ResetCircuitBuckets {
  return { deterministic: [], transient: [] };
}

/**
 * Reason-tiered breaker decision. Returns the reason's class, whether this
 * reset must escalate to a CG teardown, and the updated buckets: pruned, and
 * — when not tripping — with `nowMs` recorded in the reason's bucket.
 * `economic` and `lawful` resets never trip and are never recorded.
 */
export function resetCircuitDecisionByReason(
  reason: ResetPipelinesReason,
  buckets: ResetCircuitBuckets,
  nowMs: number,
): { cls: ResetReasonClass; trip: boolean; buckets: ResetCircuitBuckets } {
  const cls = classifyResetReason(reason);
  const next: ResetCircuitBuckets = {
    deterministic: resetCircuitDecision(buckets.deterministic, nowMs).pruned,
    transient: resetCircuitDecision(
      buckets.transient,
      nowMs,
      TRANSIENT_RESET_CIRCUIT_LIMIT,
      TRANSIENT_RESET_CIRCUIT_WINDOW_MS,
    ).pruned,
  };
  let trip = false;
  switch (cls) {
    case "deterministic":
      trip = next.deterministic.length >= RESET_CIRCUIT_LIMIT;
      break;
    case "transient":
      trip = next.transient.length >= TRANSIENT_RESET_CIRCUIT_LIMIT;
      break;
    case "economic":
    case "lawful":
      break;
  }
  if (!trip && (cls === "deterministic" || cls === "transient")) {
    next[cls].push(nowMs);
  }
  return { cls, trip, buckets: next };
}

type CustomQueryTransformMode = "all" | "missing";

export class ViewSyncerService implements ViewSyncer, ActivityBasedService {
  readonly id: string;
  // Centralized connection/group auth bookkeeping plus maintenance policy.
  // Network validation still happens in ViewSyncerService.
  readonly connContextManager: ConnectionContextManager;

  readonly #shard: ShardID;
  readonly #lc: LogContext;
  readonly #pipelines: PipelineDriver;
  readonly #stateChanges: Subscription<ReplicaState>;
  readonly #drainCoordinator: DrainCoordinator;
  readonly #keepaliveMs: number;
  readonly #slowHydrateThreshold: number;

  // The ViewSyncerService is only started in response to a connection,
  // so #lastConnectTime is always initialized to now(). This is necessary
  // to handle race conditions in which, e.g. the replica is ready and the
  // CVR is accessed before the first connection sends a request.
  //
  // Note: It is fine to update this variable outside of the lock.
  #lastConnectTime = Date.now();
  // Per-reason-class sliding windows of recent pipeline-reset timestamps (ms)
  // for the tiered reset circuit breaker (see classifyResetReason). Too many
  // same-class resets within the class's window means a reset-proof error is
  // looping → tear down the CG (loud) instead of resetting again. Cleared by
  // every successful advance (half-open semantics: the "reset-proof loop"
  // inference is only sound for back-to-back failures with no forward
  // progress between them). Per-instance: a fresh view-syncer after reconnect
  // starts clean, so the cost of a persistent failure is a slow reconnect
  // cycle, not a tight reset storm.
  #resetCircuitBuckets: ResetCircuitBuckets = emptyResetCircuitBuckets();
  // Consecutive economic-reset timestamps (ms) for the economic-reset
  // watchdog. Cleared on any successful advance. When the count exceeds
  // ECONOMIC_RESET_WATCHDOG_LIMIT within the window, the CG is torn down.
  #economicResetTimestamps: number[] = [];

  /**
   * The TTL clock is used to determine the time at which queries are considered
   * expired.
   */
  #ttlClock: TTLClock | undefined;

  /**
   * The base time for the TTL clock. This is used to compute the current TTL
   * clock value. The first time a connection is made, this is set to the
   * current time. On subsequent connections, the TTL clock is computed as the
   * difference between the current time and this base time.
   *
   * Every time we write the ttlClock this is update to the current time. That
   * way we can compute how much time has passed since the last time we set the
   * ttlClock. When we set the ttlClock we just increment it by the amount of
   * time that has passed since the last time we set it.
   */
  #ttlClockBase = Date.now();

  /**
   * We update the ttlClock every minute to ensure that it is not too much
   * out of sync with the current time.
   */
  #ttlClockInterval: ReturnType<SetTimeout> | 0 = 0;

  // Note: It is okay to add/remove clients without acquiring the lock.
  readonly #clients = new Map<string, ClientHandler>();

  // Serialize on this lock for:
  // (1) storage or database-dependent operations
  // (2) updating member variables.
  readonly #lock = new Lock();
  readonly #cvrStore: CVRStore;
  readonly #stopped = resolver();
  readonly #initialized = resolver<"initialized">();

  #cvr: CVRSnapshot | undefined;
  #pipelinesSynced = false;

  #expiredQueriesTimer: ReturnType<SetTimeout> | 0 = 0;
  #authMaintenanceTimer: ReturnType<SetTimeout> | 0 = 0;
  readonly #setTimeout: SetTimeout;
  readonly #customQueryTransformer: CustomQueryTransformer | undefined;

  // Track query replacements for thrashing detection
  readonly #queryReplacements = new Map<
    string,
    { count: number; windowStart: number }
  >();

  readonly #activeClients = getOrCreateUpDownCounter(
    "sync",
    "active-clients",
    "Number of active sync clients",
  );
  readonly #hydrations = getOrCreateCounter(
    "sync",
    "hydration",
    "Number of query hydrations",
  );
  readonly #hydrationTime = getOrCreateLatencyHistogram(
    "sync",
    "hydration-time",
    "Time to hydrate a query.",
  );
  // End-to-end hydrate wall-clock: engine compute + RPC + delivery of the
  // hydrated rows. On the TS path there is no sidecar RPC, so this is the
  // SAME span as #hydrationTime (TS compute == TS e2e). It exists for the
  // Go-primary path, where #hydrationTime is deliberately the pure-engine
  // compute span (Go-reported `timingMs`) and this histogram carries the
  // batchElapsed that also pays the sidecar RPC + TS-side consumption tax.
  // The gap between the two on Go is the serialization/RPC cost — see
  // the positional wire-format work. Comparing Go hydration-e2e-time vs TS
  // hydration-time is a FAIR e2e test that is Go-pessimistic (Go's span
  // includes the RPC tax, TS's has none).
  readonly #hydrationE2ETime = getOrCreateLatencyHistogram(
    "sync",
    "hydration-e2e-time",
    "Time to hydrate a query end-to-end (engine compute + RPC + delivery).",
  );
  readonly #transactionAdvanceTime = getOrCreateLatencyHistogram(
    "sync",
    "advance-time",
    "Time to advance all queries for a given client group after applying a new transaction to the replica.",
  );
  readonly #queryTransformations = getOrCreateCounter(
    "sync",
    "query.transformations",
    "Number of query transformations performed",
  );
  readonly #queryTransformationTime = getOrCreateLatencyHistogram(
    "sync",
    "query.transformation-time",
    "Time to transform custom queries via API server.",
  );
  readonly #queryTransformationHashChanges = getOrCreateCounter(
    "sync",
    "query.transformation-hash-changes",
    "Number of times query transformation hash changed",
  );
  readonly #queryTransformationNoOps = getOrCreateCounter(
    "sync",
    "query.transformation-no-ops",
    "Number of times query transformation resulted in no-op (hash unchanged)",
  );
  readonly #lockWaitTime = getOrCreateLatencyHistogram(
    "sync",
    "lock-wait-time",
    "Time spent waiting to acquire the ViewSyncer lock.",
  );
  readonly #pipelineResets = getOrCreateCounter(
    "sync",
    "pipeline-resets",
    "Number of pipeline resets",
  );
  // Circuit breaker: a reset-proof error (deterministic panic that survives a
  // rebuild) would otherwise reset → re-hydrate → re-panic → reset forever
  // (the 5–8s p99 storm). Counts how often the breaker tripped and forced a CG
  // teardown instead of an N+1th reset. See #resetCircuitBuckets /
  // classifyResetReason / RESET_CIRCUIT_*.
  readonly #pipelineResetCircuitTripped = getOrCreateCounter(
    "sync",
    "pipeline-reset-circuit-tripped",
    "CG torn down because resets exceeded the circuit-breaker rate (reset storm)",
  );
  readonly #rowSetSignatureDrifts = getOrCreateCounter(
    "sync",
    "query.row-set-signature-drifts",
    "Number of times re-hydration of an unchanged query produced a different " +
      "row-set signature than what is stored in the CVR (forcing a configVersion " +
      "bump and full re-execution). Expected to be near-zero in steady state; " +
      "persistent non-zero values indicate non-deterministic query execution " +
      "(e.g. Cap operator picking different N-row subsets).",
  );

  readonly #inspectorDelegate: InspectorDelegate;

  readonly #config: NormalizedZeroConfig;
  #runPriorityOp: <T>(
    lc: LogContext,
    description: string,
    op: () => Promise<T>,
  ) => Promise<T>;

  constructor(
    config: NormalizedZeroConfig,
    lc: LogContext,
    shard: ShardID,
    taskID: string,
    clientGroupID: string,
    cvrDb: PostgresDB,
    pipelineDriver: PipelineDriver,
    versionChanges: Subscription<ReplicaState>,
    drainCoordinator: DrainCoordinator,
    slowHydrateThreshold: number,
    inspectorDelegate: InspectorDelegate,
    connContextManager: ConnectionContextManager,
    customQueryTransformer: CustomQueryTransformer | undefined,
    runPriorityOp: <T>(
      lc: LogContext,
      description: string,
      op: () => Promise<T>,
    ) => Promise<T>,
    keepaliveMs = DEFAULT_KEEPALIVE_MS,
    setTimeoutFn: SetTimeout = setTimeout.bind(globalThis),
  ) {
    this.#config = config;
    this.id = clientGroupID;
    this.connContextManager = connContextManager;
    this.#shard = shard;
    this.#lc = lc;
    this.#pipelines = pipelineDriver;
    this.#stateChanges = versionChanges;
    this.#drainCoordinator = drainCoordinator;
    this.#keepaliveMs = keepaliveMs;
    this.#slowHydrateThreshold = slowHydrateThreshold;
    this.#inspectorDelegate = inspectorDelegate;
    this.#customQueryTransformer = customQueryTransformer;
    this.#cvrStore = new CVRStore(
      lc,
      cvrDb,
      shard,
      taskID,
      clientGroupID,
      // On failure, cancel the #stateChanges subscription. The run()
      // loop will then await #cvrStore.flushed() which rejects if necessary.
      () => this.#stateChanges.cancel(),
    );
    this.#setTimeout = setTimeoutFn;
    this.#runPriorityOp = runPriorityOp;
    // Wait for the first connection to init.
    this.keepalive();
  }

  #runInLockWithCVR(
    fn: (lc: LogContext, cvr: CVRSnapshot) => Promise<void> | void,
  ): Promise<void> {
    const rid = randomID();
    this.#lc.debug?.("about to acquire lock for cvr ", rid);
    const lockWaitStart = performance.now();
    return this.#lock.withLock(async () => {
      this.#lockWaitTime.recordMs(performance.now() - lockWaitStart);
      this.#lc.debug?.("acquired lock in #runInLockWithCVR ", rid);
      const lc = this.#lc.withContext("lock", rid);
      if (!this.#stateChanges.active) {
        // view-syncer has been shutdown. this can be a backlog of tasks
        // queued on the lock, or it can be a race condition in which a
        // client connects before the ViewSyncer has been deleted from the
        // ServiceRunner.
        this.#lc.debug?.("state changes are inactive");
        clearTimeout(this.#expiredQueriesTimer);
        throw new ProtocolErrorWithLevel(
          {
            kind: ErrorKind.Rehome,
            message: "Reconnect required",
            origin: ErrorOrigin.ZeroCache,
          },
          "info",
        );
      }
      // If all clients have disconnected, cancel all pending work.
      if (await this.#checkForShutdownConditionsInLock()) {
        this.#lc.info?.(`closing clientGroupID=${this.id}`);
        // Reject #initialized so that run() unblocks if it is still
        // waiting on readyState(). This is a no-op if already resolved.
        this.#initialized.reject(shutdownBeforeInitializationError());
        this.#stateChanges.cancel(); // Note: #stateChanges.active becomes false.
        return;
      }
      if (!this.#cvr) {
        this.#lc.debug?.("loading cvr");
        this.#cvr = await this.#runPriorityOp(lc, "loading cvr", () =>
          this.#cvrStore.load(lc, this.#lastConnectTime),
        );
        this.#restorePinnedUserFromCVR(this.#cvr);
        this.#ttlClock = this.#cvr.ttlClock;
        this.#ttlClockBase = Date.now();
      } else {
        // Make sure the CVR ttlClock is up to date.
        const now = Date.now();
        this.#cvr = {
          ...this.#cvr,
          ttlClock: this.#getTTLClock(now),
        };
      }

      try {
        await fn(lc, this.#cvr);
      } catch (e) {
        // Clear cached state if an error is encountered.
        this.#cvr = undefined;
        throw e;
      } finally {
        // Lock-scoped work is where validated connections gain or lose
        // schedulable auth-maintenance deadlines. Recompute the single wakeup
        // after every locked operation; out-of-lock fail/close transitions only
        // clear or relax deadlines, so a stale earlier wakeup is harmless.
        this.#scheduleAuthMaintenance(lc);
      }
    });
  }

  #restorePinnedUserFromCVR(cvr: CVRSnapshot): void {
    if (cvr.pinnedUserID !== undefined) {
      this.connContextManager.pinGroupToUser({ id: cvr.pinnedUserID });
    }
  }

  readyState(): Promise<"initialized" | "draining"> {
    return Promise.race([
      this.#initialized.promise,
      this.#drainCoordinator.draining,
    ]);
  }

  async run(): Promise<void> {
    try {
      // Wait for initialization if we need to process queries.
      // This ensures authData and cvr.clientSchema are available before
      // transforming custom queries (dependency on authData) and building
      // pipelines (dependency on cvr.clientSchema).
      if ((await this.readyState()) === "draining") {
        this.#lc.debug?.(`draining view-syncer ${this.id} before running`);
        void this.stop();
      }
      for await (const { state } of this.#stateChanges) {
        if (this.#drainCoordinator.shouldDrain()) {
          this.#lc.debug?.(`draining view-syncer ${this.id} (elective)`);
          break;
        }
        assert(state === "version-ready", "state should be version-ready"); // This is the only state change used.

        await this.#runInLockWithCVR(async (lc, cvr) => {
          const clientSchema = must(
            cvr.clientSchema,
            "cvr.clientSchema missing after initialization",
          );
          if (!this.#pipelines.initialized()) {
            // On the first version-ready signal, connect to the replica.
            this.#pipelines.init(clientSchema);
          }
          if (
            cvr.replicaVersion !== null &&
            cvr.version.stateVersion !== "00" &&
            this.#pipelines.replicaVersion < cvr.replicaVersion
          ) {
            const message = `Cannot sync from older replica: CVR=${
              cvr.replicaVersion
            }, DB=${this.#pipelines.replicaVersion}`;
            lc.info?.(`resetting CVR: ${message}`);
            throw new ClientNotFoundError(message);
          }

          if (this.#pipelinesSynced) {
            const result = await this.#advancePipelines(lc, cvr);
            if (result === "success") {
              // Half-open: a completed advance is forward progress, so any
              // earlier resets were not a reset-proof loop. Start the breaker
              // windows fresh.
              this.#resetCircuitBuckets = emptyResetCircuitBuckets();
              this.#economicResetTimestamps = [];
              return;
            }
            // Tiered reset circuit breaker: only reason classes for which a
            // repeat implies a reset-proof loop can escalate to teardown
            // (see classifyResetReason). Economic aborts and lawful
            // structural resets ARE the designed handling — resetting again
            // is correct, and tearing down would convert a working recovery
            // into a client-visible reconnect.
            const nowMs = Date.now();
            const { cls, trip, buckets } = resetCircuitDecisionByReason(
              result.reason,
              this.#resetCircuitBuckets,
              nowMs,
            );
            this.#resetCircuitBuckets = buckets;
            // Economic-reset watchdog: 'advancement-timeout' resets are exempt
            // from the tiered breaker, but a CG stuck in an abort→reset loop
            // with zero successful advances must eventually escalate to
            // teardown instead of spinning forever.
            if (cls === "economic") {
              const windowStart = nowMs - ECONOMIC_RESET_WATCHDOG_WINDOW_MS;
              this.#economicResetTimestamps = this.#economicResetTimestamps.filter(
                ts => ts > windowStart,
              );
              this.#economicResetTimestamps.push(nowMs);
              if (
                this.#economicResetTimestamps.length >=
                ECONOMIC_RESET_WATCHDOG_LIMIT
              ) {
                this.#pipelineResetCircuitTripped.add(1, {
                  reason: result.reason,
                  class: cls,
                });
                throw new Error(
                  `economic-reset watchdog tripped: ${this.#economicResetTimestamps.length} ` +
                    `economic resets within ${ECONOMIC_RESET_WATCHDOG_WINDOW_MS}ms ` +
                    `with zero successful advances (reason=${result.reason}) — ` +
                    `tearing down CG instead of looping: ${result.message}`,
                );
              }
            }
            if (trip) {
              this.#pipelineResetCircuitTripped.add(1, {
                reason: result.reason,
                class: cls,
              });
              const [limit, windowMs] =
                cls === "deterministic"
                  ? [RESET_CIRCUIT_LIMIT, RESET_CIRCUIT_WINDOW_MS]
                  : [
                      TRANSIENT_RESET_CIRCUIT_LIMIT,
                      TRANSIENT_RESET_CIRCUIT_WINDOW_MS,
                    ];
              throw new Error(
                `reset circuit breaker tripped: ${limit + 1} ${cls} pipeline ` +
                  `resets within ${windowMs}ms ` +
                  `(reason=${result.reason}) — tearing down CG instead of ` +
                  `reset-looping on a reset-proof error: ${result.message}`,
              );
            }
            lc.info?.(`resetting pipelines: ${result.message}`);
            this.#pipelineResets.add(1, { reason: result.reason });
            this.#pipelines.reset(clientSchema);
            this.#pipelinesSynced = false;
            this.connContextManager.setSharedRetransformReady(false);
          }

          // Advance the snapshot to the current version.
          const version = this.#pipelines.advanceWithoutDiff();
          const cvrVer = versionString(cvr.version);

          if (version < cvr.version.stateVersion) {
            lc.debug?.(`replica@${version} is behind cvr@${cvrVer}`);
            return; // Wait for the next advancement.
          }

          // stateVersion is at or beyond CVR version for the first time.
          lc.info?.(`init pipelines@${version} (cvr@${cvrVer})`);

          const driftedQueryIDs = await this.#hydrateUnchangedQueries(lc, cvr);
          // hydrateUnchangedQueries just transformed
          // all the custom queries, this #syncQueryPipelineSet call
          // should retransform those that are missing from #pipelines, which
          // are those which errored or changed transform hash, plus those
          // removed because their rowSetSignature drifted (Cap re-execution
          // chose a different N-row subset).
          await this.#syncQueryPipelineSet(
            lc,
            cvr,
            "missing",
            undefined,
            driftedQueryIDs,
          );
          this.#pipelinesSynced = true;
          this.connContextManager.setSharedRetransformReady(true);
        });
      }

      // If this view-syncer exited due to an elective or forced drain,
      // set the next drain timeout.
      if (this.#drainCoordinator.shouldDrain()) {
        this.#drainCoordinator.drainNextIn(this.#totalHydrationTimeMs());
      }
      await this.#cleanup();
    } catch (e) {
      this.#lc[getLogLevel(e)]?.(
        `stopping view-syncer ${this.id}: ${String(e)}`,
        e,
      );
      await this.#cleanup(e);
    } finally {
      // Always wait for the cvrStore to flush, regardless of how the service
      // was stopped.
      await this.#cvrStore
        .flushed(this.#lc)
        .catch((e) => this.#lc[getLogLevel(e)]?.(e));
      this.#lc.info?.(`view-syncer ${this.id} finished`);
      this.#stopped.resolve();
    }
  }

  // must be called from within #lock
  #removeExpiredQueries = async (
    lc: LogContext,
    cvr: CVRSnapshot,
  ): Promise<void> => {
    if (hasExpiredQueries(cvr)) {
      lc = lc.withContext("method", "#removeExpiredQueries");
      lc.debug?.("Queries have expired");
      // #syncQueryPipelineSet() will remove the expired queries.
      if (this.#pipelinesSynced) {
        await this.#syncQueryPipelineSet(lc, cvr, "missing", undefined);
      }
    }

    // Even if we have expired queries, we still need to schedule next eviction
    // since there might be inactivated queries that need to be expired queries
    // in the future.
    this.#scheduleExpireEviction(lc, cvr);
  };

  #totalHydrationTimeMs(): number {
    return this.#pipelines.totalHydrationTimeMs();
  }

  get queryCount(): number {
    return this.#pipelines.initialized() ? this.#pipelines.queries().size : 0;
  }

  get rowCount(): number {
    return this.#cvrStore.rowCount;
  }

  #keepAliveUntil: number = 0;

  /**
   * Guarantees that the ViewSyncer will remain running for at least
   * its configured `keepaliveMs`. This is called when establishing a
   * new connection to ensure that its associated ViewSyncer isn't
   * shutdown before it receives the connection.
   *
   * @return `true` if the ViewSyncer will stay alive, `false` if the
   *         ViewSyncer is shutting down.
   */
  keepalive(): boolean {
    if (!this.#stateChanges.active) {
      return false;
    }
    this.#keepAliveUntil = Date.now() + this.#keepaliveMs;
    return true;
  }

  // oxlint-disable-next-line no-unused-private-class-members -- False positive, used in #scheduleShutdown
  #shutdownTimer: NodeJS.Timeout | null = null;

  #scheduleShutdown(delayMs = 0) {
    this.#shutdownTimer ??= this.#setTimeout(() => {
      this.#shutdownTimer = null;

      // All lock tasks check for shutdown so that queued work is immediately
      // canceled when clients disconnect. Queue an empty task to ensure that
      // this check happens.
      void this.#runInLockWithCVR(() => {}).catch((e) =>
        // If an error occurs (e.g. ownership change), propagate the error
        // to the main run() loop via the #stateChanges Subscription.
        this.#stateChanges.fail(e),
      );
    }, delayMs);
  }

  async #checkForShutdownConditionsInLock(): Promise<boolean> {
    if (this.#clients.size > 0) {
      return false; // common case.
    }

    // Keep the view-syncer alive if there are pending rows being flushed.
    // It's better to do this before shutting down since it may take a
    // while, during which new connections may come in.
    await this.#cvrStore.flushed(this.#lc);

    if (Date.now() <= this.#keepAliveUntil) {
      this.#scheduleShutdown(this.#keepaliveMs); // check again later
      return false;
    }

    // If no clients have connected while waiting for the row flush, shutdown.
    return this.#clients.size === 0;
  }

  #deleteClientDueToDisconnect(clientID: string, client: ClientHandler) {
    this.connContextManager.closeConnection({
      clientID,
      wsID: client.wsID,
    });

    // Note: It is okay to delete / cleanup clients without acquiring the lock.
    // In fact, it is important to do so in order to guarantee that idle cleanup
    // is performed in a timely manner, regardless of the amount of work
    // queued on the lock.
    const c = this.#clients.get(clientID);
    if (c === client) {
      this.#clients.delete(clientID);

      if (this.#clients.size === 0) {
        // It is possible to delete a client before we read the ttl clock from
        // the CVR.
        if (this.#ttlClock !== undefined) {
          this.#updateTTLClockInCVRWithoutLock(this.#lc);
        }
        this.#stopExpireTimer();
        this.#scheduleShutdown();
      }
    }
  }

  #stopExpireTimer() {
    this.#lc.debug?.("Stopping expired queries timer");
    clearTimeout(this.#expiredQueriesTimer);
    this.#expiredQueriesTimer = 0;
  }

  #stopAuthMaintenanceTimer() {
    if (this.#authMaintenanceTimer !== 0) {
      this.#lc.debug?.("Stopping auth maintenance timer");
    }
    clearTimeout(this.#authMaintenanceTimer);
    this.#authMaintenanceTimer = 0;
  }

  /**
   * Schedules the auth maintenance wakeup from coordinator-derived
   * deadlines. The timer plumbing is intentionally separate from the actual
   * revalidation/retransform work so future policy changes only need to update
   * the maintenance workers, not the wakeup logic.
   */
  #scheduleAuthMaintenance(lc: LogContext) {
    this.#stopAuthMaintenanceTimer();

    const plan = this.connContextManager.planMaintenance();
    if (plan.earliestDeadlineAt === undefined) {
      lc.debug?.("No auth maintenance wakeup scheduled");
      return;
    }

    const delay = Math.max(0, plan.earliestDeadlineAt - Date.now());
    lc.debug?.(
      `Scheduling auth maintenance timer at ${new Date(plan.earliestDeadlineAt).toISOString()}`,
      {
        delay,
        earliestDeadlineAt: plan.earliestDeadlineAt,
      },
    );
    this.#authMaintenanceTimer = this.#setTimeout(async () => {
      try {
        this.#authMaintenanceTimer = 0;
        await this.#runInLockWithCVR((lc, cvr) =>
          this.#runAuthMaintenance(lc, cvr),
        );
      } catch (e) {
        // If an error occurs (e.g. ownership change), propagate the error
        // to the main run() loop via the #stateChanges Subscription.
        this.#stateChanges.fail(e instanceof Error ? e : new Error(String(e)));
      }
    }, delay);
  }

  async #runAuthMaintenance(lc: LogContext, _cvr: CVRSnapshot): Promise<void> {
    const plan = this.connContextManager.planMaintenance();
    if (plan.dueRevalidations.length === 0 && !plan.dueRetransform) {
      lc.debug?.("Auth maintenance woke up with no due work");
      return;
    }

    lc.debug?.("Auth maintenance woke up with pending work", {
      dueRevalidations: plan.dueRevalidations.length,
      dueRetransform: plan.dueRetransform,
    });

    for (const connCtx of plan.dueRevalidations) {
      try {
        await this.#validateConnection(connCtx);
      } catch (e) {
        if (isProtocolError(e) && isTransformFailedError(e)) {
          lc.warn?.(
            "Scheduled auth revalidation failed; deferring auth maintenance",
            {
              clientID: connCtx.clientID,
              wsID: connCtx.wsID,
              message: e.message,
            },
          );
          this.connContextManager.deferMaintenance("revalidate");
          return;
        }
        throw e;
      }
    }

    // Revalidation can change which connection is safe for shared background work.
    // Replan before deciding whether to run the group retransform.
    const refreshedPlan = this.connContextManager.planMaintenance();
    if (refreshedPlan.dueRetransform) {
      await this.#runBackgroundRetransform(lc);
    }
  }

  initConnection(
    selector: ConnectionSelector,
    initConnectionMessage: InitConnectionMessage,
  ): Source<Downstream> {
    this.#lc.debug?.("viewSyncer.initConnection");
    return startSpan(tracer, "vs.initConnection", () => {
      const connCtx =
        this.connContextManager.mustGetConnectionContext(selector);

      const lc = this.#lc
        .withContext("clientID", connCtx.clientID)
        .withContext("wsID", connCtx.wsID);

      // Setup the downstream connection.
      const downstream = Subscription.create<Downstream>({
        cleanup: (_, err) => {
          err
            ? lc[getLogLevel(err)]?.(`client closed with error`, err)
            : lc.info?.("client closed");
          this.#deleteClientDueToDisconnect(connCtx.clientID, newClient);
          this.#activeClients.add(-1, {
            [PROTOCOL_VERSION_ATTR]: connCtx.protocolVersion,
          });
        },
      });
      this.#activeClients.add(1, {
        [PROTOCOL_VERSION_ATTR]: connCtx.protocolVersion,
      });

      if (this.#clients.size === 0) {
        // First connection to this ViewSyncerService.

        // initConnection must be synchronous so that the downstream
        // subscription is returned immediately.
        const now = Date.now();
        this.#ttlClockBase = now;
      }

      const newClient = new ClientHandler(
        lc,
        this.id,
        connCtx.clientID,
        connCtx.wsID,
        this.#shard,
        connCtx.baseCookie,
        downstream,
      );
      this.#clients
        .get(connCtx.clientID)
        ?.close(`replaced by wsID: ${connCtx.wsID}`);
      this.#clients.set(connCtx.clientID, newClient);

      // Note: initConnection() must be synchronous so that `downstream` is
      // immediately returned to the caller (connection.ts). This ensures
      // that if the connection is subsequently closed, the `downstream`
      // subscription can be properly canceled even if #runInLockForClient()
      // has not had a chance to run.
      void startAsyncSpan(tracer, "vs.initConnection.async", () =>
        this.#runInLockForClient(
          connCtx,
          initConnectionMessage,
          async (lc, clientID, msg: InitConnectionBody, cvr) => {
            if (cvr.clientSchema === null && !msg.clientSchema) {
              throw new ProtocolErrorWithLevel(
                {
                  kind: ErrorKind.InvalidConnectionRequest,
                  message:
                    "The initConnection message for a new client group must include client schema.",
                  origin: ErrorOrigin.ZeroCache,
                },
                "warn",
              );
            }
            // Validate auth before sending any data is sent to this connection.
            // the #handleConfigUpdate call below will also transform
            // queries, but that may hit the transform cache so do not rely on
            // it for validation. This also ensures shared maintenance always has
            // a validated connection to fall back to.
            if (!(await this.#validateConnection(connCtx))) {
              return;
            }
            await this.#handleConfigUpdate(
              lc,
              clientID,
              msg,
              cvr,
              "all", // re transform all on new connections
              // Until the profileID is required in the URL, default it to
              // `cg${clientGroupID}`, as is done in the schema migration.
              // As clients update to the zero version with the profileID logic,
              // the value will be correspondingly in the CVR db.
              connCtx.profileID ?? `cg${this.id}`,
              connCtx,
            );
            // this.#authData  and cvr (in particular cvr.clientSchema) have been
            // initialized, signal the run loop to run.
            this.#initialized.resolve("initialized");
          },
          newClient,
        ),
      ).catch((e) => newClient.fail(e));

      return downstream;
    });
  }

  async changeDesiredQueries(
    selector: ConnectionSelector,
    msg: ChangeDesiredQueriesMessage,
  ): Promise<void> {
    await this.#runInLockForClient(
      selector,
      msg,
      (lc, clientID, msg: Partial<InitConnectionBody>, cvr) =>
        this.#handleConfigUpdate(
          lc,
          clientID,
          msg,
          cvr,
          "missing",
          undefined,
          this.connContextManager.mustGetConnectionContext(selector),
        ),
    );
  }

  async updateAuth(
    selector: ConnectionSelector,
    msg: UpdateAuthMessage,
    authRevisionChanged: boolean,
  ): Promise<void> {
    await this.#runInLockForClient(
      selector,
      msg,
      async (lc, clientID, _, cvr) => {
        // Avoid revalidation and query re-transformation if the revision is the same
        if (!authRevisionChanged) {
          lc.debug?.("Auth unchanged, skipping query re-transformation");
          return;
        }
        lc.debug?.("Auth changed, re-validating and re-transforming queries");

        const connCtx =
          this.connContextManager.mustGetConnectionContext(selector);

        // If pipelines are not yet synced, there is no transform request that
        // can absorb validation, so validate immediately.
        if (!this.#pipelinesSynced) {
          if (!(await this.#validateConnection(connCtx))) {
            return;
          }
        }

        // Re-transform all queries so auth-sensitive query expansion matches
        // the newly validated credential.
        return await this.#handleConfigUpdate(
          lc,
          clientID,
          {}, // no config updates, but we want to trigger re-transformation of custom queries if auth changed
          cvr,
          "all",
          undefined,
          connCtx,
        );
      },
    );
  }

  async deleteClients(
    selector: ConnectionSelector,
    msg: DeleteClientsMessage,
  ): Promise<string[]> {
    const deletedClientIDs = await this.#runInLockForClient(
      selector,
      [msg[0], { deleted: msg[1] }],
      (lc, clientID, msg: Partial<InitConnectionBody>, cvr) =>
        this.#handleConfigUpdate(
          lc,
          clientID,
          msg,
          cvr,
          "missing",
          undefined,
          this.connContextManager.mustGetConnectionContext(selector),
        ),
    );
    return deletedClientIDs ?? [];
  }

  #getTTLClock(now: number): TTLClock {
    // We will update ttlClock with delta from the ttlClockBase to the current time.
    const delta = now - this.#ttlClockBase;
    assert(this.#ttlClock !== undefined, "ttlClock should be defined");
    const ttlClock = ttlClockFromNumber(
      ttlClockAsNumber(this.#ttlClock) + delta,
    );
    assert(
      ttlClockAsNumber(ttlClock) <= now,
      "ttlClock should be less than or equal to now",
    );
    this.#ttlClock = ttlClock;
    this.#ttlClockBase = now;
    return ttlClock;
  }

  #flushUpdater(lc: LogContext, updater: CVRUpdater): Promise<CVRSnapshot> {
    return startAsyncSpan(tracer, "vs.#flushUpdater", () =>
      this.#runPriorityOp(lc, "flushing cvr", async () => {
        const now = Date.now();
        const ttlClock = this.#getTTLClock(now);
        const { cvr, flushed } = await updater.flush(
          lc,
          this.#lastConnectTime,
          now,
          ttlClock,
        );

        if (flushed) {
          // If the CVR was flushed, we restart the ttlClock interval.
          this.#startTTLClockInterval(lc);
        }

        return cvr;
      }),
    );
  }

  #startTTLClockInterval(lc: LogContext): void {
    this.#stopTTLClockInterval();
    this.#ttlClockInterval = this.#setTimeout(() => {
      this.#updateTTLClockInCVRWithoutLock(lc);
      this.#startTTLClockInterval(lc);
    }, TTL_CLOCK_INTERVAL);
  }

  #stopTTLClockInterval(): void {
    clearTimeout(this.#ttlClockInterval);
    this.#ttlClockInterval = 0;
  }

  #updateTTLClockInCVRWithoutLock(lc: LogContext): void {
    const rid = randomID();
    lc.debug?.("Syncing ttlClock", rid);
    const start = Date.now();
    const ttlClock = this.#getTTLClock(start);
    this.#cvrStore
      .updateTTLClock(ttlClock, start)
      .then(() => {
        lc.debug?.("Synced ttlClock", rid, `in ${Date.now() - start} ms`);
      })
      .catch((e) => {
        lc.warn?.(
          "failed to update TTL clock",
          rid,
          `after ${Date.now() - start} ms`,
          e,
        );
      });
  }

  async #updateCVRConfig(
    lc: LogContext,
    cvr: CVRSnapshot,
    clientID: string,
    customQueryTransformMode: CustomQueryTransformMode,
    connCtx: ConnectionContext | undefined,
    fn: (updater: CVRConfigDrivenUpdater) => PatchToVersion[],
  ): Promise<CVRSnapshot> {
    const updater = new CVRConfigDrivenUpdater(
      this.#cvrStore,
      cvr,
      this.#shard,
    );
    if (connCtx) {
      this.#restorePinnedUserFromCVR(cvr);
      const currentConnCtx =
        this.connContextManager.mustGetConnectionContext(connCtx);
      if (
        currentConnCtx.state !== "validated" &&
        !(await this.#validateConnection(currentConnCtx))
      ) {
        return cvr;
      }
      const pinnedUser = this.connContextManager.getGroupState().pinnedUser;
      assert(
        pinnedUser !== undefined,
        "validated connection must pin client group user",
      );
      updater.setPinnedUser(pinnedUser);
    }
    updater.ensureClient(clientID);
    const patches = fn(updater);

    this.#cvr = await this.#flushUpdater(lc, updater);

    if (cmpVersions(cvr.version, this.#cvr.version) < 0) {
      // Send pokes to catch up clients that are up to date.
      // (Clients that are behind the cvr.version need to be caught up in
      //  #syncQueryPipelineSet(), as row data may be needed for catchup)
      const newCVR = this.#cvr;
      await startAsyncSpan(
        tracer,
        "vs.#updateCVRConfig.pokeClients",
        async () => {
          const pokers = startPoke(
            this.#getClients(cvr.version),
            newCVR.version,
          );
          for (const patch of patches) {
            await pokers.addPatch(patch);
          }
          await pokers.end(newCVR.version);
        },
      );
    }

    if (this.#pipelinesSynced) {
      await this.#syncQueryPipelineSet(
        lc,
        this.#cvr,
        customQueryTransformMode,
        connCtx,
      );
    }

    return this.#cvr;
  }

  /**
   * Runs the given `fn` to process the `msg` from within the `#lock`,
   * optionally adding the `newClient` if supplied.
   */
  #runInLockForClient<B, R = void, M extends [cmd: string, B] = [string, B]>(
    selector: ConnectionSelector,
    msg: M,
    fn: (
      lc: LogContext,
      clientID: string,
      body: B,
      cvr: CVRSnapshot,
    ) => Promise<R>,
    newClient?: ClientHandler,
  ): Promise<R | undefined> {
    this.#lc.debug?.("viewSyncer.#runInLockForClient");
    const { clientID, wsID } = selector;
    const [cmd, body] = msg;

    if (newClient || !this.#clients.has(clientID)) {
      this.#lastConnectTime = Date.now();
    }

    return startAsyncSpan(
      tracer,
      `vs.#runInLockForClient(${cmd})`,
      async (span) => {
        span.setAttribute("clientGroupID", this.id);
        span.setAttribute("clientID", clientID);
        let client: ClientHandler | undefined;
        let result: R | undefined;
        let connCtx: ConnectionContext | undefined;
        try {
          await this.#runInLockWithCVR(async (lc, cvr) => {
            lc = lc
              .withContext("clientID", clientID)
              .withContext("wsID", wsID)
              .withContext("cmd", cmd);
            lc.debug?.("acquired lock for cvr");

            client = this.#clients.get(clientID);
            if (client?.wsID !== wsID) {
              lc.debug?.("mismatched wsID", client?.wsID, wsID);
              // Only respond to messages of the currently connected client.
              // Connections may have been drained or dropped due to an error.
              return;
            }

            connCtx = this.connContextManager.getConnectionContext(selector);

            if (newClient) {
              assert(
                newClient === client,
                "newClient must match existing client",
              );
              checkClientAndCVRVersions(client.version(), cvr.version);
            } else if (!this.#clients.has(clientID)) {
              lc.warn?.(`Processing ${cmd} before initConnection was received`);
            }

            lc.debug?.(cmd, body);
            result = await fn(lc, clientID, body, cvr);
          });
        } catch (e) {
          const lc = this.#lc
            .withContext("clientID", clientID)
            .withContext("wsID", wsID)
            .withContext("cmd", cmd);
          lc[getLogLevel(e)]?.(`closing connection with error`, e);
          if (connCtx) {
            this.connContextManager.failConnection(selector, connCtx.revision);
          }
          if (client) {
            // Ideally, propagate the exception to the client's downstream subscription ...
            client.fail(e);
          } else {
            // unless the exception happened before the client could be looked up.
            throw e;
          }
        }
        return result;
      },
    );
  }

  #getClients(atVersion?: CVRVersion): ClientHandler[] {
    const clients = [...this.#clients.values()];
    return atVersion
      ? clients.filter(
          (c) => cmpVersions(c.version() ?? EMPTY_CVR_VERSION, atVersion) === 0,
        )
      : clients;
  }

  // Must be called from within #lock.
  readonly #handleConfigUpdate = (
    lc: LogContext,
    clientID: string,

    {
      clientSchema,
      deleted,
      desiredQueriesPatch,
      activeClients,
    }: Partial<InitConnectionBody>,
    cvr: CVRSnapshot,
    customQueryTransformMode: CustomQueryTransformMode,
    profileID: string | undefined,
    connCtx: ConnectionContext,
  ) =>
    startAsyncSpan(tracer, "vs.#handleConfigUpdate", async () => {
      const deletedClientIDs: string[] = [];
      const deletedClientGroupIDs: string[] = [];

      cvr = await this.#updateCVRConfig(
        lc,
        cvr,
        clientID,
        customQueryTransformMode,
        connCtx,
        (updater) => {
          const { ttlClock } = cvr;
          const patches: PatchToVersion[] = [];

          if (clientSchema) {
            updater.setClientSchema(lc, clientSchema);
          }
          if (profileID) {
            updater.setProfileID(lc, profileID);
          }

          // Apply requested patches.
          lc.debug?.(
            `applying ${desiredQueriesPatch?.length ?? 0} query patches`,
          );
          if (desiredQueriesPatch?.length) {
            for (const patch of desiredQueriesPatch) {
              switch (patch.op) {
                case "put":
                  patches.push(...updater.putDesiredQueries(clientID, [patch]));
                  break;
                case "del":
                  patches.push(
                    ...updater.markDesiredQueriesAsInactive(
                      clientID,
                      [patch.hash],
                      ttlClock,
                    ),
                  );
                  break;
                case "clear":
                  patches.push(...updater.clearDesiredQueries(clientID));
                  break;
              }
            }
          }

          const clientIDsToDelete: Set<string> = new Set();

          if (activeClients) {
            // We find all the clients in this client group that are not active.
            const allClientIDs = Object.keys(cvr.clients);
            const activeClientsSet = new Set(activeClients);
            for (const id of allClientIDs) {
              if (!activeClientsSet.has(id)) {
                clientIDsToDelete.add(id);
              }
            }
          }

          if (deleted?.clientIDs?.length) {
            for (const cid of deleted.clientIDs) {
              assert(cid !== clientID, "cannot delete self");
              clientIDsToDelete.add(cid);
            }
          }

          for (const cid of clientIDsToDelete) {
            const patchesDueToClient = updater.deleteClient(cid, ttlClock);
            patches.push(...patchesDueToClient);
            deletedClientIDs.push(cid);
          }

          if (deleted?.clientGroupIDs?.length) {
            lc.debug?.(
              `ignoring ${deleted.clientGroupIDs.length} deprecated client group deletes`,
            );
          }

          return patches;
        },
      );

      // Send 'deleteClients' ack to the clients.
      if (
        (deletedClientIDs.length && deleted?.clientIDs?.length) ||
        deletedClientGroupIDs.length
      ) {
        const clients = this.#getClients();
        await startAsyncSpan(
          tracer,
          "vs.#handleConfigUpdate.sendDeleteClients",
          () =>
            Promise.allSettled(
              clients.map((client) =>
                client.sendDeleteClients(
                  lc,
                  deletedClientIDs,
                  deletedClientGroupIDs,
                ),
              ),
            ),
        );
      }

      this.#scheduleExpireEviction(lc, cvr);
      return deletedClientIDs;
    });

  #scheduleExpireEviction(lc: LogContext, cvr: CVRSnapshot): void {
    const { ttlClock } = cvr;
    this.#stopExpireTimer();

    // first see if there is any inactive query with a ttl.
    const next = nextEvictionTime(cvr);

    if (next === undefined) {
      lc.debug?.("no inactive queries with ttl");
      // no inactive queries with a ttl. Cancel existing timeout if any.
      return;
    }

    // It is common for many queries to be evicted close to the same time, so
    // we add a small delay so we can collapse multiple evictions into a
    // single timer. However, don't add the delay if we're already at the
    // maximum timer limit, as that's not about collapsing.
    const delay = Math.max(
      TTL_TIMER_HYSTERESIS,
      Math.min(
        ttlClockAsNumber(next) -
          ttlClockAsNumber(ttlClock) +
          TTL_TIMER_HYSTERESIS,
        MAX_TTL_MS,
      ),
    );

    lc.debug?.("Scheduling eviction timer to run in ", delay, "ms");
    this.#expiredQueriesTimer = this.#setTimeout(() => {
      this.#expiredQueriesTimer = 0;
      this.#runInLockWithCVR((lc, cvr) =>
        this.#removeExpiredQueries(lc, cvr),
      ).catch((e) =>
        // If an error occurs (e.g. ownership change), propagate the error
        // to the main run() loop via the #stateChanges Subscription.
        this.#stateChanges.fail(e),
      );
    }, delay);
  }

  /**
   * Adds and hydrates pipelines for queries whose results are already
   * recorded in the CVR. Namely:
   *
   * 1. The CVR state version and database version are the same.
   * 2. The transformation hash of the queries equal those in the CVR.
   *
   * Note that by definition, only "got" queries can satisfy condition (2),
   * as desired queries do not have a transformation hash.
   *
   * This is an initialization step that sets up pipeline state without
   * the expensive of loading and diffing CVR row state.
   *
   * This must be called from within the #lock.
   */
  async #hydrateUnchangedQueries(
    lc: LogContext,
    cvr: CVRSnapshot,
  ): Promise<Set<string>> {
    assert(this.#pipelines.initialized(), "pipelines must be initialized");

    // Gen-6: gate on the version the hydrate DATA will be read at, not just
    // TS's snapshot. In Go-primary mode user rows hydrate from Go's own
    // (later-pinned) snapshot; this no-CVR-updater fast path is only sound
    // when the pipeline row set provably equals the CVR row set, i.e. when
    // the DATA version matches the CVR version exactly. Gating on TS's
    // version alone let Go deliver gap rows into pipelines the CVR never
    // learned about (silent skew), and pushed the mismatch into the
    // #addAndRemoveQueries updater at an unbumped version (cvr.ts:778
    // teardown). awaitGoInit first so Go's pin is populated.
    await this.#pipelines.awaitGoInit();
    const dbVersion = this.#pipelines.hydrateVersion();
    const cvrVersion = cvr.version;

    if (cvrVersion.stateVersion !== dbVersion) {
      lc.info?.(
        `CVR (${versionToCookie(cvrVersion)}) is behind db ${dbVersion}`,
      );
      return new Set(); // hydration needs to be run with the CVR updater.
    }

    const gotQueries = Object.entries(cvr.queries).filter(
      ([_, state]) => state.transformationHash !== undefined,
    );

    const customQueries: Map<string, CustomQueryRecord> = new Map();
    const otherQueries: (ClientQueryRecord | InternalQueryRecord)[] = [];
    let inactivatedCount = 0;

    for (const [, query] of gotQueries) {
      if (
        query.type !== "internal" &&
        Object.values(query.clientState).every(
          ({ inactivatedAt }) => inactivatedAt !== undefined,
        )
      ) {
        inactivatedCount++;
        continue; // No longer desired.
      }

      if (query.type === "custom") {
        customQueries.set(query.id, query);
      } else {
        otherQueries.push(query);
      }
    }

    const transformedQueries: TransformedAndHashed[] = [];
    let customErrorCount = 0;
    let customHashMismatchCount = 0;
    let otherHashMismatchCount = 0;
    if (customQueries.size > 0 && !this.#customQueryTransformer) {
      lc.warn?.(
        "Custom/named queries were requested but no `ZERO_QUERY_URL` is configured for Zero Cache.",
      );
    }
    const backgroundContext =
      this.connContextManager.mustGetBackgroundConnectionContext();
    const customQueryTransformer = this.#customQueryTransformer;
    if (customQueryTransformer && customQueries.size > 0) {
      // Always transform custom queries during initialization to ensure
      // authorization validation with current auth context.
      const transformedCustomQueries = await this.#runPriorityOp(
        lc,
        "#hydrateUnchangedQueries transforming custom queries",
        () =>
          customQueryTransformer.transform(
            backgroundContext,
            customQueries.values(),
          ),
      );
      // Uncached results can also return the authoritative server userID
      // for that snapshot.
      if (
        transformedCustomQueries.kind === "success" &&
        !transformedCustomQueries.cached
      ) {
        this.connContextManager.validateConnection(
          backgroundContext,
          backgroundContext.revision,
          transformedCustomQueries.validation,
        );
      }

      // Only process queries that successfully transformed and transformed to
      // the same transformationHash as in the CVR here.
      // Queries that failed to transform will be retransformed by
      // #syncQueryPipelineSet, if they fail again errors will be sent to
      // the client.
      if (Array.isArray(transformedCustomQueries.result)) {
        for (const q of transformedCustomQueries.result) {
          if ("error" in q) {
            customErrorCount++;
          } else if (
            q.transformationHash !== customQueries.get(q.id)?.transformationHash
          ) {
            customHashMismatchCount++;
          } else {
            transformedQueries.push(q);
          }
        }
      }
    }

    for (const q of otherQueries) {
      const transformed = transformAndHashQuery(
        lc,
        q.id,
        q.ast,
        must(this.#pipelines.currentPermissions()).permissions ?? {
          tables: {},
        },
        backgroundContext.auth?.type === "jwt"
          ? backgroundContext.auth
          : undefined,
        q.type === "internal",
      );
      if (transformed.transformationHash === q.transformationHash) {
        // only process queries that transformed to the same
        // transformationHash as in the CVR here
        transformedQueries.push(transformed);
      } else {
        otherHashMismatchCount++;
      }
    }

    lc.info?.(
      `hydrateUnchangedQueries: ${gotQueries.length} got queries, ` +
        `${inactivatedCount} inactivated, ` +
        `${customErrorCount} custom transform errors, ` +
        `${customHashMismatchCount} custom hash mismatches, ` +
        `${otherHashMismatchCount} other hash mismatches, ` +
        `${transformedQueries.length} hydrated`,
    );

    // 1.6.1: queries whose re-hydration produced a different row-set
    // signature than the CVR stored are removed for full re-execution;
    // the caller bumps configVersion for these.
    const driftedQueryIDs = new Set<string>();

    const checkRowSetSignatureDrift = (queryID: string, count: number) => {
      // Drift detection: compare the just-computed candidate signature against
      // the signature stored in the CVR. They should match for a deterministic
      // query at the same db state. A mismatch indicates a query containing
      // the Cap operator picked a different N-row subset on re-execution.
      // Remove the query from pipelines so #syncQueryPipelineSet 'missing'
      // re-executes it via the CVRQueryDrivenUpdater path, where the row diff
      // will be properly emitted to the client.
      //
      // Skip when the stored signature is absent — legacy queries from before
      // this feature was deployed have no signature to compare against, and a
      // forced re-execution would needlessly resend rows to the client. Such
      // queries get their signature initialized whenever they next re-execute
      // via the normal path (transformation hash change, etc.), at which
      // point drift detection becomes effective for subsequent cycles.
      const storedSigHex = cvr.queries[queryID]?.rowSetSignature;
      if (storedSigHex !== undefined && storedSigHex !== null) {
        const priorSig = parseSignature(storedSigHex);
        const candidateSig = this.#pipelines.rowSetSignature(queryID) ?? 0n;
        if (priorSig !== candidateSig) {
          lc.warn?.(
            `rowSetSignature drift for query ${queryID}: ` +
              `prior=${priorSig.toString(16)} new=${candidateSig.toString(16)} ` +
              `(${count} rows). Removing from pipelines for full re-execution.`,
          );
          this.#rowSetSignatureDrifts.add(1);
          this.#pipelines.removeQuery(queryID);
          driftedQueryIDs.add(queryID);
        }
      }
    };

    const recordHydrated = (
      queryID: string,
      transformationHash: string,
      transformedAst: AST,
      elapsed: number,
      count: number,
    ) => {
      this.#hydrations.add(1);
      this.#hydrationTime.recordMs(elapsed);
      this.#addQueryMaterializationServerMetric(transformationHash, elapsed);
      this.#inspectorDelegate.addQuery(transformationHash, transformedAst);
      lc.debug?.(`hydrated ${count} rows for ${queryID} (${elapsed} ms)`);
      manualSpan(tracer, "vs.hydrateUnchangedQuery", elapsed, {
        hash: queryID,
        transformationHash,
      });
    };

    const hydrateQueries = transformedQueries.map((q) => {
      const queryID = q.id;
      const query = cvr.queries[queryID];
      const queryName = query.type === "custom" ? query.name : undefined;
      return {
        id: queryID,
        transformationHash: q.transformationHash,
        transformedAst: q.transformedAst,
        ...(queryName !== undefined && { name: queryName }),
      };
    });

    if (hydrateQueries.length > 0) {
      await this.#pipelines.awaitGoInit();
    }

    if (this.#pipelines.canBatchHydrate && hydrateQueries.length > 0) {
      const batchStart = performance.now();
      const byID = new Map(hydrateQueries.map((q) => [q.id, q]));
      const counts = new Map<string, number>();
      let completed = 0;
      let totalComputeMs = 0;

      await startAsyncSpan(
        tracer,
        "vs.#hydrateUnchangedQueries.batch",
        async (span) => {
          span.setAttribute("queries", hydrateQueries.length);
          const stream = this.#pipelines.goHydrateBatchStream(
            hydrateQueries.map((q) => ({
              transformationHash: q.transformationHash,
              queryID: q.id,
              ast: q.transformedAst,
              ...(q.name !== undefined && { queryName: q.name }),
            })),
          );
          for await (const { queryID, changes, timingMs, final } of stream) {
            const q = byID.get(queryID);
            if (!q) continue;

            let count = counts.get(queryID) ?? 0;
            const consumeStart = performance.now();
            for (const change of changes) {
              if (change === "yield") {
                await yieldProcess(lc);
              } else {
                count++;
              }
            }
            const consumeElapsed = performance.now() - consumeStart;
            counts.set(queryID, count);

            if (!final) {
              continue;
            }

            const elapsed = timingMs ?? consumeElapsed;
            totalComputeMs += elapsed;
            recordHydrated(
              q.id,
              q.transformationHash,
              q.transformedAst,
              elapsed,
              count,
            );
            checkRowSetSignatureDrift(q.id, count);
            byID.delete(queryID);
            completed++;
          }
          if (byID.size > 0) {
            throw new Error(
              `hydrateUnchangedQueries batch ended before final for ` +
                `${[...byID.keys()].join(", ")}`,
            );
          }
        },
      );

      lc.info?.(
        `hydrateUnchangedQueries batch hydrated ${completed} queries ` +
          `in ${(performance.now() - batchStart).toFixed(2)}ms ` +
          `(compute ${totalComputeMs.toFixed(2)}ms)`,
      );
      return driftedQueryIDs;
    }

    for (const {
      id: queryID,
      transformationHash,
      transformedAst,
    } of transformedQueries) {
      const query = cvr.queries[queryID];
      const queryName = query.type === "custom" ? query.name : undefined;
      const timer = new TimeSliceTimer(lc);
      let count = 0;
      await startAsyncSpan(
        tracer,
        "vs.#hydrateUnchangedQueries.addQuery",
        async (span) => {
          span.setAttribute("queryHash", queryID);
          span.setAttribute("transformationHash", transformationHash);
          span.setAttribute("table", transformedAst.table);
          if (queryName !== undefined) {
            span.setAttribute("queryName", queryName);
          }
          for await (const change of await this.#pipelines.addQuery(
            transformationHash,
            queryID,
            transformedAst,
            await timer.start(),
            queryName,
          )) {
            if (change === "yield") {
              await timer.yieldProcess("yield in hydrateUnchangedQueries");
            } else {
              count++;
            }
          }
        },
      );

      const elapsed = timer.totalElapsed();
      recordHydrated(
        queryID,
        transformationHash,
        transformedAst,
        elapsed,
        count,
      );
      checkRowSetSignatureDrift(queryID, count);
    }

    return driftedQueryIDs;
  }

  #processTransformedCustomQueries(
    lc: LogContext,
    transformedCustomQueries:
      (TransformedAndHashed | ErroredQuery)[] | TransformFailedBody,
    cb: (q: TransformedAndHashed) => void,
    customQueryMap: Map<string, CustomQueryRecord>,
  ): string[] {
    if ("kind" in transformedCustomQueries) {
      this.#sendQueryTransformErrorToClients(
        customQueryMap,
        transformedCustomQueries,
      );
      return transformedCustomQueries.queryIDs;
    }

    const appQueryErrors: ErroredQuery[] = [];

    for (const q of transformedCustomQueries) {
      if ("error" in q) {
        const errorMessage = `Error transforming custom query ${q.name}: ${q.error}${q.details ? ` ${JSON.stringify(q.details)}` : ""}`;
        lc.warn?.(errorMessage, q);
        appQueryErrors.push(q);
        continue;
      }
      cb(q);
    }

    this.#sendQueryTransformErrorToClients(customQueryMap, appQueryErrors);
    return appQueryErrors.map((q) => q.id);
  }

  #sendQueryTransformErrorToClients(
    customQueryMap: Map<string, CustomQueryRecord>,
    errorOrErrors: ErroredQuery[] | TransformFailedBody,
  ) {
    const getAffectedClientIDs = (queryIDs: string[]): Set<string> => {
      const clientIds = new Set<string>();
      for (const queryID of queryIDs) {
        const q = customQueryMap.get(queryID);
        assert(
          q,
          `got an error for query ${queryID} that does not map back to a custom query`,
        );
        Object.keys(q.clientState).forEach((id) => clientIds.add(id));
      }
      return clientIds;
    };

    // send the transform failed error to each affected client
    if ("queryIDs" in errorOrErrors) {
      for (const clientId of getAffectedClientIDs(errorOrErrors.queryIDs)) {
        this.#clients
          .get(clientId)
          ?.sendQueryTransformFailedError(errorOrErrors);
      }

      return;
    }

    // Group and send application errors to each affected client
    const appErrorGroups = new Map<string, ErroredQuery[]>();

    for (const err of errorOrErrors) {
      // Application errors need to be grouped by client
      for (const clientId of getAffectedClientIDs([err.id])) {
        const group = appErrorGroups.get(clientId) ?? [];
        group.push(err);
        appErrorGroups.set(clientId, group);
      }
    }

    for (const [clientId, errors] of appErrorGroups) {
      this.#clients.get(clientId)?.sendQueryTransformApplicationErrors(errors);
    }
  }

  #addQueryMaterializationServerMetric(queryID: string, elapsed: number) {
    this.#inspectorDelegate.addMetric(
      "query-materialization-server",
      elapsed,
      queryID,
    );
  }

  /**
   * Adds and/or removes queries to/from the PipelineDriver to bring it
   * in sync with the set of queries in the CVR (both got and desired).
   * If queries are added, removed, or queried due to a new state version,
   * a new CVR version is created and pokes sent to connected clients.
   *
   * This must be called from within the #lock.
   */
  #syncQueryPipelineSet(
    lc: LogContext,
    cvr: CVRSnapshot,
    customQueryTransformMode: CustomQueryTransformMode,
    connCtx: ConnectionContext | undefined,
    driftedQueryIDs: Set<string> = new Set(),
  ) {
    return startAsyncSpan(tracer, "vs.#syncQueryPipelineSet", async (span) => {
      span.setAttribute("clientGroupID", this.id);
      assert(
        this.#pipelines.initialized(),
        "pipelines must be initialized (syncQueryPipelineSet)",
      );

      if (this.#ttlClock === undefined) {
        // Get it from the CVR or initialize it to now.
        this.#ttlClock = cvr.ttlClock;
      }
      const now = Date.now();
      const ttlClock = this.#getTTLClock(now);

      // group cvr queries into:
      // 1. custom queries
      // 2. everything else
      // Handle transformation appropriately
      // Then hydrate as `serverQueries`
      const cvrQueryEntires = Object.entries(cvr.queries);
      const customQueries: Map<string, CustomQueryRecord> = new Map();
      const otherQueries: {
        id: string;
        query: ClientQueryRecord | InternalQueryRecord;
      }[] = [];
      const transformedQueries: {
        id: string;
        origQuery: QueryRecord;
        transformed: TransformedAndHashed;
      }[] = [];
      // When a specific connection triggered this work, use its context.
      // Only background/shared sync work falls back to the selected
      // validated connection.
      const resolvedConnCtx =
        connCtx ?? this.connContextManager.mustGetBackgroundConnectionContext();

      for (const [id, query] of cvrQueryEntires) {
        if (query.type === "custom") {
          // This should always match, no?
          assert(id === query.id, "custom query id mismatch");
          customQueries.set(id, query);
        } else {
          otherQueries.push({ id, query });
        }
      }

      for (const { id, query: origQuery } of otherQueries) {
        // This should always match, no?
        assert(id === origQuery.id, "query id mismatch");
        const transformed = transformAndHashQuery(
          lc,
          origQuery.id,
          origQuery.ast,
          must(this.#pipelines.currentPermissions()).permissions ?? {
            tables: {},
          },
          resolvedConnCtx.auth?.type === "jwt"
            ? resolvedConnCtx.auth
            : undefined,
          origQuery.type === "internal",
        );
        transformedQueries.push({
          id,
          origQuery,
          transformed,
        });
      }

      if (customQueries.size > 0 && !this.#customQueryTransformer) {
        lc.warn?.(
          "Custom/named queries were requested but no `ZERO_QUERY_URL` is configured for Zero Cache.",
        );
      }

      let erroredQueryIDs: string[] | undefined;
      const customQueriesToTransform =
        customQueryTransformMode === "all"
          ? [...customQueries.values()]
          : (customQueryTransformMode satisfies "missing") &&
            [...customQueries.values()].filter(
              (q) => !this.#pipelines.queries().has(q.id),
            );
      const customQueryTransformer = this.#customQueryTransformer;
      if (customQueryTransformer && customQueriesToTransform.length > 0) {
        // Always re-transform custom queries on client connection for security.
        // This ensures the user's API server validates authorization with the
        // current auth context.
        const transformStart = performance.now();
        let transformedCustomQueries: HashedTransformResponse;
        try {
          transformedCustomQueries = await this.#runPriorityOp(
            lc,
            "#syncQueryPipelineSet transforming custom queries",
            () =>
              customQueryTransformer.transform(
                resolvedConnCtx,
                customQueriesToTransform,
              ),
          );

          // Check if transform failed entirely (HTTP error or server-side failure).
          // This should disconnect the client and keep existing pipelines intact.
          if (transformedCustomQueries.kind === "failed") {
            throw new ProtocolErrorWithLevel(
              transformedCustomQueries.result,
              "warn",
            );
          } else {
            // If the transform wasn't cached, we mark the connection as validated.
            // This also threads the authoritative server userID through the
            // revision check so auth races do not validate stale credentials.
            if (!transformedCustomQueries.cached) {
              this.connContextManager.validateConnection(
                resolvedConnCtx,
                resolvedConnCtx.revision,
                transformedCustomQueries.validation,
              );
            }
            this.#queryTransformations.add(1, { result: "success" });
          }
        } catch (e) {
          this.#queryTransformations.add(1, { result: "error" });
          throw e;
        } finally {
          const transformDuration = performance.now() - transformStart;
          this.#queryTransformationTime.recordMs(transformDuration);
        }

        // Process the transformed queries and track which ones succeeded.
        const successfullyTransformedCustomQueries = new Map<
          string,
          TransformedAndHashed
        >();
        erroredQueryIDs = this.#processTransformedCustomQueries(
          lc,
          transformedCustomQueries.result,
          (q: TransformedAndHashed) => {
            const origQuery = customQueries.get(q.id);
            if (origQuery) {
              successfullyTransformedCustomQueries.set(q.id, q);
              transformedQueries.push({
                id: q.id,
                origQuery,
                transformed: q,
              });
            }
          },
          customQueries,
        );

        // Check for queries whose transformation hash changed and log for debugging.
        // The old pipelines will be removed and destroyed when
        // PipelineManager.addQuery is called with an existing query id and
        // different transformation hash.
        for (const [
          queryID,
          newTransform,
        ] of successfullyTransformedCustomQueries) {
          const existingTransformHash =
            cvr.queries[queryID]?.transformationHash;
          if (existingTransformHash) {
            const oldHash = existingTransformHash;
            const newHash = newTransform.transformationHash;

            if (oldHash !== newHash) {
              // Transformation changed - log and check for thrashing.
              // The unhydrateQueries mechanism below will remove the old pipeline,
              // and addQueries will add the new one.
              lc.info?.(
                `Query ${queryID} transformation changed: ${oldHash} -> ${newHash}`,
              );
              this.#checkForThrashing(queryID);
              this.#queryTransformationHashChanges.add(1);
            } else {
              // hash is the same (no re-hydration needed)
              this.#queryTransformationNoOps.add(1);
            }
          }
          // else: new query, will be added normally
        }
      }

      const removeQueriesQueryIds: Set<string> = new Set([
        ...Object.values(cvr.queries)
          .filter((q) => expired(ttlClock, q))
          .map((q) => q.id),
        ...(erroredQueryIDs || []),
      ]);
      const addQueries = transformedQueries
        .map(({ id, origQuery, transformed }) => ({
          id,
          ast: transformed.transformedAst,
          transformationHash: transformed.transformationHash,
          name: origQuery.type === "custom" ? origQuery.name : undefined,
        }))
        .filter(
          (q) =>
            !removeQueriesQueryIds.has(q.id) &&
            this.#pipelines.queries().get(q.id)?.transformationHash !==
              q.transformationHash,
        );

      lc.info?.(
        `syncQueryPipelineSet: ${cvrQueryEntires.length} CVR queries, ` +
          `${customQueriesToTransform.length} custom re-transformed, ` +
          `${erroredQueryIDs?.length ?? 0} errored, ` +
          `${removeQueriesQueryIds.size} to remove, ` +
          `${addQueries.length} to add`,
      );

      for (const q of addQueries) {
        const orig = cvr.queries[q.id];
        lc.debug?.(
          "ViewSyncer adding query",
          q.ast,
          "transformed from",
          orig.type === "custom" ? orig.name : orig.ast,
        );
      }

      if (addQueries.length > 0 || removeQueriesQueryIds.size > 0) {
        await this.#addAndRemoveQueries(
          lc,
          cvr,
          addQueries,
          Array.from(removeQueriesQueryIds, (id) => ({ id })),
          driftedQueryIDs,
        );
      } else {
        await this.#catchupClients(lc, cvr);
      }
    });
  }

  /**
   * Check if a query is being replaced too frequently (thrashing).
   * Logs a warning if the query has been replaced more than 3 times in 60 seconds.
   */
  #checkForThrashing(queryID: string) {
    const THRASH_WINDOW_MS = 60_000; // 60 seconds
    const THRASH_THRESHOLD = 3;
    const now = Date.now();

    let record = this.#queryReplacements.get(queryID);
    if (!record) {
      record = { count: 1, windowStart: now };
      this.#queryReplacements.set(queryID, record);
      return;
    }

    // If outside the time window, delete the old entry and create a new one
    if (now - record.windowStart > THRASH_WINDOW_MS) {
      this.#queryReplacements.delete(queryID);
      this.#queryReplacements.set(queryID, { count: 1, windowStart: now });
      return;
    }

    // Increment count within the window
    record.count++;

    if (record.count >= THRASH_THRESHOLD) {
      this.#lc.warn?.(
        `Query thrashing detected for query ${queryID}. ${record.count} replacements in 60s. This may indicate clients with different auth contexts connecting to the same client group.`,
      );
    }
  }

  // This must be called from within the #lock.
  #addAndRemoveQueries(
    lc: LogContext,
    cvr: CVRSnapshot,
    addQueries: {
      id: string;
      ast: AST;
      transformationHash: string;
      name?: string | undefined;
    }[],
    removeQueries: { id: string }[],
    driftedQueryIDs: Set<string> = new Set(),
  ): Promise<void> {
    return startAsyncSpan(tracer, "vs.#addAndRemoveQueries", async () => {
      assert(
        addQueries.length > 0 || removeQueries.length > 0,
        "Must have queries to add or remove",
      );
      const start = performance.now();

      // Gen-6: stamp the updater at the version the hydrate DATA is read at.
      // In Go-primary mode Go pins its snapshot AFTER TS pins its own, so
      // rows can carry _0_versions above TS's currentVersion(). Stamping at
      // TS's version alone can equal the committed CVR version exactly
      // (reconnects re-execute queries with UNCHANGED transformation hashes,
      // which bump nothing), and the first Go-delivered gap row then fires
      // cvr.ts's "Expected CVR version to have been bumped above original"
      // assertion — a full client-group teardown. hydrateVersion() is
      // max(TS version, Go's init pin, Go's last advance version); the
      // constructor bumps whenever it exceeds the CVR version, making the
      // gap rows legally stampable. Await Go init BEFORE reading it — the
      // pin is only known once init completes (generateRowChanges' own
      // awaitGoInit below happens after the updater exists, too late).
      await this.#pipelines.awaitGoInit();
      const stateVersion = this.#pipelines.hydrateVersion();
      lc = lc.withContext("stateVersion", stateVersion);
      lc.info?.(`hydrating ${addQueries.length} queries`);

      const updater = new CVRQueryDrivenUpdater(
        this.#cvrStore,
        cvr,
        stateVersion,
        this.#pipelines.replicaVersion,
        (queryID) => this.#pipelines.rowSetSignature(queryID),
      );

      // For queries being re-executed solely due to rowSetSignature drift
      // (not a transformationHash change), trackQueries does not bump
      // configVersion. Force a bump so the row diff produced by received()
      // gets propagated to the client via a poke. Must happen before
      // startPoke so the pokers see the final cookie version.
      if (addQueries.some((q) => driftedQueryIDs.has(q.id))) {
        updater.ensureNewVersion();
      }

      let pokers: PokeHandler | undefined;
      try {
        // Note: This kicks off background PG queries for CVR data associated with the
        // executed and removed queries.
        const { queryPatches, newVersion } = updater.trackQueries(
          lc,
          addQueries,
          removeQueries,
        );

        const clients = this.#getClients();
        const activePokers = (pokers = startPoke(clients, newVersion));
        for (const patch of queryPatches) {
          // Bump patches' toVersion to the post-drift-bump version so that
          // pokers don't see them as belonging to a stale cookie.
          await activePokers.addPatch(patch);
        }

        // Removing queries is easy. The pipelines are dropped, and the CVR
        // updater handles the updates and pokes.
        for (const q of removeQueries) {
          this.#pipelines.removeQuery(q.id);
          // Remove per-query server metrics when query is deleted
          this.#inspectorDelegate.removeQuery(q.id);
          // Clean up thrashing detection for removed queries
          this.#queryReplacements.delete(q.id);
        }

        let totalProcessTime = 0;
        const timer = new TimeSliceTimer(lc);
        const pipelines = this.#pipelines;
        const hydrations = this.#hydrations;
        const hydrationTime = this.#hydrationTime;
        // oxlint-disable-next-line @typescript-eslint/no-this-alias
        const self = this;

        // yield at the very beginning so that the first time slice
        // is properly processed by the time-slice queue.
        await yieldProcess(lc);

        async function* generateRowChanges(slowHydrateThreshold: number) {
          // Await Go init so the first batch of queries uses the Go path
          if (addQueries.length > 0) {
            await pipelines.awaitGoInit();
          }
          // Streaming batch hydration: send all queries to Go in one RPC,
          // yield each query's RowChanges to the WebSocket pokers AS SOON as
          // Go finishes that query — instead of waiting for the slowest one
          // in the batch. Tail-latency win for batches with uneven hydrate
          // cost.
          if (pipelines.canBatchHydrate && addQueries.length > 0) {
            const batchStart = performance.now();
            const byID = new Map<string, (typeof addQueries)[number]>();
            for (const q of addQueries) byID.set(q.id, q);
            let count = 0;
            let goComputeMs = 0;
            for await (const {
              queryID,
              changes,
              timingMs,
              final,
            } of pipelines.goHydrateBatchStream(
              addQueries.map((q) => ({
                transformationHash: q.transformationHash,
                queryID: q.id,
                ast: q.ast,
                // Custom-query name for the pipeline stub (inspector +
                // queryName log context) — the non-batch path passes it to
                // addQuery; dropping it here lost it in Go-primary (M1).
                queryName: q.name,
              })),
            )) {
              const q = byID.get(queryID);
              if (!q) continue;
              const consumeStart = performance.now();
              yield* changes as Iterable<RowChange | "yield">;
              const consumeElapsed = performance.now() - consumeStart;
              // Per-chunk streaming: this loop body runs once per CHUNK, so the
              // yield* above delivers each chunk's rows to the pokers as soon as
              // Go produces them. Per-QUERY bookkeeping (hydration_time, span,
              // count) must fire exactly once — gate it on the terminal chunk.
              // In the default (accumulate-to-final) mode every entry is
              // terminal, so this is a no-op guard and behavior is unchanged.
              if (!final) {
                continue;
              }
              // `timingMs` is Go's per-query engine COMPUTE (engine.go
              // hydrateEntry, `time.Since(start)` — excludes RPC/serialize).
              // It is undefined for internal queries (lmids/clients/...)
              // that ran through TS's #addQueryImpl, whose compute happens
              // lazily during the yield* above — so consumeElapsed IS their
              // compute. Either way `computeMs` is this query's hydrate
              // compute, which is the span #hydrationTime measures on the TS
              // path. Recording this (not the TS-side consumption of
              // already-computed Go rows) keeps hydration_time apples-to-apples
              // across engines; consumeElapsed was recorded
              // for Go user queries, which excluded Go's own compute and
              // understated Go.
              const computeMs = timingMs ?? consumeElapsed;
              totalProcessTime += computeMs;
              goComputeMs += computeMs;
              self.#addQueryMaterializationServerMetric(q.id, computeMs);
              if (computeMs > slowHydrateThreshold) {
                lc.warn?.("Slow query materialization", computeMs, q.ast);
              }
              manualSpan(tracer, "vs.addAndConsumeQuery", computeMs, {
                hash: q.id,
                transformationHash: q.transformationHash,
                ...(q.name !== undefined && { name: q.name }),
              });
              count++;
            }
            const batchElapsed = performance.now() - batchStart;
            lc.info?.(
              `[batch-hydrate-stream] ${count} queries in ${batchElapsed.toFixed(2)}ms (go compute ${goComputeMs.toFixed(2)}ms)`,
            );
            hydrations.add(1);
            // hydration_time = engine COMPUTE (Go timingMs / TS lazy consume),
            // the apples-to-apples match to TS's hydration_time (full TS
            // materialization compute, no sidecar RPC).
            hydrationTime.recordMs(totalProcessTime);
            // hydration_e2e_time = whole-batch wall-clock: Go compute + RPC +
            // TS-side consumption. The fair END-TO-END headline — Go-
            // pessimistic, since this span includes the sidecar RPC tax that
            // TS's hydration_time (recorded above as the same compute span)
            // does not pay. The gap between the two on Go is the serialization
            // cost (see positional wire-format work).
            self.#hydrationE2ETime.recordMs(batchElapsed);
            return;
          }

          for (const q of addQueries) {
            let queryLC = lc
              .withContext("hash", q.id)
              .withContext("queryHash", q.id)
              .withContext("transformationHash", q.transformationHash);
            if (q.name !== undefined) {
              queryLC = queryLC.withContext("queryName", q.name);
            }
            queryLC.debug?.(`adding pipeline for query`, q.ast);

            const result = pipelines.addQuery(
              q.transformationHash,
              q.id,
              q.ast,
              timer.startWithoutYielding(),
              q.name,
            );
            const iterable = result instanceof Promise ? await result : result;
            let queryTotal = 0;
            for await (const c of iterable) {
              if (c !== "yield") {
                queryTotal++;
              }
              yield c;
            }
            const elapsed = timer.stop();
            totalProcessTime += elapsed;

            self.#addQueryMaterializationServerMetric(q.id, elapsed);
            self.#inspectorDelegate.addQuery(q.id, q.ast);

            if (elapsed > slowHydrateThreshold) {
              queryLC.warn?.("Slow query materialization", elapsed, q.ast);
            }
            manualSpan(tracer, "vs.addAndConsumeQuery", elapsed, {
              hash: q.id,
              transformationHash: q.transformationHash,
              ...(q.name !== undefined && { name: q.name }),
            });
          }
          hydrations.add(1);
          hydrationTime.recordMs(totalProcessTime);
        }
        // #processChanges does batched de-duping of rows. Wrap all pipelines in
        // a single generator in order to maximize de-duping.
        await this.#processChanges(
          lc,
          timer,
          generateRowChanges(this.#slowHydrateThreshold),
          updater,
          activePokers,
        );

        await startAsyncSpan(
          tracer,
          "vs.#syncQueryPipelineSet.deleteUnreferencedRows",
          async () => {
            for (const patch of await updater.deleteUnreferencedRows(lc)) {
              await activePokers.addPatch(patch);
            }
          },
        );

        // Commit the changes and update the CVR snapshot.
        this.#cvr = await this.#flushUpdater(lc, updater);

        const finalVersion = this.#cvr.version;

        // Before ending the poke, catch up clients that were behind the old CVR.
        await this.#catchupClients(
          lc,
          cvr,
          finalVersion,
          addQueries.map((q) => q.id),
          activePokers,
        );

        // Signal clients to commit.
        await startAsyncSpan(tracer, "vs.#syncQueryPipelineSet.pokeEnd", () =>
          activePokers.end(finalVersion),
        );

        const wallTime = performance.now() - start;
        lc.info?.(
          `finished processing queries (process: ${totalProcessTime} ms, wall: ${wallTime} ms)`,
        );
      } catch (e) {
        await pokers?.cancel();
        this.#cvrStore.discardPendingWrites();
        throw e;
      }
    });
  }

  /**
   * @param cvr The CVR to which clients should be caught up to. This does
   *     not necessarily need to be the current CVR.
   * @param current The expected current CVR version. Before performing
   *     catchup, the snapshot read will verify that the CVR has not been
   *     concurrently modified. Note that this only needs to be done for
   *     catchup because it is the only time data from the CVR DB is
   *     "exported" without being gated by a CVR flush (which provides
   *     concurrency protection in all other cases).
   *
   *     If unspecified, the version of the `cvr` is used.
   * @param excludeQueryHashes Exclude patches from rows associated with
   *     the specified queries.
   * @param usePokers If specified, sends pokes on existing PokeHandlers,
   *     in which case the caller is responsible for sending the `pokeEnd`
   *     messages. If unspecified, the pokes will be started and ended
   *     using the version from the supplied `cvr`.
   */
  // Must be called within #lock
  #catchupClients(
    lc: LogContext,
    cvr: CVRSnapshot,
    current?: CVRVersion,
    excludeQueryHashes: string[] = [],
    usePokers?: PokeHandler,
  ) {
    return startAsyncSpan(tracer, "vs.#catchupClients", async (span) => {
      current ??= cvr.version;
      const clients = this.#getClients();
      const pokers = usePokers ?? startPoke(clients, cvr.version);
      span.setAttribute("numClients", clients.length);

      const catchupFrom = clients
        .map((c) => c.version())
        .reduce((a, b) => (cmpVersions(a, b) < 0 ? a : b), cvr.version);

      // This is an AsyncGenerator which won't execute until awaited.
      const rowPatches = this.#cvrStore.catchupRowPatches(
        lc,
        catchupFrom,
        cvr,
        current,
        excludeQueryHashes,
      );

      // This is a plain async function that kicks off immediately.
      const configPatches = this.#cvrStore.catchupConfigPatches(
        lc,
        catchupFrom,
        cvr,
        current,
      );

      // The configPatches Promise will be awaited, and exceptions propagated,
      // after the rowPatches are processed. However, a catch handler must be
      // installed on the Promise in the meantime in order to avoid Node
      // crashing with an unhandled rejection error.
      configPatches.catch(() => {});

      // await the rowPatches first so that the AsyncGenerator kicks off.
      let rowPatchCount = 0;
      for await (const rows of rowPatches) {
        for (const row of rows) {
          const { schema, table } = row;
          const rowKey = row.rowKey as RowKey;
          const toVersion = versionFromString(row.patchVersion);

          const id: RowID = { schema, table, rowKey };
          let patch: RowPatch;
          if (!row.refCounts) {
            patch = { type: "row", op: "del", id };
          } else {
            const row = must(
              this.#pipelines.getRow(table, rowKey),
              `Missing row ${table}:${stringify(rowKey)}`,
            );
            const { contents } = contentsAndVersion(row);
            patch = { type: "row", op: "put", id, contents };
          }
          const patchToVersion = { patch, toVersion };
          await pokers.addPatch(patchToVersion);
          rowPatchCount++;
        }
      }
      span.setAttribute("rowPatchCount", rowPatchCount);
      if (rowPatchCount) {
        lc.debug?.(`sent ${rowPatchCount} row patches`);
      }

      // Then await the config patches which were fetched in parallel.
      for (const patch of await configPatches) {
        await pokers.addPatch(patch);
      }

      if (!usePokers) {
        await pokers.end(cvr.version);
      }
    });
  }

  #processChanges(
    lc: LogContext,
    timer: TimeSliceTimer,
    changes: Iterable<RowChange | "yield"> | AsyncIterable<RowChange | "yield">,
    updater: CVRQueryDrivenUpdater,
    pokers: PokeHandler,
  ) {
    return startAsyncSpan(tracer, "vs.#processChanges", async () => {
      const start = performance.now();

      const rows = new CustomKeyMap<RowID, RowUpdate>(rowIDString);
      let total = 0;

      const processBatch = () =>
        startAsyncSpan(tracer, "processBatch", async () => {
          const wallElapsed = performance.now() - start;
          total += rows.size;
          lc.debug?.(
            `processing ${rows.size} (of ${total}) rows (${wallElapsed} ms)`,
          );
          const patches = await updater.received(lc, rows);

          await startAsyncSpan(
            tracer,
            "processBatch.flushToClient",
            async (span) => {
              span.setAttribute("patches", patches.length);
              for (const patch of patches) {
                await pokers.addPatch(patch);
              }
            },
          );
          rows.clear();
        });

      await startAsyncSpan(tracer, "loopingChanges", async (span) => {
        for await (const change of changes) {
          if (change === "yield") {
            await timer.yieldProcess("yield in processChanges");
            continue;
          }
          const { type, queryID, table, rowKey, row } = change;
          const rowID: RowID = { schema: "", table, rowKey: rowKey as RowKey };

          let parsedRow = rows.get(rowID);
          if (!parsedRow) {
            parsedRow = { refCounts: {} };
            rows.set(rowID, parsedRow);
          }
          parsedRow.refCounts[queryID] ??= 0;

          const updateVersion = (row: Row) => {
            // IVM can output multiple versions of a row as it goes through its
            // intermediate stages. Always update the version and contents;
            // the last version will reflect the final state.
            const { version, contents } = contentsAndVersion(row);
            parsedRow.version = version;
            parsedRow.contents = contents;
          };
          switch (type) {
            case ChangeType.ADD:
              updateVersion(row);
              parsedRow.refCounts[queryID]++;
              break;
            case ChangeType.EDIT:
              updateVersion(row);
              // No update to refCounts.
              break;
            case ChangeType.REMOVE:
              parsedRow.refCounts[queryID]--;
              break;
            default:
              unreachable(type);
          }

          if (rows.size % CURSOR_PAGE_SIZE === 0) {
            await processBatch();
          }
        }
        if (rows.size) {
          await processBatch();
        }
        span.setAttribute("totalRows", total);
      });
    });
  }

  /**
   * Advance to the current snapshot of the replica and apply / send
   * changes.
   *
   * Must be called from within the #lock.
   *
   * Returns false if the advancement failed due to a schema change.
   */
  #advancePipelines(
    lc: LogContext,
    cvr: CVRSnapshot,
  ): Promise<"success" | ResetPipelinesSignal> {
    return startAsyncSpan(tracer, "vs.#advancePipelines", async (span) => {
      span.setAttribute("clientGroupID", this.id);
      assert(
        this.#pipelines.initialized(),
        "pipelines must be initialized (advancePipelines",
      );
      const start = performance.now();

      const timer = new TimeSliceTimer(lc);
      // GO_SIDECAR: advance() returns Promise when Go backend is active
      const advanceResult = this.#pipelines.advance(timer);
      const resolved =
        advanceResult instanceof Promise ? await advanceResult : advanceResult;
      // in Go-primary mode advance() RETURNS a
      // ResetPipelinesSignal (instead of throwing it) when the Go backend is
      // unavailable, a user delta was dropped, or a truncate/schema-change
      // aborted the diff. Returning it here (rather than letting it escape as a
      // throw, which the OUTER catch in run() turns into a full client-group
      // teardown) routes through the graceful reset+re-hydrate path below.
      if (resolved instanceof ResetPipelinesSignal) {
        return resolved;
      }
      const { version, numChanges, changes, tsVersion, goVersion } = resolved;
      lc = lc.withContext("newVersion", version);

      // in Go-primary trigger mode `version` is the RECONCILED watermark
      // min(tsVersion, goVersion) — user-query data comes from Go (at goVersion)
      // while internal/control-plane data comes from TS (at tsVersion). The
      // CVR stateVersion is a completeness floor, so it must never exceed either
      // authority (over-claiming risks a client missing a gap change). Assert
      // that invariant defensively before stamping it into the CVR below.
      if (tsVersion !== undefined && goVersion !== undefined) {
        assert(
          version <= tsVersion && version <= goVersion,
          () =>
            `reconciled CVR watermark ${version} must be <= both authorities ` +
            `(V_ts=${tsVersion}, V_go=${goVersion})`,
        );
        lc.debug?.(
          `[go-primary] CVR watermark ${version} (V_ts=${tsVersion}, V_go=${goVersion})`,
        );
      }

      // the reconciled Go-primary watermark must never regress below the
      // committed CVR version — CVRQueryDrivenUpdater asserts `stateVersion >=
      // cvr.version.stateVersion` and throws a bare Error otherwise, which would
      // escape as an unrecoverable CG teardown. This should be unreachable (each
      // authority only advances, and removed the full-V_ts reset excursion),
      // but if a non-monotone reconcile ever slips through, convert it into a
      // recoverable full re-hydrate instead of crashing.
      if (version < cvr.version.stateVersion) {
        lc.warn?.(
          `[go-primary] reconciled watermark ${version} regressed below ` +
            `committed CVR ${cvr.version.stateVersion}; resetting pipelines`,
        );
        return new ResetPipelinesSignal(
          `watermark regression ${version} < ${cvr.version.stateVersion}`,
          "watermark-regression",
        );
      }

      // TODO: consider a dedicated CVR advancement updater type.
      const updater = new CVRQueryDrivenUpdater(
        this.#cvrStore,
        cvr,
        version,
        this.#pipelines.replicaVersion,
        (queryID) => this.#pipelines.rowSetSignature(queryID),
      );
      // Gen-6 (advance-path equality): when the reconciled watermark lands
      // exactly ON the committed CVR stateVersion (Go pinned at the CVR
      // version while TS's head stalled, or vice versa), the constructor
      // above bumps nothing — and the first row update would trip cvr.ts's
      // "Expected CVR version to have been bumped above original" assertion
      // (CG teardown). Force a minor-version bump so row patches have a
      // legal patchVersion. Sound: a minor bump does not claim replication
      // progress (stateVersion is unchanged — still ≤ both authorities),
      // it is monotone, and the drift-re-execution path already uses the
      // same escape hatch (updater.ensureNewVersion() in
      // #addAndRemoveQueries). Stock TS never hits this branch: with a
      // single snapshot the advance version strictly exceeds the CVR
      // version whenever there are changes.
      if (cmpVersions(updater.updatedVersion(), cvr.version) === 0) {
        lc.info?.(
          `[go-primary] advance watermark ${version} equals committed CVR ` +
            `version; forcing minor bump for row patches`,
        );
        updater.ensureNewVersion();
      }
      // Only poke clients that are at the cvr.version. New clients that
      // are behind need to first be caught up when their initConnection
      // message is processed (and #syncQueryPipelines is called).
      const pokers = startPoke(
        this.#getClients(cvr.version),
        updater.updatedVersion(),
      );
      lc.debug?.(`applying ${numChanges} to advance to ${version}`);

      try {
        await this.#processChanges(
          lc,
          await timer.start(),
          changes,
          updater,
          pokers,
        );

        // Commit the changes and update the CVR snapshot.
        this.#cvr = await this.#flushUpdater(lc, updater);
        const finalVersion = this.#cvr.version;

        // Signal clients to commit.
        await startAsyncSpan(tracer, "vs.#advancePipelines.pokeEnd", () =>
          pokers.end(finalVersion),
        );
      } catch (e) {
        await pokers.cancel();
        this.#cvrStore.discardPendingWrites();
        if (e instanceof ResetPipelinesSignal) {
          return e;
        }
        throw e;
      }

      const wallTime = performance.now() - start;
      const totalProcessTime = timer.totalElapsed();
      lc.debug?.(
        `finished processing advancement of ${numChanges} changes ((process: ${totalProcessTime} ms, wall: ${wallTime} ms))`,
      );
      this.#transactionAdvanceTime.recordMs(totalProcessTime);
      return "success";
    });
  }

  async inspect(
    selector: ConnectionSelector,
    msg: InspectUpMessage,
  ): Promise<void> {
    await this.#runInLockForClient(selector, msg, this.#handleInspect);
  }

  // oxlint-disable-next-line require-await
  #handleInspect = async (
    lc: LogContext,
    clientID: string,
    body: InspectUpBody,
    cvr: CVRSnapshot,
  ): Promise<void> => {
    const client = must(this.#clients.get(clientID));
    const connCtx = this.connContextManager.mustGetConnectionContext({
      clientID,
      wsID: client.wsID,
    });
    return handleInspect(
      lc,
      body,
      cvr,
      client,
      this.#inspectorDelegate,
      this.id,
      this.#cvrStore,
      this.#config,
      connCtx,
    );
  };

  async #runBackgroundRetransform(lc: LogContext): Promise<void> {
    const attemptRetransform = async (connCtx: ConnectionContext) => {
      await this.#syncQueryPipelineSet(
        lc,
        must(this.#cvr, "cvr missing during auth maintenance retransform"),
        "all",
        connCtx,
      );
      this.connContextManager.markBackgroundRetransformSuccess(
        {
          clientID: connCtx.clientID,
          wsID: connCtx.wsID,
        },
        connCtx.revision,
      );
    };

    let backgroundConnCtx =
      this.connContextManager.getBackgroundConnectionContext();
    if (!backgroundConnCtx) {
      // The timer may have fired using an old deadline. If there is no longer a
      // selected validated connection, shared background retransform is simply
      // unschedulable until one exists again.
      lc.debug?.("Skipping background retransform with no selected connection");
      return;
    }

    for (;;) {
      try {
        await attemptRetransform(backgroundConnCtx);
        return;
      } catch (e) {
        if (isProtocolError(e)) {
          if (isAuthErrorBody(e.errorBody)) {
            lc.warn?.(
              "Background retransform auth failed; failing connection and searching for replacement",
              {
                clientID: backgroundConnCtx.clientID,
                message: e.message,
              },
            );
            this.#failMaintenanceConnection(backgroundConnCtx, e);
          } else if (isTransformFailedError(e)) {
            lc.warn?.(
              "Background retransform failed; deferring auth maintenance",
              {
                clientID: backgroundConnCtx.clientID,
                message: e.message,
              },
            );
            this.connContextManager.deferMaintenance("retransform");
            return;
          }
        } else {
          throw e;
        }
      }

      const replacementConnCtx =
        this.connContextManager.getBackgroundConnectionContext();
      if (!replacementConnCtx) {
        // The selected connection failed and nothing valid replaced it, so
        // there is no credential left that can safely drive shared background
        // reads.
        lc.debug?.(
          "No replacement connection available for background retransform",
        );
        return;
      }

      lc.debug?.(
        "Retrying background retransform with replacement connection",
        {
          clientID: replacementConnCtx.clientID,
          wsID: replacementConnCtx.wsID,
        },
      );
      backgroundConnCtx = replacementConnCtx;
    }
  }

  async #validateConnection(connCtx: ConnectionContext): Promise<boolean> {
    try {
      let validation: ConnectionValidation | undefined = undefined;
      if (this.#customQueryTransformer) {
        const response = await this.#customQueryTransformer.validate(connCtx);
        if (response.kind === "TransformFailed") {
          throw new ProtocolErrorWithLevel(response, "warn");
        }
        validation = response.validation;
      } else {
        validation = { kind: "client-fallback" };
      }

      this.connContextManager.validateConnection(
        connCtx,
        connCtx.revision,
        validation,
      );
      return true;
    } catch (e) {
      if (isProtocolError(e) && isAuthErrorBody(e.errorBody)) {
        this.#lc.warn?.(
          "Connection auth validation failed; invalidating connection",
          {
            clientID: connCtx.clientID,
            wsID: connCtx.wsID,
            revision: connCtx.revision,
            message: e.message,
          },
        );
        this.#failMaintenanceConnection(connCtx, e);
        return false;
      }
      throw e;
    }
  }

  #failMaintenanceConnection(connCtx: ConnectionContext, error: ProtocolError) {
    const failed = this.connContextManager.failConnection(
      connCtx,
      connCtx.revision,
    );
    if (!failed) {
      return;
    }

    const wrapped = wrapWithProtocolError(error);
    const client = this.#clients.get(connCtx.clientID);
    if (client?.wsID === connCtx.wsID) {
      client.fail(wrapped);
    }
  }

  stop(): Promise<void> {
    this.#lc.info?.("stopping view syncer");
    this.connContextManager.setSharedRetransformReady(false);
    this.#initialized.reject(shutdownBeforeInitializationError());
    this.#stateChanges.cancel();
    return this.#stopped.promise;
  }

  async #cleanup(err?: unknown) {
    this.connContextManager.setSharedRetransformReady(false);
    this.#stopTTLClockInterval();
    this.#stopExpireTimer();
    this.#stopAuthMaintenanceTimer();

    for (const client of this.#clients.values()) {
      if (err) {
        client.fail(err);
      } else {
        client.close(`closed clientGroupID=${this.id}`);
      }
    }

    // Wait for existing lock logic to complete before
    // cleaning up the pipelines and closing db connections.
    await this.#lock.withLock(() => {});
    // Await the Go engine teardown so a same-cg recycle can't race a
    // destroy RPC against a new engine's init on the shared sidecar.
    await this.#pipelines.destroy();
  }

  /**
   * Test helper: Manually mark initialization as complete.
   * This should only be used in tests that don't call initConnection().
   */
  markInitialized() {
    this.#initialized.resolve("initialized");
  }
}

// Update CVR after every N rows. Lower values stream patches to clients
// sooner (less buffering) at the cost of more frequent CVR updater calls.
// Default 10000 matches Go IVM's hydrateChunkSize / advanceChunkSize so
// the streaming chunk boundary aligns with the CVR flush boundary.
const CURSOR_PAGE_SIZE = parseInt(
  process.env.ZERO_CURSOR_PAGE_SIZE ?? "10000",
  10,
);

// A global Lock acts as a queue to run a single IVM time slice per iteration
// of the node event loop, thus bounding I/O delay to the duration of a single
// time slice.
//
// Refresher:
// https://nodejs.org/en/learn/asynchronous-work/event-loop-timers-and-nexttick#phases-overview
//
// Note that recursive use of setImmediate() (i.e. calling setImmediate() from
// within a setImmediate() callback), results in enqueuing the latter
// callback in the *next* event loop iteration, as documented in:
// https://nodejs.org/api/timers.html#setimmediatecallback-args
//
// This effectively achieves the desired one-per-event-loop-iteration behavior.
const timeSliceQueue = new Lock();

function yieldProcess(_lc: LogContext) {
  return timeSliceQueue.withLock(() => new Promise(setImmediate));
}

function contentsAndVersion(row: Row) {
  const { [ZERO_VERSION_COLUMN_NAME]: version, ...contents } = row;
  if (typeof version !== "string" || version.length === 0) {
    throw new Error(`Invalid _0_version in ${stringify(row)}`);
  }
  return { contents, version };
}

const NEW_CVR_VERSION = { stateVersion: "00" };

function checkClientAndCVRVersions(
  client: NullableCVRVersion,
  cvr: CVRVersion,
) {
  if (
    cmpVersions(cvr, NEW_CVR_VERSION) === 0 &&
    cmpVersions(client, NEW_CVR_VERSION) > 0
  ) {
    // CVR is empty but client is not.
    throw new ClientNotFoundError("Client not found");
  }

  if (cmpVersions(client, cvr) > 0) {
    // Client is ahead of a non-empty CVR.
    throw new ProtocolError({
      kind: ErrorKind.InvalidConnectionRequestBaseCookie,
      message: `CVR is at version ${versionString(cvr)}`,
      origin: ErrorOrigin.ZeroCache,
    });
  }
}

function isTransformFailedError(error: ProtocolError): boolean {
  return (
    error.errorBody.kind === ErrorKind.TransformFailed &&
    !isAuthErrorBody(error.errorBody)
  );
}

/**
 * A query must be expired for all clients in order to be considered
 * expired.
 */
function expired(
  ttlClock: TTLClock,
  q: InternalQueryRecord | ClientQueryRecord | CustomQueryRecord,
): boolean {
  if (q.type === "internal") {
    return false;
  }

  for (const clientState of Object.values(q.clientState)) {
    const { ttl, inactivatedAt } = clientState;
    if (inactivatedAt === undefined) {
      return false;
    }

    const clampedTTL = clampTTL(ttl);
    if (
      ttlClockAsNumber(inactivatedAt) + clampedTTL >
      ttlClockAsNumber(ttlClock)
    ) {
      return false;
    }
  }
  return true;
}

function hasExpiredQueries(cvr: CVRSnapshot): boolean {
  const { ttlClock } = cvr;
  for (const q of Object.values(cvr.queries)) {
    if (expired(ttlClock, q)) {
      return true;
    }
  }
  return false;
}

export class TimeSliceTimer {
  #total = 0;
  #start = 0;
  #lc: LogContext;

  constructor(lc: LogContext) {
    this.#lc = lc;
  }

  async start() {
    // yield at the very beginning so that the first time slice
    // is properly processed by the time-slice queue.
    await yieldProcess(this.#lc);
    return this.startWithoutYielding();
  }

  startWithoutYielding() {
    this.#total = 0;
    this.#startLap();
    return this;
  }

  async yieldProcess(_msgForTesting?: string) {
    this.#stopLap();
    await yieldProcess(this.#lc);
    this.#startLap();
  }

  #startLap() {
    assert(this.#start === 0, "already running");
    this.#start = performance.now();
  }

  elapsedLap() {
    assert(this.#start !== 0, "not running");
    return performance.now() - this.#start;
  }

  /**
   * True iff a lap is currently in progress. Callers can use this to
   * decide whether to invoke {@link elapsedLap}/{@link #stopLap} which
   * would otherwise assert.
   *
   * Specifically: the Go-primary advance in {@link PipelineDriver} opens
   * async windows mid-iteration (the `await goPromise` boundary and the
   * setImmediate yields in its replay loop) so the surrounding
   * view-syncer's stop sequence can race in and stop the timer underneath
   * the still-running TS advance generator. This accessor lets that path
   * short-circuit gracefully instead of throwing.
   */
  running(): boolean {
    return this.#start !== 0;
  }

  #stopLap() {
    assert(this.#start !== 0, "not running");
    this.#total += performance.now() - this.#start;
    this.#start = 0;
  }

  /** @returns the total elapsed time */
  stop(): number {
    this.#stopLap();
    return this.#total;
  }

  /**
   * @returns the elapsed time. This can be called while the Timer is running
   *          or after it has been stopped.
   */
  totalElapsed(): number {
    return this.#start === 0
      ? this.#total
      : this.#total + performance.now() - this.#start;
  }
}
