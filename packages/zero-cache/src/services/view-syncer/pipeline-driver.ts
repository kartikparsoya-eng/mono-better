import type { LogContext } from "@rocicorp/logger";
import { assert, unreachable } from "../../../../shared/src/asserts.ts";
import { deepEqual, type JSONValue } from "../../../../shared/src/json.ts";
import { must } from "../../../../shared/src/must.ts";
import type { AST, LiteralValue } from "../../../../zero-protocol/src/ast.ts";
import type { ClientSchema } from "../../../../zero-protocol/src/client-schema.ts";
import type { Row } from "../../../../zero-protocol/src/data.ts";
import type { PrimaryKey } from "../../../../zero-protocol/src/primary-key.ts";
import { buildPipeline } from "../../../../zql/src/builder/builder.ts";
import {
  Debug,
  runtimeDebugFlags,
} from "../../../../zql/src/builder/debug-delegate.ts";
import { ChangeIndex } from "../../../../zql/src/ivm/change-index.ts";
import { ChangeType } from "../../../../zql/src/ivm/change-type.ts";
import type { Change } from "../../../../zql/src/ivm/change.ts";
import type { Node } from "../../../../zql/src/ivm/data.ts";
import {
  skipYields,
  throwOutput,
  type FetchRequest,
  type Input,
  type Output,
  type Storage,
} from "../../../../zql/src/ivm/operator.ts";
import type { SourceSchema } from "../../../../zql/src/ivm/schema.ts";
import {
  type Source,
  type SourceChange,
  type SourceInput,
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from "../../../../zql/src/ivm/source.ts";
import { planQuery } from "../../../../zql/src/planner/planner-builder.ts";
import type { ConnectionCostModel } from "../../../../zql/src/planner/planner-connection.ts";
import { completeOrdering } from "../../../../zql/src/query/complete-ordering.ts";
import { MeasurePushOperator } from "../../../../zql/src/query/measure-push-operator.ts";
import type { ClientGroupStorage } from "../../../../zqlite/src/database-storage.ts";
import type { Database } from "../../../../zqlite/src/db.ts";
import {
  resolveSimpleScalarSubqueries,
  type CompanionSubquery,
} from "../../../../zqlite/src/resolve-scalar-subqueries.ts";
import { createSQLiteCostModel } from "../../../../zqlite/src/sqlite-cost-model.ts";
import { TableSource } from "../../../../zqlite/src/table-source.ts";
import {
  reloadPermissionsIfChanged,
  type LoadedPermissions,
} from "../../auth/load-permissions.ts";
import type { LogConfig, ZeroConfig } from "../../config/zero-config.ts";
import { computeZqlSpecs, mustGetTableSpec } from "../../db/lite-tables.ts";
import type { LiteAndZqlSpec, LiteTableSpec } from "../../db/specs.ts";
import {
  getOrCreateCounter,
  getOrCreateLatencyHistogram,
} from "../../observability/metrics.ts";
import type { InspectorDelegate } from "../../server/inspector-delegate.ts";
import {
  max as maxLexiVersion,
  min as minLexiVersion,
} from "../../types/lexi-version.ts";
import { isEnum as isLiteEnum, isArray } from "../../types/lite.ts";
import { type RowKey } from "../../types/row-key.ts";
import { type ShardID } from "../../types/shards.ts";
import {
  getSubscriptionState,
  ZERO_VERSION_COLUMN_NAME,
} from "../replicator/schema/replication-state.ts";
import { checkClientSchema } from "./client-schema.ts";
import {
  type GoComputeBackend,
  createGoComputeBackend,
  isGoSidecarEnabled,
  goPullWindow,
} from "./go-sidecar/go-compute-backend.ts";
import type {
  AdvanceToHeadStreamChunk,
  TableTiming,
} from "./go-sidecar/go-ivm-client.ts";
import {
  AdvanceAbortedError,
  PermanentDataError,
  ScalarResetError,
  StaleInitEpochError,
} from "./go-sidecar/go-ivm-client.ts";
import type { SidecarManager } from "./go-sidecar/sidecar-manager.ts";
import { parseSignature, rowIDSignatureUnit } from "./row-set-signature.ts";
import type { Snapshotter } from "./snapshotter.ts";
import { ResetPipelinesSignal, type SnapshotDiff } from "./snapshotter.ts";

type RowOp<Op extends Omit<ChangeType, ChangeType.CHILD>> = {
  readonly type: Op;
  readonly queryID: string;
  readonly table: string;
  readonly rowKey: Row;
  readonly row: Row;
};

export type RowAdd = RowOp<ChangeType.ADD>;

export type RowRemove = RowOp<ChangeType.REMOVE>;

export type RowEdit = RowOp<ChangeType.EDIT>;

export type RowChange = RowAdd | RowRemove | RowEdit;

export type AdvanceResult = {
  version: string;
  numChanges: number;
  changes: Iterable<RowChange | "yield"> | AsyncIterable<RowChange | "yield">;
  //  observability: when Go-primary serves user queries via advanceToHead,
  // `version` above is the RECONCILED watermark min(tsVersion, goVersion). These
  // expose the two un-reconciled authorities so the view-syncer can assert
  // monotonicity / log the split. Both undefined on the TS-only and push paths
  // (where the watermark is simply TS's version).
  tsVersion?: string | undefined;
  goVersion?: string | undefined;
};

/**
 *  watermark reconciliation. In Go-primary
 * trigger mode the user-query data is at Go's `goVersion` (V_go) while
 * internal/control-plane data is at TS's `tsVersion` (V_ts). The CVR
 * stateVersion is a COMPLETENESS FLOOR — client catchup/poke keys off the
 * `patchVersion` stamped at the committed CVR version, not the row's own
 * `_0_version` — so it may only be committed at a version BOTH authorities have
 * crossed: min(V_ts, V_go). Under-claiming is safe (the ahead side's extra rows
 * are an idempotent superset, re-delivered next cycle at the committed
 * patchVersion); over-claiming would let a client miss a change in the gap.
 *
 * In push mode `goVersion` is undefined (Go applied TS's diff, so its version is
 * TS's by construction) and the watermark is simply V_ts.
 *
 * `min` is monotone in each argument and each authority only advances, so the
 * reconciled watermark is non-decreasing — the CVR monotonicity invariant holds.
 */
export function reconcileGoPrimaryWatermark(
  tsVersion: string,
  goVersion: string | undefined,
): { version: string; tsVersion: string; goVersion: string | undefined } {
  // Treat empty string same as undefined — Go omitting `version` on the
  // final frame decodes as '' via `v.version ?? ''` in go-ivm-client.ts.
  // Without this guard, '' < "00" in lexi-version min() regresses the
  // CVR watermark to '' causing full re-hydration for all clients.
  if (goVersion === undefined || goVersion === "") {
    return { version: tsVersion, tsVersion, goVersion: undefined };
  }
  return {
    version: minLexiVersion(tsVersion, goVersion),
    tsVersion,
    goVersion,
  };
}

/**
 * Gen-6: the stateVersion CVR **hydrate** updaters must be stamped at when
 * the Go backend owns user-query hydrates — an upper bound on the
 * `_0_version` of every row the hydrate can deliver.
 *
 * Go pins its snapshot AFTER TS pins its own (init runs after TS's pin;
 * each advance re-pins Go after TS), so hydrated rows can carry versions
 * above `tsVersion`. Stamping at `tsVersion` alone can leave the updater
 * EQUAL to the committed CVR version (reconnects re-execute queries with
 * unchanged transformation hashes, which bump nothing) and the first
 * Go-delivered gap row then trips cvr.ts's "Expected CVR version to have
 * been bumped above original" assertion — a full client-group teardown.
 *
 * max() is safe in each argument: the CVR constructor requires
 * stateVersion ≥ cvr.stateVersion (all three inputs are ≥ it — each
 * authority and the CVR only advance), and stamping AT the data's version
 * is exactly stock semantics (stock's single snapshot makes data version ≡
 * stamp). Empty/undefined/null Go components are ignored (prior
 * sidecar, no advance yet).
 */
export function goHydrateStampVersion(
  tsVersion: string,
  goPinnedVersion: string | undefined,
  goDataVersion: string | null,
): string {
  const versions: [string, ...string[]] = [tsVersion];
  if (goPinnedVersion !== undefined && goPinnedVersion !== "") {
    versions.push(goPinnedVersion);
  }
  if (goDataVersion !== null && goDataVersion !== "") {
    versions.push(goDataVersion);
  }
  return maxLexiVersion(...versions);
}

/**
 *  advance-dispatch decision (Go-primary mode). Given the LIVE
 * Go backend availability and the mode the CURRENT user-query pipelines were
 * built in (#goUserPipelineMode), decide how advance() must proceed. The two
 * reset outcomes make advance() RETURN a ResetPipelinesSignal (never throw it)
 * so the view-syncer rebuilds the pipelines for the live Go state before the
 * next advance.
 *
 *   'go-advance'      Go UP and the user pipelines are Go-owned stubs (or none
 *                     yet) → run the Go-primary advance (Go owns user queries).
 *   'reset-recovered' Go UP but the pipelines were degraded to real TS while Go
 *                     was down → rebuild as Go-owned stubs first, else real TS
 *                     pipelines AND Go would both emit user rows (double-count).
 *   'reset-degrade'   Go DOWN and the pipelines are Go-owned stubs → the
 *                     TS-native advance emits NOTHING for them: a silent client
 *                     freeze with the cookie advancing past the gap (watermark
 *                     over-claim). Reset so re-registration builds REAL TS
 *                     pipelines (graceful degradation to TS-serving). This fires
 *                     on routine sidecar restarts and the drift-breaker cooldown,
 *                     not just terminal failure.
 *   'ts-native'       Go DOWN and the pipelines are already real TS (or none) →
 *                     the TS-native advance serves correctly; do NOT reset (that
 *                     would loop every advance for the whole outage / cooldown).
 */
export type GoPrimaryDispatchDecision =
  "go-advance" | "reset-recovered" | "reset-degrade" | "ts-native";

export function decideGoPrimaryDispatch(
  goInitialized: boolean,
  pipelineMode: "go" | "ts" | undefined,
): GoPrimaryDispatchDecision {
  if (goInitialized) {
    return pipelineMode === "ts" ? "reset-recovered" : "go-advance";
  }
  return pipelineMode === "go" ? "reset-degrade" : "ts-native";
}

/**
 *  classification of a Go-primary advance RPC failure (advanceToHeadStream)
 * as a PURE decision — the metric counters and logging live in
 * the #classifyGoPrimaryAdvanceError method that wraps this.
 *
 *   'protocol'     wire-level violation/corruption (chunk order, missing final
 *                  chunk, oversized frame, protocolRev mismatch) — RE-THROW; a
 *                  reset can't fix a wire bug without a sidecar process restart.
 *   'stale-epoch'  this instance was superseded by a successor's initEpoch —
 *                  RE-THROW so teardown completes.
 *   'data-error'   permanent, non-retryable bad replica data (ivm.DataError /
 *                  RPC_CODE_DATA_ERROR) — RE-THROW (teardown), NEVER reset: a
 *                  reset re-reads the same bad row and loops forever.
 *   'advance-aborted'  Go's ECONOMIC advancement-abort fired (TS's own
 *                  #shouldAdvanceYieldMaybeAbortAdvance formula running inside
 *                  the Go advance) — DROP → ResetPipelinesSignal with TS's own
 *                  'advancement-timeout' reason: byte-identical recovery to a
 *                  TS-native abort.
 *   'scalar-reset' a resolved scalar subquery's value changed mid-advance
 *                  (RPC_CODE_SCALAR_RESET → ScalarResetError) — DROP →
 *                  ResetPipelinesSignal('scalar-subquery'), the identical
 *                  signal TS-native's companion push throws for the same
 *                  event (:1468). NOT unclassified/teardown: this is a
 *                  designed-for transparent reset.
 *   'sidecar'      sidecar unavailable / restart in flight — DROP → reset.
 *   'unclassified' anything else — RE-THROW (CG teardown + client reconnect,
 *                  exactly TS's disposition for an unexpected error). This
 *                  bucket used to DROP → reset, which was the reset-storm
 *                  engine: with no fixed RPC timeout on the in-process
 *                  transport (computeBoundTimeoutMs), the economic abort
 *                  owning the load-coupled case, and clean failures retried in
 *                  place (RetryableAdvanceError), whatever still lands here is
 *                  a genuine bug — and TS's answer to bugs is teardown, whose
 *                  client-reconnect backoff is natural admission control the
 *                  immediate-reset path never had.
 *
 * The DROP buckets escalate to a ResetPipelinesSignal (full re-hydrate)
 * instead of the legacy "return [] + #scheduleGoReset", which left a permanent
 * (prev→head] gap: the async reset rebuilt Go at head and DISCARDED its hydrate
 * output, so the dropped user delta was never delivered. The order matters —
 * protocol message patterns are checked before the stale-epoch instance so a
 * protocol violation that also happens to be a stale-epoch error escalates as a
 * protocol violation (both re-throw, so the buckets are observably distinct but
 * behaviourally identical here).
 */
export type GoAdvanceErrorClass =
  | "protocol"
  | "stale-epoch"
  | "data-error"
  | "advance-aborted"
  | "scalar-reset"
  | "sidecar"
  | "unclassified";

/**
 * Gen-5 (2026-07-07 abort-loop forensics): the two pure inputs that keep
 * Go's economic advancement-abort CONVERGENT under sustained writes.
 *
 * Background: Go runs TS's own abort formula (advance_abort.go) with a
 * budget TS computes — totalHydrationTimeMs, the priced cost of the reset
 * the abort would trigger. TS-native prices it with the WALL of each
 * pipeline's hydrate (see the note above the #addQueryImpl pipelines.set:
 * "a wall-clock measurement taken across the time-sliced hydrate"), which
 * under load is seconds — so TS-native effectively never aborts at this
 * catalog, and when the system slows, re-hydrates slow too, growing the
 * next budget: self-healing economics.
 *
 * The Go-primary integration originally stored Go's ENGINE-internal hydrate
 * time (goResult.timingMs, 4–54ms at this catalog) — ~100× below the true
 * reset cost (query re-transform REST round-trips, CVR rewrite, poke
 * re-delivery, re-registration). The budget floor-pinned at 50ms while a
 * ~150-change backlog costs >50ms CPU, and unlike TS the budget never grew:
 * abort → reset → same backlog re-accumulates during the seconds-long reset
 * → abort at the SAME position — observed as 11–14× consecutive aborts at
 * identical positions, breaker trips, and cascading CVR-version errors
 * (16/50 clients lost at ~14 writes/s, where stock TS at the identical
 * load had zero aborts and zero client losses).
 */

/**
 * The hydration cost to record for a Go-owned pipeline entry: the maximum
 * of Go's engine-internal hydrate time and the TS-observed wall of the
 * hydrate. Unit-parity with TS-native entries (wall), with the engine time
 * kept as a floor. Wall ≥ engine always in practice (the engine runs inside
 * the awaited call), so this is effectively the TS wall — the max() guards
 * the degenerate clock-skew/instant-await cases.
 */
export function goHydrationCostMs(
  engineTimingMs: number | undefined,
  tsWallMs: number,
): number {
  return Math.max(engineTimingMs ?? 0, tsWallMs);
}

/**
 * Escalation backstop: double the abort budget per CONSECUTIVE economic
 * abort (capped at 8×), cleared by the first completed advance. TS-native
 * needs no explicit escalation because its budget input self-heals (slow
 * system → slow re-hydrate → bigger next budget); Go's re-hydrates stay
 * fast even under storm, so a budget that underprices once would underprice
 * identically forever — the loop pathology above. Doubling converges by
 * construction; the cap keeps the abort meaningful as a big-transaction
 * circuit breaker, and the GO_IVM_ADVANCE_BUDGET_MS wall backstop (60s)
 * still bounds runaway advances (and with them the WAL pin) regardless.
 */
export function escalatedAbortBudgetMs(
  baseMs: number,
  consecutiveAborts: number,
): number {
  const exp = Math.min(Math.max(consecutiveAborts, 0), 3);
  return baseMs * 2 ** exp;
}

/**
 * Terminal escalation for an abort streak: once consecutiveAborts reaches
 * this, escalatedAbortBudgetMs has been maxed (8×, exp caps at 3) and STILL
 * aborted — the budget lever is exhausted, and every further advance would
 * abort at the same backlog position forever. Send suppressAbort=true on the
 * next advance: one un-abortable catch-up clears the backlog (Go's
 * GO_IVM_ADVANCE_BUDGET_MS wall backstop, 60s, still bounds it — and with it
 * the WAL pin), the advance completes, and the streak resets to 0. This is
 * what makes an unbounded abort→reset loop structurally impossible, which in
 * turn is why the view-syncer's reset breaker exempts 'advancement-timeout'
 * resets from CG teardown (classifyResetReason: 'economic').
 */
export const SUPPRESS_ABORT_AFTER_STREAK = 3;

/**
 * Pure decision: suppress the economic abort on the advance about to be
 * issued? True exactly when the streak says the maxed-out budget already
 * failed (see SUPPRESS_ABORT_AFTER_STREAK).
 */
export function shouldSuppressAbort(consecutiveAborts: number): boolean {
  return consecutiveAborts >= SUPPRESS_ABORT_AFTER_STREAK;
}

/**
 * The minimum abort budget ever sent to Go — the price floor of a Go reset.
 *
 * Why a floor exists at all (2026-07-07 rerun forensics): 50/77 residual
 * aborts carried a budget of literally 0ms. Budget≈0 arises STRUCTURALLY,
 * not as a bug: internal-only CGs (lmids/mutationResults entries register
 * through the batch path's noopTimer → hydrationTimeMs 0), CGs whose user
 * queries are TTL-expired at reconnect (empty map), and stub entries
 * mid-registration. Go still does real work on every advance for such CGs
 * — handleInit created a Source per schema table, and each advance replays
 * every change through prev-tx source maintenance regardless of query
 * count (~50ms CPU per ~150-change burst) — so a 0 budget aborted every
 * burst forever. Two reasons that can never be economical: (1) a Go reset
 * is never remotely free — resetEngine is destroy + re-init (per-table
 * presence probes + Sources) + full re-hydrate + TS re-registration and
 * query re-transform round-trips, hundreds of ms at minimum; (2) the
 * source-maintenance CPU the abort "saves" is re-paid by the reset's own
 * leapfrog + re-hydrate anyway. TS-native never manifests this only
 * because its no-pipeline advances cost ~0 lap-ms — its formula never
 * trips. The floor also covers what the measured hydrate wall structurally
 * omits (re-transform REST round-trips, CVR rewrite, poke re-delivery).
 *
 * Genuine economics are preserved: huge-transaction advances cost seconds
 * of CPU and still abort; GO_IVM_MAX_DIFF_CHANGES and the
 * GO_IVM_ADVANCE_BUDGET_MS wall backstop still guard runaways.
 */
export const GO_ADVANCE_ABORT_BUDGET_FLOOR_MS = 250;

/**
 * The budget actually sent on advanceToHeadStream: honest base pricing
 * (goHydrationCostMs entries), streak escalation (escalatedAbortBudgetMs),
 * and the reset-cost floor — in that order.
 */
export function goAdvanceAbortBudgetMs(
  baseMs: number,
  consecutiveAborts: number,
): number {
  return Math.max(
    escalatedAbortBudgetMs(baseMs, consecutiveAborts),
    GO_ADVANCE_ABORT_BUDGET_FLOOR_MS,
  );
}

export function classifyGoPrimaryAdvanceError(e: unknown): GoAdvanceErrorClass {
  const msg = e instanceof Error ? e.message : String(e);
  if (
    msg.includes("chunk order violation") ||
    msg.includes("finished without a final chunk") ||
    msg.includes("header missing version") ||
    msg.includes("Frame too large") ||
    msg.includes("protocolRev mismatch")
  ) {
    return "protocol";
  }
  if (e instanceof StaleInitEpochError) {
    return "stale-epoch";
  }
  // Go's economic advancement-abort (RPC_CODE_ADVANCE_ABORTED). Checked via
  // instanceof after the protocol patterns (an abort message can never match
  // them, but the pinned precedence stays untouched).
  if (e instanceof AdvanceAbortedError) {
    return "advance-aborted";
  }
  // Scalar-subquery reset (RPC_CODE_SCALAR_RESET): Go's companion pipeline
  // detected a resolved scalar value change mid-advance. Same disposition as
  // TS-native's own throw at :1468 — reset with reason 'scalar-subquery'.
  if (e instanceof ScalarResetError) {
    return "scalar-reset";
  }
  // Permanent data error (RPC_CODE_DATA_ERROR → PermanentDataError): bad
  // replica data the sidecar can't represent. Checked before the 'sidecar' /
  // 'unclassified' DROP buckets so a poison row tears down ONCE instead of
  // reset-looping. The `instanceof` is the robust path; the message fallback
  // catches any DataError that reached us as a plain Error (defense in depth).
  if (
    e instanceof PermanentDataError ||
    msg.includes("FromSQLiteType") ||
    msg.includes("cannot compare values of different types")
  ) {
    return "data-error";
  }
  if (
    msg.includes("Sidecar is not running") ||
    msg.includes("Connection closed") ||
    msg.includes("Not connected") ||
    msg.includes("engine not initialized") ||
    msg.includes("client group destroyed")
  ) {
    return "sidecar";
  }
  return "unclassified";
}

/**
 * eagerly drain a snapshotter diff, invoking `onEntry` for each
 * change, but CATCH the ResetPipelinesSignal the diff iterator throws on a
 * truncate / schema change and RETURN it instead of letting it propagate. The
 * Go-primary advance paths buffer the diff eagerly (so both TS and Go can
 * consume it); without this catch the throw escapes #advancePipelines and lands
 * in run()'s outer catch — a full client-group teardown (all clients
 * disconnected) on every truncate or schema change. Returning the signal routes
 * through the view-syncer's graceful reset + re-hydrate (the same self-heal the
 * TS-native lazy path gets). Any non-reset error propagates unchanged.
 */
export function drainDiffCatchingReset<T>(
  diff: Iterable<T>,
  onEntry: (entry: T) => void,
): ResetPipelinesSignal | undefined {
  try {
    for (const entry of diff) {
      onEntry(entry);
    }
    return undefined;
  } catch (e) {
    if (e instanceof ResetPipelinesSignal) {
      return e;
    }
    throw e;
  }
}

type CompanionPipeline = {
  readonly input: Input;
  readonly childField: string;
  readonly resolvedValue: LiteralValue | null | undefined;
};

type Pipeline = {
  readonly input: Input;
  readonly hydrationTimeMs: number;
  readonly transformedAst: AST;
  readonly transformationHash: string;
  readonly queryName?: string | undefined;
  readonly companions: readonly CompanionPipeline[];
};

type QueryInfo = {
  readonly transformedAst: AST;
  readonly transformationHash: string;
  readonly queryName?: string | undefined;
};

type QueryLogInfo = {
  readonly queryHash: string;
  readonly transformationHash: string;
  readonly queryName?: string | undefined;
};

type AdvanceContext = {
  readonly timer: Timer;
  readonly totalHydrationTimeMs: number;
  readonly numChanges: number;
  pos: number;
  // When true, #shouldAdvanceYieldMaybeAbortAdvance still yields cooperatively
  // but does NOT throw ResetPipelinesSignal — used by #goPrimaryAdvance's
  // internal-query replay, whose economic abort runs inside the Go advance.
  readonly suppressAbort: boolean;
};

type HydrateContext = {
  readonly timer: Timer;
};

export type Timer = {
  elapsedLap: () => number;
  totalElapsed: () => number;
  /**
   * True iff a lap is currently in progress. Called by TableSource.#shouldYield
   * at runtime to decide whether elapsedLap is safe to invoke. Was omitted
   * from the exported type, forcing every noop-timer site to `as unknown as
   * Timer` to keep tsc happy. Declaring it here means tsc verifies new
   * Timer-typed values include it — preventing the silent runtime
   * `TypeError: t.running is not a function` we hit at runtime.
   */
  running: () => boolean;
};

/**
 * No matter how fast hydration is, advancement is given at least this long to
 * complete before doing a pipeline reset.
 */
const MIN_ADVANCEMENT_TIME_LIMIT_MS = 50;

/**
 * Manages the state of IVM pipelines for a given ViewSyncer (i.e. client group).
 */
export class PipelineDriver {
  readonly #tables = new Map<string, TableSource>();
  // Query id to pipeline
  readonly #pipelines = new Map<string, Pipeline>();
  /**
   * XOR signature of the set of rows currently attached to each active
   * query, maintained as RowChanges are yielded from {@link addQuery} and
   * {@link advance}. ADDs / REMOVEs XOR the row's unit in (XOR is
   * self-inverse, so one op serves both directions); EDITs are no-ops.
   * Hydration implicitly reseeds from `0n` because {@link addQuery} calls
   * {@link removeQuery} first, which deletes the entry.
   */
  readonly #rowSetSignatures = new Map<string, bigint>();

  readonly #lc: LogContext;
  readonly #snapshotter: Snapshotter;
  readonly #storage: ClientGroupStorage;
  readonly #shardID: ShardID;
  readonly #logConfig: LogConfig;
  readonly #config: ZeroConfig | undefined;
  readonly #tableSpecs = new Map<string, LiteAndZqlSpec>();
  readonly #allTableNames = new Set<string>();
  readonly #costModels: WeakMap<Database, ConnectionCostModel> | undefined;
  readonly #yieldThresholdMs: () => number;
  #streamer: Streamer | null = null;
  #hydrateContext: HydrateContext | null = null;
  #advanceContext: AdvanceContext | null = null;
  #replicaVersion: string | null = null;
  #primaryKeys: Map<string, PrimaryKey> | null = null;
  #permissions: LoadedPermissions | null = null;

  readonly #advanceTime = getOrCreateLatencyHistogram(
    "sync",
    "ivm.advance-time",
    "Time to advance all queries for a given client group in response to a single change.",
  );

  // Wall time the TS view-syncer spends BLOCKED on the Go backend advance
  // stream (the NAPI/RPC round-trip + Go compute delivered over the wire)
  // for one client-group advance — accumulated across the header frame and
  // every subsequent chunk `.next()`. This is the span MED-CROSS-3 promised
  // but never actually recorded. Decomposition:
  //   advance-go-rpc-time  = RPC round-trip + Go compute (this metric)
  //   ivm.advance-time     = Go per-table compute (summed; #recordGoPrimaryAdvanceTimings)
  //   ⇒ advance-go-rpc-time − Σ ivm.advance-time ≈ pure NAPI transport
  //   ⇒ advance-time (view-syncer, whole batch) − advance-go-rpc-time
  //                                          ≈ TS-side reconcile + poke + CVR
  // Only populated in Go-primary; the TS-native path never crosses the wire.
  readonly #advanceGoRpcTime = getOrCreateLatencyHistogram(
    "sync",
    "ivm.advance-go-rpc-time",
    "Wall time the TS view-syncer spent blocked on the Go backend advance stream (NAPI/RPC round-trip + Go compute delivered over the wire) for a single client-group advance.",
  );

  // Go-reported engine-internal end-to-end wall of the whole advance (entry →
  // terminal flush), INCLUDING the drive-mode diff-derivation phase that
  // Σ ivm.advance-time omits. Splits the advance-go-rpc-time gap:
  //   advance-go-rpc-time − advance-go-internal-time ≈ TRUE wire latency
  //   advance-go-internal-time − Σ ivm.advance-time  ≈ uncounted diff/fetch work
  // If this ≈ advance-go-rpc-time, the gap is Go compute (SQLite/diff), NOT
  // transport — and positional-rows/wire-format is the wrong lever.
  readonly #advanceGoInternalTime = getOrCreateLatencyHistogram(
    "sync",
    "ivm.advance-go-internal-time",
    "Engine-internal end-to-end wall time of a Go-primary advance as measured inside the Go sidecar (diff derivation + pushes + fanout + serialization), excluding the RPC/NAPI wire latency.",
  );

  readonly #conflictRowsDeleted = getOrCreateCounter(
    "sync",
    "ivm.conflict-rows-deleted",
    "Number of rows deleted because they conflicted with added row",
  );

  // Bucketed counter for advance-dropped events in Go-primary mode. previously
  // fix, ALL errors from Go's advance were silently swallowed as
  // empty changes — the CVR committed the empty diff and the client view
  // diverged with no operator-visible signal. The forensics gap was total:
  // a wire-protocol violation (chunk-order, missing terminal frame),
  // a stale-epoch error, and a sidecar restart all looked identical.
  // Each reason gets its own counter so dashboards can distinguish them.
  readonly #advanceDroppedProtocol = getOrCreateCounter(
    "sync",
    "ivm.advance-dropped-protocol",
    "Go-primary advance dropped due to wire-protocol violation (escalated to restart)",
  );
  readonly #advanceDroppedDataError = getOrCreateCounter(
    "sync",
    "ivm.advance-dropped-data-error",
    "Go-primary advance hit permanent bad replica data (CG torn down, NOT reset — prevents reset storm)",
  );
  readonly #advanceDroppedSidecar = getOrCreateCounter(
    "sync",
    "ivm.advance-dropped-sidecar",
    "Go-primary advance dropped due to sidecar unavailability / restart",
  );
  readonly #advanceDroppedStaleEpoch = getOrCreateCounter(
    "sync",
    "ivm.advance-dropped-stale-epoch",
    "Go-primary advance dropped due to stale initEpoch (torn-down view-syncer)",
  );
  readonly #advanceDroppedOther = getOrCreateCounter(
    "sync",
    "ivm.advance-dropped-other",
    "Go-primary advance failed unclassified (escalated to CG teardown — TS semantics for unexpected errors)",
  );
  /**
   * Go's economic advancement-abort fired (TS's own formula running inside
   * the Go advance) — resolves as ResetPipelinesSignal('advancement-timeout'),
   * the same reason TS-native aborts carry; counted separately so dashboards
   * can tell economically-justified resets from failure-driven ones.
   */
  readonly #advanceAbortedEconomic = getOrCreateCounter(
    "sync",
    "ivm.advance-aborted-economic",
    "Go advance hit the economic advancement-abort (reset via TS's advancement-timeout path)",
  );

  /**
   * Consecutive economic advancement-aborts (advance-aborted class) with no
   * completed advance in between — the input to escalatedAbortBudgetMs.
   * Incremented in #classifyGoPrimaryAdvanceError's 'advance-aborted' case,
   * cleared when a drive advance completes. Instance state: survives
   * pipeline RESETS (the loop this breaks is abort→reset→abort on one
   * driver), dies with the view-syncer on teardown — a fresh instance
   * starting at 1× is correct.
   */
  #consecutiveAdvanceAborts = 0;
  /**
   * Advances sent with suppressAbort=true because the abort streak exhausted
   * the budget-escalation lever (see SUPPRESS_ABORT_AFTER_STREAK). Should be
   * near-zero in steady state; a sustained rate means the budget pricing is
   * chronically under the true advance cost at this catalog.
   */
  readonly #advanceAbortSuppressed = getOrCreateCounter(
    "sync",
    "ivm.advance-abort-suppressed",
    "Go advances issued with the economic abort suppressed (abort-streak catch-up)",
  );
  /**
   * A resolved scalar subquery's value changed mid-advance in Go — resolves
   * as ResetPipelinesSignal('scalar-subquery'), identical to TS-native's
   * companion-push throw. Counted separately from failure-driven resets.
   */
  readonly #advanceScalarReset = getOrCreateCounter(
    "sync",
    "ivm.advance-scalar-reset",
    "Go advance hit a scalar-subquery value change (reset via TS's scalar-subquery path)",
  );
  /**
   * D11: per-reason counter for #scheduleGoReset. Pre-fix every reset
   * looked the same in metrics; each call increments with a {reason}
   * attribute so dashboards can attribute restarts to the trigger.
   */
  readonly #goResetScheduled = getOrCreateCounter(
    "sync",
    "ivm.go-reset-scheduled",
    "Go engine resets scheduled (label: reason)",
  );

  readonly #inspectorDelegate: InspectorDelegate;
  readonly #goBackend: GoComputeBackend | null = null;
  #goInitPromise: Promise<void> | null = null;
  /**
   * Gen-6: the latest KNOWN stateVersion of Go's data plane — the max of
   * Go's advance-reported versions observed by this driver. Combined with
   * the backend's init-time snapshotter pin (see {@link hydrateVersion})
   * it bounds the version of every row Go can deliver to a hydrate, so the
   * CVR hydrate updater is stamped at (at least) the data's version and
   * new rows never arrive under an unbumped CVR version (cvr.ts:778).
   * Monotone; never regresses (Go's snapshotter only advances, and a
   * re-init re-pins at the then-current head ≥ any prior value).
   */
  #goDataVersion: string | null = null;
  /**
   * how the CURRENT user-query pipelines were built, so the advance
   * dispatch can detect a Go-availability flip and rebuild in the right mode.
   *   'go'      — Go-owned stubs (TS emits nothing for user queries).
   *   'ts'      — real TS pipelines (degraded because Go was unavailable at
   *               build time, or a pure-TS deployment).
   *   undefined — no user pipelines built yet (or only internal queries).
   * Only consulted in Go-primary mode. A mismatch — Go DOWN with
   * 'go' stubs (would silently freeze + over-claim the watermark), or Go UP
   * with 'ts' pipelines (real TS + Go would both emit → double-count) — makes
   * #advanceDispatch return a ResetPipelinesSignal so re-registration rebuilds
   * for the live Go state. Tracking the mode (rather than resetting on every
   * Go-down advance) is what keeps the degradation a ONE-TIME event instead of
   * a reset loop for the whole outage / drift-breaker cooldown.
   */
  #goUserPipelineMode: "go" | "ts" | undefined = undefined;
  /** Set while #scheduleGoReset is running; collapses concurrent reset requests. */
  #goResetInFlight = false;
  /**
   * Set whenever a reset is requested *during* an in-flight reset, so we
   * reschedule once the current one completes. A plain boolean would drop
   * the second request entirely.
   */
  #goResetDirty = false;
  /** Retry attempts of the current reset cycle; resets to 0 on success. */
  #goResetRetries = 0;
  // Snapshotter version that the TableSources are currently bound to. Used
  // by #advance's finally-realign: if the advance loop threw before the
  // success-path setDB ran, TableSources stay bound to the old snapshot while
  // the snapshotter has moved forward — re-bind them to the current snapshot.
  #tableSourcesVersion: string | null = null;

  constructor(
    lc: LogContext,
    logConfig: LogConfig,
    snapshotter: Snapshotter,
    shardID: ShardID,
    storage: ClientGroupStorage,
    clientGroupID: string,
    inspectorDelegate: InspectorDelegate,
    yieldThresholdMs: () => number,
    enablePlanner?: boolean,
    config?: ZeroConfig,
    sidecarManager?: SidecarManager,
  ) {
    this.#lc = lc.withContext("clientGroupID", clientGroupID);
    this.#snapshotter = snapshotter;
    this.#storage = storage;
    this.#shardID = shardID;
    this.#logConfig = logConfig;
    this.#config = config;
    this.#inspectorDelegate = inspectorDelegate;
    this.#costModels = enablePlanner ? new WeakMap() : undefined;
    this.#yieldThresholdMs = yieldThresholdMs;
    this.#goBackend =
      isGoSidecarEnabled(config) && sidecarManager
        ? createGoComputeBackend(
            sidecarManager,
            clientGroupID,
            // Re-read tables from the current snapshot on every (re-)init,
            // so post-restart re-init picks up fresh data instead of a
            // stale snapshot captured at construction.
            () => this.#currentTablesForGo(),
            // Re-register the active queries after a restart-driven reinit,
            // otherwise Go would have empty pipelines while TS thinks they
            // exist (a restart-correctness gap).
            //
            // Filter out internal control-plane queries (permissions /
            // clients / mutations). Go never registers a Source for those
            // tables (#currentTablesForGo skips them), so re-registering an
            // internal query during reset makes engine.AddQueries panic
            // "no source for table <appID>_<shard>.clients" → resetEngine
            // throws → the drift recovery cascades into client-connection
            // failures. Every other dispatch site already applies this
            // filter (see #goHydrate, addQueries);
            // the reset re-register callback was the one that missed it.
            () =>
              [...this.#pipelines.entries()]
                .filter(
                  ([queryID, p]) =>
                    !this.#isInternalQueryID(queryID) &&
                    !this.#isInternalTable(p.transformedAst.table),
                )
                .map(([queryID, p]) => ({
                  queryID,
                  ast: p.transformedAst,
                })),
            // Make the shard's appID authoritative on the advanceToHead
            // wire so the sidecar watches the right permissions table even if
            // its GO_IVM_APP_ID env was set inconsistently.
            // pullWindow: ABI v3 credit-gated hydration window (see
            // goHydrateBatchStream).
            {
              appID: this.#shardID.appID,
              pullWindow: goPullWindow(config),
            },
          )
        : null;
  }

  // Internal-plumbing predicates (see #currentTablesForGo for context).
  // <appID>.permissions and <appID>_<shard>.clients are Zero's control
  // plane; user tables live in a different schema (no app-prefix).
  #isInternalTable(name: string): boolean {
    const { appID, shardNum } = this.#shardID;
    return (
      name.startsWith(`${appID}.`) || name.startsWith(`${appID}_${shardNum}.`)
    );
  }

  #isInternalQueryID(queryID: string): boolean {
    return queryID === "lmids" || queryID === "mutationResults";
  }

  /**
   * Materialize the current snapshot's tables in the shape the Go sidecar
   * wants (columns + primaryKey + rows). Used both for the initial init
   * and for re-init after a sidecar restart.
   */
  #currentTablesForGo(): Record<
    string,
    {
      columns: Record<
        string,
        {
          type: "boolean" | "number" | "string" | "null" | "json";
          optional?: boolean;
        }
      >;
      primaryKey: string[];
      uniqueKeys?: string[][] | undefined;
      minRowVersion?: string | null | undefined;
      rows: Record<string, unknown>[];
    }
  > {
    // Dispatch invariant: this method is called from the Go backend's
    // (re-)init callback, which can run from the restart handler OUTSIDE the
    // ViewSyncer lock. It is safe ONLY because it is fully SYNCHRONOUS — the
    // snapshot reference captured here is read consistently through to the end
    // with no intervening `await`, so a concurrent advance (which can only run
    // between awaits, JS being single-threaded) cannot swap the snapshot
    // mid-build. DO NOT introduce an `await` into this method; if row reads
    // ever need to go async, snapshot `db`/`version` first and pin them, or
    // hoist the call back under the lock.
    //
    // Bail fast if the snapshotter was torn down (CG eviction / worker
    // reassignment): current() still returns the old Snapshot (destroy() closes
    // the connection but doesn't clear #curr), so reads below would throw
    // "database connection is not open" once per table. One typed throw lets the
    // caller (resetEngine / #scheduleGoReset) abandon the reset cleanly.
    if (this.#snapshotter.destroyed) {
      throw new Error(
        "snapshotter destroyed — CG torn down; aborting Go (re-)init",
      );
    }
    // The sidecar's leaves read SQLite directly (table mode), so shipping
    // row contents is pure waste — and materializing every user table via
    // `SELECT *` .all() in one synchronous pass OOMs the syncer worker on
    // real datasets. Schemas/PKs/uniqueKeys ship; rows stay empty.
    const tables: Record<
      string,
      {
        columns: Record<
          string,
          {
            type: "boolean" | "number" | "string" | "null" | "json";
            optional?: boolean;
          }
        >;
        primaryKey: string[];
        uniqueKeys?: string[][] | undefined;
        minRowVersion?: string | null | undefined;
        rows: Record<string, unknown>[];
      }
    > = {};
    const warn = (msg: string) => this.#lc.warn?.(`[go-ivm pgType] ${msg}`);
    for (const [name, spec] of this.#tableSpecs.entries()) {
      // Skip Zero-internal plumbing tables (<appID>.permissions,
      // <appID>_<shard>.clients, etc). These are written by zero-cache
      // itself, only feed the `lmids`/`mutationResults` internal queries
      // that TS handles natively, and have caused Go sidecar panics when
      // the in-memory snapshot diverges from SQLite across sidecar
      // restarts. Go-primary mode is
      // safe to skip these: internal queries always route through TS,
      // since TS's TableSource reads live from SQLite and self-heals.
      if (this.#isInternalTable(name)) {
        this.#lc.debug?.(`[go-ivm] skipping internal table ${name}`);
        continue;
      }
      const columns: Record<
        string,
        {
          type: "boolean" | "number" | "string" | "null" | "json";
          optional?: boolean;
        }
      > = {};
      for (const [col, colSpec] of Object.entries(spec.tableSpec.columns)) {
        // Forward nullability so Go's nullable-aware SQL (IS NULL /
        // (? IS NULL OR field > ?)) fires for cursor pagination through a NULL
        // on a nullable order column — otherwise Go yields 0 rows where TS
        // yields N. notNull may be true/false/null/undefined; only an explicit
        // true means non-nullable.
        columns[col] = {
          type: pgTypeToGoType(colSpec.dataType, warn),
          optional: colSpec.notNull !== true,
        };
      }
      let rows: Record<string, unknown>[] = [];
      // uniqueKeys: forward all unique-index column sets to Go so its
      // scalar-subquery resolver can detect at-most-one-row subqueries
      // (the Phase 2 port of resolveSimpleScalarSubqueries). Falls back to
      // [primaryKey] when liteTableSpec didn't capture uniqueKeys, so the
      // Go resolver still has something useful for the common pk-only
      // case rather than treating the table as having no unique keys.
      const tableSpec = spec.tableSpec as unknown as {
        uniqueKeys?: string[][];
      };
      const uniqueKeys: string[][] =
        tableSpec.uniqueKeys && tableSpec.uniqueKeys.length > 0
          ? tableSpec.uniqueKeys.map((k) => [...k])
          : [[...spec.tableSpec.primaryKey]];
      tables[name] = {
        columns,
        primaryKey: [
          ...(this.#primaryKeys?.get(name) ?? spec.tableSpec.primaryKey),
        ],
        uniqueKeys,
        // Forward minRowVersion so the Go streamer can bump emitted rows'
        // _0_version when below it (audit item K).
        minRowVersion: spec.tableSpec.minRowVersion,
        rows,
      };
    }
    return tables;
  }

  /**
   * Initializes the PipelineDriver to the current head of the database.
   * Queries can then be added (i.e. hydrated) with {@link addQuery()}.
   *
   * Must only be called once.
   */
  init(clientSchema: ClientSchema) {
    assert(!this.#snapshotter.initialized(), "Already initialized");
    this.#snapshotter.init();
    this.#initAndResetCommon(clientSchema);
    this.#maybeInitGoBackend(clientSchema);
  }

  #maybeInitGoBackend(_clientSchema: ClientSchema) {
    if (!this.#goBackend) return;
    const tables = this.#currentTablesForGo();
    this.#lc.info?.(
      `init ${Object.keys(tables).length} tables (schemas only — ` +
        `table-mode sidecar reads rows from SQLite directly)`,
    );
    const promise = this.#goBackend.initEngine(tables);
    this.#goInitPromise = promise;
    promise
      .then(() => this.#lc.info?.("Go backend initialized"))
      .catch((err) => {
        this.#lc.error?.("Go backend init failed:", err);
        // Don't leave a rejected promise sitting on #goInitPromise — the
        // dispatch path would await it and throw, killing the ViewSyncer.
        // Null it so dispatch falls through to the TS path based purely on
        // the initialized flag.
        if (this.#goInitPromise === promise) this.#goInitPromise = null;
      });
  }

  /**
   * Re-initialize the Go sidecar after a snapshot leapfrog (reset or
   * advanceWithoutDiff). Destroys the old engine and re-sends all rows
   * from the current SQLite snapshot so Go stays in sync.
   */
  #maybeResetGoBackend() {
    if (!this.#goBackend || !this.#goBackend.initialized) return;
    this.#lc.info?.("Resetting Go backend (snapshot leapfrog)");
    // resetEngine reads the snapshot itself at reinit time (after
    // its destroy await) — do not pre-capture here.
    const promise = this.#goBackend.resetEngine();
    this.#goInitPromise = promise;
    promise
      .then(() => this.#lc.info?.("Go backend reset complete"))
      .catch((err) => {
        this.#lc.error?.("Go backend reset failed:", err);
        if (this.#goInitPromise === promise) this.#goInitPromise = null;
      });
  }

  /**
   * @returns Whether the PipelineDriver has been initialized.
   */
  initialized(): boolean {
    return this.#snapshotter.initialized();
  }

  /**
   * Clears the current pipelines and TableSources, returning the PipelineDriver
   * to its initial state. This should be called in response to a schema change,
   * as TableSources need to be recomputed.
   */
  reset(clientSchema: ClientSchema) {
    for (const pipeline of this.#pipelines.values()) {
      pipeline.input.destroy();
      for (const companion of pipeline.companions) {
        companion.input.destroy();
      }
    }
    this.#pipelines.clear();
    this.#tables.clear();
    this.#allTableNames.clear();
    this.#rowSetSignatures.clear();
    // pipelines are gone; the next (re-)registration sets the mode afresh
    // for the live Go state.
    this.#goUserPipelineMode = undefined;
    this.#initAndResetCommon(clientSchema);
    // Re-initialize Go sidecar with fresh snapshot (leapfrog)
    this.#maybeResetGoBackend();
  }

  #initAndResetCommon(clientSchema: ClientSchema) {
    const { db, version } = this.#snapshotter.current();
    this.#tableSourcesVersion = version;
    const fullTables = new Map<string, LiteTableSpec>();
    computeZqlSpecs(
      this.#lc,
      db.db,
      { includeBackfillingColumns: false },
      this.#tableSpecs,
      fullTables,
    );
    checkClientSchema(
      this.#shardID,
      clientSchema,
      this.#tableSpecs,
      fullTables,
    );
    this.#allTableNames.clear();
    for (const table of fullTables.keys()) {
      this.#allTableNames.add(table);
    }
    const primaryKeys = this.#primaryKeys ?? new Map<string, PrimaryKey>();
    this.#primaryKeys = primaryKeys;
    primaryKeys.clear();
    for (const [table, spec] of this.#tableSpecs.entries()) {
      primaryKeys.set(table, spec.tableSpec.primaryKey);
    }
    buildPrimaryKeys(clientSchema, primaryKeys);
    const { replicaVersion } = getSubscriptionState(db);
    this.#replicaVersion = replicaVersion;
  }

  /** @returns The replica version. The PipelineDriver must have been initialized. */
  get replicaVersion(): string {
    return must(this.#replicaVersion, "Not yet initialized");
  }

  /**
   * Returns the current version of the database. This will reflect the
   * latest version change when calling {@link advance()} once the
   * iteration has begun.
   */
  currentVersion(): string {
    assert(this.initialized(), "Not yet initialized");
    return this.#snapshotter.current().version;
  }

  /**
   * Gen-6: the stateVersion to stamp CVR **hydrate** updaters at — an upper
   * bound on the version of every row the hydrate can deliver.
   *
   * Stock TS hydrates from its own snapshot, so the data version IS
   * {@link currentVersion} and the two never diverge. In Go-primary mode the
   * user-query rows come from GO's independently pinned snapshot, which is
   * taken LATER than TS's (init runs after TS's pin; each advance re-pins Go
   * after TS): rows written in that gap carry `_0_version`s above TS's
   * version. Stamping the hydrate updater at TS's version alone can leave it
   * EQUAL to the committed CVR version (reconnects with unchanged
   * transformation hashes bump nothing), and receiving a gap row under an
   * unbumped CVR version trips cvr.ts's "Expected CVR version to have been
   * bumped above original" assertion — a full client-group teardown.
   *
   * Returns max(TS snapshot version, Go's init-time snapshotter pin, the
   * latest Go advance-reported version). The Go components only apply while
   * the Go backend owns hydrates (`initialized`); degraded/TS-native mode
   * returns {@link currentVersion} — stock behavior. Callers must
   * {@link awaitGoInit} first so the init-time pin is populated.
   */
  hydrateVersion(): string {
    const ts = this.currentVersion();
    if (!this.#goBackend?.initialized) {
      return ts;
    }
    return goHydrateStampVersion(
      ts,
      this.#goBackend.pinnedVersion,
      this.#goDataVersion,
    );
  }

  /**
   * Gen-6: record a Go-reported data version (advanceToHeadStream results).
   * Monotone — ignores empty/undefined and never regresses, so a late or
   * out-of-order report cannot shrink the hydrate stamp.
   */
  #noteGoDataVersion(version: string | undefined): void {
    if (version === undefined || version === "") {
      return;
    }
    if (this.#goDataVersion === null || version > this.#goDataVersion) {
      this.#goDataVersion = version;
    }
  }

  /**
   * Returns the current upstream {app}.permissions, or `null` if none are defined.
   */
  currentPermissions(): LoadedPermissions | null {
    assert(this.initialized(), "Not yet initialized");
    const res = reloadPermissionsIfChanged(
      this.#lc,
      this.#snapshotter.current().db,
      this.#shardID.appID,
      this.#permissions,
      this.#config,
    );
    if (res.changed) {
      this.#permissions = res.permissions;
      this.#lc.debug?.(
        "Reloaded permissions",
        JSON.stringify(this.#permissions),
      );
    }
    return this.#permissions;
  }

  advanceWithoutDiff(): string {
    const { db, version } = this.#snapshotter.advanceWithoutDiff().curr;
    for (const table of this.#tables.values()) {
      table.setDB(db.db);
    }
    this.#tableSourcesVersion = version;
    // Re-initialize Go sidecar with fresh snapshot (leapfrog)
    this.#maybeResetGoBackend();
    return version;
  }

  #ensureCostModelExistsIfEnabled(db: Database) {
    let existing = this.#costModels?.get(db);
    if (existing) {
      return existing;
    }
    if (this.#costModels) {
      const costModel = createSQLiteCostModel(db, this.#tableSpecs);
      this.#costModels.set(db, costModel);
      return costModel;
    }
    return undefined;
  }

  // Plans an AST through the same completeOrdering + cost-model planner pass
  // that buildPipeline applies internally for TS. Used to keep the AST sent
  // to Go in sync with the AST TS materializes against — without this the
  // planner's `flip: true` decorations (which route OR-with-CSQ through
  // FlippedJoin + UnionFanIn merge-with-dedup) never reach Go, so Go's plain
  // Join + applyOr path attaches the inner-CSQ relationship and the streamer
  // over-emits it (continued).
  #planAstForGo(ast: AST): AST {
    const planned = completeOrdering(
      ast,
      (tableName) => must(this.#getSource(tableName)).tableSchema.primaryKey,
    );
    if (!this.#costModels) {
      return planned;
    }
    const db = this.#snapshotter.current().db.db;
    const costModel = this.#ensureCostModelExistsIfEnabled(db);
    if (!costModel) {
      return planned;
    }
    // cost-model planning is an optimisation, not a
    // correctness requirement — the ordering-completed `planned` AST already
    // runs correctly on Go. A planner fault on a skewed/edge-case schema
    // previously threw out of here and killed the whole hydrate/advance
    // dispatch. Degrade gracefully to the unplanned AST instead, warning once
    // so the gap stays visible.
    try {
      return planQuery(planned, costModel);
    } catch (e) {
      this.#lc.warn?.(
        `[go-ivm] cost-model planning failed; falling back to unplanned ` +
          `ordering for this query`,
        e,
      );
      return planned;
    }
  }

  /**
   * Clears storage used for the pipelines. Call this when the
   * PipelineDriver will no longer be used.
   */
  destroy(): Promise<void> {
    this.#storage.destroy();
    this.#snapshotter.destroy();
    // await the Go engine teardown rather than fire-and-
    // forget. On a shared sidecar a rapid recycle — a new ViewSyncer for the
    // SAME client group starting before this one's teardown lands — could
    // otherwise race this group's destroy RPC against the new engine's init,
    // tearing down freshly-initialised state. Awaiting serialises destroy
    // before any recreate. Errors are swallowed (teardown is best-effort and
    // the Go side evicts idle groups regardless).
    return this.#goBackend?.destroy().catch(() => {}) ?? Promise.resolve();
  }

  /** @return Map from query ID to PipelineInfo for all added queries. */
  queries(): ReadonlyMap<string, QueryInfo> {
    return this.#pipelines;
  }

  totalHydrationTimeMs(): number {
    let total = 0;
    for (const pipeline of this.#pipelines.values()) {
      total += pipeline.hydrationTimeMs;
    }
    return total;
  }

  #resolveScalarSubqueries(ast: AST): {
    ast: AST;
    companionRows: { table: string; row: Row }[];
    companions: CompanionSubquery[];
    companionInputs: Input[];
  } {
    const companionRows: { table: string; row: Row }[] = [];
    const companionInputs: Input[] = [];

    const executor = (
      subqueryAST: AST,
      childField: string,
    ): LiteralValue | null | undefined => {
      const input = buildPipeline(
        subqueryAST,
        {
          getSource: (name) => this.#getSource(name),
          createStorage: () => this.#createStorage(),
          decorateSourceInput: (input: SourceInput): Input => input,
          decorateInput: (input) => input,
          addEdge() {},
          decorateFilterInput: (input) => input,
        },
        "scalar-subquery",
      );
      // Consume the full stream rather than using first() to avoid
      // triggering early return on Take's #initialFetch assertion.
      // The subquery AST already has limit: 1, so at most one row is produced.
      let node: Node | undefined;
      for (const n of skipYields(input.fetch({}))) {
        node ??= n;
      }
      if (!node) {
        // Keep the companion alive even with no results — it will
        // detect a future insert that creates the row.
        companionInputs.push(input);
        return undefined;
      }
      companionRows.push({ table: subqueryAST.table, row: node.row as Row });
      companionInputs.push(input);
      return (node.row[childField] as LiteralValue) ?? null;
    };

    const { ast: resolved, companions } = resolveSimpleScalarSubqueries(
      ast,
      this.#tableSpecs,
      executor,
    );
    return { ast: resolved, companionRows, companions, companionInputs };
  }

  /**
   * Adds a pipeline for the query. The method will hydrate the query using the
   * driver's current snapshot of the database and return a stream of results.
   * Henceforth, updates to the query will be returned when the driver is
   * {@link advance}d. The query and its pipeline can be removed with
   * {@link removeQuery()}.
   *
   * If a query with the same queryID is already added, the existing pipeline
   * will be removed and destroyed before adding the new pipeline.
   *
   * @param timer The caller-controlled {@link Timer} used to determine the
   *        final hydration time. (The caller may pause and resume the timer
   *        when yielding the thread for time-slicing).
   * @return The rows from the initial hydration of the query.
   */
  addQuery(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
    queryName?: string,
  ):
    | Iterable<RowChange | "yield">
    | AsyncIterable<RowChange | "yield">
    | Promise<
        Iterable<RowChange | "yield"> | AsyncIterable<RowChange | "yield">
      > {
    // If Go backend init is pending, await it first
    if (
      this.#goInitPromise &&
      this.#goBackend &&
      !this.#goBackend.initialized
    ) {
      // M1: handle BOTH settle arms with the same dispatch. If Go init/reset
      // REJECTS, the raw promise must not escape — that throw would reach
      // run()'s outer catch and tear down the whole CG. #goInitPromise is
      // nulled by its own .catch (see #maybeInitGoBackend), so the retry sees
      // Go as uninitialized and #addQueryDispatch degrades to the TS-native
      // path (mirrors awaitGoInit's swallow-and-fall-back).
      const dispatch = () =>
        this.#addQueryDispatch(
          transformationHash,
          queryID,
          query,
          timer,
          queryName,
        );
      return this.#goInitPromise.then(dispatch, dispatch);
    }
    // If a per-CG recovery is in flight (drift, sidecar-restart, etc.),
    // wait for it to finish before deciding TS vs Go. Pre-fix the
    // dispatch checked initialized synchronously and fell through to
    // TS-native during the recovery window — that created a phantom
    // TS pipeline (rooted at a user-table TableSource) that persisted
    // beyond the recovery and emitted duplicate RowChanges on every
    // subsequent advance. The new query sees up to ~tens of ms extra
    // latency in exchange for routing to Go correctly.
    if (this.#goBackend) {
      return this.#goBackend
        .whenRecovered()
        .then(() =>
          this.#addQueryDispatch(
            transformationHash,
            queryID,
            query,
            timer,
            queryName,
          ),
        );
    }
    return this.#addQueryDispatch(
      transformationHash,
      queryID,
      query,
      timer,
      queryName,
    );
  }

  #addQueryDispatch(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
    queryName?: string,
  ):
    | Iterable<RowChange | "yield">
    | AsyncIterable<RowChange | "yield">
    | Promise<
        Iterable<RowChange | "yield"> | AsyncIterable<RowChange | "yield">
      > {
    // Internal queries (lmids, mutationResults, queries rooted at the
    // <appID>.permissions or <appID>_<shard>.clients tables) always
    // run via TS — their source tables are excluded from Go's data
    // path (Fix #1), and TS's TableSource over live SQLite self-heals
    // any state drift. Routes regardless of mode so Go-primary doesn't
    // break mutation acks (which depend on the lmids subscription).
    if (
      this.#isInternalQueryID(queryID) ||
      this.#isInternalTable(query.table)
    ) {
      return this.#trackRowSetSignatures(
        this.#addQueryImpl(
          transformationHash,
          queryID,
          query,
          timer,
          queryName,
        ),
      );
    }
    // When Go backend is active, hydrate via sidecar (Go-owned
    // stub pipeline). record the build mode so a later Go-availability flip
    // triggers a rebuild instead of a silent freeze / double-emit.
    if (this.#goBackend?.initialized) {
      this.#goUserPipelineMode = "go";
      return this.#goHydrate(transformationHash, queryID, query, queryName);
    }
    // Real TS pipeline. In a Go-primary deployment this is the DEGRADED path
    // (Go unavailable at build time); mark it so advance() serves TS-native and
    // rebuilds Go-owned stubs once Go recovers
    if (this.#goBackend) {
      this.#goUserPipelineMode = "ts";
    }
    return this.#trackRowSetSignatures(
      this.#addQueryImpl(transformationHash, queryID, query, timer, queryName),
    );
  }

  async #goHydrate(
    transformationHash: string,
    queryID: string,
    query: AST,
    queryName?: string,
  ): Promise<AsyncIterable<RowChange | "yield">> {
    this.removeQuery(queryID);
    // Plan the AST the same way the batch path does so Go's pipeline
    // gets the planner's flip:true annotation and the side-effect of
    // creating TableSources in this.#tables (via #planAstForGo →
    // completeOrdering → #getSource). Without this, single-query
    // hydrate fed Go a raw AST →  over-emit on OR-with-CSQ
    // shapes, and post-reconnect getRow() panicked because the
    // TableSource was never created. (Audit fix F.)
    const planned = this.#planAstForGo(query);
    const hydrateStartMs = performance.now();
    const stream = this.#goBackend!.hydrateStreamPull(queryID, planned);

    // Store a minimal pipeline entry for queries() map and hydration time
    // tracking (no TS pipeline needed — Go handles push processing).
    // hydrationTimeMs feeds totalHydrationTimeMs() — the abort budget — so
    // it must price the RESET an abort triggers, in TS-native's units
    // (wall; see the note above #addQueryImpl's pipelines.set). Go's
    // engine-internal timingMs alone underpriced it ~100× → floor-pinned
    // budget → the abort loop (see goHydrationCostMs).
    this.#pipelines.set(queryID, {
      input: {
        destroy() {},
        fetch: () => ({}) as never,
        cleanup: () => ({}) as never,
        getSchema: () => ({}) as never,
        setOutput: () => {},
      } as unknown as Input,
      hydrationTimeMs: 0,
      transformedAst: planned,
      transformationHash,
      companions: [],
      ...(queryName !== undefined && { queryName }),
    });

    const self = this;
    async function* yieldGoHydration(): AsyncIterable<RowChange | "yield"> {
      let i = 0;
      let sawFinal = false;
      let fallbackDelta = 0n;
      let finalSigDelta: string | undefined;
      try {
        for await (const r of stream) {
          for (const rc of r.changes as RowChange[]) {
            if (i > 0 && i % 100 === 0) {
              yield "yield";
            }
            if (rc.type !== ChangeType.EDIT) {
              fallbackDelta ^= rowIDSignatureUnit({
                schema: "",
                table: rc.table,
                rowKey: rc.rowKey as RowKey,
              });
            }
            yield rc;
            i++;
          }
          if (r.final) {
            sawFinal = true;
            finalSigDelta = r.sigDelta;
            const pipeline = self.#pipelines.get(queryID);
            if (pipeline) {
              self.#pipelines.set(queryID, {
                ...pipeline,
                hydrationTimeMs: goHydrationCostMs(
                  r.timingMs,
                  performance.now() - hydrateStartMs,
                ),
              });
            }
          }
        }
      } catch (e) {
        self.#rowSetSignatures.delete(queryID);
        throw e;
      }
      if (!sawFinal) {
        self.#rowSetSignatures.delete(queryID);
        throw new Error(`go hydrate stream ended without final for ${queryID}`);
      }
      self.#xorRowSetSignature(
        queryID,
        finalSigDelta !== undefined
          ? parseSignature(finalSigDelta)
          : fallbackDelta,
      );
    }

    return yieldGoHydration();
  }

  /**
   * Batch hydrate multiple queries via the Go sidecar (Go-primary mode),
   * streaming per-query results AS SOON as Go finishes that query.
   * Tail-latency optimisation: fast queries reach the WebSocket client
   * before slow queries in the same batch complete.
   *
   * The returned iterable yields entries in COMPLETION order, not input
   * order — callers must not rely on positional correspondence with
   * `queries`.
   */
  async *goHydrateBatchStream(
    queries: {
      transformationHash: string;
      queryID: string;
      ast: AST;
      // Custom-query name, threaded to the pipeline stub exactly like the
      // per-query #goHydrate path (:2169) — observability only (inspector
      // + queryName log context). Review M1: the batch path dropped it.
      queryName?: string | undefined;
    }[],
  ): AsyncIterable<{
    queryID: string;
    changes: Iterable<RowChange | "yield">;
    // Go's per-query engine COMPUTE wall-time (engine.go hydrateEntry,
    // `time.Since(start)` — fetch+materialize, excludes RPC/serialize).
    // Surfaced so the view-syncer records the engine-compute span into
    // hydration_time (apples-to-apples with TS) instead of the TS-side
    // consumption of already-computed rows. `undefined` for internal
    // queries (lmids/clients/permissions) that run through TS's
    // #addQueryImpl — their compute is lazy in the yielded generator, so
    // the consumer times it during drain.
    timingMs: number | undefined;
    // In per-chunk mode, true only on a query's terminal chunk; the
    // view-syncer gates once-per-query metric recording on it. Always true in
    // the default (accumulate-to-final) mode and for internal TS queries.
    final: boolean;
  }> {
    for (const q of queries) {
      this.removeQuery(q.queryID);
    }

    // Split internal queries (lmids, mutationResults, control-plane tables)
    // off the batch — they must run via TS's #addQueryImpl since their
    // source tables are excluded from Go's data path (the same invariant
    // enforced by #addQueryDispatch for the per-query path). Without this
    // split, the Go sidecar panics with `no source for table` on the
    // <appID>.clients / <appID>_<shard>.permissions queries that show up
    // in every first-time hydrate batch.
    const internalQueries: typeof queries = [];
    const userQueries: typeof queries = [];
    for (const q of queries) {
      if (
        this.#isInternalQueryID(q.queryID) ||
        this.#isInternalTable(q.ast.table)
      ) {
        internalQueries.push(q);
      } else {
        userQueries.push(q);
      }
    }

    // Run internal queries through TS first. Their results yield with the
    // same {queryID, changes} shape the Go side does, so the caller is
    // mode-agnostic.
    //
    // Yield-token caveat: #addQueryImpl emits 'yield' tokens for
    // cooperative time-slicing, but in Go-primary the view-syncer's
    // batch consumer never started the TimeSliceTimer — calling
    // `timer.yieldProcess()` on a 'yield' would trip `not running`
    // assert and crash the ViewSyncer. Internal queries are tiny
    // (lmids/mutationResults/clients/permissions are O(rows = active
    // connections)), so dropping the cooperative yields here is safe
    // and contained.
    const noopTimer = {
      elapsedLap: () => 0,
      totalElapsed: () => 0,
      running: () => true,
    } as unknown as Timer;
    for (const q of internalQueries) {
      const raw = this.#trackRowSetSignatures(
        this.#addQueryImpl(
          q.transformationHash,
          q.queryID,
          q.ast,
          noopTimer,
          q.queryName,
        ),
      );
      function* dropYields(): Iterable<RowChange | "yield"> {
        for (const c of raw) {
          if (c !== "yield") yield c;
        }
      }
      yield {
        queryID: q.queryID,
        changes: dropYields(),
        timingMs: undefined,
        final: true,
      };
    }

    if (userQueries.length === 0) {
      return;
    }
    // about to register Go-owned user stubs — record the build mode.
    this.#goUserPipelineMode = "go";

    const plannedUserQueries = userQueries.map((q) => ({
      ...q,
      ast: this.#planAstForGo(q.ast),
    }));
    const byQueryID = new Map<string, (typeof plannedUserQueries)[number]>();
    // Track which queryIDs had a no-op stub registered during this stream
    // cleanup on failure. On failure, these stubs must be cleaned up — otherwise they
    // linger in #pipelines as stale no-op entries.
    const stubQueryIDs = new Set<string>();
    for (const q of plannedUserQueries) byQueryID.set(q.queryID, q);

    // PULL delivery (ABI v3): Go produces each row
    // only as this generator's consumer demands it — the for-await below IS
    // the demand clock (view-syncer pulls one entry, forwards to the pokers,
    // awaits downstream, pulls the next; Go stays ≤ pullWindow deliveries
    // ahead). Abandoning this generator mid-stream (consumer return())
    // closes the inner iterator, which cancels the Go producer (cursor
    // close, pool-reader release) via goivm_stream_cancel. Heap is bounded
    // by the credit window, and one RPC per batch keeps Go's per-query
    // goroutines maximally parallel while the single ordered queue
    // interleaves their rows.
    const batchHydrateStartMs = performance.now();
    const stream = this.#goBackend!.hydrateManyStreamPull(
      plannedUserQueries.map((q) => ({
        queryID: q.queryID,
        ast: q.ast,
      })),
    );
    try {
      for await (const r of stream) {
        const q = byQueryID.get(r.queryID);
        if (!q) continue;
        // Stub registration once per query (real timingMs lands on the
        // terminal entry), signature tracking, final-gated pruning.
        if (!this.#pipelines.has(q.queryID) || r.final) {
          // Track stub registration for cleanup on failure.
          if (!this.#pipelines.has(q.queryID)) {
            stubQueryIDs.add(q.queryID);
          }
          // Abort-budget pricing on the terminal entry (stub registrations
          // keep the engine time until then): the batch hydrates its queries
          // in PARALLEL on one pull stream, so attributing each query its
          // full first→final wall would sum to ~N× the batch wall.
          // Amortize instead — wall-so-far ÷ batch size — so
          // SUM(entries) ≈ the batch's true parallel re-hydrate wall
          // (conservative vs TS-native's serial sum), with Go's engine time
          // as the per-query floor via goHydrationCostMs.
          this.#pipelines.set(q.queryID, {
            input: {
              destroy() {},
              fetch: () => ({}) as never,
              cleanup: () => ({}) as never,
              getSchema: () => ({}) as never,
              setOutput: () => {},
            } as unknown as Input,
            hydrationTimeMs: r.final
              ? goHydrationCostMs(
                  r.timingMs,
                  (performance.now() - batchHydrateStartMs) /
                    plannedUserQueries.length,
                )
              : (r.timingMs ?? 0),
            transformedAst: q.ast,
            transformationHash: q.transformationHash,
            companions: [],
            ...(q.queryName !== undefined && { queryName: q.queryName }),
          });
        }
        const changesArr = r.changes as RowChange[];
        const sigDelta = r.final ? r.sigDelta : undefined;
        // Go's sigDelta is the cumulative XOR of ALL rows across ALL partials.
        // Non-final chunks did per-row XOR (no sigDelta available). Without a
        // reset, the final chunk's sigDelta would double-count the non-final
        // rows: sig = perRowXOR(non-final) ^ sigDelta(all) = XOR(final only).
        // Reset to 0 so the sigDelta becomes the sole contribution:
        // sig = 0 ^ sigDelta(all) = XOR(all rows) ✓
        if (r.final && sigDelta !== undefined) {
          this.#rowSetSignatures.set(q.queryID, 0n);
        }
        function* yieldGoHydration(): Iterable<RowChange | "yield"> {
          for (const rc of changesArr) {
            yield rc;
          }
        }
        yield {
          queryID: q.queryID,
          changes: this.#trackRowSetSignatures(
            yieldGoHydration(),
            sigDelta !== undefined ? { [q.queryID]: sigDelta } : undefined,
          ),
          timingMs: r.timingMs,
          final: r.final,
        };
        if (r.final && sigDelta !== undefined && changesArr.length === 0) {
          this.#xorRowSetSignature(q.queryID, parseSignature(sigDelta));
        }
        if (r.final) byQueryID.delete(q.queryID);
      }
      if (byQueryID.size > 0) {
        for (const queryID of byQueryID.keys()) {
          this.#rowSetSignatures.delete(queryID);
        }
        const missing = [...byQueryID.keys()].join(", ");
        throw new Error(
          `goHydrateBatchStream: ${byQueryID.size} queries never received a final frame: ${missing}`,
        );
      }
    } catch (e) {
      // Finding 9: a stream that died mid-query leaves that query's XOR
      // row-set signature PARTIALLY accumulated (the yielded chunks already
      // passed through #trackRowSetSignatures). Any later re-hydrate XORs
      // its full row set ON TOP of the residue — the duplicated rows
      // self-cancel and the signature is permanently wrong → spurious
      // signature-mismatch resets later. Purge the residue for every query
      // that never reached its final entry; whatever re-hydrates them
      // starts from 0n. Applies to iterator throws too: idle-timeout
      // cancel, sidecar restart, wire errors.
      for (const queryID of byQueryID.keys()) {
        this.#rowSetSignatures.delete(queryID);
        // remove no-op pipeline stubs registered during this stream.
        // Without this, a failed hydrate leaves stale no-op entries in
        // #pipelines (destroy() is a no-op, fetch returns garbage) that
        // inflate pipeline counts and confuse downstream logic.
        if (stubQueryIDs.has(queryID)) {
          this.#pipelines.delete(queryID);
        }
      }
      throw e;
    }
  }

  /** Whether batch hydration is available (Go-primary). */
  get canBatchHydrate(): boolean {
    return !!this.#goBackend?.initialized;
  }

  /**
   * Await Go backend initialization if pending. Call before checking
   * canBatchHydrate to ensure the initial batch of queries uses the
   * Go path instead of falling through to per-query TS hydration.
   */
  async awaitGoInit(): Promise<void> {
    if (!this.#goBackend) return;
    // Use the backend's whenInitialized() which is restart-aware. The plain
    // #goInitPromise can resolve from a prior epoch's init while a restart's
    // re-init is mid-flight — that path silently fell through to TS.
    await this.#goBackend.whenInitialized();
    // Also drain the explicit init promise (covers the very-first init
    // before any whenInitialized state exists).
    if (this.#goInitPromise) {
      try {
        await this.#goInitPromise;
      } catch {
        // Swallow — caller's dispatch path will fall back to TS based on
        // the initialized flag.
      }
    }
  }

  *#addQueryImpl(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
    queryName?: string,
  ): Iterable<RowChange | "yield"> {
    assert(
      this.initialized(),
      "Pipeline driver must be initialized before adding queries",
    );
    this.removeQuery(queryID);
    const debugDelegate = runtimeDebugFlags.trackRowsVended
      ? new Debug()
      : undefined;

    const costModel = this.#ensureCostModelExistsIfEnabled(
      this.#snapshotter.current().db.db,
    );

    assert(
      this.#advanceContext === null,
      "Cannot hydrate while advance is in progress",
    );
    this.#hydrateContext = {
      timer,
    };
    try {
      const {
        ast: resolvedQuery,
        companionRows,
        companions: companionMeta,
        companionInputs,
      } = this.#resolveScalarSubqueries(query);

      const input = buildPipeline(
        resolvedQuery,
        {
          debug: debugDelegate,
          enableNotExists: true, // Server-side can handle NOT EXISTS
          getSource: (name) => this.#getSource(name),
          createStorage: () => this.#createStorage(),
          decorateSourceInput: (input: SourceInput, _queryID: string): Input =>
            new MeasurePushOperator(
              new QueryFailureLoggingOperator(
                this.#lc,
                input,
                queryID,
                transformationHash,
                queryName,
              ),
              queryID,
              this.#inspectorDelegate,
              "query-update-server",
            ),
          decorateInput: (input) => input,
          addEdge() {},
          decorateFilterInput: (input) => input,
        },
        queryID,
        costModel,
      );
      const schema = input.getSchema();
      input.setOutput({
        push: (change) => {
          const streamer = this.#streamer;
          assert(streamer, "must #startAccumulating() before pushing changes");
          streamer.accumulate(queryID, schema, [change]);
          return [];
        },
      });

      yield* hydrateInternal(
        input,
        queryID,
        must(this.#primaryKeys),
        this.#tableSpecs,
      );

      for (const { table, row } of companionRows) {
        const primaryKey = mustGetPrimaryKey(this.#primaryKeys, table);
        yield {
          type: ChangeType.ADD,
          queryID,
          table,
          rowKey: getRowKey(primaryKey, row),
          row,
        } as RowChange;
      }

      const hydrationTimeMs = timer.totalElapsed();
      if (runtimeDebugFlags.trackRowCountsVended) {
        if (hydrationTimeMs > this.#logConfig.slowHydrateThreshold) {
          let totalRowsConsidered = 0;
          const lc = this.#lc
            .withContext("queryID", queryID)
            .withContext("hydrationTimeMs", hydrationTimeMs);
          for (const tableName of this.#tables.keys()) {
            const entries = Object.entries(
              debugDelegate?.getVendedRowCounts()[tableName] ?? {},
            );
            totalRowsConsidered += entries.reduce(
              (acc, entry) => acc + entry[1],
              0,
            );
            lc.info?.(tableName + " VENDED: ", entries);
          }
          lc.info?.(`Total rows considered: ${totalRowsConsidered}`);
        }
      }
      debugDelegate?.reset();

      // Set up live companion pipelines for reactive scalar subquery monitoring.
      const liveCompanions: CompanionPipeline[] = [];
      for (let i = 0; i < companionMeta.length; i++) {
        const meta = companionMeta[i];
        const companionInput = companionInputs[i];
        const companionSchema = companionInput.getSchema();
        const { childField, resolvedValue } = meta;
        companionInput.setOutput({
          push: (change: Change) => {
            let newValue: LiteralValue | null | undefined;
            switch (change[ChangeIndex.TYPE]) {
              case ChangeType.ADD:
              case ChangeType.EDIT:
                newValue =
                  (change[ChangeIndex.NODE].row[childField] as LiteralValue) ??
                  null;
                break;
              case ChangeType.REMOVE:
                newValue = undefined;
                break;
              case ChangeType.CHILD:
                return [];
            }
            if (!scalarValuesEqual(newValue, resolvedValue)) {
              throw new ResetPipelinesSignal(
                `Scalar subquery value changed for ${meta.ast.table}: ` +
                  `${String(resolvedValue)} -> ${String(newValue)}`,
                "scalar-subquery",
              );
            }
            const streamer = this.#streamer;
            assert(
              streamer,
              "must #startAccumulating() before pushing changes",
            );
            streamer.accumulate(queryID, companionSchema, [change]);
            return [];
          },
        });
        liveCompanions.push({
          input: companionInput,
          childField,
          resolvedValue,
        });
      }

      // Note: hydrationTimeMs is a wall-clock measurement taken across the
      // (time-sliced, yield-punctuated) hydrate, so it can overestimate pure
      // processing time. It is the value stored on the pipeline and used by
      // the adaptive hydrate circuit-breaker math and the slow-hydrate
      // warning; there is no separate precise-reset pass.
      this.#pipelines.set(queryID, {
        input,
        hydrationTimeMs,
        transformedAst: resolvedQuery,
        transformationHash,
        ...(queryName !== undefined && { queryName }),
        companions: liveCompanions,
      });
    } catch (e) {
      logQueryFailure(
        this.#lc,
        { queryHash: queryID, transformationHash, queryName },
        "query hydration failed",
        e,
      );
      throw e;
    } finally {
      this.#hydrateContext = null;
    }
  }

  /**
   * Removes the pipeline for the query. This is a no-op if the query
   * was not added.
   */
  removeQuery(queryID: string) {
    const pipeline = this.#pipelines.get(queryID);
    if (pipeline) {
      this.#pipelines.delete(queryID);
      pipeline.input.destroy();
      for (const companion of pipeline.companions) {
        companion.input.destroy();
      }
    }
    this.#rowSetSignatures.delete(queryID);
    // Fire-and-forget: notify Go sidecar
    this.#goBackend?.removeQuery(queryID).catch(() => {});
  }

  /**
   * Current XOR signature of the row-set attached to `queryID`, or
   * `undefined` if no pipeline for the query is currently active.
   * Maintained incrementally by {@link addQuery} and {@link advance}.
   */
  rowSetSignature(queryID: string): bigint | undefined {
    return this.#rowSetSignatures.get(queryID);
  }

  /**
   * Wraps an iterable of RowChanges, XORing each row's unit hash into the
   * query's signature (ADDs and REMOVEs share the same op; EDITs are no-ops).
   * Used to intercept the yield streams from {@link addQuery} and
   * {@link advance}.
   */
  *#trackRowSetSignatures(
    changes: Iterable<RowChange | "yield">,
    sigDeltas?: Readonly<Record<string, string>>,
  ): Iterable<RowChange | "yield"> {
    const appliedDeltas = new Set<string>();
    for (const change of changes) {
      if (change !== "yield" && change.type !== ChangeType.EDIT) {
        const sigDelta = sigDeltas?.[change.queryID];
        if (sigDelta !== undefined) {
          if (!appliedDeltas.has(change.queryID)) {
            const cur = this.#rowSetSignatures.get(change.queryID) ?? 0n;
            this.#rowSetSignatures.set(
              change.queryID,
              cur ^ parseSignature(sigDelta),
            );
            appliedDeltas.add(change.queryID);
          }
        } else {
          const cur = this.#rowSetSignatures.get(change.queryID) ?? 0n;
          const unit = rowIDSignatureUnit({
            schema: "",
            table: change.table,
            rowKey: change.rowKey as RowKey,
          });
          this.#rowSetSignatures.set(change.queryID, cur ^ unit);
        }
      }
      yield change;
    }
  }

  #xorRowSetSignature(queryID: string, delta: bigint): void {
    const cur = this.#rowSetSignatures.get(queryID) ?? 0n;
    this.#rowSetSignatures.set(queryID, cur ^ delta);
  }

  #applyGoAdvanceSigDeltas(
    sigDeltas: Readonly<Record<string, string>> | undefined,
    fallbackDeltas: ReadonlyMap<string, bigint>,
  ): void {
    const applied = new Set<string>();
    if (sigDeltas !== undefined) {
      for (const [queryID, delta] of Object.entries(sigDeltas)) {
        this.#xorRowSetSignature(queryID, parseSignature(delta));
        applied.add(queryID);
      }
    }
    for (const [queryID, delta] of fallbackDeltas) {
      if (!applied.has(queryID)) {
        this.#xorRowSetSignature(queryID, delta);
      }
    }
  }

  /**
   * Returns the value of the row with the given primary key `pk`,
   * or `undefined` if there is no such row. The pipeline must have been
   * initialized.
   */
  getRow(table: string, pk: RowKey): Row | undefined {
    assert(this.initialized(), "Not yet initialized");
    // Include the table name in the error message. Without it a bare-must()
    // failure during CVR catchup ("Unexpected undefined value") fires before
    // ViewSyncer's outer must can wrap it with "Missing row ...", obscuring
    // which table source went missing. Suspect: query removed mid-CVR-catchup
    // while view-syncer still has a refCount for one of its rows.
    const source = must(
      this.#tables.get(table),
      `pipelineDriver.getRow: no TableSource for table=${table} ` +
        `(pk=${JSON.stringify(pk)}). Known tables: ${[...this.#tables.keys()].join(",")}`,
    );
    return source.getRow(pk as Row);
  }

  /**
   * Advances to the new head of the database.
   *
   * @param timer The caller-controlled {@link Timer} that will be used to
   *        measure the progress of the advancement and abort with a
   *        {@link ResetPipelinesSignal} if it is estimated to take longer
   *        than a hydration.
   * @return The resulting row changes for all added queries. Note that the
   *         `changes` must be iterated over in their entirety in order to
   *         advance the database snapshot.
   */
  advance(
    timer: Timer,
  ):
    | AdvanceResult
    | ResetPipelinesSignal
    | Promise<AdvanceResult | ResetPipelinesSignal> {
    assert(
      this.initialized(),
      "Pipeline driver must be initialized before advancing",
    );
    // If Go backend init is pending, await it first
    if (
      this.#goInitPromise &&
      this.#goBackend &&
      !this.#goBackend.initialized
    ) {
      // M1: see #addQuery — a rejected Go init/reset must degrade to the
      // TS-native advance, not escape and tear down the CG.
      const dispatch = () => this.#advanceDispatch(timer);
      return this.#goInitPromise.then(dispatch, dispatch);
    }
    return this.#advanceDispatch(timer);
  }

  #advanceDispatch(
    timer: Timer,
  ):
    | AdvanceResult
    | ResetPipelinesSignal
    | Promise<AdvanceResult | ResetPipelinesSignal> {
    const diff = this.#snapshotter.advance(
      this.#tableSpecs,
      this.#allTableNames,
    );
    const { prev, curr, changes } = diff;
    this.#lc.debug?.(
      `advance ${prev.version} => ${curr.version}: ${changes} changes`,
    );

    // Go-primary: reconcile the LIVE Go availability against the
    // mode the current user pipelines were built in (#goUserPipelineMode). A
    // flip in either direction must rebuild the pipelines (via a returned
    // ResetPipelinesSignal) before we can safely advance. See
    // decideGoPrimaryDispatch for the full decision matrix and rationale.
    if (this.#goBackend) {
      switch (
        decideGoPrimaryDispatch(
          this.#goBackend.initialized,
          this.#goUserPipelineMode,
        )
      ) {
        case "go-advance":
          // Go-primary: dual-run TS + Go on disjoint table sets. Go gets the
          // diff with internal tables filtered out and handles user queries.
          // TS handles internal queries (lmids, mutationResults) via its real
          // #addQueryImpl pipelines. User-query pipelines in TS are stubs (no
          // setOutput callback), so TS's #advance walks the full diff but only
          // emits for internal queries. Merging is safe (table-disjoint sets).
          return this.#goPrimaryAdvance(diff, timer, curr.version, changes);
        case "reset-recovered":
          // Go recovered but user pipelines were degraded to real TS while it
          // was down — running #goPrimaryAdvance now would DOUBLE-emit user
          // rows (TS's real pipelines AND Go both emit). Rebuild as stubs.
          return new ResetPipelinesSignal(
            "Go backend recovered; rebuilding Go-owned pipelines " +
              "(were degraded to TS while Go was unavailable)",
            "go-primary-unavailable",
          );
        case "reset-degrade":
          // Go is DOWN in primary mode with Go-owned stub pipelines; the
          // TS-native advance below would emit NOTHING for them — a silent
          // freeze with the cookie advancing past the gap (watermark
          // over-claim). Reset so re-registration (which checks `initialized`)
          // rebuilds REAL TS pipelines → graceful TS-serving.
          return new ResetPipelinesSignal(
            "Go backend unavailable in primary mode at advance time; " +
              "rebuilding TS pipelines (avoids silent watermark over-claim)",
            "go-primary-unavailable",
          );
        case "ts-native":
          // Pipelines are already real TS (or none) — the TS-native advance
          // below serves correctly; do NOT reset (that would loop every advance
          // for the whole outage / drift-breaker cooldown).
          break;
      }
    }

    return {
      version: curr.version,
      numChanges: changes,
      changes: this.#trackRowSetSignatures(this.#advance(diff, timer, changes)),
    };
  }

  /**
   * Go-primary advance: runs Go's advanceToHeadStream for user queries and
   * TS's #advance for internal queries in parallel, then concatenates
   * the results. Internal tables are filtered so the Go side never sees
   * control-plane diffs.
   */
  async #goPrimaryAdvance(
    diff: SnapshotDiff,
    timer: Timer,
    version: string,
    numChanges: number,
  ): Promise<AdvanceResult | ResetPipelinesSignal> {
    // Buffer the diff so both TS and Go can consume it. Filter the Go
    // side to drop internal tables (Fix #1 invariant — those rows go
    // through TS's TableSource which self-heals against live SQLite).
    const buffered: Array<{
      table: string;
      prevValues: Readonly<Row>[];
      nextValue: Readonly<Row> | null;
      rowKey: RowKey;
    }> = [];
    // consume the diff EAGERLY to buffer it for both engines, but
    // route through drainDiffCatchingReset so the ResetPipelinesSignal the diff
    // iterator throws on a truncate / schema change is RETURNED (graceful
    // view-syncer reset + re-hydrate) instead of escaping #advancePipelines into
    // run()'s outer catch (full client-group teardown on every truncate).
    const resetSignal = drainDiffCatchingReset(diff, (entry) => {
      if (this.#isInternalTable(entry.table)) {
        // TS advances its real internal-query pipelines from these
        // (lmids / mutationResults); always replay them.
        buffered.push(entry);
        return;
      }
      // User-table change (lean primary): TS holds only stub user pipelines
      // (Go owns them) and keeps its user TableSources current via the snapshot
      // setDB in #advance — NOT via these pushes — so replaying user changes on
      // TS is pure redundant work; drop them from TS's replay buffer. Go
      // derives its own diff (advanceToHead trigger), so it needs nothing here.
    });
    if (resetSignal) {
      return resetSignal;
    }
    const replayDiff: SnapshotDiff = {
      prev: diff.prev,
      curr: diff.curr,
      changes: diff.changes,
      [Symbol.iterator]: () => buffered[Symbol.iterator](),
    };

    const suppressAbort = shouldSuppressAbort(this.#consecutiveAdvanceAborts);
    if (suppressAbort) {
      this.#advanceAbortSuppressed.add(1);
      this.#lc.warn?.(
        `[go-primary] advance abort streak=${this.#consecutiveAdvanceAborts} ` +
          `exhausted the budget escalation — suppressing the economic abort ` +
          `for this advance (60s wall backstop still applies)`,
      );
    }
    const abortBudget = {
      totalHydrationTimeMs: goAdvanceAbortBudgetMs(
        this.totalHydrationTimeMs(),
        this.#consecutiveAdvanceAborts,
      ),
      ...(suppressAbort ? { suppressAbort: true } : {}),
    };

    // Accumulates the wall time spent awaiting Go stream frames across this
    // advance. Captured by the yieldMerged closure below (a `let`, so the
    // closure's `+=` mutates this same binding) and recorded to
    // #advanceGoRpcTime once the stream is fully consumed / closed.
    let goRpcWallMs = 0;
    const goIterator =
      this.#goBackend!.advanceToHeadStreamChunks(abortBudget)[
        Symbol.asyncIterator
      ]();
    const rpcStart0 = performance.now();
    const firstGoChunkPromise = goIterator.next();

    let firstGoChunk: AdvanceToHeadStreamChunk;
    try {
      const first = await firstGoChunkPromise;
      goRpcWallMs += performance.now() - rpcStart0;
      if (first.done) {
        throw new Error("advanceToHeadStream finished before first frame");
      }
      firstGoChunk = first.value;
    } catch (e) {
      // Time-to-first-frame failure is still a real RPC sample. yieldMerged
      // never runs on this path, so record here before the early return.
      goRpcWallMs += performance.now() - rpcStart0;
      this.#advanceGoRpcTime.recordMs(goRpcWallMs);
      const classified = this.#classifyGoPrimaryAdvanceError(e);
      return classified instanceof ResetPipelinesSignal
        ? classified
        : {
            version: diff.prev.version,
            numChanges,
            changes: classified,
          };
    }

    const resetFromGo = (reset: { reason: string; msg: string }) => {
      this.#lc.info?.(
        `[go-primary] Go reported reset ${reset.reason} ` +
          `(${reset.msg}); escalating to pipeline reset`,
      );
      return new ResetPipelinesSignal(
        `Go reported reset ${reset.reason} (${reset.msg})`,
        "go-primary-drop",
      );
    };

    if (firstGoChunk.header !== true || firstGoChunk.final) {
      await goIterator.return?.();
      throw new Error(
        "advanceToHeadStream expected non-final header as first frame",
      );
    }

    const headerGoVersion = firstGoChunk.version;
    if (headerGoVersion === undefined) {
      await goIterator.return?.();
      throw new Error("advanceToHeadStream header missing version");
    }

    if (headerGoVersion < version) {
      await goIterator.return?.();
      return new ResetPipelinesSignal(
        `Go header version ${headerGoVersion} fell behind TS version ${version}`,
        "go-primary-drop",
      );
    }

    const reconciled = reconcileGoPrimaryWatermark(version, headerGoVersion);
    const self = this;
    async function* yieldMerged(): AsyncIterable<RowChange | "yield"> {
      const fallbackDeltas = new Map<string, bigint>();
      let finalSigDeltas: Record<string, string> | undefined;
      let finalGoVersion = headerGoVersion;
      let sawFinal = false;
      try {
        // Run TS's #advance over the replay buffer on the consumer's pull clock.
        // Only internal-query pipelines are connected, so user-table pushes are
        // no-ops on TS. The buffer already excludes user changes, so pass its
        // actual length for the advance-time heuristic.
        yield* self.#trackRowSetSignatures(
          self.#advance(replayDiff, timer, buffered.length, true),
        );

        try {
          for (;;) {
            const rpcStartN = performance.now();
            const next = await goIterator.next();
            goRpcWallMs += performance.now() - rpcStartN;
            if (next.done) break;
            const chunk = next.value;
            if (sawFinal) {
              throw new Error(
                "advanceToHeadStream yielded a frame after the final frame",
              );
            }
            if (chunk.reset) {
              throw resetFromGo(chunk.reset);
            }
            for (const change of chunk.changes as unknown as RowChange[]) {
              if (change.type !== ChangeType.EDIT) {
                const unit = rowIDSignatureUnit({
                  schema: "",
                  table: change.table,
                  rowKey: change.rowKey as RowKey,
                });
                fallbackDeltas.set(
                  change.queryID,
                  (fallbackDeltas.get(change.queryID) ?? 0n) ^ unit,
                );
              }
              yield change;
            }
            if (chunk.final) {
              self.#recordGoPrimaryAdvanceTimings(chunk.timings);
              if (chunk.goWallMs !== undefined) {
                self.#advanceGoInternalTime.recordMs(chunk.goWallMs);
              }
              finalSigDeltas = chunk.sigDeltas;
              finalGoVersion = chunk.version || finalGoVersion;
              sawFinal = true;
            }
          }
        } catch (e) {
          if (e instanceof ResetPipelinesSignal) {
            throw e;
          }
          const classified = self.#classifyGoPrimaryAdvanceError(e);
          if (classified instanceof ResetPipelinesSignal) {
            throw classified;
          }
          throw e;
        }
      } finally {
        await goIterator.return?.();
        // One sample per advance that entered the stream (success, streaming
        // error, or reset). The time-to-first-frame-failure path records its
        // own sample and never reaches here, so no double-count.
        self.#advanceGoRpcTime.recordMs(goRpcWallMs);
      }

      if (!sawFinal) {
        throw new Error("advanceToHeadStream ended without a final frame");
      }
      if (finalGoVersion !== undefined && finalGoVersion < version) {
        throw new ResetPipelinesSignal(
          `Go final version ${finalGoVersion} fell behind TS version ${version}`,
          "go-primary-drop",
        );
      }
      self.#consecutiveAdvanceAborts = 0;
      self.#noteGoDataVersion(finalGoVersion);
      self.#applyGoAdvanceSigDeltas(finalSigDeltas, fallbackDeltas);
    }

    return {
      version: reconciled.version,
      numChanges,
      changes: yieldMerged(),
      tsVersion: reconciled.tsVersion,
      goVersion: reconciled.goVersion,
    };
  }

  /**
   * Record Go-side per-table advance timings into the same #advanceTime
   * histogram the TS-native path populates, so Go-primary has timing
   * attribution parity instead of dropping `timings` on the floor. Also feeds
   * the per-query `query-update-server` inspector metric (the TS-native path
   * populates it via MeasurePushOperator, which can't exist across the RPC
   * boundary): Go only reports per-(table, op) timings, so attribute each
   * table's time to every user query whose top-level table matches. This is an
   * APPROXIMATION — a query reading a table only through a related/CSQ subquery
   * isn't attributed, and a table feeding N queries credits all N — but it makes
   * the metric non-zero and directionally useful instead of permanently empty.
   */
  #recordGoPrimaryAdvanceTimings(timings: TableTiming[] | undefined): void {
    if (!timings) return;
    const tableToQueries = new Map<string, string[]>();
    for (const [qid, p] of this.#pipelines) {
      if (this.#isInternalQueryID(qid)) continue;
      const t = p.transformedAst.table;
      const list = tableToQueries.get(t);
      if (list) list.push(qid);
      else tableToQueries.set(t, [qid]);
    }
    for (const t of timings) {
      this.#advanceTime.recordMs(t.ms, {
        table: t.table,
        type: t.type === 0 ? "add" : t.type === 1 ? "remove" : "edit",
      });
      for (const qid of tableToQueries.get(t.table) ?? []) {
        this.#inspectorDelegate.addMetric("query-update-server", t.ms, qid);
      }
    }
  }

  /**
   * Classify a Go-primary advance RPC failure (advanceToHeadStream).
   * Pre-fix the catch was a silent swallow of ALL errors, indistinguishable
   * from a legitimately-empty advance. Bucket by class so dashboards detect each
   * failure mode and operators take the right action:
   *
   *   1. Protocol violation (chunk order, missing final chunk, decode/frame):
   *      wire-level bug or corruption — can't trust Go's state. RE-THROW so the
   *      caller surfaces it instead of committing an empty CVR diff. A reset
   *      won't fix a wire bug without a sidecar process restart.
   *   2. StaleInitEpochError: this instance was torn down and a successor's
   *      epoch took over. Retrying is futile — RE-THROW so teardown completes.
   *   3. Sidecar unavailable / restart-related: the advance can't be trusted.
   *      return a ResetPipelinesSignal so the view-syncer re-hydrates all
   *      pipelines at head. The legacy "return [] + #scheduleGoReset" path
   *      floored this cycle's watermark at prev but the async Go reset rebuilt
   *      at head with its hydrate output DISCARDED — the (prev→head] user delta
   *      was never delivered (permanent gap). A full re-hydrate heals it.
   *   4. Other unclassified (incl. RPC timeouts under load): same  escalation.
   *
   * Returns a ResetPipelinesSignal for the drop cases (caller RETURNS it from
   * #goPrimaryAdvance → graceful re-hydrate); throws for the escalation cases
   * (protocol/stale → caller surfaces it → teardown + reconnect).
   */
  #classifyGoPrimaryAdvanceError(
    e: unknown,
  ): RowChange[] | ResetPipelinesSignal {
    const msg = e instanceof Error ? e.message : String(e);

    switch (classifyGoPrimaryAdvanceError(e)) {
      case "protocol":
        this.#advanceDroppedProtocol.add(1);
        this.#lc.error?.(
          `[go-primary] Go advance failed with PROTOCOL VIOLATION (escalating): ${msg}`,
        );
        throw e;
      case "stale-epoch":
        this.#advanceDroppedStaleEpoch.add(1);
        this.#lc.warn?.(
          `[go-primary] Go advance rejected by sidecar (stale initEpoch); ` +
            `this view-syncer instance is torn down: ${msg}`,
        );
        throw e;
      case "data-error":
        // Permanent bad replica data — re-throw (CG teardown) like
        // TS-native's UnsupportedValueError. NEVER reset: a reset re-reads the
        // same row and re-panics, looping forever AND re-paying every CG's
        // hydrate (the sustained 5–8s p99 reset storm). One poison row drops
        // ONE CG, not the whole pod.
        this.#advanceDroppedDataError.add(1);
        this.#lc.error?.(
          `[go-primary] Go advance hit PERMANENT data error (tearing down CG, ` +
            `NOT resetting — bad replica data, retry cannot fix): ${msg}`,
        );
        throw e;
      case "advance-aborted":
        // Go's economic abort — TS's own advancement-timeout, computed inside
        // the Go advance with the same inputs (elapsed vs the CG's priced
        // totalHydrationTimeMs, progress vs numChanges). Resolve to the SAME
        // signal + reason a TS-native abort produces; the message is Go's
        // byte-identical rendering of TS's template. The streak feeds
        // escalatedAbortBudgetMs so consecutive aborts cannot loop at the
        // same backlog position.
        this.#consecutiveAdvanceAborts++;
        this.#advanceAbortedEconomic.add(1);
        this.#lc.info?.(`[go-primary] ${msg}`);
        return new ResetPipelinesSignal(msg, "advancement-timeout");
      case "scalar-reset":
        // A resolved scalar subquery's value changed mid-advance (Go's
        // companion detection, engine.ScalarResetError → -32105). TS-native
        // throws ResetPipelinesSignal('scalar-subquery') from its own
        // companion push for the identical event — same signal, same reason,
        // same transparent reset + re-hydrate. Before this branch the error
        // landed in 'unclassified' → re-throw → CG teardown: a "manual
        // reload"-class client symptom on a designed-for seamless path.
        this.#advanceScalarReset.add(1);
        this.#lc.info?.(`[go-primary] scalar-subquery reset: ${msg}`);
        return new ResetPipelinesSignal(msg, "scalar-subquery");
      case "sidecar":
        this.#advanceDroppedSidecar.add(1);
        this.#lc.warn?.(
          `[go-primary] Go advance dropped (sidecar restart in flight); ` +
            `escalating to pipeline reset: ${msg}`,
        );
        return new ResetPipelinesSignal(
          `Go advance dropped (sidecar restart): ${msg}`,
          "go-primary-drop",
        );
      case "unclassified":
        // TS semantics for an unexpected error: RE-THROW → run()'s outer
        // catch → CG teardown → clients reconnect (with their own backoff —
        // natural admission control). This bucket used to DROP → reset,
        // which under load (RPC timeouts) became the reset storm: every
        // timeout re-paid every CG's hydrate on an already-saturated worker.
        // The load-coupled causes now have owners — no in-process wall-clock
        // timeouts (computeBoundTimeoutMs), the economic abort
        // ('advance-aborted' above), clean-failure in-place retries
        // (RetryableAdvanceError in GoComputeBackend) — so whatever lands
        // here is a genuine bug, and resets don't fix bugs.
        this.#advanceDroppedOther.add(1);
        this.#lc.error?.(
          `[go-primary] Go advance failed (unclassified); escalating to CG ` +
            `teardown (TS unexpected-error semantics): ${msg}`,
        );
        throw e;
    }
  }

  /**
   * Schedule a best-effort reset of the Go engine from the current snapshot.
   * Used after a Go failure that leaves engine state suspect — reinitializing
   * from a fresh snapshot read resets it.
   *
   * Idempotent: collapses concurrent reset requests so a burst of failures
   * doesn't spawn N parallel re-inits.
   */
  #scheduleGoReset(reason: string): void {
    if (!this.#goBackend) return;
    // If the snapshotter has been torn down (CG eviction / worker reassignment),
    // there is nothing to reset — resetEngine would re-read a CLOSED snapshot
    // connection in #currentTablesForGo and fail-loop ("database connection is
    // not open" per table, ×N, then a failed reset that retries). The CG is
    // gone; skip cleanly. (More likely under the  trigger path, whose longer
    // advanceToHead window widens the reset-vs-teardown overlap.)
    if (this.#snapshotter.destroyed) {
      this.#lc.debug?.(
        `[go-reset] snapshotter torn down; skipping reset (${reason})`,
      );
      return;
    }
    // Record EVERY caller (even ones we coalesce with #goResetDirty) so the
    // metric reflects the real trigger rate, not just the post-dedup
    // executed-resets count. Dashboard queries that want executed count can
    // sum minus dirty-coalesced count separately.
    this.#goResetScheduled.add(1, { reason });
    if (this.#goResetInFlight) {
      // Don't drop the request — record it so we re-fire after the in-flight
      // reset completes.
      this.#goResetDirty = true;
      return;
    }
    this.#goResetInFlight = true;
    const MAX_RESET_RETRIES = 3;
    this.#lc.warn?.(`[go-reset] Scheduling Go reset (${reason})`);
    // resetEngine reads the snapshot at reinit time (after its
    // destroy await), not now — pre-capturing here loaded a stale snapshot
    // and amplified drift into a reset loop.
    this.#goInitPromise = this.#goBackend.resetEngine();
    this.#goInitPromise
      .then(() => {
        this.#lc.info?.(`[go-reset] Go reset complete (${reason})`);
        this.#goResetRetries = 0;
      })
      .catch((err) => {
        // If the snapshotter was torn down DURING the reset (destroy raced the
        // resetEngine destroy-await → #currentTablesForGo read a closed conn),
        // the CG is gone — abandon cleanly instead of logging an error and
        // retrying into the same closed connection.
        if (this.#snapshotter.destroyed) {
          this.#lc.debug?.(
            `[go-reset] snapshotter torn down during reset; abandoning (${reason})`,
          );
          this.#goResetRetries = 0;
          this.#goResetDirty = false;
          return;
        }
        this.#lc.error?.(`[go-reset] Go reset failed (${reason}):`, err);
        // Reset itself failed — retry with bounded attempts. After cap,
        // give up and let the system stay in TS-only fallback until the
        // next operational signal (sidecar restart, schema change, etc.).
        if (this.#goResetRetries < MAX_RESET_RETRIES) {
          this.#goResetRetries++;
          this.#goResetDirty = true;
        } else {
          this.#lc.error?.(
            `[go-reset] Go reset retries exhausted (${this.#goResetRetries}); ` +
              `staying on TS fallback`,
          );
          this.#goResetRetries = 0;
        }
      })
      .finally(() => {
        this.#goResetInFlight = false;
        if (this.#goResetDirty) {
          this.#goResetDirty = false;
          // Fire a follow-up reset to cover failures that arrived during
          // the just-completed cycle.
          this.#scheduleGoReset(`${reason} (follow-up)`);
        }
      });
  }

  *#advance(
    diff: SnapshotDiff,
    timer: Timer,
    numChanges: number,
    suppressAbort: boolean = false,
  ): Iterable<RowChange | "yield"> {
    assert(
      this.#hydrateContext === null,
      "Cannot advance while hydration is in progress",
    );
    const totalHydrationTimeMs = this.totalHydrationTimeMs();
    this.#advanceContext = {
      timer,
      totalHydrationTimeMs,
      numChanges,
      pos: 0,
      suppressAbort,
    };
    this.#lc.debug?.(
      `starting pipeline advancement of ${numChanges} changes with an ` +
        `advancement time limited based on total hydration time of ` +
        `${totalHydrationTimeMs} ms.`,
    );
    try {
      for (const { table, prevValues, nextValue } of diff) {
        // Advance progress is checked each time a row is fetched
        // from a TableSource during push processing, but some pushes
        // don't read any rows.  Check progress here before processing
        // the next change.
        if (this.#shouldAdvanceYieldMaybeAbortAdvance()) {
          yield "yield";
        }
        const start = timer.totalElapsed();

        // `type` label for the #advanceTime histogram. Previously left
        // undeclared → recorded as undefined, while the Go path passes a
        // real string; histogram dimensions diverged.
        let type: "add" | "remove" | "edit" | undefined;
        try {
          const tableSource = this.#tables.get(table);
          if (!tableSource) {
            // no pipelines read from this table, so no need to process the change
            continue;
          }
          const primaryKey = mustGetPrimaryKey(this.#primaryKeys, table);
          let editOldRow: Row | undefined = undefined;
          for (const prevValue of prevValues) {
            if (
              nextValue &&
              deepEqual(
                getRowKey(primaryKey, prevValue as Row) as JSONValue,
                getRowKey(primaryKey, nextValue as Row) as JSONValue,
              )
            ) {
              editOldRow = prevValue;
            } else {
              if (nextValue) {
                this.#conflictRowsDeleted.add(1);
              }
              type = "remove";
              yield* this.#push(
                tableSource,
                makeSourceChangeRemove(prevValue as Row),
              );
            }
          }
          if (nextValue) {
            if (editOldRow) {
              type = "edit";
              yield* this.#push(
                tableSource,
                makeSourceChangeEdit(nextValue as Row, editOldRow),
              );
            } else {
              type = "add";
              yield* this.#push(
                tableSource,
                makeSourceChangeAdd(nextValue as Row),
              );
            }
          }
        } finally {
          this.#advanceContext.pos++;
        }

        const elapsed = timer.totalElapsed() - start;
        this.#advanceTime.recordMs(elapsed, {
          table,
          type,
        });
      }

      // Set the new snapshot on all TableSources.
      const { curr } = diff;
      for (const table of this.#tables.values()) {
        table.setDB(curr.db.db);
      }
      this.#tableSourcesVersion = curr.version;
      this.#ensureCostModelExistsIfEnabled(curr.db.db);
      this.#lc.debug?.(`Advanced to ${curr.version}`);
    } finally {
      this.#advanceContext = null;
      // If the advance loop threw (ResetPipelinesSignal or unexpected
      // error in #push), the success-path setDB + version update above
      // never ran — TableSources stay bound to the old snapshot while
      // the snapshotter has already moved forward.
      //
      // Realign now: bind TableSources to the current snapshot and
      // sync the version field. The advance's diff wasn't fully
      // applied, but the post-advance state is still the snapshotter's
      // current snapshot — TableSources reading at that frame is
      // correct (their next fetch sees the same point-in-time the
      // snapshotter exposes). The caller's restart machinery is
      // responsible for rebuilding any operator state that depends
      // on the dropped diff.
      const { curr } = diff;
      if (this.#tableSourcesVersion !== curr.version) {
        for (const table of this.#tables.values()) {
          table.setDB(curr.db.db);
        }
        this.#tableSourcesVersion = curr.version;
      }
    }
  }

  /** Implements `BuilderDelegate.getSource()` */
  #getSource(tableName: string): Source {
    let source = this.#tables.get(tableName);
    if (source) {
      return source;
    }

    const tableSpec = mustGetTableSpec(this.#tableSpecs, tableName);
    const primaryKey = mustGetPrimaryKey(this.#primaryKeys, tableName);

    const { db } = this.#snapshotter.current();
    source = new TableSource(
      this.#lc,
      this.#logConfig,
      db.db,
      tableName,
      tableSpec.zqlSpec,
      primaryKey,
      () => this.#shouldYield(),
    );
    this.#tables.set(tableName, source);
    this.#lc.debug?.(`created TableSource for ${tableName}`);
    return source;
  }

  #shouldYield(): boolean {
    if (this.#hydrateContext) {
      // Shadow-mode opens an async boundary mid-iteration (await Go
      // RPC); the surrounding view-syncer's stop sequence can stop
      // the timer underneath the still-running TS hydrate. Guard
      // against the assertion — return false so the generator exits
      // its loop cleanly on the next iteration instead of throwing.
      // No-op in Go-primary (no TS hydrate generator interleaved with
      // async Go calls) and TS-only (no async window inside hydrate).
      const t = this.#hydrateContext.timer;
      if (!t.running()) return false;
      return t.elapsedLap() > this.#yieldThresholdMs();
    }
    if (this.#advanceContext) {
      return this.#shouldAdvanceYieldMaybeAbortAdvance();
    }
    throw new Error("shouldYield called outside of hydration or advancement");
  }

  /**
   * Cancel the advancement processing, by throwing a ResetPipelinesSignal, if
   * it has taken longer than half the total hydration time to make it through
   * half of the advancement, or if processing time exceeds total hydration
   * time.  This serves as both a circuit breaker for very large transactions,
   * as well as a bound on the amount of time the previous connection locks
   * the inactive WAL file (as the lock prevents WAL2 from switching to the
   * free WAL when the current one is over the size limit, which can make
   * the WAL grow continuously and compound slowness).
   * This is checked:
   * 1. before starting to process each change in an advancement is processed
   * 2. whenever a row is fetched from a TableSource during push processing
   */
  #shouldAdvanceYieldMaybeAbortAdvance(): boolean {
    const {
      pos,
      numChanges,
      timer: advanceTimer,
      totalHydrationTimeMs,
      suppressAbort,
    } = must(this.#advanceContext);
    const elapsed = advanceTimer.totalElapsed();
    if (
      !suppressAbort &&
      elapsed > MIN_ADVANCEMENT_TIME_LIMIT_MS &&
      (elapsed > totalHydrationTimeMs ||
        (elapsed > totalHydrationTimeMs / 2 && pos <= numChanges / 2))
    ) {
      throw new ResetPipelinesSignal(
        `Advancement exceeded timeout at ${pos} of ${numChanges} changes ` +
          `after ${elapsed} ms. Advancement time limited based on total ` +
          `hydration time of ${totalHydrationTimeMs} ms.`,
        "advancement-timeout",
      );
    }
    // The async boundaries in #goPrimaryAdvance (the setImmediate yields in
    // its replay loop, the `await goPromise`) let the surrounding view-syncer
    // stop the timer mid-iteration. Skip the elapsedLap (which would assert)
    // and return false — the advance generator finishes its current step and
    // exits on the next loop tick.
    if (!advanceTimer.running()) return false;
    return advanceTimer.elapsedLap() > this.#yieldThresholdMs();
  }

  /** Implements `BuilderDelegate.createStorage()` */
  #createStorage(): Storage {
    return this.#storage.createStorage();
  }

  *#push(
    source: TableSource,
    change: SourceChange,
  ): Iterable<RowChange | "yield"> {
    this.#startAccumulating();
    try {
      for (const val of source.genPush(change)) {
        if (val === "yield") {
          yield "yield";
        }
        for (const changeOrYield of this.#stopAccumulating().stream()) {
          yield changeOrYield;
        }
        this.#startAccumulating();
      }
    } finally {
      if (this.#streamer !== null) {
        this.#stopAccumulating();
      }
    }
  }

  #startAccumulating() {
    assert(this.#streamer === null, "Streamer already started");
    this.#streamer = new Streamer(
      must(this.#primaryKeys),
      this.#tableSpecs,
      (queryID, error) =>
        this.#logQueryFailure(queryID, "query pipeline failed", error),
    );
  }

  #stopAccumulating(): Streamer {
    const streamer = this.#streamer;
    assert(streamer, "Streamer not started");
    this.#streamer = null;
    return streamer;
  }

  #logQueryFailure(queryID: string, message: string, error: unknown): void {
    const pipeline = this.#pipelines.get(queryID);
    const queryInfo = pipeline
      ? {
          queryHash: queryID,
          transformationHash: pipeline.transformationHash,
          queryName: pipeline.queryName,
        }
      : undefined;
    logQueryFailure(this.#lc, queryInfo, message, error);
  }
}

class Streamer {
  readonly #primaryKeys: Map<string, PrimaryKey>;
  readonly #tableSpecs: Map<string, LiteAndZqlSpec>;
  readonly #logQueryFailure:
    ((queryID: string, error: unknown) => void) | undefined;

  constructor(
    primaryKeys: Map<string, PrimaryKey>,
    tableSpecs: Map<string, LiteAndZqlSpec>,
    logQueryFailure?: (queryID: string, error: unknown) => void,
  ) {
    this.#primaryKeys = primaryKeys;
    this.#tableSpecs = tableSpecs;
    this.#logQueryFailure = logQueryFailure;
  }

  readonly #changes: [
    queryID: string,
    schema: SourceSchema,
    changes: Iterable<Change | "yield">,
  ][] = [];

  accumulate(
    queryID: string,
    schema: SourceSchema,
    changes: Iterable<Change | "yield">,
  ): this {
    this.#changes.push([queryID, schema, changes]);
    return this;
  }

  *stream(): Iterable<RowChange | "yield"> {
    for (const [queryID, schema, changes] of this.#changes) {
      try {
        yield* this.#streamChanges(queryID, schema, changes);
      } catch (e) {
        this.#logQueryFailure?.(queryID, e);
        throw e;
      }
    }
  }

  *#streamChanges(
    queryID: string,
    schema: SourceSchema,
    changes: Iterable<Change | "yield">,
  ): Iterable<RowChange | "yield"> {
    // We do not sync rows gathered by the permissions
    // system to the client.
    if (schema.system === "permissions") {
      return;
    }

    for (const change of changes) {
      if (change === "yield") {
        yield change;
        continue;
      }
      const type = change[ChangeIndex.TYPE];
      switch (type) {
        case ChangeType.REMOVE:
        case ChangeType.ADD: {
          yield* this.#streamNodes(queryID, schema, type, () => [
            change[ChangeIndex.NODE],
          ]);
          break;
        }

        case ChangeType.CHILD: {
          const child = change[ChangeIndex.CHILD_DATA];
          const childSchema = must(
            schema.relationships[child.relationshipName],
          );

          yield* this.#streamChanges(queryID, childSchema, [child.change]);
          break;
        }
        case ChangeType.EDIT:
          yield* this.#streamNodes(queryID, schema, type, () => [
            { row: change[ChangeIndex.NODE].row, relationships: {} },
          ]);
          break;
        default:
          unreachable(change[ChangeIndex.TYPE]);
      }
    }
  }

  *#streamNodes(
    queryID: string,
    schema: SourceSchema,
    op: ChangeType.ADD | ChangeType.REMOVE | ChangeType.EDIT,
    nodes: () => Iterable<Node | "yield">,
  ): Iterable<RowChange | "yield"> {
    const { tableName: table, system } = schema;

    const primaryKey = must(this.#primaryKeys.get(table));
    const spec = must(this.#tableSpecs.get(table)).tableSpec;

    // We do not sync rows gathered by the permissions
    // system to the client.
    if (system === "permissions") {
      return;
    }

    for (const node of nodes()) {
      if (node === "yield") {
        yield node;
        continue;
      }
      const { relationships } = node;
      let { row } = node;
      const rowKey = getRowKey(primaryKey, row);
      if (op !== ChangeType.REMOVE) {
        const rowVersion = row[ZERO_VERSION_COLUMN_NAME];
        if (
          typeof rowVersion === "string" &&
          rowVersion < (spec.minRowVersion ?? "00")
        ) {
          row = { ...row, [ZERO_VERSION_COLUMN_NAME]: spec.minRowVersion };
        }
      }

      yield {
        type: op,
        queryID,
        table,
        rowKey,
        row: op === ChangeType.REMOVE ? undefined : row,
      } as RowChange;

      for (const [relationship, children] of Object.entries(relationships)) {
        const childSchema = must(schema.relationships[relationship]);
        yield* this.#streamNodes(queryID, childSchema, op, children);
      }
    }
  }
}

class QueryFailureLoggingOperator implements Input, Output {
  readonly #lc: LogContext;
  readonly #input: Input;
  readonly #queryHash: string;
  readonly #transformationHash: string;
  readonly #queryName: string | undefined;
  #output: Output = throwOutput;

  constructor(
    lc: LogContext,
    input: Input,
    queryHash: string,
    transformationHash: string,
    queryName?: string,
  ) {
    this.#lc = lc;
    this.#input = input;
    this.#queryHash = queryHash;
    this.#transformationHash = transformationHash;
    this.#queryName = queryName;
    input.setOutput(this);
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  destroy(): void {
    this.#input.destroy();
  }

  fetch(req: FetchRequest): Iterable<Node | "yield"> {
    return this.#input.fetch(req);
  }

  *push(change: Change): Iterable<"yield"> {
    try {
      yield* this.#output.push(change, this);
    } catch (e) {
      logQueryFailure(
        this.#lc,
        {
          queryHash: this.#queryHash,
          transformationHash: this.#transformationHash,
          queryName: this.#queryName,
        },
        "query pipeline failed",
        e,
      );
      throw e;
    }
  }
}

function logQueryFailure(
  lc: LogContext,
  queryInfo: QueryLogInfo | undefined,
  message: string,
  error: unknown,
): void {
  if (error instanceof ResetPipelinesSignal) {
    return;
  }
  let queryLC = lc;
  if (queryInfo) {
    queryLC = queryLC
      .withContext("queryHash", queryInfo.queryHash)
      .withContext("transformationHash", queryInfo.transformationHash);
    if (queryInfo.queryName !== undefined) {
      queryLC = queryLC.withContext("queryName", queryInfo.queryName);
    }
  }
  queryLC.error?.(message, error);
}

function* toAdds(nodes: Iterable<Node | "yield">): Iterable<Change | "yield"> {
  for (const node of nodes) {
    if (node === "yield") {
      yield node;
      continue;
    }
    yield [ChangeType.ADD, node, null];
  }
}

function getRowKey(cols: PrimaryKey, row: Row): RowKey {
  return Object.fromEntries(cols.map((col) => [col, must(row[col])]));
}

/**
 * Core hydration logic used by {@link PipelineDriver#addQuery}, extracted to a
 * function for reuse by the analyze-query RPC path so that analysis hydrates
 * queries the same way the view-syncer does in production.
 */
export function* hydrate(
  input: Input,
  hash: string,
  clientSchema: ClientSchema,
  tableSpecs: Map<string, LiteAndZqlSpec>,
): Iterable<RowChange | "yield"> {
  const res = input.fetch({});
  const streamer = new Streamer(
    buildPrimaryKeys(clientSchema),
    tableSpecs,
  ).accumulate(hash, input.getSchema(), toAdds(res));
  yield* streamer.stream();
}

export function* hydrateInternal(
  input: Input,
  hash: string,
  primaryKeys: Map<string, PrimaryKey>,
  tableSpecs: Map<string, LiteAndZqlSpec>,
): Iterable<RowChange | "yield"> {
  const res = input.fetch({});
  const streamer = new Streamer(primaryKeys, tableSpecs).accumulate(
    hash,
    input.getSchema(),
    toAdds(res),
  );
  yield* streamer.stream();
}

function buildPrimaryKeys(
  clientSchema: ClientSchema,
  primaryKeys: Map<string, PrimaryKey> = new Map<string, PrimaryKey>(),
) {
  for (const [tableName, { primaryKey }] of Object.entries(
    clientSchema.tables,
  )) {
    primaryKeys.set(tableName, primaryKey as unknown as PrimaryKey);
  }
  return primaryKeys;
}

function mustGetPrimaryKey(
  primaryKeys: Map<string, PrimaryKey> | null,
  table: string,
): PrimaryKey {
  const pKeys = must(primaryKeys, "primaryKey map must be non-null");

  const rv = pKeys.get(table);
  assert(
    rv,
    () =>
      // oxlint-disable-next-line e18e/prefer-array-to-sorted
      `table '${table}' is not one of: ${[...pKeys.keys()].sort()}. ` +
      `Check the spelling and ensure that the table has a primary key.`,
  );
  return rv;
}

/**
 * Compares two scalar subquery resolved values for equality.
 * Unlike `valuesEqual` in data.ts (which treats null != null for join
 * semantics), this uses identity semantics: undefined === undefined
 * (no row matched), null === null (row matched but field was NULL).
 */
function scalarValuesEqual(
  a: LiteralValue | null | undefined,
  b: LiteralValue | null | undefined,
): boolean {
  return a === b;
}

/**
 * Map an upstream PostgreSQL type name to the column-type tag the Go
 * sidecar understands ('boolean' | 'number' | 'string' | 'null' | 'json').
 *
 * Unrecognized types previously fell through silently to 'string', which
 * silently mis-types bytea (would be base64 / hex on TS, raw bytes / TEXT on
 * Go), arrays (Postgres `int4[]` literal text, e.g. `{1,2,3}`), INTERVAL,
 * geometric types, network types, range types. We now log a one-time
 * warning per unrecognized type so operators see the gap, and document
 * the explicit-handling exceptions.
 *
 * Caller-side de-dup of warnings is handled via a module-level Set so a
 * 50-table schema doesn't produce 50 lines for the same unknown type.
 */
const pgTypeWarningsSeen = new Set<string>();
export function pgTypeToGoType(
  pgType: string,
  warn?: (msg: string) => void,
): "string" | "number" | "boolean" | "null" | "json" {
  // dataType may be in "lite type string" format: "bool|nn", "int4|nn",
  // "varchar(255)|nn" etc. Extract the upstream type (before any pipe
  // delimiter), strip any "(N)" args (e.g. char(32) → char), and lowercase —
  // exactly mirroring `formatTypeForLookup` in types/pg-data-type.ts so this
  // Go-dispatch mapping stays in lock-step with the canonical
  // `pgToZqlTypeMap`. The previous hand-rolled list was a
  // divergent copy that dropped TIME/TIMETZ, bare INT, the SERIAL family,
  // bare FLOAT, and never stripped `(N)` (so `varchar(255)` fell through to
  // the unknown→string warn path). Keep this list byte-for-byte aligned with
  // pgToZqlTypeMap — if a type is added there, add it here too.
  const delim = pgType.indexOf("|");
  const upstream = delim > 0 ? pgType.substring(0, delim) : pgType;
  const argStart = upstream.indexOf("(");
  const t = (argStart > 0 ? upstream.substring(0, argStart) : upstream)
    .trim()
    .toUpperCase();
  // Arrays (including enum-arrays) map to 'json': the replicator stores ALL
  // array columns as JSON.stringify'd text in SQLite, and both sides must
  // JSON.parse them into real arrays. This MUST be checked before the enum
  // check below — an enum-ARRAY (e.g. `TicketPriority[]|TEXT_ENUM|TEXT_ARRAY`)
  // carries BOTH the |TEXT_ENUM and |TEXT_ARRAY attributes (plus `[]`), but
  // the CONTAINER is JSON, so the array property takes precedence over the
  // enum-ness (which only affects how individual elements are compared).
  // Without this, enum-arrays hit isLiteEnum → 'string', causing Go's
  // FromSQLiteType('string', ...) to skip JSON parsing → a +2-byte drift
  // per enum-array column (two extra `\"` chars from JSON.stringify on the
  // Go side vs a bare array on the TS side). isArray also catches the legacy
  // `|TEXT_ARRAY[]` form that the plain `t.endsWith('[]')` missed.
  if (isArray(pgType)) return "json";
  // Enums: the LiteTypeString carries a `|TEXT_ENUM` attribute (e.g.
  // `TicketPriority|NOT_NULL|TEXT_ENUM`) that the upstream-name extraction above
  // strips, so a user-defined enum name fell through to the "unrecognised type"
  // warning. Enums are TEXT-backed and compared as their string labels on BOTH
  // sides (the SQLite replica stores them as TEXT); the canonical TS mapper
  // (`dataTypeToZqlValueType`) likewise returns 'string' for enums. Map to
  // 'string' WITHOUT a warning — this is correct, not a gap. Checked before the
  // name lookups so an enum named like a builtin can't be mis-typed.
  if (isLiteEnum(pgType)) return "string";
  if (t === "BOOL" || t === "BOOLEAN") return "boolean";
  if (
    // Integer + serial families (PG rewrites SERIAL → INTEGER, but the
    // declared type may still surface as serial in a lite type string).
    t === "SMALLINT" ||
    t === "INTEGER" ||
    t === "INT" ||
    t === "INT2" ||
    t === "INT4" ||
    t === "INT8" ||
    t === "BIGINT" ||
    t === "SMALLSERIAL" ||
    t === "SERIAL" ||
    t === "SERIAL2" ||
    t === "SERIAL4" ||
    t === "SERIAL8" ||
    t === "BIGSERIAL" ||
    // Real / floating / fixed-point.
    t === "REAL" ||
    t === "DOUBLE PRECISION" ||
    t === "FLOAT" ||
    t === "FLOAT4" ||
    t === "FLOAT8" ||
    t === "NUMERIC" ||
    t === "DECIMAL" ||
    // Date / time — all mapped to number (epoch-based) like the canonical map.
    t === "DATE" ||
    t === "TIME" ||
    t === "TIMETZ" ||
    t === "TIME WITH TIME ZONE" ||
    t === "TIME WITHOUT TIME ZONE" ||
    t === "TIMESTAMP" ||
    t === "TIMESTAMPTZ" ||
    t === "TIMESTAMP WITH TIME ZONE" ||
    t === "TIMESTAMP WITHOUT TIME ZONE"
  ) {
    return "number";
  }
  if (t === "JSON" || t === "JSONB") return "json";
  // Explicitly recognised string-shaped types — keep this list growing.
  if (
    t === "TEXT" ||
    t === "VARCHAR" ||
    t === "CHARACTER VARYING" ||
    t === "CHAR" ||
    t === "CHARACTER" ||
    t === "BPCHAR" ||
    t === "UUID" ||
    t === "CITEXT" ||
    t === "NAME"
  ) {
    return "string";
  }
  // Postgres array types (e.g. INT4[], TEXT[]) are handled by the early
  // `isArray` check above (which also catches enum-arrays and the legacy
  // |TEXT_ARRAY[] form). The previous `t.endsWith('[]')` here was redundant
  // and missed the enum-array case (isLiteEnum fired first → 'string').
  // BYTEA: text-encoded binary (hex on PG side via SQLite replica). Both
  // sides treat as string for now; document the limitation.
  if (t === "BYTEA") {
    if (warn && !pgTypeWarningsSeen.has(t)) {
      pgTypeWarningsSeen.add(t);
      warn(
        `BYTEA treated as text-encoded string — binary content opaque to Go IVM`,
      );
    }
    return "string";
  }
  // Truly unknown type — fall back to string but log once so the gap is
  // visible. Operators can add explicit mappings as they appear.
  if (warn && !pgTypeWarningsSeen.has(t)) {
    pgTypeWarningsSeen.add(t);
    warn(
      `unrecognised PostgreSQL type "${t}" mapped to 'string' — Go IVM may produce wrong results`,
    );
  }
  return "string";
}
