import {existsSync, readFileSync, writeFileSync} from 'node:fs';
import type {LogContext} from '@rocicorp/logger';
import UrlPattern from 'url-pattern';
import {assert} from '../../../shared/src/asserts.ts';
import {h32} from '../../../shared/src/hash.ts';
import {getOrCreateGauge} from '../observability/metrics.ts';
import {RunningState} from '../services/running-state.ts';
import type {Service} from '../services/service.ts';
import type {IncomingMessageSubset} from '../types/http.ts';
import {MESSAGE_TYPES, type Worker} from '../types/processes.ts';
import {installWebSocketHandoff} from '../types/websocket-handoff.ts';
import {getConnectParams} from '../workers/connect-params.ts';

export type SyncerClientGroupLoad = {
  clientGroupID: string;
  activeConnections: number;
  queries: number;
  rows: number;
};

export type SyncerLoadReport = {
  workerIndex: number;
  activeClientGroups: number;
  activeConnections: number;
  queries: number;
  rows: number;
  clientGroups: SyncerClientGroupLoad[];
  timestamp: number;
};

export type SyncerLoadMessage = [
  typeof MESSAGE_TYPES.syncerLoad,
  SyncerLoadReport,
];

export type SyncerRehomeRequest = {
  clientGroupID: string;
  fromWorkerIndex: number;
  toWorkerIndex: number;
  reason: string;
  timestamp: number;
};

export type SyncerRehomeMessage = [
  typeof MESSAGE_TYPES.syncerRehome,
  SyncerRehomeRequest,
];

type ControlledRehomeOptions = {
  enabled: boolean;
  sustainedReports?: number | undefined;
  minScoreDelta?: number | undefined;
  minDurationMs?: number | undefined;
  cooldownMs?: number | undefined;
  maxTargetScore?: number | undefined;
  maxRehomesPerWindow?: number | undefined;
  rehomeWindowMs?: number | undefined;
  onRehome: (request: SyncerRehomeRequest) => void;
};

type NormalizedControlledRehomeOptions = {
  enabled: true;
  sustainedReports: number;
  minScoreDelta: number;
  minDurationMs: number;
  cooldownMs: number;
  maxTargetScore: number;
  maxRehomesPerWindow: number;
  rehomeWindowMs: number;
  onRehome: (request: SyncerRehomeRequest) => void;
};

type SyncerAssignmentRouterOptions = {
  taskID: string;
  syncerCount: number;
  assignmentsFile: string | undefined;
  lc: LogContext;
  controlledRehome?: ControlledRehomeOptions | undefined;
};

const DEFAULT_REHOME_SUSTAINED_REPORTS = 3;
const DEFAULT_REHOME_MIN_SCORE_DELTA = 2_000;
const DEFAULT_REHOME_MIN_DURATION_MS = 30_000;
const DEFAULT_REHOME_COOLDOWN_MS = 60_000;
const DEFAULT_REHOME_MAX_TARGET_SCORE = 25_000;
const DEFAULT_REHOME_MAX_PER_WINDOW = 3;
const DEFAULT_REHOME_WINDOW_MS = 10 * 60_000;

export class SyncerAssignmentRouter {
  readonly #taskID: string;
  readonly #syncerCount: number;
  readonly #assignmentsFile: string | undefined;
  readonly #lc: LogContext;
  readonly #controlledRehome: NormalizedControlledRehomeOptions | undefined;
  readonly #assignments = new Map<string, number>();
  readonly #assignmentCounts: number[];
  readonly #loadReports = new Map<number, SyncerLoadReport>();
  readonly #rehomeCooldownUntilByCG = new Map<string, number>();
  #recentRehomeTimestamps: number[] = [];
  #imbalancePair: string | undefined;
  #imbalanceSince = 0;
  #imbalanceStreak = 0;
  #nextRehomeAt = 0;

  constructor({
    taskID,
    syncerCount,
    assignmentsFile,
    lc,
    controlledRehome,
  }: SyncerAssignmentRouterOptions) {
    this.#taskID = taskID;
    this.#syncerCount = syncerCount;
    this.#assignmentsFile = assignmentsFile;
    this.#lc = lc;
    this.#controlledRehome =
      controlledRehome?.enabled === true
        ? {
            ...controlledRehome,
            enabled: true,
            sustainedReports:
              controlledRehome.sustainedReports ??
              DEFAULT_REHOME_SUSTAINED_REPORTS,
            minScoreDelta:
              controlledRehome.minScoreDelta ?? DEFAULT_REHOME_MIN_SCORE_DELTA,
            minDurationMs:
              controlledRehome.minDurationMs ?? DEFAULT_REHOME_MIN_DURATION_MS,
            cooldownMs:
              controlledRehome.cooldownMs ?? DEFAULT_REHOME_COOLDOWN_MS,
            maxTargetScore:
              controlledRehome.maxTargetScore ??
              DEFAULT_REHOME_MAX_TARGET_SCORE,
            maxRehomesPerWindow:
              controlledRehome.maxRehomesPerWindow ??
              DEFAULT_REHOME_MAX_PER_WINDOW,
            rehomeWindowMs:
              controlledRehome.rehomeWindowMs ?? DEFAULT_REHOME_WINDOW_MS,
          }
        : undefined;
    this.#assignmentCounts = new Array<number>(syncerCount).fill(0);

    if (assignmentsFile && existsSync(assignmentsFile)) {
      try {
        const data = JSON.parse(readFileSync(assignmentsFile, 'utf-8'));
        for (const [cg, idx] of Object.entries(data)) {
          if (
            typeof idx === 'number' &&
            Number.isInteger(idx) &&
            idx >= 0 &&
            idx < syncerCount
          ) {
            this.#assignments.set(cg, idx);
            this.#assignmentCounts[idx]++;
          }
        }
        lc.info?.(
          `loaded ${this.#assignments.size} syncer assignments from ${assignmentsFile}`,
        );
      } catch (e) {
        lc.warn?.(`failed to load syncer assignments, starting fresh`, e);
      }
    }
  }

  updateLoad(listenerIndex: number, report: SyncerLoadReport) {
    if (listenerIndex < 0 || listenerIndex >= this.#syncerCount) {
      return;
    }
    this.#loadReports.set(listenerIndex, report);
    this.#maybeRehome();
  }

  assign(clientGroupID: string): number {
    const existing = this.#assignments.get(clientGroupID);
    if (existing !== undefined) {
      return existing;
    }

    const syncerIdx = this.#assignmentsFile
      ? this.#leastLoadedSyncer(clientGroupID)
      : this.#hashedSyncer(clientGroupID);
    this.#assignments.set(clientGroupID, syncerIdx);
    this.#assignmentCounts[syncerIdx]++;
    this.#persistAssignments();
    return syncerIdx;
  }

  #hashedSyncer(clientGroupID: string): number {
    return h32(this.#taskID + '/' + clientGroupID) % this.#syncerCount;
  }

  #leastLoadedSyncer(clientGroupID: string): number {
    let bestIdx = 0;
    let bestScore = Number.POSITIVE_INFINITY;
    let bestTie = Number.NEGATIVE_INFINITY;

    for (let i = 0; i < this.#syncerCount; i++) {
      const score = this.#score(i);
      // Rendezvous tie-break keeps equal-score assignment stable without
      // first-worker bias.
      const tie = h32(`${clientGroupID}/${i}`);
      if (score < bestScore || (score === bestScore && tie > bestTie)) {
        bestIdx = i;
        bestScore = score;
        bestTie = tie;
      }
    }

    return bestIdx;
  }

  #score(index: number): number {
    const report = this.#loadReports.get(index);
    if (!report) {
      return this.#assignmentCounts[index] * 1000;
    }
    return (
      report.activeClientGroups * 1000 +
      report.activeConnections * 25 +
      report.queries +
      Math.ceil(report.rows / 1000)
    );
  }

  #maybeRehome() {
    const controlledRehome = this.#controlledRehome;
    if (!controlledRehome || this.#syncerCount < 2) {
      return;
    }
    if (this.#loadReports.size < this.#syncerCount) {
      return;
    }

    let totalScore = 0;
    let hotIdx = -1;
    let hotScore = Number.NEGATIVE_INFINITY;
    let coldIdx = -1;
    let coldScore = Number.POSITIVE_INFINITY;
    for (let i = 0; i < this.#syncerCount; i++) {
      const score = this.#score(i);
      totalScore += score;
      if (score > hotScore) {
        hotIdx = i;
        hotScore = score;
      }
      if (score < coldScore) {
        coldIdx = i;
        coldScore = score;
      }
    }
    if (hotIdx === -1 || coldIdx === -1 || hotIdx === coldIdx) {
      return;
    }

    const scoreDelta = hotScore - coldScore;
    const pair = `${hotIdx}->${coldIdx}`;
    const now = Date.now();
    if (scoreDelta < controlledRehome.minScoreDelta) {
      this.#imbalancePair = undefined;
      this.#imbalanceSince = 0;
      this.#imbalanceStreak = 0;
      return;
    }
    const averageScore = totalScore / this.#syncerCount;
    if (
      coldScore > controlledRehome.maxTargetScore ||
      coldScore + controlledRehome.minScoreDelta > averageScore
    ) {
      this.#resetImbalance();
      return;
    }

    if (this.#imbalancePair === pair) {
      this.#imbalanceStreak++;
    } else {
      this.#imbalancePair = pair;
      this.#imbalanceSince = now;
      this.#imbalanceStreak = 1;
    }
    if (
      this.#imbalanceStreak < controlledRehome.sustainedReports ||
      now - this.#imbalanceSince < controlledRehome.minDurationMs
    ) {
      return;
    }

    if (now < this.#nextRehomeAt) {
      return;
    }
    this.#pruneRecentRehomes(now, controlledRehome.rehomeWindowMs);
    if (
      this.#recentRehomeTimestamps.length >=
      controlledRehome.maxRehomesPerWindow
    ) {
      return;
    }

    const candidate = this.#pickRehomeCandidate(
      hotIdx,
      hotScore,
      coldScore,
      averageScore,
      now,
    );
    if (!candidate) {
      return;
    }

    const existing = this.#assignments.get(candidate.clientGroupID);
    if (existing !== undefined && existing !== hotIdx) {
      return;
    }

    this.#assignments.set(candidate.clientGroupID, coldIdx);
    if (existing === hotIdx) {
      this.#assignmentCounts[hotIdx] = Math.max(
        0,
        this.#assignmentCounts[hotIdx] - 1,
      );
    }
    this.#assignmentCounts[coldIdx]++;
    this.#persistAssignments();

    this.#nextRehomeAt = now + controlledRehome.cooldownMs;
    this.#rehomeCooldownUntilByCG.set(
      candidate.clientGroupID,
      now + controlledRehome.cooldownMs,
    );
    this.#recentRehomeTimestamps.push(now);
    this.#resetImbalance();

    const request = {
      clientGroupID: candidate.clientGroupID,
      fromWorkerIndex: hotIdx,
      toWorkerIndex: coldIdx,
      reason: `syncer load score ${hotScore} exceeds ${coldScore} by ${scoreDelta}; moving estimated score ${clientGroupScore(candidate)}`,
      timestamp: now,
    };
    this.#lc.info?.(
      `rehome client group ${candidate.clientGroupID} from syncer ${hotIdx} to ${coldIdx}`,
      request,
    );
    controlledRehome.onRehome(request);
  }

  #pickRehomeCandidate(
    sourceIdx: number,
    sourceScore: number,
    targetScore: number,
    averageScore: number,
    now: number,
  ): SyncerClientGroupLoad | undefined {
    const controlledRehome = this.#controlledRehome;
    if (!controlledRehome) {
      return undefined;
    }
    const report = this.#loadReports.get(sourceIdx);
    if (!report) {
      return undefined;
    }

    let best: SyncerClientGroupLoad | undefined;
    let bestScore = Number.NEGATIVE_INFINITY;
    let bestTie = Number.NEGATIVE_INFINITY;
    for (const clientGroup of report.clientGroups) {
      if (clientGroup.activeConnections <= 0) {
        continue;
      }
      if (this.#assignments.get(clientGroup.clientGroupID) !== sourceIdx) {
        continue;
      }
      const cooldownUntil =
        this.#rehomeCooldownUntilByCG.get(clientGroup.clientGroupID) ?? 0;
      if (now < cooldownUntil) {
        continue;
      }

      const score = clientGroupScore(clientGroup);
      const sourceAfter = sourceScore - score;
      const targetAfter = targetScore + score;
      if (
        targetAfter > controlledRehome.maxTargetScore ||
        targetAfter > averageScore ||
        Math.max(sourceAfter, targetAfter) >= sourceScore
      ) {
        continue;
      }
      const tie = h32(clientGroup.clientGroupID);
      if (score > bestScore || (score === bestScore && tie > bestTie)) {
        best = clientGroup;
        bestScore = score;
        bestTie = tie;
      }
    }
    return best;
  }

  #resetImbalance() {
    this.#imbalancePair = undefined;
    this.#imbalanceSince = 0;
    this.#imbalanceStreak = 0;
  }

  #pruneRecentRehomes(now: number, windowMs: number) {
    this.#recentRehomeTimestamps = this.#recentRehomeTimestamps.filter(
      timestamp => now - timestamp < windowMs,
    );
  }

  #persistAssignments() {
    if (!this.#assignmentsFile) return;
    try {
      writeFileSync(
        this.#assignmentsFile,
        JSON.stringify(Object.fromEntries(this.#assignments)),
      );
    } catch (e) {
      this.#lc.warn?.(`failed to persist syncer assignments`, e);
    }
  }
}

function clientGroupScore(clientGroup: SyncerClientGroupLoad): number {
  return (
    1000 +
    clientGroup.activeConnections * 25 +
    clientGroup.queries +
    Math.ceil(clientGroup.rows / 1000)
  );
}

export class WorkerDispatcher implements Service {
  readonly id = 'worker-dispatcher';
  readonly #lc: LogContext;

  readonly #state = new RunningState(this.id);

  constructor(
    lc: LogContext,
    taskID: string,
    parent: Worker,
    syncers: Worker[],
    mutator: Worker | undefined,
    changeStreamer: Worker | undefined,
    // When defined, switch from hash-based to load-aware routing and
    // persist the cg→syncer mapping to this path so assignments survive
    // restart. Wired via ZERO_SYNCER_LOAD_AWARE_ROUTING=1 in main.ts.
    assignmentsFile?: string | undefined,
    controlledRehome = false,
  ) {
    this.#lc = lc;

    const syncerRouter = new SyncerAssignmentRouter({
      taskID,
      syncerCount: syncers.length,
      assignmentsFile,
      lc,
      controlledRehome:
        controlledRehome && assignmentsFile
          ? {
              enabled: true,
              onRehome: request => {
                const source = syncers[request.fromWorkerIndex];
                if (!source) {
                  lc.warn?.(
                    `rehome source syncer ${request.fromWorkerIndex} missing`,
                    request,
                  );
                  return;
                }
                source.send([
                  MESSAGE_TYPES.syncerRehome,
                  request,
                ] satisfies SyncerRehomeMessage);
              },
            }
          : undefined,
    });
    syncers.forEach((syncer, index) => {
      syncer.onMessageType<SyncerLoadMessage>(
        MESSAGE_TYPES.syncerLoad,
        report => syncerRouter.updateLoad(index, report),
      );
    });

    function connectParams(req: IncomingMessageSubset) {
      const {headers, url: u} = req;
      const url = new URL(u ?? '', 'http://unused/');
      const p = parsePath(url);
      if (!p) {
        throw new Error(`Invalid URL: ${u}`);
      }
      const version = Number(p.version);
      if (Number.isNaN(version)) {
        throw new Error(`Invalid version: ${u}`);
      }
      const {params, error} = getConnectParams(version, url, headers);
      if (error !== null) {
        throw new Error(error);
      }
      return params;
    }

    const handlePush = (req: IncomingMessageSubset) => {
      assert(
        mutator !== undefined,
        'Received a push for a custom mutation but no `push.url` was configured.',
      );
      return {payload: connectParams(req), sender: mutator};
    };

    let maxProtocolVersion = 0;
    getOrCreateGauge(
      'sync',
      'max-protocol-version',
      'Latest sync protocol version from a connecting client',
    ).addCallback(result => {
      if (maxProtocolVersion) {
        result.observe(maxProtocolVersion);
      }
    });

    const handleSync = (req: IncomingMessageSubset) => {
      assert(syncers.length, 'Received a sync request with no sync workers.');
      const params = connectParams(req);
      const {clientGroupID, protocolVersion} = params;
      maxProtocolVersion = Math.max(maxProtocolVersion, protocolVersion);

      // Routing strategy is selected by SyncerAssignmentRouter: hash-based by
      // default, sticky load-aware + persistent when assignmentsFile is set.
      const syncer = syncerRouter.assign(clientGroupID);

      lc.debug?.(`connecting ${clientGroupID} to syncer ${syncer}`);
      return {payload: params, sender: syncers[syncer]};
    };

    const handleChangeStream = (req: IncomingMessageSubset) => {
      // Note: The change-streamer is generally not dispatched via the main
      //       port, and in particular, should *not* be accessible via that
      //       port in single-node mode. However, this plumbing is maintained
      //       for the purpose of allowing --lazy-startup of the
      //       replication-manager as a possible future feature.
      assert(
        syncers.length === 0 && mutator === undefined,
        'Dispatch to the change-streamer via the main port ' +
          'is only allowed in multi-node mode',
      );
      assert(
        changeStreamer,
        'Received a change-streamer request without a change-streamer worker',
      );
      const url = new URL(req.url ?? '', 'http://unused/');
      const path = parsePath(url);
      if (!path) {
        throw new Error(`Invalid URL: ${req.url}`);
      }

      return {
        payload: path.action,
        sender: changeStreamer,
      };
    };

    // handoff messages from this ZeroDispatcher to the appropriate worker (pool).
    installWebSocketHandoff<unknown>(
      lc,
      request => {
        const {url: u} = request;
        const url = new URL(u ?? '', 'http://unused/');
        const path = parsePath(url);
        if (!path) {
          throw new Error(`Invalid URL: ${u}`);
        }
        switch (path.worker) {
          case 'sync':
            return handleSync(request);
          case 'replication':
            return handleChangeStream(request);
          case 'mutate':
            return handlePush(request);
          default:
            throw new Error(`Invalid URL: ${u}`);
        }
      },
      parent,
    );
  }

  run() {
    const readyStart = Date.now();
    getOrCreateGauge('server', 'uptime', {
      description: 'Cumulative uptime, starting from when requests are served',
      unit: 's',
    }).addCallback(result => result.observe((Date.now() - readyStart) / 1000));

    return this.#state.stopped();
  }

  stop() {
    this.#state.stop(this.#lc);
    return this.#state.stopped();
  }
}

const URL_PATTERN = new UrlPattern('(/:base)/:worker/v:version/:action');

export function parsePath(url: URL):
  | {
      base?: string;
      worker: 'sync' | 'mutate' | 'replication';
      version: string;
      action: string;
    }
  | undefined {
  // The match() returns both null and undefined.
  return URL_PATTERN.match(url.pathname) || undefined;
} // The server allows the client to use any /:base/ path to facilitate
// servicing requests on the same domain as the application.
