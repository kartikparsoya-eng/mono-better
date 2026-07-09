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

export type SyncerLoadReport = {
  workerIndex: number;
  activeClientGroups: number;
  activeConnections: number;
  queries: number;
  rows: number;
  timestamp: number;
};

export type SyncerLoadMessage = [
  typeof MESSAGE_TYPES.syncerLoad,
  SyncerLoadReport,
];

type SyncerAssignmentRouterOptions = {
  taskID: string;
  syncerCount: number;
  assignmentsFile: string | undefined;
  lc: LogContext;
};

export class SyncerAssignmentRouter {
  readonly #taskID: string;
  readonly #syncerCount: number;
  readonly #assignmentsFile: string | undefined;
  readonly #lc: LogContext;
  readonly #assignments = new Map<string, number>();
  readonly #assignmentCounts: number[];
  readonly #loadReports = new Map<number, SyncerLoadReport>();

  constructor({
    taskID,
    syncerCount,
    assignmentsFile,
    lc,
  }: SyncerAssignmentRouterOptions) {
    this.#taskID = taskID;
    this.#syncerCount = syncerCount;
    this.#assignmentsFile = assignmentsFile;
    this.#lc = lc;
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
  ) {
    this.#lc = lc;

    const syncerRouter = new SyncerAssignmentRouter({
      taskID,
      syncerCount: syncers.length,
      assignmentsFile,
      lc,
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
