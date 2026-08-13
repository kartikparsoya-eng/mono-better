import {spawn, type ChildProcess, type SendHandle} from 'node:child_process';
import {EventEmitter} from 'node:events';
import {Socket} from 'node:net';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {must} from '../../../shared/src/must.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {initEventSink} from '../observability/events.ts';
import {
  exitAfter,
  ProcessManager,
  runUntilKilled,
  type WorkerType,
} from '../services/life-cycle.ts';
import {
  restoreReplica,
  startReplicaBackupProcess,
} from '../services/litestream/commands.ts';
import type {ReplicaState} from '../services/replicator/replicator.ts';
import {
  childWorker,
  parentWorker,
  singleProcessMode,
  wrap,
  type Message,
  type Worker,
} from '../types/processes.ts';
import type {Subscription} from '../types/subscription.ts';
import {
  createNotifierFrom,
  handleSubscriptionsFrom,
  type ReplicaFileMode,
  subscribeTo,
} from '../workers/replicator.ts';
import {createLogContext} from './logging.ts';
import {startOtelAuto} from './otel-start.ts';
import {
  notifyRustSyncers,
  proxyUpgradeToRust,
  rustSyncerEnv,
  type UpgradeHandoff,
} from './rust-syncer-bridge.ts';
import {WorkerDispatcher} from './worker-dispatcher.ts';
import {
  CHANGE_STREAMER_URL,
  MUTATOR_URL,
  REAPER_URL,
  REPLICATOR_URL,
  SHADOW_SYNCER_URL,
  SYNCER_URL,
} from './worker-urls.ts';

const clientConnectionBifurcated = false;

const useRustSyncer = process.env.ZERO_SYNCER === 'rust';

// rust-syncer runs as a separate process listening on its own WebSocket +
// HTTP ports, so it cannot receive the dispatcher's IPC file-descriptor
// handoff. These bases give each syncer index `i` a unique pair of ports
// (`ws = wsBase + i`, `http = httpBase + i`); the dispatcher reverse-proxies
// client upgrades to the WS port and fans replica notifications to the HTTP
// port. Override via ZERO_RUST_SYNCER_BASE_PORT / _HTTP_BASE_PORT.
const RUST_SYNCER_WS_BASE_PORT = Number(
  process.env.ZERO_RUST_SYNCER_BASE_PORT ?? 3100,
);
const RUST_SYNCER_HTTP_BASE_PORT = Number(
  process.env.ZERO_RUST_SYNCER_HTTP_BASE_PORT ?? 3200,
);

// Default LogContext, overridden in runWorker
let lc = new LogContext('info', {}, consoleLogSink);

export default async function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
): Promise<void> {
  const startMs = Date.now();
  const config = getNormalizedZeroConfig({env});

  startOtelAuto(
    createLogContext(config, 'dispatcher', 0, false),
    'dispatcher',
    0,
  );
  lc = createLogContext(config, 'dispatcher');
  initEventSink(lc, config);

  const processes = new ProcessManager(lc, parent);

  // The Rust syncer scales *within a single process* via a bounded pool of
  // hash-sharded async executors (doc 91: `packages/zero-cache/docs/
  // rust-cvr-port/91-sharded-executor-design.md`). Running `numSyncWorkers`
  // separate rust-syncer processes would duplicate the replica per process,
  // fragment the CVR connection budget into per-process slices too small to
  // subdivide across executors, and oversubscribe cores (`workers × executors`
  // threads). So for the rust path we run exactly ONE rust-syncer, hand it the
  // whole CVR/upstream connection budget, and let it fan out to `K ≈ cores`
  // executors internally. TS syncers keep the multi-process model unchanged.
  const numSyncers =
    useRustSyncer && config.numSyncWorkers > 0 ? 1 : config.numSyncWorkers;
  if (config.enableCrudMutations && config.upstream.maxConns < numSyncers) {
    throw new Error(
      `Insufficient upstream connections (${config.upstream.maxConns}) for ${numSyncers} syncers.` +
        `Increase ZERO_UPSTREAM_MAX_CONNS or decrease ZERO_NUM_SYNC_WORKERS (which defaults to available cores).`,
    );
  }
  if (config.cvr.maxConns < numSyncers) {
    throw new Error(
      `Insufficient cvr connections (${config.cvr.maxConns}) for ${numSyncers} syncers.` +
        `Increase ZERO_CVR_MAX_CONNS or decrease ZERO_NUM_SYNC_WORKERS (which defaults to available cores).`,
    );
  }

  const internalFlags: string[] =
    numSyncers === 0
      ? []
      : [
          '--upstream-max-conns-per-worker',
          String(Math.floor(config.upstream.maxConns / numSyncers)),
          '--cvr-max-conns-per-worker',
          String(Math.floor(config.cvr.maxConns / numSyncers)),
        ];

  function loadWorker(
    moduleUrl: URL,
    type: WorkerType,
    id?: string | number,
    ...args: string[]
  ): Worker {
    const worker = childWorker(moduleUrl, env, ...args, ...internalFlags);
    const name = path.basename(moduleUrl.pathname) + (id ? ` (${id})` : '');
    return processes.addWorker(worker, type, name);
  }

  // The HTTP `/notify` port of each spawned rust-syncer, indexed by syncer id,
  // used by the notification fan-out below.
  const rustSyncerHttpPorts: number[] = [];

  // Per-worker CVR connection budget, matching the `--cvr-max-conns-per-worker`
  // flag handed to TS syncers (whole config divided across the syncers).
  const cvrMaxConnsPerWorker =
    numSyncers > 0
      ? Math.floor(config.cvr.maxConns / numSyncers)
      : config.cvr.maxConns;

  // Spawn the rust-syncer binary as a child process instead of a TS worker.
  // The binary communicates readiness via stdout: ["ready", {"ready": true}]
  function loadRustSyncer(id: number, fileMode: ReplicaFileMode): Worker {
    const wsPort = RUST_SYNCER_WS_BASE_PORT + id;
    const httpPort = RUST_SYNCER_HTTP_BASE_PORT + id;
    rustSyncerHttpPorts[id] = httpPort;

    // Binary location: `ZERO_RUST_SYNCER_PATH` if set (dev runs from the cargo
    // target, prod from the packaged dist), else the default next to the
    // compiled syncer module.
    const bin =
      process.env.ZERO_RUST_SYNCER_PATH ??
      path.join(path.dirname(fileURLToPath(SYNCER_URL)), '..', 'rust-syncer');
    // Config is resolved TS-side (single source of truth) and passed via env
    // under the names main.rs reads — replica path (with file-mode applied),
    // cvr db, shard, task id, and auth. Each syncer also gets its own
    // PORT/HTTP_PORT so multiple syncers don't collide.
    const child = spawn(bin, [], {
      detached: process.platform !== 'win32',
      env: {
        ...env,
        ...rustSyncerEnv(
          config,
          fileMode,
          wsPort,
          httpPort,
          cvrMaxConnsPerWorker,
        ),
      },
      stdio: ['inherit', 'pipe', 'inherit'],
    }) as ChildProcess;

    // Wrap ChildProcess into a Worker-compatible interface. `wrap` adds the
    // onMessageType/onceMessageType methods the ProcessManager relies on (e.g.
    // to await the 'ready' message); messages are emitted on `raw` below.
    const raw = new EventEmitter();
    // The dispatcher hands off a client upgrade via `send(handoffMsg, socket)`.
    // Since the binary has no IPC channel, reverse-proxy the socket to its WS
    // port instead of fd-passing. Non-handoff sends (no socket) are dropped.
    const emitter: Worker = Object.assign(wrap(raw), {
      pid: child.pid,
      kill(signal?: NodeJS.Signals): void {
        child.kill(signal);
      },
      send<M extends Message<unknown>>(
        msg: M,
        sendHandle?: SendHandle,
      ): boolean {
        if (sendHandle instanceof Socket) {
          proxyUpgradeToRust(
            lc,
            msg as unknown as UpgradeHandoff,
            sendHandle,
            wsPort,
          );
          return true;
        }
        return false;
      },
    });

    // Listen for ready message on stdout.
    let buffer = '';
    child.stdout?.on('data', (chunk: Buffer) => {
      buffer += chunk.toString();
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';
      for (const line of lines) {
        const trimmed = line.trim();
        // Lines that parse as a JSON array are IPC-style messages (e.g. the
        // ["ready", …] handshake). Everything else is the binary's own logging
        // — forward it to the parent's stdout so rust-syncer stays observable.
        if (trimmed.startsWith('[')) {
          try {
            raw.emit('message', JSON.parse(trimmed));
            continue;
          } catch {
            // Not JSON — fall through and treat as a log line.
          }
        }
        if (line.length > 0) {
          process.stdout.write(`${line}\n`);
        }
      }
    });
    child.on('exit', (code, signal) => {
      raw.emit('close', code, signal);
    });
    child.on('error', err => {
      raw.emit('error', err);
    });

    return processes.addWorker(emitter, 'user-facing', `rust-syncer (${id})`);
  }

  const {
    taskID,
    changeStreamer: {mode: changeStreamerMode, uri: changeStreamerURI},
    litestream,
  } = config;
  const runChangeStreamer =
    changeStreamerMode === 'dedicated' && changeStreamerURI === undefined;

  let changeStreamer: Worker | undefined;

  if (!runChangeStreamer) {
    changeStreamer = undefined;
    if (litestream.executable) {
      // For view-syncers, the backup is restored here. For the replication-manager,
      // the backup is restored in the change-streamer worker.
      await restoreReplica(lc, config, null);
    }
  } else {
    const {promise: changeStreamerReady, resolve: changeStreamerStarted} =
      resolver();
    changeStreamer = loadWorker(CHANGE_STREAMER_URL, 'supporting').once(
      'message',
      changeStreamerStarted,
    );

    // Wait for the change-streamer to be ready to guarantee that a replica
    // file is present.
    await changeStreamerReady;

    if (litestream.backupURL) {
      // Start a backup replicator and corresponding litestream backup process.
      const {promise: backupReady, resolve} = resolver();
      const mode: ReplicaFileMode = 'backup';
      loadWorker(REPLICATOR_URL, 'supporting', mode, mode).once(
        // Wait for the Replicator's first message (i.e. "ready") before starting
        // litestream backup in order to avoid contending on the lock when the
        // replicator first prepares the db file.
        'message',
        () => {
          processes.addSubprocess(
            startReplicaBackupProcess(lc, config),
            'supporting',
            'litestream',
          );
          resolve();
        },
      );
      await backupReady;
    }
  }

  if (numSyncers > 0) {
    const {promise: reaperReady, resolve: reaperStarted} = resolver();
    loadWorker(REAPER_URL, 'supporting').once('message', reaperStarted);
    // Before starting the view-syncers, ensure that the reaper has started
    // up, indicating that any CVR db migrations have been performed.
    await reaperReady;
  }

  // Only run the shadow-sync canary on the replication-manager (or in
  // single-node mode, where it also owns upstream). Running on every
  // view-syncer would hammer the upstream with N redundant canaries.
  if (config.shadowSync.enabled && runChangeStreamer) {
    const {promise: shadowReady, resolve: shadowStarted} = resolver();
    loadWorker(SHADOW_SYNCER_URL, 'supporting').once('message', shadowStarted);
    await shadowReady;
  }

  const syncers: Worker[] = [];
  // The replica-notification subscription that drives the rust-syncer HTTP
  // notify fan-out; cancelled on dispatcher shutdown.
  let rustNotifySubscription: Subscription<ReplicaState> | undefined;
  if (numSyncers) {
    const mode: ReplicaFileMode =
      runChangeStreamer && litestream.backupURL ? 'serving-copy' : 'serving';
    const {promise: replicaReady, resolve} = resolver();
    const replicator = loadWorker(
      REPLICATOR_URL,
      'supporting',
      mode,
      mode,
    ).once('message', () => {
      subscribeTo(lc, replicator);
      resolve();
    });
    await replicaReady;

    const notifier = createNotifierFrom(lc, replicator);
    for (let i = 0; i < numSyncers; i++) {
      if (useRustSyncer) {
        syncers.push(loadRustSyncer(i, mode));
      } else {
        syncers.push(loadWorker(SYNCER_URL, 'user-facing', i, mode, String(i)));
      }
    }
    if (useRustSyncer) {
      // rust-syncer has no IPC channel, so relay replica commit notifications
      // over HTTP: on each `version-ready` from the replicator, POST /notify to
      // every rust-syncer so it advances its hosted CGs and pokes clients. The
      // subscription is cancelled on dispatcher shutdown (below).
      rustNotifySubscription = notifier.subscribe();
      const subscription = rustNotifySubscription;
      void (async () => {
        for await (const _state of subscription) {
          await notifyRustSyncers(lc, rustSyncerHttpPorts);
        }
      })();
    } else {
      syncers.forEach(syncer => handleSubscriptionsFrom(lc, syncer, notifier));
    }
  }
  let mutator: Worker | undefined;
  if (clientConnectionBifurcated) {
    mutator = loadWorker(MUTATOR_URL, 'supporting', 'mutator');
  }

  lc.info?.('waiting for workers to be ready ...');
  const logWaiting = setInterval(
    () => lc.info?.(`still waiting for ${processes.initializing().join(', ')}`),
    10_000,
  );
  await processes.allWorkersReady();
  clearInterval(logWaiting);
  lc.info?.(`all workers ready (${Date.now() - startMs} ms)`);

  parent.send(['ready', {ready: true}]);

  try {
    await runUntilKilled(
      lc,
      parent,
      new WorkerDispatcher(
        lc,
        taskID,
        parent,
        syncers,
        mutator,
        changeStreamer,
      ),
    );
  } catch (err) {
    processes.logErrorAndExit(err, 'dispatcher');
  } finally {
    // Stop the rust-syncer notify fan-out so its subscription doesn't linger.
    rustNotifySubscription?.cancel();
  }

  await processes.done();
}

if (!singleProcessMode()) {
  void exitAfter(lc, () => runWorker(must(parentWorker), process.env));
}
