import path from 'node:path';
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
import {
  childWorker,
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {
  createNotifierFrom,
  handleSubscriptionsFrom,
  type ReplicaFileMode,
  subscribeTo,
} from '../workers/replicator.ts';
import {createLogContext} from './logging.ts';
import {startOtelAuto} from './otel-start.ts';
import {WorkerDispatcher} from './worker-dispatcher.ts';
import {
  CHANGE_STREAMER_URL,
  MUTATOR_URL,
  REAPER_URL,
  REPLICATOR_URL,
  SHADOW_SYNCER_URL,
  SYNCER_URL,
} from './worker-urls.ts';
import {spawn, type ChildProcess} from 'node:child_process';
import {EventEmitter} from 'node:events';
import {fileURLToPath} from 'node:url';

const clientConnectionBifurcated = false;

const useRustSyncer = process.env.ZERO_SYNCER === 'rust';

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

  const {numSyncWorkers: numSyncers} = config;
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

  // Spawn the rust-syncer binary as a child process instead of a TS worker.
  // The binary communicates readiness via stdout: ["ready", {"ready": true}]
  function loadRustSyncer(id: number, ...args: string[]): Worker {
    const bin = path.join(
      path.dirname(fileURLToPath(SYNCER_URL)),
      '..',
      'rust-syncer',
    );
    const child = spawn(bin, [...args, ...internalFlags], {
      detached: process.platform !== 'win32',
      env,
      stdio: ['inherit', 'pipe', 'inherit'],
    }) as ChildProcess;

    // Wrap ChildProcess into a Worker-compatible interface.
    const emitter = new EventEmitter() as Worker;
    emitter.kill = (signal?: string) => child.kill(signal as any);
    (emitter as any).pid = child.pid;
    (emitter as any).send = (_msg: unknown) => false; // no IPC to binary

    // Listen for ready message on stdout.
    let buffer = '';
    child.stdout?.on('data', (chunk: Buffer) => {
      buffer += chunk.toString();
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';
      for (const line of lines) {
        const trimmed = line.trim();
        if (trimmed.startsWith('[')) {
          try {
            const msg = JSON.parse(trimmed);
            emitter.emit('message', msg);
          } catch {
            // Non-JSON output, ignore
          }
        }
      }
    });
    child.on('exit', (code, signal) => {
      emitter.emit('close', code, signal);
    });
    child.on('error', err => {
      emitter.emit('error', err);
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
        syncers.push(loadRustSyncer(i, mode, String(i)));
      } else {
        syncers.push(loadWorker(SYNCER_URL, 'user-facing', i, mode, String(i)));
      }
    }
    if (!useRustSyncer) {
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
  }

  await processes.done();
}

if (!singleProcessMode()) {
  void exitAfter(lc, () => runWorker(must(parentWorker), process.env));
}
