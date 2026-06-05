import {randomUUID} from 'node:crypto';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {pid} from 'node:process';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import {randInt} from '../../../shared/src/rand.ts';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import * as v from '../../../shared/src/valita.ts';
import {DatabaseStorage} from '../../../zqlite/src/database-storage.ts';
import type {ValidateLegacyJWT} from '../auth/auth.ts';
import {tokenConfigOptions, verifyToken} from '../auth/jwt.ts';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {CustomQueryTransformer} from '../custom-queries/transform-query.ts';
import {warmupConnections} from '../db/warmup.ts';
import {initEventSink} from '../observability/events.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {MutagenService} from '../services/mutagen/mutagen.ts';
import {PusherService} from '../services/mutagen/pusher.ts';
import type {ReplicaState} from '../services/replicator/replicator.ts';
import {
  type ConnectionContextManager,
  ConnectionContextManagerImpl,
} from '../services/view-syncer/connection-context-manager.ts';
import type {DrainCoordinator} from '../services/view-syncer/drain-coordinator.ts';
import {PipelineDriver} from '../services/view-syncer/pipeline-driver.ts';
import {
  isGoSidecarEnabled,
  SidecarManager,
} from '../services/view-syncer/go-sidecar/index.ts';
import {Snapshotter} from '../services/view-syncer/snapshotter.ts';
import {ViewSyncerService} from '../services/view-syncer/view-syncer.ts';
import {ProtocolErrorWithLevel} from '../types/error-with-level.ts';
import {pgClient} from '../types/pg.ts';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {getShardID} from '../types/shards.ts';
import type {Subscription} from '../types/subscription.ts';
import {replicaFileModeSchema, replicaFileName} from '../workers/replicator.ts';
import {Syncer} from '../workers/syncer.ts';
import {startAnonymousTelemetry} from './anonymous-otel-start.ts';
import {InspectorDelegate} from './inspector-delegate.ts';
import {createLogContext} from './logging.ts';
import {startOtelAuto} from './otel-start.ts';
import {isPriorityOpRunning, runPriorityOp} from './priority-op.ts';

function randomID() {
  return randInt(1, Number.MAX_SAFE_INTEGER).toString(36);
}

function getCustomQueryConfig(
  config: Pick<NormalizedZeroConfig, 'query' | 'getQueries'>,
) {
  const queryConfig = config.query?.url ? config.query : config.getQueries;

  if (!queryConfig?.url) {
    return undefined;
  }

  return {
    url: queryConfig.url,
    apiKey: queryConfig.apiKey,
    allowedClientHeaders: queryConfig.allowedClientHeaders,
    forwardCookies: queryConfig.forwardCookies ?? false,
  };
}

export default function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...args: string[]
): Promise<void> {
  assert(args.length >= 2, `expected [fileMode, workerIndex, ...flags]`);
  const fileMode = v.parse(args[0], replicaFileModeSchema);
  const workerIndex = Number(args[1]);
  const config = getNormalizedZeroConfig({env, argv: args.slice(2)});

  startOtelAuto(
    createLogContext(config, 'syncer', workerIndex, false),
    'syncer',
    workerIndex,
  );
  const lc = createLogContext(config, 'syncer', workerIndex);
  initEventSink(lc, config);

  const {cvr, upstream, enableCrudMutations} = config;

  const replicaFile = replicaFileName(config.replica.file, fileMode);
  lc.debug?.(`running view-syncer on ${replicaFile}`);

  const cvrDB = pgClient(lc, cvr.db, `sync-worker-${pid}-cvr`, {
    max: must(cvr.maxConnsPerWorker, 'cvr.maxConnsPerWorker must be set'),
  });

  const upstreamDB = enableCrudMutations
    ? pgClient(lc, upstream.db, `sync-worker-${pid}-upstream`, {
        max: must(
          upstream.maxConnsPerWorker,
          'upstream.maxConnsPerWorker must be set',
        ),
      })
    : undefined;

  const dbWarmup = Promise.allSettled([
    warmupConnections(lc, cvrDB, 'cvr'),
    upstreamDB ? warmupConnections(lc, upstreamDB, 'upstream') : promiseVoid,
  ]);

  const tmpDir = config.storageDBTmpDir ?? tmpdir();
  const operatorStorage = DatabaseStorage.create(
    lc,
    path.join(tmpDir, `sync-worker-${randomUUID()}`),
  );
  const writeAuthzStorage = DatabaseStorage.create(
    lc,
    path.join(tmpDir, `mutagen-${randomUUID()}`),
  );

  const shard = getShardID(config);

  // Go IVM sidecar: one process per client group set. When
  // goSidecar.externallyManaged is true the sidecar is shared across all
  // workers in this zero-cache (typically spawned by the container
  // entrypoint), and this manager just connects.
  let sidecarManager: SidecarManager | undefined;
  if (isGoSidecarEnabled(config)) {
    const binaryPath = config.goSidecar.binaryPath;
    const externallyManaged = config.goSidecar.externallyManaged ?? false;
    const socketPath = config.goSidecar.socketPath;
    // M4: the goSidecar flags are independent booleans whose valid combinations
    // are resolved by precedence at dispatch time (shadow wins; primary flags
    // imply others). Warn on contradictory/incomplete combinations that would
    // otherwise silently degrade, so an operator isn't left wondering why a mode
    // they configured isn't running.
    {
      const sc = config.goSidecar;
      const anyPrimary =
        (sc.advanceToHead ?? false) ||
        (sc.advanceDrive ?? false) ||
        (sc.goPrimaryTrigger ?? false) ||
        (sc.leanPrimary ?? false);
      if ((sc.shadowMode ?? false) && anyPrimary) {
        lc.warn?.(
          'goSidecar: shadowMode and a go-primary flag are both set; shadow ' +
            'mode wins and the primary flags are ignored this run.',
        );
      }
      if ((sc.leanPrimary ?? false) && !(sc.goPrimaryTrigger ?? false)) {
        lc.warn?.(
          'goSidecar.leanPrimary=true without goSidecar.goPrimaryTrigger=true ' +
            'has no effect (lean only applies to the trigger-primary path).',
        );
      }
    }
    if (externallyManaged && !socketPath) {
      lc.error?.(
        'goSidecar.externallyManaged=true requires goSidecar.socketPath to be set; falling back to TS',
      );
    } else {
      const sidecarLc = lc.withContext('component', 'go-ivm');
      // O1: derive the sidecar's GO_IVM_* mode env from the SAME goSidecar
      // config that drives the TS dispatch, so a spawned sidecar is armed
      // exactly as the worker expects (advanceToHead / advanceDrive both require
      // the sidecar's snapshotter + table source mode). Without this the two
      // processes are configured independently and a mismatch silently drops
      // every user delta. (Ignored for externallyManaged — the owner sets env on
      // the shared process; a startup mode handshake there is a follow-up.)
      const sc = config.goSidecar;
      const wantsAdvanceToHead =
        (sc.advanceToHead ?? false) ||
        (sc.advanceDrive ?? false) ||
        (sc.goPrimaryTrigger ?? false);
      const spawnEnv: Record<string, string> = {GO_IVM_APP_ID: shard.appID};
      if (wantsAdvanceToHead) {
        spawnEnv.GO_IVM_ADVANCE_TO_HEAD = 'true';
        spawnEnv.GO_IVM_SOURCE_MODE = 'table';
      }
      if (sc.advanceDrive ?? false) {
        spawnEnv.GO_IVM_ADVANCE_DRIVE = 'true';
      }
      sidecarManager = new SidecarManager({
        binaryPath,
        ...(socketPath ? {socketPath} : {}),
        externallyManaged,
        spawnEnv,
        logger: (level, msg, err) => {
          // Route sidecar stdout/stderr + manager events through LogContext
          // instead of raw process.std{out,err}.write so structured logging
          // stays structured. REVIEW-ts-integration LOW-2.
          if (level === 'error') sidecarLc.error?.(msg, err ?? '');
          else if (level === 'warn') sidecarLc.warn?.(msg, err ?? '');
          else sidecarLc.info?.(msg);
        },
      });
      void sidecarManager.start().then(
        () =>
          lc.info?.(
            externallyManaged
              ? `Connected to shared Go IVM sidecar at ${socketPath}`
              : 'Go IVM sidecar started',
          ),
        err => {
          lc.error?.('Failed to start Go IVM sidecar, falling back to TS', err);
          sidecarManager = undefined;
        },
      );
    }
  }

  const customQueryConfig = getCustomQueryConfig(config);
  const pushConfig =
    config.push.url === undefined && config.mutate.url === undefined
      ? undefined
      : {
          ...config.push,
          ...config.mutate,
          url: must(
            config.push.url ?? config.mutate.url,
            'No push or mutate URL configured',
          ),
        };

  /** @deprecated used in JWT validation */
  let validateLegacyJWT: ValidateLegacyJWT | undefined = undefined;

  const tokenOptions = tokenConfigOptions(config.auth ?? {});
  if (tokenOptions.length === 1) {
    validateLegacyJWT = async (token, {userID}) => {
      if (!userID) {
        throw new ProtocolErrorWithLevel(
          {
            kind: 'Unauthorized',
            message: 'UserID is required for JWT validation.',
            origin: 'zeroCache',
          },
          'warn',
        );
      }

      const decoded = await verifyToken(config.auth, token, {
        subject: userID,
        ...(config.auth?.issuer && {issuer: config.auth.issuer}),
        ...(config.auth?.audience && {
          audience: config.auth.audience,
        }),
      });
      return {
        type: 'jwt',
        raw: token,
        decoded,
      };
    };
  }

  const viewSyncerFactory = (
    id: string,
    sub: Subscription<ReplicaState>,
    drainCoordinator: DrainCoordinator,
  ) => {
    const logger = lc
      .withContext('component', 'view-syncer')
      .withContext('clientGroupID', id)
      .withContext('instance', randomID());

    const customQueryTransformer =
      customQueryConfig && new CustomQueryTransformer(logger, shard);
    const contextManager = new ConnectionContextManagerImpl(
      logger,
      config.auth.revalidateIntervalSeconds,
      config.auth.retransformIntervalSeconds,
      customQueryConfig,
      pushConfig,
      validateLegacyJWT,
    );

    lc.debug?.(
      `creating view syncer. Query Planner Enabled: ${config.enableQueryPlanner}`,
    );

    const inspectorDelegate = new InspectorDelegate(customQueryTransformer);

    const priorityOpRunningYieldThresholdMs = Math.max(
      config.yieldThresholdMs / 4,
      2,
    );
    const normalYieldThresholdMs = Math.max(config.yieldThresholdMs, 2);

    return new ViewSyncerService(
      config,
      logger,
      shard,
      config.taskID,
      id,
      cvrDB,
      new PipelineDriver(
        logger,
        config.log,
        new Snapshotter(logger, replicaFile, shard),
        shard,
        operatorStorage.createClientGroupStorage(id),
        id,
        inspectorDelegate,
        () =>
          isPriorityOpRunning()
            ? priorityOpRunningYieldThresholdMs
            : normalYieldThresholdMs,
        config.enableQueryPlanner,
        config,
        sidecarManager,
      ),
      sub,
      drainCoordinator,
      config.log.slowHydrateThreshold,
      inspectorDelegate,
      contextManager,
      customQueryTransformer,
      runPriorityOp,
    );
  };

  const mutagenFactory = upstreamDB
    ? (id: string) =>
        new MutagenService(
          lc
            .withContext('component', 'mutagen')
            .withContext('clientGroupID', id),
          shard,
          id,
          upstreamDB,
          config,
          writeAuthzStorage,
        )
    : undefined;

  const pusherFactory =
    pushConfig === undefined
      ? undefined
      : (id: string, contextManager: ConnectionContextManager) =>
          new PusherService(
            config,
            lc.withContext('clientGroupID', id),
            id,
            contextManager,
          );

  const syncer = new Syncer(
    lc,
    config,
    viewSyncerFactory,
    mutagenFactory,
    pusherFactory,
    parent,
    validateLegacyJWT,
  );

  startAnonymousTelemetry(lc, config);

  void dbWarmup.then(() => parent.send(['ready', {ready: true}]));

  // Stop sidecar on process exit
  if (sidecarManager) {
    process.on('beforeExit', () => {
      void sidecarManager?.stop();
    });
  }

  return runUntilKilled(lc, parent, syncer);
}

// fork()
if (!singleProcessMode()) {
  void exitAfter(() =>
    runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
