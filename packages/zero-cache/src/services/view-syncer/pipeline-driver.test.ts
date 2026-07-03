import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import type {Database as DB} from '../../../../zqlite/src/db.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {listTables} from '../../db/lite-tables.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import type {RowKey} from '../../types/row-key.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import {populateFromExistingTables} from '../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  ReplicationMessages,
  type FakeReplicator,
} from '../replicator/test-utils.ts';
import {getMutationResultsQuery} from './cvr.ts';
import type * as GoComputeBackendModule from './go-sidecar/go-compute-backend.ts';
import {PipelineDriver, type AdvanceResult, type RowChange, type ShadowHydrateResult, type Timer} from './pipeline-driver.ts';
import {rowIDSignatureUnit} from './row-set-signature.ts';
import type {RowID} from './schema/types.ts';
import {ResetPipelinesSignal, Snapshotter} from './snapshotter.ts';
import {TimeSliceTimer} from './view-syncer.ts';

// Seam for injecting a fake GoComputeBackend without a live sidecar.
// `PipelineDriver` builds its (truly private) #goBackend inside the
// constructor via createGoComputeBackend(); the only way to substitute a
// fake is to mock that factory. We preserve every other export (the
// isGo*/goDriftAudit* config helpers the constructor depends on) and
// override just the factory, which returns whatever the active test parks
// on `goBackendMock.backend`. Existing tests pass no config/sidecarManager,
// so the constructor short-circuits before the factory is ever called —
// this mock is inert for them (backend stays null).
const goBackendMock = vi.hoisted(() => ({backend: null as unknown}));

vi.mock('./go-sidecar/go-compute-backend.ts', async importOriginal => {
  const actual = await importOriginal<typeof GoComputeBackendModule>();
  return {
    ...actual,
    createGoComputeBackend: (() =>
      goBackendMock.backend) as typeof actual.createGoComputeBackend,
  };
});

const NO_TIME_ADVANCEMENT_TIMER: Timer = {
  elapsedLap: () => 0,
  totalElapsed: () => 0,
  running: () => true,
};

describe('view-syncer/pipeline-driver', () => {
  const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
  const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
  let dbFile: DbFile;
  let db: DB;
  let lc: LogContext;
  let logSink: TestLogSink;
  let pipelines: PipelineDriver;
  let replicator: FakeReplicator;

  beforeEach(() => {
    logSink = new TestLogSink();
    lc = new LogContext('error', undefined, logSink);
    dbFile = new DbFile('pipelines_test');
    dbFile.connect(lc).pragma('journal_mode = wal2');

    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();

    pipelines = new PipelineDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
      shardID,
      new DatabaseStorage(storage).createClientGroupStorage('foo-client-group'),
      'pipeline-driver.test.ts',
      new InspectorDelegate(undefined),
      () => 200 /** yield threshold */,
    );

    db = dbFile.connect(lc);
    initReplicationState(db, ['zero_data'], '123');
    db.exec(/*sql*/ `
      CREATE TABLE "${mutationsTableName}" (
        "clientGroupID"  TEXT,
        "clientID"       TEXT,
        "mutationID"     INTEGER,
        "result"         TEXT,
        _0_version       TEXT NOT NULL,
        PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
      );
      CREATE TABLE issues (
        id TEXT PRIMARY KEY,
        closed BOOL,
        ignored BYTEA,
        _0_version TEXT NOT NULL
      );
      CREATE TABLE comments (
        id TEXT PRIMARY KEY, 
        issueID TEXT,
        upvotes INTEGER,
        ignored BYTEA,
        stillBeingBackfilled TEXT,
         _0_version TEXT NOT NULL);
      CREATE TABLE "issueLabels" (
        issueID TEXT,
        labelID TEXT,
        legacyID "TEXT|NOT_NULL",
        _0_version TEXT NOT NULL,
        PRIMARY KEY (issueID, labelID)
      );
      CREATE UNIQUE INDEX issues_a ON issueLabels (legacyID);  -- Test that this doesn't trip up IVM.
      CREATE TABLE "labels" (
        id TEXT PRIMARY KEY,
        name TEXT,
        _0_version TEXT NOT NULL
      );

      INSERT INTO ISSUES (id, closed, ignored, _0_version) VALUES ('1', 0, 1728345600000, '123');
      INSERT INTO ISSUES (id, closed, ignored, _0_version) VALUES ('2', 1, 1722902400000, '123');
      INSERT INTO ISSUES (id, closed, ignored, _0_version) VALUES ('3', 0, null, '123');
      INSERT INTO COMMENTS (id, issueID, upvotes, _0_version) VALUES ('10', '1', 0, '123');
      INSERT INTO COMMENTS (id, issueID, upvotes, _0_version) VALUES ('20', '2', 1, '123');
      INSERT INTO COMMENTS (id, issueID, upvotes, _0_version) VALUES ('21', '2', 10000, '123');
      INSERT INTO COMMENTS (id, issueID, upvotes, _0_version) VALUES ('22', '2', 20000, '123');

      INSERT INTO "issueLabels" (issueID, labelID, legacyID, _0_version) VALUES ('1', '1', '1-1', '123');
      INSERT INTO "labels" (id, name, _0_version) VALUES ('1', 'bug', '123');

      CREATE TABLE uniques (
        id "TEXT|NOT_NULL",
        name "TEXT|NOT_NULL",
        _0_version TEXT NOT NULL
      );
      CREATE UNIQUE INDEX uniques_id ON uniques (id);
      CREATE UNIQUE INDEX uniques_name ON uniques (name);

      INSERT INTO "uniques" (id, name, _0_version) VALUES ('foo', 'bar', '123');
      INSERT INTO "uniques" (id, name, _0_version) VALUES ('boo', 'dar', '123');

      CREATE TABLE backfilling (id TEXT PRIMARY KEY, _0_version TEXT NOT NULL);
      `);

    // Initialize ColumnMetadata and mark columns/tables as being backfilled,
    // to verify that it does not appear in the pipeline results.
    populateFromExistingTables(db, listTables(db, false));
    db.exec(/*sql*/ `
      UPDATE "_zero.column_metadata" 
        SET backfill = '{"upstreamID":123}'
        WHERE table_name = 'comments' 
         AND column_name = 'stillBeingBackfilled';
      UPDATE "_zero.column_metadata" 
        SET backfill = '{"upstreamID":456}'
        WHERE table_name = 'backfilling' ;
      `);
    replicator = fakeReplicator(lc, db);
  });

  afterEach(() => {
    dbFile.delete();
  });

  const issues = table('issues')
    .columns({
      id: string(),
      closed: boolean(),
    })
    .primaryKey('id');
  const comments = table('comments')
    .columns({
      id: string(),
      issueID: string(),
      upvotes: number(),
    })
    .primaryKey('id');
  const issueLabels = table('issueLabels')
    .columns({
      issueID: string(),
      labelID: string(),
      legacyID: string(),
    })
    .primaryKey('issueID', 'labelID');
  const labels = table('labels')
    .columns({
      id: string(),
      name: string(),
    })
    .primaryKey('id');
  const uniques = table('uniques')
    .columns({
      id: string(),
      name: string(),
    })
    .primaryKey('id');

  const clientSchema = createSchema({
    tables: [issues, comments, issueLabels, labels, uniques],
  });

  const subsetClientSchema = createSchema({
    tables: [issues],
  });

  const ISSUES_AND_COMMENTS: AST = {
    table: 'issues',
    orderBy: [['id', 'desc']],
    related: [
      {
        system: 'client',
        correlation: {
          parentField: ['id'],
          childField: ['issueID'],
        },
        subquery: {
          table: 'comments',
          alias: 'comments',
          orderBy: [['id', 'desc']],
        },
      },
    ],
  };

  const ISSUES_QUERY_WITH_EXISTS: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        system: 'client',
        correlation: {
          parentField: ['id'],
          childField: ['issueID'],
        },
        subquery: {
          table: 'issueLabels',
          alias: 'labels',
          orderBy: [
            ['issueID', 'asc'],
            ['labelID', 'asc'],
          ],
          where: {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            related: {
              system: 'client',
              correlation: {
                parentField: ['labelID'],
                childField: ['id'],
              },
              subquery: {
                table: 'labels',
                alias: 'labels',
                orderBy: [['id', 'asc']],
                where: {
                  type: 'simple',
                  left: {
                    type: 'column',
                    name: 'name',
                  },
                  op: '=',
                  right: {
                    type: 'literal',
                    value: 'bug',
                  },
                },
              },
            },
          },
        },
      },
    },
  };

  const ISSUES_QUERY_WITH_EXISTS_FROM_PERMISSIONS: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        system: 'permissions',
        correlation: {
          parentField: ['id'],
          childField: ['issueID'],
        },
        subquery: {
          table: 'issueLabels',
          alias: 'labels',
          orderBy: [
            ['issueID', 'asc'],
            ['labelID', 'asc'],
          ],
          where: {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            related: {
              system: 'permissions',
              correlation: {
                parentField: ['labelID'],
                childField: ['id'],
              },
              subquery: {
                table: 'labels',
                alias: 'labels',
                orderBy: [['id', 'asc']],
                where: {
                  type: 'simple',
                  left: {
                    type: 'column',
                    name: 'name',
                  },
                  op: '=',
                  right: {
                    type: 'literal',
                    value: 'bug',
                  },
                },
              },
            },
          },
        },
      },
    },
  };

  const ISSUES_QUERY_WITH_EXISTS_FROM_PERMISSIONS2: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        system: 'client',
        correlation: {
          parentField: ['id'],
          childField: ['issueID'],
        },
        subquery: {
          table: 'issueLabels',
          alias: 'labels',
          orderBy: [
            ['issueID', 'asc'],
            ['labelID', 'asc'],
          ],
          where: {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            related: {
              system: 'permissions',
              correlation: {
                parentField: ['labelID'],
                childField: ['id'],
              },
              subquery: {
                table: 'labels',
                alias: 'labels',
                orderBy: [['id', 'asc']],
                where: {
                  type: 'simple',
                  left: {
                    type: 'column',
                    name: 'name',
                  },
                  op: '=',
                  right: {
                    type: 'literal',
                    value: 'bug',
                  },
                },
              },
            },
          },
        },
      },
    },
  };

  const UNIQUES_QUERY: AST = {
    table: 'uniques',
    orderBy: [['id', 'desc']],
  };

  const ISSUES_WITH_SCALAR_SUBQUERY: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      scalar: true,
      related: {
        correlation: {
          parentField: ['id'],
          childField: ['issueID'],
        },
        subquery: {
          table: 'comments',
          orderBy: [['id', 'asc']],
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'id'},
            right: {type: 'literal', value: '10'},
          },
        },
      },
    },
  };

  const ISSUES_WITH_NONEXISTENT_SCALAR_SUBQUERY: AST = {
    table: 'issues',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      scalar: true,
      related: {
        correlation: {
          parentField: ['id'],
          childField: ['issueID'],
        },
        subquery: {
          table: 'comments',
          orderBy: [['id', 'asc']],
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'id'},
            right: {type: 'literal', value: 'nonexistent'},
          },
        },
      },
    },
  };

  const messages = new ReplicationMessages({
    issues: 'id',
    comments: 'id',
    issueLabels: ['issueID', 'labelID'],
    uniques: 'id',
    backfilling: 'id',
    [mutationsTableName]: ['clientGroupID', 'clientID', 'mutationID'],
  });

  function startTimer() {
    return new TimeSliceTimer(lc).startWithoutYielding();
  }

  // Helper: in tests Go is never active, so addQuery always returns sync Iterable.
  function addQuery(...args: Parameters<typeof pipelines.addQuery>): Iterable<RowChange | 'yield'> {
    return pipelines.addQuery(...args) as Iterable<RowChange | 'yield'>;
  }

  // Regression gate for the Go-primary batch-hydrate backpressure bound
  // (pipeline-driver.ts goHydrateBatchStream, GO_HYDRATE_SUB_BATCH=8).
  //
  // The socket delivers Go's per-query results with no backpressure while the
  // consumer drains them one query at a time into (slow) CVR flushes. A single
  // hydrateManyStream over the whole query set would buffer the CG's ENTIRE
  // hydrate result set in the JS heap — a reconnect-storm OOM at scale. The fix
  // sub-batches the RPC: at most GO_HYDRATE_SUB_BATCH queries are requested at
  // once, and the next sub-batch isn't issued until the previous one's results
  // have been fully drained. This proves that bound (previously only "trusted
  // the soak" with zero automated coverage).
  test('goHydrateBatchStream bounds in-flight queries to GO_HYDRATE_SUB_BATCH and drains each sub-batch before the next RPC', async () => {
    type GoQuery = {queryID: string; ast: AST};
    type RpcResult = {
      queryID: string;
      changes: unknown[];
      timingMs: number | undefined;
    };
    type RpcCall = {
      size: number;
      queryIDs: string[];
      // How many results the consumer had drained when this RPC was issued.
      consumedAtStart: number;
      // How many RPCs were still unsettled when this one was issued.
      inFlightAtStart: number;
    };

    const calls: RpcCall[] = [];
    const seen: string[] = [];
    let drainedChanges = 0;
    let inFlight = 0;

    const fakeBackend = {
      sidecarSourceMode: 'table' as const,
      initialized: true,
      initEngine: () => Promise.resolve(),
      resetEngine: () => Promise.resolve(),
      removeQuery: () => Promise.resolve(),
      destroy: () => Promise.resolve(),
      hydrateManyStream(
        qs: GoQuery[],
        onResult: (r: RpcResult) => void,
      ): Promise<void> {
        calls.push({
          size: qs.length,
          queryIDs: qs.map(q => q.queryID),
          consumedAtStart: seen.length,
          inFlightAtStart: inFlight,
        });
        inFlight++;
        return (async () => {
          for (const q of qs) {
            // Force a real async boundary between results so the drain loop
            // parks on its wake promise — exercises the backpressure path
            // rather than a synchronous fast-track.
            await Promise.resolve();
            onResult({queryID: q.queryID, changes: [], timingMs: 1});
          }
        })().finally(() => {
          inFlight--;
        });
      },
    };
    goBackendMock.backend = fakeBackend;

    const goStorageDb = new Database(lc, ':memory:');
    goStorageDb.prepare(CREATE_STORAGE_TABLE).run();

    const goPrimary = new PipelineDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
      shardID,
      new DatabaseStorage(goStorageDb).createClientGroupStorage(
        'go-primary-client-group',
      ),
      'pipeline-driver.test.ts',
      new InspectorDelegate(undefined),
      () => 200,
      // Planner OFF: keeps #planAstForGo on the ordering-only path (no
      // cost-model DB needed) while still exercising the real dispatch.
      false,
      // Go-primary, non-shadow: enables the batch-hydrate path.
      {
        goSidecar: {enabled: true, goPrimaryTrigger: true},
      } as unknown as ConstructorParameters<typeof PipelineDriver>[9],
      // Truthy sidecarManager so the constructor reaches createGoComputeBackend
      // (mocked above to return our fake); its contents are unused.
      {} as unknown as ConstructorParameters<typeof PipelineDriver>[10],
    );

    try {
      goPrimary.init(clientSchema);

      // 20 user queries → windows of [8, 8, 4].
      const queries = Array.from({length: 20}, (_, i) => ({
        transformationHash: `hash${i}`,
        queryID: `q${i}`,
        ast: UNIQUES_QUERY,
      }));

      for await (const entry of goPrimary.goHydrateBatchStream(queries)) {
        // Drain the per-query change generator like the real view-syncer does.
        for (const c of entry.changes) {
          if (c !== 'yield') drainedChanges++;
        }
        seen.push(entry.queryID);
        // Contract: the envelope surfaces Go's per-query engine-compute
        // timingMs (undefined only for internal queries run through TS).
        // These are all user queries (UNIQUES_QUERY), so the fake backend's
        // timingMs: 1 must be forwarded verbatim — the view-syncer records
        // this into hydration_time as the apples-to-apples engine span.
        expect(entry.timingMs).toBe(1);
      }

      // Every query hydrated exactly once (empty result sets → no changes).
      expect(seen.length).toBe(20);
      expect(new Set(seen)).toEqual(new Set(queries.map(q => q.queryID)));
      expect(drainedChanges).toBe(0);

      // Sub-batched into windows of 8: [8, 8, 4]. No RPC ever carries more
      // than GO_HYDRATE_SUB_BATCH queries.
      expect(calls.map(c => c.size)).toEqual([8, 8, 4]);

      // Bound #1: at most one Go hydrate RPC in flight per CG — the prior RPC
      // is fully settled before the next is issued.
      expect(calls.map(c => c.inFlightAtStart)).toEqual([0, 0, 0]);

      // Bound #2: the prior sub-batch is FULLY drained into the consumer
      // before the next RPC is requested (k*8 results consumed at window k),
      // so at most one sub-batch's results sit buffered in the heap at once.
      expect(calls.map(c => c.consumedAtStart)).toEqual([0, 8, 16]);

      // Windows partition the input in order.
      expect(calls[0].queryIDs).toEqual(
        queries.slice(0, 8).map(q => q.queryID),
      );
      expect(calls[1].queryIDs).toEqual(
        queries.slice(8, 16).map(q => q.queryID),
      );
      expect(calls[2].queryIDs).toEqual(
        queries.slice(16, 20).map(q => q.queryID),
      );
    } finally {
      goBackendMock.backend = null;
      await goPrimary.destroy();
    }
  });

  // H2-heal end-to-end wiring (pipeline-driver.ts #healConfirmedDrift):
  //
  // The detached drift-audit timer cannot return a ResetPipelinesSignal itself,
  // so a confirmed Go-primary drift is a TWO-PART heal:
  //   1. #scheduleGoReset → goBackend.resetEngine() rebuilds Go's pipelines
  //      IMMEDIATELY so drift stops compounding into subsequent advances.
  //   2. #pendingClientResetReason is parked; the NEXT advance() returns a
  //      ResetPipelinesSignal tagged 'drift-audit-heal' which drives the
  //      view-syncer's F2 reset path (pipelines.reset → hydrateUnchangedQueries
  //      → CVR poke) to correct rows ALREADY delivered to clients.
  //
  // The heal gate fires ONLY when Go disagrees with BOTH ground truths: the
  // TS-IVM pipeline (setDiffers) AND the SQL oracle (go-vs-sql-drift). This
  // test makes Go return an EMPTY audit hydrate while the real SQLite replica
  // (issues 1/2/3) backs both the TS-IVM audit and the SQL oracle, so the
  // confirmed-set-drift branch fires. Previously this path had ZERO automated
  // coverage — only ever exercised by the live soak.
  test('drift-audit confirmed Go-vs-SQL drift triggers engine reset + drift-audit-heal signal', async () => {
    // Fake timers must be installed BEFORE the driver is constructed so the
    // audit's setInterval (pipeline-driver.ts #runDriftAudit) is captured.
    vi.useFakeTimers();

    const DRIFT_AUDIT_INTERVAL_MS = 8000;
    const ISSUES_ONLY: AST = {table: 'issues', orderBy: [['id', 'asc']]};

    const resetEngine = vi.fn(() => Promise.resolve());
    // Go hydrate returns EMPTY for the audit's transient query → Go's row set
    // differs from the TS-IVM pipeline AND the SQL oracle (both see 3 issues).
    const fakeBackend = {
      sidecarSourceMode: 'table' as const,
      initialized: true,
      epoch: 0,
      initEngine: () => Promise.resolve(),
      resetEngine,
      removeQuery: () => Promise.resolve(),
      destroy: () => Promise.resolve(),
      whenRecovered: () => Promise.resolve(),
      refreshSnapshot: () => Promise.resolve(),
      // >= tsExpected so the pipeline-count FREEZE path (which would reset for a
      // different reason and never park the client-reset flag) is NOT taken.
      pipelineCount: () => Promise.resolve(1),
      // Go-primary addQuery routes through #goHydrate, which parks a stub
      // pipeline whose transformedAst the audit re-hydrates against SQLite.
      hydrate: () => Promise.resolve({changes: [], timingMs: 0}),
      hydrateManyStream(
        qs: {queryID: string; ast: AST}[],
        onResult: (r: {
          queryID: string;
          changes: unknown[];
          timingMs: number;
        }) => void,
      ): Promise<void> {
        for (const q of qs) {
          onResult({queryID: q.queryID, changes: [], timingMs: 1});
        }
        return Promise.resolve();
      },
    };
    goBackendMock.backend = fakeBackend;

    const goStorageDb = new Database(lc, ':memory:');
    goStorageDb.prepare(CREATE_STORAGE_TABLE).run();

    const goPrimary = new PipelineDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
      shardID,
      new DatabaseStorage(goStorageDb).createClientGroupStorage(
        'heal-client-group',
      ),
      'pipeline-driver.test.ts',
      new InspectorDelegate(undefined),
      () => 200,
      // Planner OFF: #planAstForGo stays on the ordering-only path (no
      // cost-model DB needed).
      false,
      // Go-primary, non-shadow, audit enabled (driftAuditIntervalMs > 0). The
      // shadow gate in #healConfirmedDrift is OPEN only in this mode.
      {
        goSidecar: {
          enabled: true,
          goPrimaryTrigger: true,
          driftAuditIntervalMs: DRIFT_AUDIT_INTERVAL_MS,
        },
      } as unknown as ConstructorParameters<typeof PipelineDriver>[9],
      {} as unknown as ConstructorParameters<typeof PipelineDriver>[10],
    );

    try {
      goPrimary.init(clientSchema);

      // Register one user query so the round-robin audit has a target. In
      // Go-primary mode addQuery is async (gated on whenRecovered).
      const stream = await goPrimary.addQuery(
        'hash-issues',
        'q-issues',
        ISSUES_ONLY,
        NO_TIME_ADVANCEMENT_TIMER,
      );
      for (const _ of stream) {
        // drain
      }

      // Fire the detached audit once. Flushes the audit's async chain
      // (pipelineCount → refreshSnapshot → hydrateManyStream → compare).
      await vi.advanceTimersByTimeAsync(DRIFT_AUDIT_INTERVAL_MS);

      // Part 1: engine rebuilt immediately so drift stops compounding.
      expect(resetEngine).toHaveBeenCalledTimes(1);

      // Part 2: the next advance() returns the client re-hydrate signal,
      // tagged 'drift-audit-heal'.
      const result = goPrimary.advance(NO_TIME_ADVANCEMENT_TIMER);
      expect(result).toBeInstanceOf(ResetPipelinesSignal);
      expect((result as ResetPipelinesSignal).reason).toBe('drift-audit-heal');
    } finally {
      goBackendMock.backend = null;
      vi.useRealTimers();
      await goPrimary.destroy();
    }
  });

  // #4b: content-aware incremental reconcile. The accumulator stores PK→row
  // CONTENT (not just PK membership), so a same-PK content drift that persists
  // across audit cycles with stable membership (no ADD/REMOVE touching the PK)
  // is caught — the window the PK-set-only accumulator missed (#shadowCompare
  // catches content drift per-batch but only WITHIN one advance). This test
  // drives two audit cycles with a MUTABLE fake hydrate that returns issue 1
  // closed=false on cycle 1 (seeds the accumulator with `false`) then closed=true
  // on cycle 2 (the "fresh hydrate" disagrees with the accumulated `false` on the
  // SAME PK, stable membership) → the reconcile logs
  // `[drift-audit][incremental-content]`.
  test('drift-audit incremental-content: same-PK content drift across cycles logs [drift-audit][incremental-content]', async () => {
    vi.useFakeTimers();
    const DRIFT_AUDIT_INTERVAL_MS = 8000;
    const ISSUES_ONLY: AST = {table: 'issues', orderBy: [['id', 'asc']]};

    const sink = new TestLogSink();
    const auditLc = new LogContext('info', undefined, sink);

    // Mutable hydrate: returns all 3 issues matching the SQLite replica on
    // cycle 1 (so the SQL oracle confirms and does NOT heal/taint the
    // accumulator), then flips issue 1's `closed` false→true on cycle 2 — a
    // content drift on a stable-PK row between audit cycles. Membership stays
    // {1,2,3} on both cycles, so only the content reconcile fires.
    let auditCallCount = 0;
    const fakeBackend = {
      sidecarSourceMode: 'table' as const,
      initialized: true,
      epoch: 0,
      initEngine: () => Promise.resolve(),
      resetEngine: () => Promise.resolve(),
      removeQuery: () => Promise.resolve(),
      destroy: () => Promise.resolve(),
      whenRecovered: () => Promise.resolve(),
      refreshSnapshot: () => Promise.resolve(),
      pipelineCount: () => Promise.resolve(1),
      hydrate: () => Promise.resolve({changes: [], timingMs: 0}),
      hydrateManyStream(
        qs: {queryID: string; ast: AST}[],
        onResult: (r: {queryID: string; changes: unknown[]; timingMs: number}) => void,
      ): Promise<void> {
        auditCallCount++;
        // Replica: issue 1 closed=false, 2=true, 3=false. Cycle 1 matches the
        // replica exactly (SQL oracle confirms → no heal → no taint). Cycle 2
        // flips issue 1 to true — same PK set, differing content on issue 1.
        const issue1Closed = auditCallCount === 1 ? false : true;
        const rowChanges = [
          {type: 0, queryID: qs[0]!.queryID, table: 'issues', rowKey: {id: '1'}, row: {id: '1', closed: issue1Closed, _0_version: '123'}},
          {type: 0, queryID: qs[0]!.queryID, table: 'issues', rowKey: {id: '2'}, row: {id: '2', closed: true, _0_version: '123'}},
          {type: 0, queryID: qs[0]!.queryID, table: 'issues', rowKey: {id: '3'}, row: {id: '3', closed: false, _0_version: '123'}},
        ];
        for (const q of qs) {
          onResult({queryID: q.queryID, changes: rowChanges, timingMs: 1});
        }
        return Promise.resolve();
      },
    };
    goBackendMock.backend = fakeBackend;

    const goStorageDb = new Database(lc, ':memory:');
    goStorageDb.prepare(CREATE_STORAGE_TABLE).run();

    const goPrimary = new PipelineDriver(
      auditLc,
      testLogConfig,
      new Snapshotter(auditLc, dbFile.path, {appID: shardID.appID}),
      shardID,
      new DatabaseStorage(goStorageDb).createClientGroupStorage(
        'incremental-content-cg',
      ),
      'pipeline-driver.test.ts',
      new InspectorDelegate(undefined),
      () => 200,
      false,
      {
        goSidecar: {
          enabled: true,
          goPrimaryTrigger: true,
          driftAuditIntervalMs: DRIFT_AUDIT_INTERVAL_MS,
        },
      } as unknown as ConstructorParameters<typeof PipelineDriver>[9],
      {} as unknown as ConstructorParameters<typeof PipelineDriver>[10],
    );

    try {
      goPrimary.init(clientSchema);

      const stream = await goPrimary.addQuery(
        'hash-issues-content',
        'q-issues-content',
        ISSUES_ONLY,
        NO_TIME_ADVANCEMENT_TIMER,
      );
      for (const _ of stream) {
        // drain
      }

      // Cycle 1: seeds the accumulator with issues {1:false, 2:true, 3:false}
      // (matching the replica, so the SQL oracle confirms and does NOT taint
      // the accumulator via a heal). No incremental-content mismatch yet (the
      // accumulator was empty before this seed).
      await vi.advanceTimersByTimeAsync(DRIFT_AUDIT_INTERVAL_MS);

      // Cycle 2: hydrate returns issue 1 closed=true; the accumulator still
      // holds closed=false (no advance between cycles). Same PK set {1,2,3},
      // differing content on issue 1 → [drift-audit][incremental-content].
      await vi.advanceTimersByTimeAsync(DRIFT_AUDIT_INTERVAL_MS);

      const logText = sink.messages.map(m => m[2].join(' ')).join('\n');
      expect(logText).toContain('[drift-audit][incremental-content]');
      expect(logText).toContain('q-issues-content');
      // Membership is stable (issue 1 present on both cycles) → the
      // membership-only incremental log must NOT fire. Distinguish it from the
      // content log by its unique tail ("accumulation diverged from a fresh
      // hydrate" vs the content log's "accumulated content diverged").
      expect(logText).not.toContain('accumulation diverged from a fresh hydrate');
    } finally {
      goBackendMock.backend = null;
      vi.useRealTimers();
      await goPrimary.destroy();
    }
  });

  // Oracle-blind divergence: when BOTH Go and TS match the SQL oracle on the
  // main table but differ in related-table (join fan-out) rows, the divergence
  // is in a dimension the single-table oracle CANNOT adjudicate. The patched
  // classifier (pipeline-driver.ts ~4065) runs #sqlGroundTruthCompare on TS's
  // changes too (not just Go's) and, when both match, logs "oracle-blind
  // divergence" (info) and falls through to the raw MISMATCH detail — instead
  // of the old path which returned early blaming TS without checking TS vs SQL.
  test('shadow classifier: oracle-blind divergence when both Go and TS match SQL on main table but differ in related-table fan-out', async () => {
    const sink = new TestLogSink();
    const shadowLc = new LogContext('info', undefined, sink);

    // Go changes: 3 issues (main table) + 4 comments (related table).
    // Issues emitted in id DESC order (matching AST orderBy).
    const goRowChanges = [
      {type: 0, queryID: 'q-oracle-blind', table: 'issues', rowKey: {id: '3'}, row: {id: '3', closed: false, _0_version: '123'}},
      {type: 0, queryID: 'q-oracle-blind', table: 'issues', rowKey: {id: '2'}, row: {id: '2', closed: true, _0_version: '123'}},
      {type: 0, queryID: 'q-oracle-blind', table: 'issues', rowKey: {id: '1'}, row: {id: '1', closed: false, _0_version: '123'}},
      {type: 0, queryID: 'q-oracle-blind', table: 'comments', rowKey: {id: '10'}, row: {id: '10', issueID: '1', upvotes: 0, _0_version: '123'}},
      {type: 0, queryID: 'q-oracle-blind', table: 'comments', rowKey: {id: '20'}, row: {id: '20', issueID: '2', upvotes: 1, _0_version: '123'}},
      {type: 0, queryID: 'q-oracle-blind', table: 'comments', rowKey: {id: '21'}, row: {id: '21', issueID: '2', upvotes: 10000, _0_version: '123'}},
      {type: 0, queryID: 'q-oracle-blind', table: 'comments', rowKey: {id: '22'}, row: {id: '22', issueID: '2', upvotes: 20000, _0_version: '123'}},
    ];

    const fakeBackend = {
      sidecarSourceMode: 'table' as const,
      initialized: true,
      initEngine: () => Promise.resolve(),
      resetEngine: () => Promise.resolve(),
      removeQuery: () => Promise.resolve(),
      destroy: () => Promise.resolve(),
      hydrateManyStream(
        qs: {queryID: string; ast: AST}[],
        onResult: (r: {queryID: string; changes: unknown[]; timingMs: number}) => void,
      ): Promise<void> {
        for (const q of qs) {
          onResult({queryID: q.queryID, changes: goRowChanges, timingMs: 1});
        }
        return Promise.resolve();
      },
    };
    // MUST be set before PipelineDriver constructor — the constructor calls
    // createGoComputeBackend() (mocked to return goBackendMock.backend).
    goBackendMock.backend = fakeBackend;

    const goStorageDb = new Database(lc, ':memory:');
    goStorageDb.prepare(CREATE_STORAGE_TABLE).run();

    const shadowDriver = new PipelineDriver(
      shadowLc,
      testLogConfig,
      new Snapshotter(shadowLc, dbFile.path, {appID: shardID.appID}),
      shardID,
      new DatabaseStorage(goStorageDb).createClientGroupStorage(
        'shadow-oracle-blind-cg',
      ),
      'pipeline-driver.test.ts',
      new InspectorDelegate(undefined),
      () => 200,
      false,
      {
        goSidecar: {enabled: true, shadowMode: true},
      } as unknown as ConstructorParameters<typeof PipelineDriver>[9],
      {} as unknown as ConstructorParameters<typeof PipelineDriver>[10],
    );

    try {
      shadowDriver.init(clientSchema);

      // TS changes: 3 issues (same main-table rows as Go) + 2 comments (fewer
      // related-table rows — join fan-out difference the oracle can't see).
      const tsChanges: RowChange[] = [
        {type: ChangeType.ADD, queryID: 'q-oracle-blind', table: 'issues', rowKey: {id: '3'}, row: {id: '3', closed: false, _0_version: '123'}},
        {type: ChangeType.ADD, queryID: 'q-oracle-blind', table: 'issues', rowKey: {id: '2'}, row: {id: '2', closed: true, _0_version: '123'}},
        {type: ChangeType.ADD, queryID: 'q-oracle-blind', table: 'issues', rowKey: {id: '1'}, row: {id: '1', closed: false, _0_version: '123'}},
        {type: ChangeType.ADD, queryID: 'q-oracle-blind', table: 'comments', rowKey: {id: '10'}, row: {id: '10', issueID: '1', upvotes: 0, _0_version: '123'}},
        {type: ChangeType.ADD, queryID: 'q-oracle-blind', table: 'comments', rowKey: {id: '20'}, row: {id: '20', issueID: '2', upvotes: 1, _0_version: '123'}},
      ];

      const tsResultsPerQuery = new Map<string, ShadowHydrateResult>([
        ['q-oracle-blind', {changes: tsChanges, total: tsChanges.length}],
      ]);

      await shadowDriver.shadowBatchCompare(
        [{queryID: 'q-oracle-blind', ast: ISSUES_AND_COMMENTS}],
        tsResultsPerQuery,
      );

      const logText = sink.messages.map(m => m[2].join(' ')).join('\n');

      // The patched classifier should recognize both engines match SQL on the
      // main table and attribute the divergence to oracle-blind join fan-out.
      expect(logText).toContain('oracle-blind divergence');
      expect(logText).toContain('BOTH engines match SQL');
      // It should NOT blame TS (the old buggy behavior).
      expect(logText).not.toContain('ts-only divergence');
      // The raw MISMATCH detail should still surface (fall-through).
      expect(logText).toContain('MISMATCH in batch-hydrate');
    } finally {
      goBackendMock.backend = null;
      await shadowDriver.destroy();
    }
  });

  function changes(timer: Timer = NO_TIME_ADVANCEMENT_TIMER) {
    return [...(pipelines.advance(timer) as AdvanceResult).changes];
  }

  test('replica version', () => {
    pipelines.init(clientSchema);
    expect(pipelines.replicaVersion).toBe('123');
  });

  test('add query', () => {
    pipelines.init(clientSchema);

    expect([
      ...addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "3",
          },
          "rowKey": {
            "id": "3",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "closed": true,
            "id": "2",
          },
          "rowKey": {
            "id": "2",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "22",
            "issueID": "2",
            "upvotes": 20000,
          },
          "rowKey": {
            "id": "22",
          },
          "table": "comments",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "21",
            "issueID": "2",
            "upvotes": 10000,
          },
          "rowKey": {
            "id": "21",
          },
          "table": "comments",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "20",
            "issueID": "2",
            "upvotes": 1,
          },
          "rowKey": {
            "id": "20",
          },
          "table": "comments",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "1",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "10",
            "issueID": "1",
            "upvotes": 0,
          },
          "rowKey": {
            "id": "10",
          },
          "table": "comments",
          "type": 0,
        },
      ]
    `);

    // Adding a query with the same hash should be a noop.
    expect([
      ...addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "3",
          },
          "rowKey": {
            "id": "3",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "closed": true,
            "id": "2",
          },
          "rowKey": {
            "id": "2",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "22",
            "issueID": "2",
            "upvotes": 20000,
          },
          "rowKey": {
            "id": "22",
          },
          "table": "comments",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "21",
            "issueID": "2",
            "upvotes": 10000,
          },
          "rowKey": {
            "id": "21",
          },
          "table": "comments",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "20",
            "issueID": "2",
            "upvotes": 1,
          },
          "rowKey": {
            "id": "20",
          },
          "table": "comments",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "1",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "10",
            "issueID": "1",
            "upvotes": 0,
          },
          "rowKey": {
            "id": "10",
          },
          "table": "comments",
          "type": 0,
        },
      ]
    `);
  });

  test('logs query identity when query hydration fails', () => {
    pipelines.init(clientSchema);

    expect(() => [
      ...pipelines.addQuery(
        'hash1',
        'queryID1',
        {table: 'doesNotExist'},
        startTimer(),
        'myQuery',
      ),
    ]).toThrowError(/doesNotExist/);

    const failureLog = logSink.messages.find(
      ([level, context, args]) =>
        level === 'error' &&
        context?.queryHash === 'queryID1' &&
        args[0] === 'query hydration failed',
    );
    expect(failureLog?.[1]).toMatchObject({
      queryHash: 'queryID1',
      queryName: 'myQuery',
      transformationHash: 'hash1',
    });
  });

  test('insert', () => {
    pipelines.init(clientSchema);
    [
      ...addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

    replicator.processTransaction(
      '134',
      messages.insert('comments', {id: '31', issueID: '3', upvotes: BigInt(0)}),
      messages.insert('comments', {
        id: '41',
        issueID: '4',
        upvotes: BigInt(Number.MAX_SAFE_INTEGER),
      }),
      messages.insert('backfilling', {id: 123}), // should be ignored
      messages.insert('issues', {id: '4', closed: 0}),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "id": "31",
            "issueID": "3",
            "upvotes": 0,
          },
          "rowKey": {
            "id": "31",
          },
          "table": "comments",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "closed": false,
            "id": "4",
          },
          "rowKey": {
            "id": "4",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "id": "41",
            "issueID": "4",
            "upvotes": 9007199254740991,
          },
          "rowKey": {
            "id": "41",
          },
          "table": "comments",
          "type": 0,
        },
      ]
    `);
  });

  test('delete', () => {
    pipelines.init(clientSchema);
    [
      ...addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

    replicator.processTransaction(
      '134',
      messages.delete('issues', {id: '1'}),
      messages.delete('comments', {id: '21'}),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": 1,
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "10",
          },
          "table": "comments",
          "type": 1,
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "21",
          },
          "table": "comments",
          "type": 1,
        },
      ]
    `);
  });

  test('truncate', () => {
    pipelines.init(clientSchema);
    [
      ...addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

    replicator.processTransaction('134', messages.truncate('comments'));

    expect(() => changes()).toThrowError(ResetPipelinesSignal);
  });

  test('update', () => {
    pipelines.init(clientSchema);
    [
      ...addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

    replicator.processTransaction(
      '134',
      messages.update('comments', {id: '22', issueID: '3', upvotes: 20000}),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "22",
          },
          "table": "comments",
          "type": 1,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "id": "22",
            "issueID": "3",
            "upvotes": 20000,
          },
          "rowKey": {
            "id": "22",
          },
          "table": "comments",
          "type": 0,
        },
      ]
    `);

    replicator.processTransaction(
      '135',
      messages.update('comments', {id: '22', issueID: '3', upvotes: 10}),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "135",
            "id": "22",
            "issueID": "3",
            "upvotes": 10,
          },
          "rowKey": {
            "id": "22",
          },
          "table": "comments",
          "type": 2,
        },
      ]
    `);
  });

  test('rowSetSignature reflects hydrate + advance deltas', () => {
    const toID = (c: RowChange): RowID => ({
      schema: '',
      table: c.table,
      rowKey: c.rowKey as RowKey,
    });
    const sigFromChanges = (changes: readonly RowChange[]) => {
      let sig = 0n;
      for (const c of changes) {
        if (c.type === ChangeType.EDIT) continue;
        sig ^= rowIDSignatureUnit(toID(c));
      }
      return sig;
    };
    const onlyRowChanges = (
      xs: Iterable<RowChange | 'yield'>,
    ): readonly RowChange[] =>
      [...xs].filter((c): c is RowChange => c !== 'yield');

    pipelines.init(clientSchema);
    const hydrated = onlyRowChanges(
      addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    );
    expect(pipelines.rowSetSignature('queryID1')).toEqual(
      sigFromChanges(hydrated),
    );

    // Delete issues/1 (cascades to comments/10) and insert a fresh issues/4.
    replicator.processTransaction(
      '134',
      messages.delete('issues', {id: '1'}),
      messages.insert('issues', {id: '4', closed: 0}),
    );
    const advanced = onlyRowChanges(changes());
    expect(pipelines.rowSetSignature('queryID1')).toEqual(
      sigFromChanges([...hydrated, ...advanced]),
    );

    // An update that doesn't touch relationship keys yields EDITs only;
    // the signature must stay the same.
    const sigBeforeEdit = pipelines.rowSetSignature('queryID1');
    replicator.processTransaction(
      '135',
      messages.update('comments', {id: '22', issueID: '2', upvotes: 99}),
    );
    const afterEdit = onlyRowChanges(changes());
    expect(afterEdit.length).toBeGreaterThan(0);
    expect(afterEdit.every(c => c.type === ChangeType.EDIT)).toBe(true);
    expect(pipelines.rowSetSignature('queryID1')).toEqual(sigBeforeEdit);

    // removeQuery clears the entry.
    pipelines.removeQuery('queryID1');
    expect(pipelines.rowSetSignature('queryID1')).toBeUndefined();
  });

  test('rowSetSignature resets on re-execution (addQuery with same queryID)', () => {
    const toID = (c: RowChange): RowID => ({
      schema: '',
      table: c.table,
      rowKey: c.rowKey as RowKey,
    });
    const sigFromChanges = (changes: readonly RowChange[]) => {
      let sig = 0n;
      for (const c of changes) {
        if (c.type === ChangeType.EDIT) continue;
        sig ^= rowIDSignatureUnit(toID(c));
      }
      return sig;
    };
    const onlyRowChanges = (
      xs: Iterable<RowChange | 'yield'>,
    ): readonly RowChange[] =>
      [...xs].filter((c): c is RowChange => c !== 'yield');

    pipelines.init(clientSchema);

    const firstChanges = onlyRowChanges(
      addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    );
    const firstSig = pipelines.rowSetSignature('queryID1');
    expect(firstSig).toEqual(sigFromChanges(firstChanges));
    expect(firstSig).not.toEqual(0n);

    // Re-execute with a new transformation hash. addQuery internally calls
    // removeQuery, which must reset the signature before hydration accumulates
    // from 0. If it didn't, the second hydration's XORs would cancel the
    // first's (same AST, same rows) and land at 0n.
    const secondChanges = onlyRowChanges(
      addQuery(
        'hash2',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    );
    expect(pipelines.rowSetSignature('queryID1')).toEqual(
      sigFromChanges(secondChanges),
    );
    expect(pipelines.rowSetSignature('queryID1')).toEqual(firstSig);
  });

  test('rowSetSignature is maintained independently per query', () => {
    const toID = (c: RowChange): RowID => ({
      schema: '',
      table: c.table,
      rowKey: c.rowKey as RowKey,
    });
    const sigFromChanges = (changes: readonly RowChange[]) => {
      let sig = 0n;
      for (const c of changes) {
        if (c.type === ChangeType.EDIT) continue;
        sig ^= rowIDSignatureUnit(toID(c));
      }
      return sig;
    };
    const onlyRowChanges = (
      xs: Iterable<RowChange | 'yield'>,
    ): readonly RowChange[] =>
      [...xs].filter((c): c is RowChange => c !== 'yield');

    const ISSUES_ONLY: AST = {table: 'issues', orderBy: [['id', 'desc']]};

    pipelines.init(clientSchema);

    const commentedHydrated = onlyRowChanges(
      addQuery(
        'hash-issues-comments',
        'qIssuesComments',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    );
    const issuesHydrated = onlyRowChanges(
      addQuery(
        'hash-issues',
        'qIssuesOnly',
        ISSUES_ONLY,
        startTimer(),
      ),
    );

    expect(pipelines.rowSetSignature('qIssuesComments')).toEqual(
      sigFromChanges(commentedHydrated),
    );
    expect(pipelines.rowSetSignature('qIssuesOnly')).toEqual(
      sigFromChanges(issuesHydrated),
    );

    const issuesOnlySigBefore = pipelines.rowSetSignature('qIssuesOnly');

    // Delete a comment: only qIssuesComments reads from the comments table,
    // so only its signature should change. qIssuesOnly's pipeline never sees
    // the event, so its signature is untouched.
    replicator.processTransaction(
      '134',
      messages.delete('comments', {id: '22'}),
    );
    const advanced = onlyRowChanges(changes());
    expect(advanced.length).toBeGreaterThan(0);
    expect(advanced.every(c => c.queryID === 'qIssuesComments')).toBe(true);

    expect(pipelines.rowSetSignature('qIssuesComments')).toEqual(
      sigFromChanges([...commentedHydrated, ...advanced]),
    );
    expect(pipelines.rowSetSignature('qIssuesOnly')).toEqual(
      issuesOnlySigBefore,
    );
  });

  test('timeout on slow advancement', () => {
    pipelines.init(clientSchema);
    [
      ...addQuery('hash1', 'queryID1', ISSUES_AND_COMMENTS, {
        // hydration time
        totalElapsed: () => 100,
        elapsedLap: () => 100,
        running: () => true,
      }),
    ];

    replicator.processTransaction('134', messages.insert('issues', {id: 'i1'}));

    // 60ms is larger than half of the hydration time.
    const advResult1 = pipelines.advance({totalElapsed: () => 60, elapsedLap: () => 60, running: () => true}) as AdvanceResult;
    expect(() => [
      ...advResult1.changes,
    ]).toThrowErrorMatchingInlineSnapshot(
      `[ResetPipelinesSignal: Advancement exceeded timeout at 0 of 1 changes after 60 ms. Advancement time limited based on total hydration time of 100 ms.]`,
    );

    // Test that after reset hydration and advancement work.
    pipelines.reset(clientSchema);

    expect(pipelines.queries()).toEqual(new Map());

    [
      ...addQuery('hash1', 'queryID1', ISSUES_AND_COMMENTS, {
        // hydration time
        totalElapsed: () => 100,
        elapsedLap: () => 100,
        running: () => true,
      }),
    ];

    replicator.processTransaction('140', messages.insert('issues', {id: 'i1'}));

    const advResult2 = pipelines.advance({totalElapsed: () => 20, elapsedLap: () => 20, running: () => true}) as AdvanceResult;
    expect(() => [
      ...advResult2.changes,
    ]).not.toThrow();
  });

  test('advancement timeout has a minimum limit', () => {
    pipelines.init(clientSchema);
    [
      ...addQuery('hash1', 'queryID1', ISSUES_AND_COMMENTS, {
        // very low hydration time
        totalElapsed: () => 25,
        elapsedLap: () => 25,
        running: () => true,
      }),
    ];

    replicator.processTransaction('134', messages.insert('issues', {id: 'i1'}));

    // 29 is larger than the hydration time but less than the minimum
    // advancement time limit
    const advResult3 = pipelines.advance({totalElapsed: () => 29, elapsedLap: () => 29, running: () => true}) as AdvanceResult;
    expect(() => [
      ...advResult3.changes,
    ]).not.toThrow();
  });

  test('reset', () => {
    pipelines.init(clientSchema);
    [
      ...addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

    expect(pipelines.queries().size).toEqual(1);
    expect(pipelines.queries().get('queryID1')?.transformationHash).toEqual(
      'hash1',
    );
    expect(pipelines.queries().get('queryID1')?.transformedAst).toEqual(
      ISSUES_AND_COMMENTS,
    );

    replicator.processTransaction(
      '134',
      messages.addColumn('issues', 'newColumn', {dataType: 'TEXT', pos: 0}),
    );

    // Update one of the rows after the schema change.
    replicator.processTransaction('135', messages.update('issues', {id: '2'}));

    pipelines.advanceWithoutDiff();
    pipelines.reset(clientSchema);

    expect(pipelines.queries()).toEqual(new Map());

    // Under the hood, the row versions are the same but the minRowVersion is
    // bumped in the tableMetadata.
    expect(
      db.prepare(`SELECT id, _0_version FROM issues ORDER BY id`).all(),
    ).toMatchObject([
      {id: '1', _0_version: '123'},
      {id: '2', _0_version: '135'},
      {id: '3', _0_version: '123'},
    ]);

    expect(
      db.prepare(`SELECT minRowVersion FROM "_zero.tableMetadata"`).get(),
    ).toMatchObject({minRowVersion: '134'});

    // The newColumn should be reflected after a reset, with the bumped
    // minRowVersion for older rows.
    expect([
      ...addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "closed": false,
            "id": "3",
            "newColumn": null,
          },
          "rowKey": {
            "id": "3",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "135",
            "closed": true,
            "id": "2",
            "newColumn": null,
          },
          "rowKey": {
            "id": "2",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "22",
            "issueID": "2",
            "upvotes": 20000,
          },
          "rowKey": {
            "id": "22",
          },
          "table": "comments",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "21",
            "issueID": "2",
            "upvotes": 10000,
          },
          "rowKey": {
            "id": "21",
          },
          "table": "comments",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "20",
            "issueID": "2",
            "upvotes": 1,
          },
          "rowKey": {
            "id": "20",
          },
          "table": "comments",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "closed": false,
            "id": "1",
            "newColumn": null,
          },
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "10",
            "issueID": "1",
            "upvotes": 0,
          },
          "rowKey": {
            "id": "10",
          },
          "table": "comments",
          "type": 0,
        },
      ]
    `);
  });

  test('update unique non-primary key', () => {
    pipelines.init(clientSchema);
    expect([
      ...addQuery('hash1', 'queryID1', UNIQUES_QUERY, startTimer()),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "foo",
            "name": "bar",
          },
          "rowKey": {
            "id": "foo",
          },
          "table": "uniques",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "boo",
            "name": "dar",
          },
          "rowKey": {
            "id": "boo",
          },
          "table": "uniques",
          "type": 0,
        },
      ]
    `);

    replicator.processTransaction(
      '134',
      messages.update('uniques', {id: 'boo', name: 'far'}),
    );

    // Although this can be considered an edit of a row keyed by {id: 'boo'},
    // rows are ultimately referred to by their union key ['id', 'name'],
    // in which case this update must be represented as:
    // - `remove{id: 'boo', name: 'dar'}`
    // - `add{id: 'boo', name: 'far'}`
    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "id": "boo",
            "name": "far",
          },
          "rowKey": {
            "id": "boo",
          },
          "table": "uniques",
          "type": 2,
        },
      ]
    `);
  });

  test('unique constraint conflict due to changelog compression', () => {
    pipelines.init(clientSchema);
    expect([
      ...addQuery('hash1', 'queryID1', UNIQUES_QUERY, startTimer()),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "foo",
            "name": "bar",
          },
          "rowKey": {
            "id": "foo",
          },
          "table": "uniques",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "boo",
            "name": "dar",
          },
          "rowKey": {
            "id": "boo",
          },
          "table": "uniques",
          "type": 0,
        },
      ]
    `);

    replicator.processTransaction(
      '134',
      messages.delete('uniques', {id: 'foo'}),
      messages.insert('uniques', {id: 'baz', name: 'bar'}),
      messages.insert('uniques', {id: 'foo', name: 'wuzzy'}),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "foo",
          },
          "table": "uniques",
          "type": 1,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "id": "baz",
            "name": "bar",
          },
          "rowKey": {
            "id": "baz",
          },
          "table": "uniques",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "id": "foo",
            "name": "wuzzy",
          },
          "rowKey": {
            "id": "foo",
          },
          "table": "uniques",
          "type": 0,
        },
      ]
    `);
  });

  test('whereExists query', () => {
    pipelines.init(clientSchema);
    [
      ...addQuery(
        'hash1',
        'queryID',
        ISSUES_QUERY_WITH_EXISTS,
        startTimer(),
      ),
    ];

    replicator.processTransaction(
      '134',
      messages.delete('issueLabels', {
        issueID: '1',
        labelID: '1',
        legacyID: '1-1',
      }),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID",
          "row": undefined,
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": 1,
        },
        {
          "queryID": "queryID",
          "row": undefined,
          "rowKey": {
            "issueID": "1",
            "labelID": "1",
          },
          "table": "issueLabels",
          "type": 1,
        },
        {
          "queryID": "queryID",
          "row": undefined,
          "rowKey": {
            "id": "1",
          },
          "table": "labels",
          "type": 1,
        },
      ]
    `);
  });

  test('subset client schema can hydrate whereExists helper tables', () => {
    pipelines.init(subsetClientSchema);

    expect([
      ...addQuery(
        'hash-subset-schema-exists',
        'querySubsetSchemaExists',
        ISSUES_QUERY_WITH_EXISTS,
        startTimer(),
      ),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "querySubsetSchemaExists",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "1",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "querySubsetSchemaExists",
          "row": {
            "_0_version": "123",
            "issueID": "1",
            "labelID": "1",
            "legacyID": "1-1",
          },
          "rowKey": {
            "legacyID": "1-1",
          },
          "table": "issueLabels",
          "type": 0,
        },
        {
          "queryID": "querySubsetSchemaExists",
          "row": {
            "_0_version": "123",
            "id": "1",
            "name": "bug",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "labels",
          "type": 0,
        },
      ]
    `);
  });

  test('whereExists added by permissions return no rows', () => {
    pipelines.init(clientSchema);
    expect([
      ...addQuery(
        'hash1',
        'queryID1',
        ISSUES_QUERY_WITH_EXISTS_FROM_PERMISSIONS,
        startTimer(),
      ),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "1",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": 0,
        },
      ]
    `);

    expect([
      ...addQuery(
        'hash2',
        'queryID',
        ISSUES_QUERY_WITH_EXISTS_FROM_PERMISSIONS2,
        startTimer(),
      ),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "1",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "queryID",
          "row": {
            "_0_version": "123",
            "issueID": "1",
            "labelID": "1",
            "legacyID": "1-1",
          },
          "rowKey": {
            "issueID": "1",
            "labelID": "1",
          },
          "table": "issueLabels",
          "type": 0,
        },
      ]
    `);
  });

  test('whereExists generates the correct number of add and remove changes', () => {
    const query: AST = {
      table: 'issues',
      where: {
        type: 'and',
        conditions: [
          {
            op: '=',
            left: {
              name: 'closed',
              type: 'column',
            },
            type: 'simple',
            right: {
              type: 'literal',
              value: true,
            },
          },
          {
            op: 'EXISTS',
            type: 'correlatedSubquery',
            related: {
              subquery: {
                alias: 'zsubq_labels',
                table: 'issueLabels',
                where: {
                  op: 'EXISTS',
                  type: 'correlatedSubquery',
                  related: {
                    subquery: {
                      alias: 'zsubq_labels',
                      table: 'labels',
                      where: {
                        op: '=',
                        left: {
                          name: 'name',
                          type: 'column',
                        },
                        type: 'simple',
                        right: {
                          type: 'literal',
                          value: 'bug',
                        },
                      },
                      orderBy: [['id', 'asc']],
                    },
                    system: 'client',
                    correlation: {
                      childField: ['id'],
                      parentField: ['labelID'],
                    },
                  },
                },
                orderBy: [
                  ['issueID', 'asc'],
                  ['labelID', 'asc'],
                ],
              },
              system: 'client',
              correlation: {
                childField: ['issueID'],
                parentField: ['id'],
              },
            },
          },
        ],
      },
      orderBy: [['id', 'desc']],
      related: [
        {
          subquery: {
            alias: 'issueLabels',
            table: 'issueLabels',
            orderBy: [
              ['issueID', 'asc'],
              ['labelID', 'asc'],
            ],
            related: [
              {
                hidden: true,
                subquery: {
                  alias: 'labels',
                  table: 'labels',
                  orderBy: [['id', 'asc']],
                },
                system: 'client',
                correlation: {
                  childField: ['id'],
                  parentField: ['labelID'],
                },
              },
            ],
          },
          system: 'client',
          correlation: {
            childField: ['issueID'],
            parentField: ['id'],
          },
        },
      ],
    };

    pipelines.init(clientSchema);
    [...addQuery('hash1', 'queryID1', query, startTimer())];

    replicator.processTransaction(
      '134',
      messages.insert('issueLabels', {
        issueID: '2',
        labelID: '1',
        legacyID: '2-1',
      }),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "closed": true,
            "id": "2",
          },
          "rowKey": {
            "id": "2",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "issueID": "2",
            "labelID": "1",
            "legacyID": "2-1",
          },
          "rowKey": {
            "issueID": "2",
            "labelID": "1",
          },
          "table": "issueLabels",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "1",
            "name": "bug",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "labels",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "issueID": "2",
            "labelID": "1",
            "legacyID": "2-1",
          },
          "rowKey": {
            "issueID": "2",
            "labelID": "1",
          },
          "table": "issueLabels",
          "type": 0,
        },
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "123",
            "id": "1",
            "name": "bug",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "labels",
          "type": 0,
        },
      ]
    `);

    replicator.processTransaction(
      '135',
      messages.delete('issueLabels', {
        issueID: '2',
        labelID: '1',
        legacyID: '2-1',
      }),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "2",
          },
          "table": "issues",
          "type": 1,
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "issueID": "2",
            "labelID": "1",
          },
          "table": "issueLabels",
          "type": 1,
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "1",
          },
          "table": "labels",
          "type": 1,
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "issueID": "2",
            "labelID": "1",
          },
          "table": "issueLabels",
          "type": 1,
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "1",
          },
          "table": "labels",
          "type": 1,
        },
      ]
    `);
  });

  test('getRow', () => {
    pipelines.init(clientSchema);

    [
      ...addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

    // Post-hydration
    expect(pipelines.getRow('issues', {id: '1'})).toEqual({
      id: '1',
      closed: false,
      ['_0_version']: '123',
    });

    expect(pipelines.getRow('comments', {id: '22'})).toEqual({
      id: '22',
      issueID: '2',
      upvotes: 20000,
      ['_0_version']: '123',
    });

    replicator.processTransaction(
      '134',
      messages.update('comments', {id: '22', issueID: '3', upvotes: 20000}),
    );
    changes();

    // Post-advancement
    expect(pipelines.getRow('comments', {id: '22'})).toEqual({
      id: '22',
      issueID: '3',
      upvotes: 20000,
      ['_0_version']: '134',
    });

    [
      ...addQuery(
        'hash2',
        'queryID2',
        ISSUES_QUERY_WITH_EXISTS,
        startTimer(),
      ),
    ];

    // getRow should work with any row key
    expect(
      pipelines.getRow('issueLabels', {issueID: '1', labelID: '1'}),
    ).toEqual({
      issueID: '1',
      labelID: '1',
      legacyID: '1-1',
      ['_0_version']: '123',
    });

    expect(pipelines.getRow('issueLabels', {legacyID: '1-1'})).toEqual({
      issueID: '1',
      labelID: '1',
      legacyID: '1-1',
      ['_0_version']: '123',
    });
  });

  test('get mutation results', () => {
    pipelines.init(clientSchema);
    const mutationResultsQuery = getMutationResultsQuery(
      upstreamSchema(shardID),
      'cg1',
    );

    replicator.processTransaction(
      '134',
      messages.insert(mutationsTableName, {
        clientGroupID: 'cg1',
        clientID: 'c1',
        mutationID: 1,
        result: {},
      }),
    );

    [
      ...addQuery(
        mutationResultsQuery.id,
        'queryID1',
        mutationResultsQuery.ast,
        startTimer(),
      ),
    ];

    expect(
      pipelines.getRow(mutationsTableName, {
        clientGroupID: 'cg1',
        clientID: 'c1',
        mutationID: 1,
      }),
    ).toMatchInlineSnapshot(`undefined`);
  });

  test('multiple advancements', () => {
    pipelines.init(clientSchema);
    [
      ...addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

    replicator.processTransaction(
      '134',
      messages.insert('issues', {id: '4', closed: 0}),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "134",
            "closed": false,
            "id": "4",
          },
          "rowKey": {
            "id": "4",
          },
          "table": "issues",
          "type": 0,
        },
      ]
    `);

    replicator.processTransaction(
      '156',
      messages.insert('comments', {id: '41', issueID: '4', upvotes: 10}),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": {
            "_0_version": "156",
            "id": "41",
            "issueID": "4",
            "upvotes": 10,
          },
          "rowKey": {
            "id": "41",
          },
          "table": "comments",
          "type": 0,
        },
      ]
    `);

    replicator.processTransaction('189', messages.delete('issues', {id: '4'}));

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "4",
          },
          "table": "issues",
          "type": 1,
        },
        {
          "queryID": "queryID1",
          "row": undefined,
          "rowKey": {
            "id": "41",
          },
          "table": "comments",
          "type": 1,
        },
      ]
    `);
  });

  test('remove query', () => {
    pipelines.init(clientSchema);
    [
      ...addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

    expect(pipelines.queries().size).toEqual(1);
    expect(pipelines.queries().get('queryID1')?.transformationHash).toEqual(
      'hash1',
    );
    expect(pipelines.queries().get('queryID1')?.transformedAst).toEqual(
      ISSUES_AND_COMMENTS,
    );

    pipelines.removeQuery('queryID1');
    expect(pipelines.queries()).toEqual(new Map());

    replicator.processTransaction(
      '134',
      messages.insert('comments', {id: '31', issueID: '3', upvotes: 0}),
      messages.insert('comments', {id: '41', issueID: '4', upvotes: 0}),
      messages.insert('issues', {id: '4', closed: 1}),
    );

    expect(pipelines.currentVersion()).toBe('123');
    expect(changes()).toHaveLength(0);
    expect(pipelines.currentVersion()).toBe('134');
  });

  test('push fails on out of bounds numbers', () => {
    pipelines.init(clientSchema);
    [
      ...addQuery(
        'hash1',
        'queryID1',
        ISSUES_AND_COMMENTS,
        startTimer(),
      ),
    ];

    replicator.processTransaction(
      '134',
      messages.insert('comments', {
        id: '31',
        issueID: '3',
        upvotes: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
      }),
    );

    expect(() => changes()).toThrowError();
  });

  test('scalar subquery resolves to literal', () => {
    pipelines.init(clientSchema);

    // Comment '10' has issueID='1', so the subquery resolves to id = '1'
    const results = [
      ...addQuery(
        'hash-scalar',
        'queryScalar',
        ISSUES_WITH_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ];

    expect(results).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryScalar",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "1",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "queryScalar",
          "row": {
            "_0_version": "123",
            "id": "10",
            "issueID": "1",
            "upvotes": 0,
          },
          "rowKey": {
            "id": "10",
          },
          "table": "comments",
          "type": 0,
        },
      ]
    `);

    // The transformedAst should have the scalar subquery resolved to a simple condition
    expect(
      pipelines.queries().get('queryScalar')?.transformedAst.where,
    ).toEqual({
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'id'},
      right: {type: 'literal', value: '1'},
    });
  });

  test('subset client schema can hydrate scalar subquery companion tables', () => {
    pipelines.init(subsetClientSchema);

    expect([
      ...addQuery(
        'hash-scalar-subset-schema',
        'queryScalarSubsetSchema',
        ISSUES_WITH_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ]).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryScalarSubsetSchema",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "1",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "queryScalarSubsetSchema",
          "row": {
            "_0_version": "123",
            "id": "10",
            "issueID": "1",
            "upvotes": 0,
          },
          "rowKey": {
            "id": "10",
          },
          "table": "comments",
          "type": 0,
        },
      ]
    `);

    expect(
      pipelines.queries().get('queryScalarSubsetSchema')?.transformedAst.where,
    ).toEqual({
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'id'},
      right: {type: 'literal', value: '1'},
    });
  });

  test('scalar subquery with no matching rows', () => {
    pipelines.init(clientSchema);

    const results = [
      ...addQuery(
        'hash-scalar-none',
        'queryScalarNone',
        ISSUES_WITH_NONEXISTENT_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ];

    expect(results).toEqual([]);

    // The transformedAst should have ALWAYS_FALSE
    expect(
      pipelines.queries().get('queryScalarNone')?.transformedAst.where,
    ).toEqual({
      type: 'simple',
      op: '=',
      left: {type: 'literal', value: 1},
      right: {type: 'literal', value: 0},
    });
  });

  test('scalar subquery in AND with other conditions', () => {
    pipelines.init(clientSchema);

    const queryWithAnd: AST = {
      table: 'issues',
      orderBy: [['id', 'asc']],
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'closed'},
            right: {type: 'literal', value: false},
          },
          {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            scalar: true,
            related: {
              correlation: {
                parentField: ['id'],
                childField: ['issueID'],
              },
              subquery: {
                table: 'comments',
                orderBy: [['id', 'asc']],
                where: {
                  type: 'simple',
                  op: '=',
                  left: {type: 'column', name: 'id'},
                  right: {type: 'literal', value: '10'},
                },
              },
            },
          },
        ],
      },
    };

    const results = [
      ...addQuery(
        'hash-scalar-and',
        'queryScalarAnd',
        queryWithAnd,
        startTimer(),
      ),
    ];

    // Issue '1' is not closed and matches the subquery
    expect(results).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryScalarAnd",
          "row": {
            "_0_version": "123",
            "closed": false,
            "id": "1",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": 0,
        },
        {
          "queryID": "queryScalarAnd",
          "row": {
            "_0_version": "123",
            "id": "10",
            "issueID": "1",
            "upvotes": 0,
          },
          "rowKey": {
            "id": "10",
          },
          "table": "comments",
          "type": 0,
        },
      ]
    `);

    // The transformedAst should have the scalar subquery resolved within the AND
    expect(
      pipelines.queries().get('queryScalarAnd')?.transformedAst.where,
    ).toEqual({
      type: 'and',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'closed'},
          right: {type: 'literal', value: false},
        },
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: '1'},
        },
      ],
    });
  });

  test('advancement after scalar subquery resolution', () => {
    pipelines.init(clientSchema);

    // This resolves to `issues WHERE id = '1'`
    [
      ...addQuery(
        'hash-scalar',
        'queryScalar',
        ISSUES_WITH_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ];

    replicator.processTransaction(
      '134',
      messages.insert('issues', {id: '5', closed: 0}),
      messages.update('issues', {id: '1', closed: 1}),
    );

    // Only the edit to issue '1' should appear (it matches the resolved filter),
    // NOT the insert of issue '5' (which doesn't match id = '1').
    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryScalar",
          "row": {
            "_0_version": "134",
            "closed": true,
            "id": "1",
          },
          "rowKey": {
            "id": "1",
          },
          "table": "issues",
          "type": 2,
        },
      ]
    `);
  });

  test('subset client schema advances scalar companion tables', () => {
    pipelines.init(subsetClientSchema);

    [
      ...addQuery(
        'hash-scalar-subset-schema',
        'queryScalarSubsetSchema',
        ISSUES_WITH_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ];

    replicator.processTransaction(
      '134',
      messages.update('comments', {id: '10', issueID: '1', upvotes: 5}),
    );

    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryScalarSubsetSchema",
          "row": {
            "_0_version": "134",
            "id": "10",
            "issueID": "1",
            "upvotes": 5,
          },
          "rowKey": {
            "id": "10",
          },
          "table": "comments",
          "type": 2,
        },
      ]
    `);
  });

  test('companion pipeline throws ResetPipelinesSignal when scalar value changes', () => {
    pipelines.init(clientSchema);

    // Resolves comment '10' (issueID='1'), so query becomes `issues WHERE id = '1'`
    [
      ...addQuery(
        'hash-scalar',
        'queryScalar',
        ISSUES_WITH_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ];

    // Change comment '10' issueID from '1' to '2' — the scalar value changes
    replicator.processTransaction(
      '134',
      messages.update('comments', {id: '10', issueID: '2', upvotes: 0}),
    );

    expect(() => changes()).toThrowError(ResetPipelinesSignal);
  });

  test('companion pipeline does not throw when scalar value stays same', () => {
    pipelines.init(clientSchema);

    // Resolves comment '10' (issueID='1'), so query becomes `issues WHERE id = '1'`
    [
      ...addQuery(
        'hash-scalar',
        'queryScalar',
        ISSUES_WITH_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ];

    // Change a different column (upvotes) on comment '10' — issueID stays '1'
    replicator.processTransaction(
      '134',
      messages.update('comments', {id: '10', issueID: '1', upvotes: 5}),
    );

    // No ResetPipelinesSignal, and the companion row change is synced
    expect(changes()).toMatchInlineSnapshot(`
      [
        {
          "queryID": "queryScalar",
          "row": {
            "_0_version": "134",
            "id": "10",
            "issueID": "1",
            "upvotes": 5,
          },
          "rowKey": {
            "id": "10",
          },
          "table": "comments",
          "type": 2,
        },
      ]
    `);
  });

  test('companion pipeline throws ResetPipelinesSignal when companion row deleted', () => {
    pipelines.init(clientSchema);

    // Resolves comment '10' (issueID='1'), so query becomes `issues WHERE id = '1'`
    [
      ...addQuery(
        'hash-scalar',
        'queryScalar',
        ISSUES_WITH_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ];

    // Delete comment '10' — the scalar value goes from '1' to undefined (no row)
    replicator.processTransaction(
      '134',
      messages.delete('comments', {id: '10'}),
    );

    expect(() => changes()).toThrowError(ResetPipelinesSignal);
  });

  test('companion pipeline throws ResetPipelinesSignal when companion row added', () => {
    pipelines.init(clientSchema);

    replicator.processTransaction(
      '134',
      messages.delete('comments', {id: '10'}),
    );

    changes();

    [
      ...addQuery(
        'hash-scalar',
        'queryScalar',
        ISSUES_WITH_SCALAR_SUBQUERY,
        startTimer(),
      ),
    ];

    // Insert comment '10' — the scalar value goes from undefined to '1'
    replicator.processTransaction(
      '135',
      messages.insert('comments', {id: '10', issueID: '1', upvotes: 0}),
    );

    expect(() => changes()).toThrowError(ResetPipelinesSignal);
  });
});
