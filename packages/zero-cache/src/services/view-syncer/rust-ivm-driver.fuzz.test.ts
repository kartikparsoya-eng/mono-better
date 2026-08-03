import './rust-ivm-addon-setup.ts'; // MUST be first: guarantees the wal2 addon.
import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
// The corpus generator shared with the engine-level fuzzer (agentic gen.mjs).
// Same schemas/queries/pushes — incl. PK divergence — now driven through the
// FULL prod driver path instead of the raw engine.
import {genFixture} from '../../../../rust-ivm/agentic/fuzz/gen.mjs';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {listTables} from '../../db/lite-tables.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import {populateFromExistingTables} from '../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  DriverParityTrace,
  errorTrace,
  firstTraceDifference,
  type CanonicalValue,
  type StreamTraceEvent,
} from './driver-parity-trace.ts';
import {PipelineDriver} from './pipeline-driver.ts';
import {
  fixtureClientSchema,
  fixturePushesDML,
  fixtureReplicaDDL,
  UntranslatableFixture,
  type Fixture,
} from './rust-ivm-differential-harness.ts';
import {RustIVMDriver} from './rust-ivm-driver.ts';
import {Snapshotter} from './snapshotter.ts';

// -----------------------------------------------------------------------------
// FUZZ-DRIVEN DRIVER DIFFERENTIAL: for each generated fixture, drive the SAME
// query/pushes through RustIVMDriver and the reference PipelineDriver on ONE
// wal2 replica and compare the complete canonical public trace.
//
// This DISCOVERS new driver-seam divergences the way the engine fuzzer discovers
// engine ones — but through the production glue (buildNapiTableSpecs, planAst,
// streaming, advance) that the engine fuzzer bypasses.
//
// The trace preserves stream order and yields, errors, versions, query state,
// transformed ASTs, and row-set signatures. Only fixtures that the shared
// fixture translator cannot represent are skipped; driver errors are compared.
// We count tested-vs-skipped and fail if nothing was exercised.
//
// Range via env: DRIVER_FUZZ_START (default 0), DRIVER_FUZZ_SEEDS (default 80).
// -----------------------------------------------------------------------------

const ADDON_PATH = process.env['RUST_IVM_ADDON_PATH'];
const NO_TIMER = {elapsedLap: () => 0, totalElapsed: () => 0} as any;
const START = Number(process.env['DRIVER_FUZZ_START'] ?? '0');
const COUNT = Number(process.env['DRIVER_FUZZ_SEEDS'] ?? '80');

describe.skipIf(!ADDON_PATH)(
  'view-syncer/rust-ivm-driver fuzz differential',
  () => {
    const shardID: ShardID = {appID: 'zeroz', shardNum: 1};
    const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
    const BASE = '8400bivbkg';
    let lc: LogContext;

    beforeEach(() => {
      lc = new LogContext('error', undefined, new TestLogSink());
    });
    afterEach(() => {});

    function newStorage() {
      const storage = new Database(lc, ':memory:');
      storage.prepare(CREATE_STORAGE_TABLE).run();
      return new DatabaseStorage(storage);
    }
    function makeRust(cs: any, path: string): RustIVMDriver {
      const d = new RustIVMDriver(
        lc,
        testLogConfig,
        shardID,
        newStorage().createClientGroupStorage('fz-rust'),
        'fz-rust',
        new InspectorDelegate(undefined),
        () => 200,
        false,
        undefined,
        path,
      );
      d.init(cs);
      return d;
    }
    function makeTs(cs: any, path: string): PipelineDriver {
      const d = new PipelineDriver(
        lc,
        testLogConfig,
        new Snapshotter(lc, path, {appID: shardID.appID}),
        shardID,
        newStorage().createClientGroupStorage('fz-ts'),
        'fz-ts',
        new InspectorDelegate(undefined),
        () => 200,
        false,
      );
      d.init(cs);
      return d;
    }
    function sameTrace(a: unknown, b: unknown): boolean {
      return JSON.stringify(a) === JSON.stringify(b);
    }

    function traceSummary(event: unknown): string {
      return JSON.stringify(event)?.slice(0, 2_000) ?? '<missing>';
    }

    function traceDiff(rust: unknown, ts: unknown): string {
      return JSON.stringify(firstTraceDifference(rust, ts));
    }

    function displayCanonical(value: CanonicalValue): unknown {
      switch (value.type) {
        case 'undefined':
          return '<undefined>';
        case 'null':
          return null;
        case 'boolean':
        case 'string':
        case 'number':
          return value.value;
        case 'bigint':
          return `${value.value}n`;
        case 'bytes':
          return value.value;
        case 'array':
          return value.value.map(displayCanonical);
        case 'object':
          return Object.fromEntries(
            value.value.map(([key, child]) => [key, displayCanonical(child)]),
          );
        case 'symbol':
        case 'function':
          return value.value;
      }
    }

    function changeKeys(events: StreamTraceEvent[] | undefined): unknown[] {
      return (events ?? [])
        .filter(event => event.kind === 'change')
        .map(event => {
          const change = displayCanonical(event.change) as Record<
            string,
            unknown
          >;
          return {
            type: change.type,
            table: change.table,
            rowKey: change.rowKey,
          };
        });
    }

    test('fuzz corpus parity across the driver path', async () => {
      const findings: string[] = [];
      const skipped: Record<string, number> = {};
      const bump = (k: string) => (skipped[k] = (skipped[k] ?? 0) + 1);
      let tested = 0;

      for (let seed = START; seed < START + COUNT; seed++) {
        const fixture = genFixture(seed) as unknown as Fixture;
        // Translate once; used to seed the shared replica + both drivers.
        let ddl: string;
        let cs: unknown;
        try {
          ddl = fixtureReplicaDDL(fixture.tables, BASE);
          cs = fixtureClientSchema(fixture.tables);
        } catch (e) {
          bump(
            e instanceof UntranslatableFixture
              ? 'untranslatable'
              : 'translate-error',
          );
          continue;
        }

        const dbFile = new DbFile(`rust_ivm_fuzz_${seed}`);
        let rust: RustIVMDriver | undefined;
        let ts: PipelineDriver | undefined;
        try {
          dbFile.connect(lc).pragma('journal_mode = wal2');
          const db = dbFile.connect(lc);
          initReplicationState(db, ['zero_data'], BASE);
          db.exec(/*sql*/ `
          CREATE TABLE "${mutationsTableName}" (
            "clientGroupID" TEXT, "clientID" TEXT, "mutationID" INTEGER,
            "result" TEXT, _0_version TEXT NOT NULL,
            PRIMARY KEY ("clientGroupID","clientID","mutationID")
          );
        `);
          try {
            db.exec(ddl);
          } catch {
            bump('ddl-error');
            continue;
          }
          populateFromExistingTables(db, listTables(db, false));

          // --- init both (asymmetry = finding; both-throw = skip) ---
          let rErr: Error | undefined;
          let tErr: Error | undefined;
          try {
            rust = makeRust(cs, dbFile.path);
          } catch (e) {
            rErr = e as Error;
          }
          try {
            ts = makeTs(cs, dbFile.path);
          } catch (e) {
            tErr = e as Error;
          }
          if (rErr && tErr) {
            tested++;
            if (!sameTrace(errorTrace(rErr), errorTrace(tErr))) {
              findings.push(
                `seed ${seed} INIT error mismatch: rust=${JSON.stringify(errorTrace(rErr))} ts=${JSON.stringify(errorTrace(tErr))}`,
              );
            }
            continue;
          }
          if (!!rErr !== !!tErr) {
            findings.push(
              `seed ${seed} INIT asymmetry: rust=${rErr?.message ?? 'ok'} ts=${tErr?.message ?? 'ok'}`,
            );
            continue;
          }

          const rustTrace = new DriverParityTrace(rust!);
          const tsTrace = new DriverParityTrace(ts!);
          rustTrace.recordState('initialized');
          tsTrace.recordState('initialized');
          if (!sameTrace(rustTrace.events(), tsTrace.events())) {
            findings.push(
              `seed ${seed} INITIAL STATE mismatch: ${traceDiff(rustTrace.events()[0], tsTrace.events()[0])}`,
            );
          }

          // --- hydrate: exact ordered public trace, including errors/state ---
          const ast = fixture.ast as any;
          const rustHydrateEvents = await rustTrace.addQuery(
            'h',
            'q',
            ast,
            NO_TIMER,
          );
          const tsHydrateEvents = await tsTrace.addQuery(
            'h',
            'q',
            ast,
            NO_TIMER,
          );
          const rustHydrate = rustTrace.events().at(-1);
          const tsHydrate = tsTrace.events().at(-1);
          if (!sameTrace(rustHydrate, tsHydrate)) {
            findings.push(
              `seed ${seed} HYDRATE trace mismatch: ${traceDiff(rustHydrate, tsHydrate)}\n  rustKeys=${JSON.stringify(changeKeys(rustHydrateEvents))}\n  tsKeys=${JSON.stringify(changeKeys(tsHydrateEvents))}`,
            );
          }
          const hydrationSucceeded =
            rustHydrate?.outcome.status === 'ok' &&
            tsHydrate?.outcome.status === 'ok';
          if (!hydrationSucceeded) {
            await rustTrace.removeQuery('q');
            await tsTrace.removeQuery('q');
            tested++;
            continue;
          }

          // --- advance (only if the fixture has pushes) ---
          if (fixture.pushes?.length) {
            const V1 = '8500000001';
            try {
              db.exec(
                fixturePushesDML(fixture.tables, fixture.pushes, V1) +
                  `\nUPDATE "_zero.replicationState" SET stateVersion = '${V1}';`,
              );
            } catch {
              bump('push-apply-error');
              tested++;
              continue;
            }
            await rustTrace.advance(NO_TIMER);
            await tsTrace.advance(NO_TIMER);
            const rustAdvance = rustTrace.events().at(-1);
            const tsAdvance = tsTrace.events().at(-1);
            if (!sameTrace(rustAdvance, tsAdvance)) {
              findings.push(
                `seed ${seed} ADVANCE trace mismatch: ${traceDiff(rustAdvance, tsAdvance)}\n  rust=${traceSummary(rustAdvance)}\n  ts=${traceSummary(tsAdvance)}`,
              );
            }
          }
          await rustTrace.removeQuery('q');
          await tsTrace.removeQuery('q');
          const rustRemove = rustTrace.events().at(-1);
          const tsRemove = tsTrace.events().at(-1);
          if (!sameTrace(rustRemove, tsRemove)) {
            findings.push(
              `seed ${seed} REMOVE trace mismatch: ${traceDiff(rustRemove, tsRemove)}\n  rust=${traceSummary(rustRemove)}\n  ts=${traceSummary(tsRemove)}`,
            );
          }
          tested++;
        } finally {
          try {
            rust?.removeQuery('q');
          } catch {
            /* ignore */
          }
          if (rust) {
            await rust.destroy().catch(() => {});
          }
          ts?.destroy();
          dbFile.delete();
        }
      }

      const skipTotal = Object.values(skipped).reduce((a, b) => a + b, 0);
      // eslint-disable-next-line no-console
      console.log(
        `[driver-fuzz] seeds ${START}..${START + COUNT - 1}: tested=${tested} ` +
          `skipped=${skipTotal} ${JSON.stringify(skipped)} ` +
          `findings=${findings.length}`,
      );

      // False-negative guard: a run that tested ~nothing is not "all clear".
      expect(
        tested,
        'no fixtures were actually exercised — check translation',
      ).toBeGreaterThan(Math.floor(COUNT * 0.25));
      expect(findings, findings.slice(0, 8).join('\n\n')).toEqual([]);
    });
  },
);
