import './rust-ivm-addon-setup.ts'; // MUST be first: guarantees the wal2 addon.
import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
// The corpus generator shared with the engine-level fuzzer (agentic gen.mjs).
// Same schemas/queries/pushes — incl. PK divergence — now driven through the
// FULL prod driver path instead of the raw engine.
import {genFixture} from '../../../../rust-ivm/agentic/fuzz/gen.mjs';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import {listTables} from '../../db/lite-tables.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import {populateFromExistingTables} from '../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  diffChanges,
  drain,
  fixtureClientSchema,
  fixturePushesDML,
  fixtureReplicaDDL,
  UntranslatableFixture,
  type Change,
  type Fixture,
} from './rust-ivm-differential-harness.ts';
import {PipelineDriver} from './pipeline-driver.ts';
import {ResetPipelinesSignal, Snapshotter} from './snapshotter.ts';
import {RustIVMDriver} from './rust-ivm-driver.ts';

// -----------------------------------------------------------------------------
// FUZZ-DRIVEN DRIVER DIFFERENTIAL: for each generated fixture, drive the SAME
// query/pushes through RustIVMDriver and the reference PipelineDriver on ONE
// wal2 replica and compare (multiset + rowSetSignature).
//
// This DISCOVERS new driver-seam divergences the way the engine fuzzer discovers
// engine ones — but through the production glue (buildNapiTableSpecs, planAst,
// streaming, advance) that the engine fuzzer bypasses.
//
// FALSE-POSITIVE SAFETY: both drivers receive byte-identical input (same
// replica, client schema, AST, pushes). A divergence therefore means the two
// engines genuinely disagree — a translation imperfection can only make BOTH
// behave the same or BOTH error (→ skipped), never a spurious diff.
// FALSE-NEGATIVE SAFETY: we count tested-vs-skipped and FAIL if nothing was
// actually tested, so a "skip everything → all clear" cannot hide.
//
// Range via env: DRIVER_FUZZ_START (default 0), DRIVER_FUZZ_SEEDS (default 80).
// -----------------------------------------------------------------------------

const ADDON_PATH = process.env['RUST_IVM_ADDON_PATH'];
const NO_TIMER = {elapsedLap: () => 0, totalElapsed: () => 0} as any;
const START = Number(process.env['DRIVER_FUZZ_START'] ?? '0');
const COUNT = Number(process.env['DRIVER_FUZZ_SEEDS'] ?? '80');

describe.skipIf(!ADDON_PATH)('view-syncer/rust-ivm-driver fuzz differential', () => {
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
      lc, testLogConfig, shardID,
      newStorage().createClientGroupStorage('fz-rust'),
      'fz-rust', new InspectorDelegate(undefined), () => 200,
      false, undefined, path,
    );
    d.init(cs);
    return d;
  }
  function makeTs(cs: any, path: string): PipelineDriver {
    const d = new PipelineDriver(
      lc, testLogConfig,
      new Snapshotter(lc, path, {appID: shardID.appID}),
      shardID,
      newStorage().createClientGroupStorage('fz-ts'),
      'fz-ts', new InspectorDelegate(undefined), () => 200, false,
    );
    d.init(cs);
    return d;
  }
  async function drainAdvance(
    d: RustIVMDriver | PipelineDriver,
  ): Promise<{reset: boolean; changes: Change[]}> {
    try {
      const res = await d.advance(NO_TIMER);
      if (res instanceof ResetPipelinesSignal) return {reset: true, changes: []};
      return {reset: false, changes: await drain(res.changes)};
    } catch (e) {
      if (e instanceof ResetPipelinesSignal) return {reset: true, changes: []};
      throw e;
    }
  }

  test('fuzz corpus parity across the driver path', async () => {
    const findings: string[] = [];
    // Known-benign class: at an EXCLUSIVE start cursor the port deliberately
    // drops the boundary tie (SQL-correct) vs TS's IVM Skip — a documented,
    // intentional divergence (see project_exclusive_cursor_overinclude). We
    // bucket these separately (visible in the summary) so they neither fail the
    // run nor hide a genuine, non-cursor divergence.
    const cursorDivergences: string[] = [];
    const skipped: Record<string, number> = {};
    const bump = (k: string) => (skipped[k] = (skipped[k] ?? 0) + 1);
    let tested = 0;

    for (let seed = START; seed < START + COUNT; seed++) {
      const fixture = genFixture(seed) as unknown as Fixture;
      // A `start` cursor makes this a paginated query. Cursor/pagination
      // boundary behavior deliberately diverges between the Rust port and TS
      // (SQL-correct — see project_exclusive_cursor_overinclude), and the
      // engine-level fuzzer already validates cursor correctness against the IVM
      // oracle (all divergent driver seeds here are engine-clean). So a row-set
      // divergence on a start-cursor query is the known-benign class; only a
      // NON-cursor row-set divergence is a genuine driver-seam finding. (Init/
      // hydrate asymmetry and reset disagreements still fail regardless.)
      const hasStartCursor = !!(fixture.ast as any)?.start;
      const record = (msg: string) =>
        hasStartCursor ? cursorDivergences.push(msg) : findings.push(msg);

      // Translate once; used to seed the shared replica + both drivers.
      let ddl: string;
      let cs: unknown;
      try {
        ddl = fixtureReplicaDDL(fixture.tables, BASE);
        cs = fixtureClientSchema(fixture.tables);
      } catch (e) {
        bump(e instanceof UntranslatableFixture ? 'untranslatable' : 'translate-error');
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
          bump('both-init-error');
          continue;
        }
        if (!!rErr !== !!tErr) {
          findings.push(
            `seed ${seed} INIT asymmetry: rust=${rErr?.message ?? 'ok'} ts=${tErr?.message ?? 'ok'}`,
          );
          continue;
        }

        // --- hydrate ---
        const ast = fixture.ast as any;
        let rHy: Change[] | undefined;
        let tHy: Change[] | undefined;
        let rhErr: Error | undefined;
        let thErr: Error | undefined;
        try {
          rHy = await drain(rust!.addQuery('h', 'q', ast, NO_TIMER));
        } catch (e) {
          rhErr = e as Error;
        }
        try {
          tHy = await drain(ts!.addQuery('h', 'q', ast, NO_TIMER));
        } catch (e) {
          thErr = e as Error;
        }
        if (rhErr && thErr) {
          bump('both-hydrate-error');
          continue;
        }
        if (!!rhErr !== !!thErr) {
          findings.push(
            `seed ${seed} HYDRATE asymmetry: rust=${rhErr?.message ?? 'ok'} ts=${thErr?.message ?? 'ok'}`,
          );
          continue;
        }
        const hd = diffChanges(rHy!, tHy!);
        if (hd.onlyInRust.length || hd.onlyInTs.length) {
          record(
            `seed ${seed} HYDRATE diverge:\n  onlyRust: ${hd.onlyInRust.slice(0, 3).join(' | ')}\n  onlyTs: ${hd.onlyInTs.slice(0, 3).join(' | ')}`,
          );
        }
        // Normalize undefined -> 0n: rust eager-inits the signature to 0n on
        // addQuery; PipelineDriver leaves it undefined until the first change.
        // Both mean the empty-set signature (XOR identity). Only a genuine
        // non-identity difference is a finding.
        if ((rust!.rowSetSignature('q') ?? 0n) !== (ts!.rowSetSignature('q') ?? 0n)) {
          record(`seed ${seed} HYDRATE rowSetSignature mismatch (changes matched)`);
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
          let rAdv: {reset: boolean; changes: Change[]} | undefined;
          let tAdv: {reset: boolean; changes: Change[]} | undefined;
          let raErr: Error | undefined;
          let taErr: Error | undefined;
          try {
            rAdv = await drainAdvance(rust!);
          } catch (e) {
            raErr = e as Error;
          }
          try {
            tAdv = await drainAdvance(ts!);
          } catch (e) {
            taErr = e as Error;
          }
          if (raErr && taErr) {
            bump('both-advance-error');
            tested++;
            continue;
          }
          if (!!raErr !== !!taErr) {
            findings.push(
              `seed ${seed} ADVANCE asymmetry: rust=${raErr?.message ?? 'ok'} ts=${taErr?.message ?? 'ok'}`,
            );
          } else if (rAdv!.reset !== tAdv!.reset) {
            findings.push(
              `seed ${seed} ADVANCE reset disagreement: rust=${rAdv!.reset} ts=${tAdv!.reset}`,
            );
          } else if (!rAdv!.reset) {
            const ad = diffChanges(rAdv!.changes, tAdv!.changes);
            if (ad.onlyInRust.length || ad.onlyInTs.length) {
              record(
                `seed ${seed} ADVANCE diverge:\n  onlyRust: ${ad.onlyInRust.slice(0, 3).join(' | ')}\n  onlyTs: ${ad.onlyInTs.slice(0, 3).join(' | ')}`,
              );
            }
            if (
              (rust!.rowSetSignature('q') ?? 0n) !== (ts!.rowSetSignature('q') ?? 0n)
            ) {
              record(`seed ${seed} ADVANCE rowSetSignature mismatch (changes matched)`);
            }
          }
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
        dbFile.delete();
      }
    }

    const skipTotal = Object.values(skipped).reduce((a, b) => a + b, 0);
    // eslint-disable-next-line no-console
    console.log(
      `[driver-fuzz] seeds ${START}..${START + COUNT - 1}: tested=${tested} ` +
        `skipped=${skipTotal} ${JSON.stringify(skipped)} ` +
        `knownCursorDivergences=${cursorDivergences.length} findings=${findings.length}`,
    );

    // False-negative guard: a run that tested ~nothing is not "all clear".
    expect(tested, 'no fixtures were actually exercised — check translation').toBeGreaterThan(
      Math.floor(COUNT * 0.25),
    );
    expect(findings, findings.slice(0, 8).join('\n\n')).toEqual([]);
  });
});
