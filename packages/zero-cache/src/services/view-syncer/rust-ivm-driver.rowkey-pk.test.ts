import './rust-ivm-addon-setup.ts'; // MUST be first: guarantees the wal2 addon.
import {describe, expect, test} from 'vitest';
import type {LiteAndZqlSpec} from '../../db/specs.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import {buildNapiTableSpecs} from './rust-ivm-driver.ts';

// Regression + discovery guard for the rowKey primary-key derivation bug.
//
// THE BUG (rust-ivm-driver.ts): the specs handed to the napi engine were keyed
// by `spec.tableSpec.primaryKey` — the LiteSpec/replica primary key — instead of
// the CLIENT-schema primaryKey. For tables whose Zero primaryKey column differs
// from the replica PK column (messages -> messageId, conversations ->
// conversationId, reactions -> reactionId, ...), the engine emitted rowKeys
// keyed by the wrong column. The client then looked up its declared PK column,
// found it absent, and crashed in toPrimaryKeyString with "Expected string,
// number or boolean. Got undefined" -> Zero error state -> mutation rejected ->
// disconnect ("Connection Lost" / "Messages loading forever").
//
// WHY IT WAS INVISIBLE: the differential fuzzer exercises the ENGINE directly
// (engine.init with hand-built specs where replica-PK == client-PK == 'id'), so
// this driver-seam derivation never ran and the divergence could not even be
// expressed. These tests drive the exact seam (buildNapiTableSpecs) and, crucially,
// randomize the divergence so a NEW mismatch in the derivation is caught, not just
// this one.

// Minimal LiteAndZqlSpec fixture. buildNapiTableSpecs reads only tableSpec.{primaryKey,
// uniqueKeys, minRowVersion} and zqlSpec.
function liteSpec(
  replicaPK: string[],
  cols: string[],
  uniqueKeys: string[][] = [replicaPK],
): LiteAndZqlSpec {
  const zqlSpec: Record<string, {type: string; optional?: boolean}> = {};
  for (const c of cols) zqlSpec[c] = {type: 'string', optional: false};
  return {
    tableSpec: {
      primaryKey: replicaPK as unknown as PrimaryKey,
      uniqueKeys: uniqueKeys as unknown as PrimaryKey[],
      minRowVersion: null,
    },
    zqlSpec,
  } as unknown as LiteAndZqlSpec;
}

describe('rust-ivm-driver buildNapiTableSpecs rowKey PK derivation', () => {
  test('keys rows by the CLIENT-schema PK, not the LiteSpec/replica PK', () => {
    // messages: replica PK = ['id'] (surrogate), client PK = ['messageId'].
    const tableSpecs = new Map<string, LiteAndZqlSpec>([
      ['messages', liteSpec(['id'], ['id', 'messageId', 'body'])],
    ]);
    const primaryKeys = new Map<string, PrimaryKey>([
      ['messages', ['messageId'] as unknown as PrimaryKey],
    ]);

    const [spec] = buildNapiTableSpecs(tableSpecs, primaryKeys);

    // The bug shipped ['id'] here; the fix ships the client PK.
    expect(spec.primaryKey).toEqual(['messageId']);
    // uniqueKeys still come from the LiteSpec (drives scalar-EXISTS resolution).
    expect(spec.uniqueKeys).toEqual([['id']]);
  });

  test('falls back to LiteSpec PK for tables absent from the client-schema map', () => {
    // A server-only/permissions table not in the client schema keeps its LiteSpec PK.
    const tableSpecs = new Map<string, LiteAndZqlSpec>([
      ['perms', liteSpec(['id'], ['id'])],
    ]);
    const [spec] = buildNapiTableSpecs(tableSpecs, new Map());
    expect(spec.primaryKey).toEqual(['id']);
  });

  // DISCOVERY: randomize divergent PKs across many tables and assert the invariant
  // "engine PK == client PK for every client-schema table" always holds. Any future
  // regression in the derivation (not just line 449) fails here.
  test('property: engine PK always equals the client PK for client-schema tables', () => {
    const names = ['messageId', 'conversationId', 'reactionId', 'countId', 'id'];
    for (let seed = 0; seed < 200; seed++) {
      const tableSpecs = new Map<string, LiteAndZqlSpec>();
      const primaryKeys = new Map<string, PrimaryKey>();
      const nTables = 1 + (seed % 4);
      for (let t = 0; t < nTables; t++) {
        const table = `t${t}`;
        const clientPKName = names[(seed + t) % names.length];
        // Replica PK deliberately diverges: always a surrogate 'id'.
        const cols = ['id', clientPKName, 'v'];
        tableSpecs.set(table, liteSpec(['id'], cols));
        primaryKeys.set(table, [clientPKName] as unknown as PrimaryKey);
      }
      const specs = buildNapiTableSpecs(tableSpecs, primaryKeys);
      for (const s of specs) {
        expect(s.primaryKey).toEqual([...(primaryKeys.get(s.table) ?? [])]);
        // The client PK column must be a real column of the table (else the rowKey
        // is missing/undefined on the client — the exact failure we guard against).
        for (const pkCol of s.primaryKey) {
          expect(Object.keys(s.columns)).toContain(pkCol);
        }
      }
    }
  });
});
