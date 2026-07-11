import {describe, expect, test} from 'vitest';
import {pgTypeToGoType} from './pipeline-driver.ts';

// Reproduction and fix verification for the user_group_mappings 2-byte
// shadow drift.
//
// Root cause (fixed): pgTypeToGoType mapped PostgreSQL array types (INT4[],
// TEXT[], …) to 'string', but the Zero client schema types the same columns
// as json<number[]>() / json<string[]>() → 'json'.  This made the Go sidecar
// keep the raw SQLite TEXT value as a plain string, while TS parsed it into a
// real JS array via JSON.parse.  When the shadow comparator called
// stableStringify on each row, the string got two extra " quote characters
// that the array did not — exactly +2 bytes per array column.
//
// Fix: map PG array types to 'json' in pgTypeToGoType, so Go's
// FromSQLiteType('json', ...) does json.Unmarshal — matching TS's JSON.parse.

// --- stableStringify (copied from pipeline-driver.ts:4996, not exported) ---
function stableStringify(v: unknown): string {
  if (v === undefined) return '"__undef__"';
  if (v === null) return 'null';
  if (typeof v === 'bigint') {
    if (
      v <= BigInt(Number.MAX_SAFE_INTEGER) &&
      v >= BigInt(Number.MIN_SAFE_INTEGER)
    ) {
      return String(Number(v));
    }
    return `"__bigint:${v.toString()}__"`;
  }
  if (typeof v === 'number') {
    if (Number.isNaN(v)) return '"__nan__"';
    if (v === Infinity) return '"__inf__"';
    if (v === -Infinity) return '"__-inf__"';
    return JSON.stringify(v);
  }
  if (typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) {
    return '[' + v.map(stableStringify).join(',') + ']';
  }
  const obj = v as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return (
    '{' +
    keys.map(k => JSON.stringify(k) + ':' + stableStringify(obj[k])).join(',') +
    '}'
  );
}

// --- TS fromSQLiteType (table-source.ts:607, simulated) ---
function tsFromSQLiteType(valueType: string, v: unknown): unknown {
  if (v === null) return null;
  switch (valueType) {
    case 'boolean':
      return !!v;
    case 'number':
    case 'string':
    case 'null':
      return v;
    case 'json':
      return JSON.parse(v as string);
  }
  return v;
}

// --- Go FromSQLiteType (query_builder.go:360, simulated) ---
function goFromSQLiteType(colType: string, v: unknown): unknown {
  if (v === null) return null;
  switch (colType) {
    case 'string':
      return v; // raw passthrough
    case 'json':
      return JSON.parse(v as string);
    case 'boolean':
      return !!v;
    case 'number':
      return Number(v);
    default:
      return v;
  }
}

// --- toSQLiteType (query-builder.ts:183, what the replicator writes) ---
function toSQLiteType(type: string, v: unknown): unknown {
  switch (type) {
    case 'json':
      return JSON.stringify(v);
    default:
      return v;
  }
}

describe('user_group_mappings 2-byte shadow drift (fixed)', () => {
  // The Zero client schema declares onCallSetNumbers as json<number[]>().
  // The upstream PG type is INTEGER[] → lite type string "int4[]".
  // The SQLite replica stores the value as TEXT via toSQLiteType('json', [1,2,3])
  // → JSON.stringify([1,2,3]) → "[1,2,3]" (the 7-char string [1,2,3]).

  const SQLITE_VALUE = toSQLiteType('json', [1, 2, 3]); // "[1,2,3]"

  test('FIX: pgTypeToGoType maps INT4[] to json (was string)', () => {
    expect(pgTypeToGoType('int4[]')).toBe('json');
    expect(pgTypeToGoType('text[]')).toBe('json');
    expect(pgTypeToGoType('varchar[]')).toBe('json');
  });

  test('TS parses the SQLite value as a JSON array (json column type)', () => {
    const tsValue = tsFromSQLiteType('json', SQLITE_VALUE);
    expect(Array.isArray(tsValue)).toBe(true);
    expect(tsValue).toEqual([1, 2, 3]);
  });

  test('FIX: Go now also parses the SQLite value as a JSON array', () => {
    const goColType = pgTypeToGoType('int4[]'); // now 'json'
    const goValue = goFromSQLiteType(goColType, SQLITE_VALUE);
    expect(Array.isArray(goValue)).toBe(true);
    expect(goValue).toEqual([1, 2, 3]);
  });

  test('FIX: stableStringify produces identical output for Go and TS', () => {
    const tsValue = tsFromSQLiteType('json', SQLITE_VALUE); // [1,2,3]
    const goColType = pgTypeToGoType('int4[]'); // 'json'
    const goValue = goFromSQLiteType(goColType, SQLITE_VALUE); // [1,2,3]

    const tsStr = stableStringify(tsValue);
    const goStr = stableStringify(goValue);

    expect(tsStr).toBe('[1,2,3]');
    expect(goStr).toBe('[1,2,3]');
    expect(goStr).toBe(tsStr);
    expect(goStr.length).toBe(tsStr.length);
  });

  test('FIX: full row stableStringify has zero drift', () => {
    const sqliteRow = {
      id: 'cmoqxushi0000abcd',
      userId: 'user123',
      userGroupId: 'group456',
      responsibility: 'MEMBER',
      onCallSetNumber: 1,
      onCallSetNumbers: SQLITE_VALUE,
      createdAt: 1719000000000,
      updatedAt: 1719000000000,
    };

    const tsRow: Record<string, unknown> = {};
    const goRow: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(sqliteRow)) {
      let tsType: string, goType: string;
      if (k === 'onCallSetNumbers') {
        tsType = 'json';
        goType = pgTypeToGoType('int4[]'); // 'json' — the fix
      } else if (
        k === 'createdAt' ||
        k === 'updatedAt' ||
        k === 'onCallSetNumber'
      ) {
        tsType = 'number';
        goType = 'number';
      } else {
        tsType = 'string';
        goType = 'string';
      }
      tsRow[k] = tsFromSQLiteType(tsType, v);
      goRow[k] = goFromSQLiteType(goType, v);
    }

    expect(stableStringify(goRow)).toBe(stableStringify(tsRow));
    expect(stableStringify(goRow).length).toBe(stableStringify(tsRow).length);
  });

  test('OLD behavior (string mapping) would have produced +2 byte drift', () => {
    // Document the old broken behavior for posterity. If pgTypeToGoType
    // still mapped to 'string', Go would keep the raw "[1,2,3]" string,
    // and stableStringify would wrap it in quotes (+2 bytes).
    const oldValue = goFromSQLiteType('string', SQLITE_VALUE); // raw "[1,2,3]"
    const oldStr = stableStringify(oldValue); // '"[1,2,3]"'
    const tsStr = stableStringify(tsFromSQLiteType('json', SQLITE_VALUE)); // '[1,2,3]'

    expect(oldStr.length - tsStr.length).toBe(2);
    expect(oldStr).toBe('"[1,2,3]"');
    expect(tsStr).toBe('[1,2,3]');
  });

  test('drift would scale with array length but delta was always exactly +2', () => {
    // The +2 was from the two quote chars JSON.stringify adds around a string.
    // It did NOT depend on array length — it was always exactly 2.
    const cases = [[1], [1, 2], [1, 2, 3], [1, 2, 3, 4, 5], [42], []];
    for (const arr of cases) {
      const sv = toSQLiteType('json', arr);
      const ts = stableStringify(tsFromSQLiteType('json', sv));
      const oldGo = stableStringify(goFromSQLiteType('string', sv));
      expect(oldGo.length - ts.length).toBe(2);
    }
  });
});
