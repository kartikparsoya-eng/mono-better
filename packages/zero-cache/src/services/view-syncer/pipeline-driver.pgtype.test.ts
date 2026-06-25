import {describe, expect, test, vi} from 'vitest';
import {pgTypeToGoType} from './pipeline-driver.ts';

// pgTypeToGoType maps a replica LiteTypeString (the upstream type plus
// `|`-attributes like |NOT_NULL / |TEXT_ENUM / |TEXT_ARRAY) to the column-type
// tag the Go sidecar understands. These tests pin the enum handling fixed after
// the P3 soak surfaced `[go-ivm pgType] unrecognised PostgreSQL type
// "TICKETPRIORITY"` warnings: a user-defined enum name isn't in the builtin
// lookup, but the `|TEXT_ENUM` attribute marks it as a string-backed enum —
// exactly as the canonical TS mapper (dataTypeToZqlValueType) treats it.
describe('view-syncer/pipeline-driver: pgTypeToGoType', () => {
  test('enum (|TEXT_ENUM) maps to string WITHOUT a warning', () => {
    const warn = vi.fn();
    // The real replica strings, e.g. SELECT ... pragma_table_info(tickets):
    expect(pgTypeToGoType('TicketPriority|NOT_NULL|TEXT_ENUM', warn)).toBe(
      'string',
    );
    expect(pgTypeToGoType('ActivityClassification|TEXT_ENUM', warn)).toBe(
      'string',
    );
    expect(warn).not.toHaveBeenCalled();
  });

  test('enum check wins over a builtin-looking enum name', () => {
    // An enum literally named "int8" must still be a string, not a number —
    // the |TEXT_ENUM attribute is authoritative and checked first.
    const warn = vi.fn();
    expect(pgTypeToGoType('int8|TEXT_ENUM', warn)).toBe('string');
    expect(warn).not.toHaveBeenCalled();
  });

  test('builtin scalar types are unchanged', () => {
    expect(pgTypeToGoType('bool')).toBe('boolean');
    expect(pgTypeToGoType('int8|NOT_NULL')).toBe('number');
    expect(pgTypeToGoType('timestamptz')).toBe('number');
    expect(pgTypeToGoType('varchar(255)|NOT_NULL')).toBe('string');
    expect(pgTypeToGoType('uuid')).toBe('string');
    expect(pgTypeToGoType('jsonb')).toBe('json');
  });

  test('PG array types map to json (matches Zero schema json<T[]>()', () => {
    // The Zero client schema types all PG array columns as json<T[]>(...):
    //   onCallSetNumbers: json<number[]>()  (PG: int4[])
    //   referenceTicket:  json<string[]>()  (PG: text[])
    //   to/cc/bcc/replyTo: json<string[]>() (PG: text[])
    //   deliveryMethods:  json<Enum[]>()     (PG: enum[])
    // The replicator stores these as JSON.stringify'd text in SQLite; both
    // sides must JSON.parse them into real arrays to match.
    expect(pgTypeToGoType('int4[]')).toBe('json');
    expect(pgTypeToGoType('text[]')).toBe('json');
    expect(pgTypeToGoType('varchar[]')).toBe('json');
    expect(pgTypeToGoType('int8[]')).toBe('json');
    expect(pgTypeToGoType('bool[]')).toBe('json');
  });

  test('a genuinely unknown NON-enum type still warns', () => {
    const warn = vi.fn();
    // A custom domain/composite that is not flagged as an enum: still falls
    // back to string, but SHOULD warn so the gap stays visible.
    expect(pgTypeToGoType('SomeUnmappedDomainType_xyz', warn)).toBe('string');
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0][0]).toContain('unrecognised');
  });

  test('PG array types no longer emit a warning (correctly mapped to json)', () => {
    const warn = vi.fn();
    expect(pgTypeToGoType('int4[]', warn)).toBe('json');
    expect(warn).not.toHaveBeenCalled();
  });

  test('enum-ARRAY columns map to json, NOT string (the array wins over the enum)', () => {
    // An enum-array LiteTypeString carries BOTH |TEXT_ENUM and |TEXT_ARRAY
    // (plus `[]`), e.g. `TicketPriority[]|TEXT_ENUM|TEXT_ARRAY`. The
    // replicator stores ALL array columns as JSON.stringify'd text, so the
    // container is json regardless of the element type. Previously isLiteEnum
    // fired first → 'string', causing Go's FromSQLiteType('string', ...) to
    // skip JSON parsing → +2-byte drift per enum-array column. The fix checks
    // isArray before isLiteEnum.
    const warn = vi.fn();
    expect(pgTypeToGoType('TicketPriority[]|TEXT_ENUM|TEXT_ARRAY', warn)).toBe(
      'json',
    );
    expect(
      pgTypeToGoType('TicketPriority[]|NOT_NULL|TEXT_ENUM|TEXT_ARRAY', warn),
    ).toBe('json');
    expect(
      pgTypeToGoType('ActivityClassification[]|TEXT_ENUM|TEXT_ARRAY', warn),
    ).toBe('json');
    expect(warn).not.toHaveBeenCalled();
  });

  test('legacy |TEXT_ARRAY[] form is detected as array → json', () => {
    // The legacy format puts `[]` in the attribute suffix rather than the
    // upstream type name (e.g. `int8|TEXT_ARRAY[]`). The old `t.endsWith('[]')`
    // check missed this because it tested the upstream name only. isArray
    // checks the full LiteTypeString for `|TEXT_ARRAY`, so it catches it.
    expect(pgTypeToGoType('int8|TEXT_ARRAY[]')).toBe('json');
    expect(pgTypeToGoType('text|TEXT_ARRAY[]')).toBe('json');
  });

  test('plain enum (non-array) still maps to string (no regression from the array-first reorder)', () => {
    const warn = vi.fn();
    expect(pgTypeToGoType('TicketPriority|TEXT_ENUM', warn)).toBe('string');
    expect(pgTypeToGoType('TicketPriority|NOT_NULL|TEXT_ENUM', warn)).toBe(
      'string',
    );
    expect(warn).not.toHaveBeenCalled();
  });
});
