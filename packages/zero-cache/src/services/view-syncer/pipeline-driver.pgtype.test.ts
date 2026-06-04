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

  test('a genuinely unknown NON-enum type still warns', () => {
    const warn = vi.fn();
    // A custom domain/composite that is not flagged as an enum: still falls
    // back to string, but SHOULD warn so the gap stays visible.
    expect(pgTypeToGoType('SomeUnmappedDomainType_xyz', warn)).toBe('string');
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0][0]).toContain('unrecognised');
  });
});
