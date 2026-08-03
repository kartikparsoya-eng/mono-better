import {describe, expect, test} from 'vitest';
import {parse, RawJSON, stringify} from './bigint-json.ts';

describe('types/json', () => {
  type Case = {
    serialized: string;
    deserialized: unknown;
  };

  const cases: Case[] = [
    {
      serialized: '9007199254740991',
      deserialized: 9007199254740991,
    },
    {
      serialized: '9007199254740993',
      deserialized: 9007199254740993n,
    },
    {
      serialized: '{"big":90071992547409930000000000}',
      deserialized: {big: 90071992547409930000000000n},
    },
  ];

  for (const c of cases) {
    test(c.serialized, () => {
      expect(parse(c.serialized)).toEqual(c.deserialized);
      expect(stringify(c.deserialized)).toBe(c.serialized);
    });
  }
});

describe('RawJSON splicing', () => {
  test('splices pre-encoded JSON verbatim instead of escaping it', () => {
    const out = stringify({
      a: 1,
      row: new RawJSON('{"id":"r1","n":42,"ok":true}'),
    });
    expect(out).toBe('{"a":1,"row":{"id":"r1","n":42,"ok":true}}');
    // and the result is real JSON, not a string-wrapped blob
    expect(JSON.parse(out)).toEqual({a: 1, row: {id: 'r1', n: 42, ok: true}});
  });

  test('coexists with bigint serialization', () => {
    const out = stringify({
      big: 9007199254740993n,
      row: new RawJSON('{"id":"r1"}'),
    });
    expect(out).toBe('{"big":9007199254740993,"row":{"id":"r1"}}');
  });

  test('splices arrays, nulls and nested objects', () => {
    expect(stringify({v: new RawJSON('[1,2,3]')})).toBe('{"v":[1,2,3]}');
    expect(stringify({v: new RawJSON('null')})).toBe('{"v":null}');
    expect(stringify({v: new RawJSON('{"a":{"b":[1]}}')})).toBe(
      '{"v":{"a":{"b":[1]}}}',
    );
  });

  test('a plain string is still escaped as a string', () => {
    // guards against the splice accidentally applying to ordinary strings
    expect(stringify({v: '{"id":"r1"}'})).toBe('{"v":"{\\"id\\":\\"r1\\"}"}');
  });

  test('round-trips through parse', () => {
    const out = stringify({row: new RawJSON('{"id":"r1","n":42}')});
    expect(parse(out)).toEqual({row: {id: 'r1', n: 42}});
  });
});
