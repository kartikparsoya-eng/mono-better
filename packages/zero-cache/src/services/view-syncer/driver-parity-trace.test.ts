import {describe, expect, test} from 'vitest';
import {
  canonicalValue,
  DriverParityTrace,
  errorTrace,
  firstTraceDifference,
} from './driver-parity-trace.ts';

describe('driver parity trace', () => {
  test('canonicalization preserves types, missing fields, and sequence order', () => {
    expect(canonicalValue({value: undefined})).not.toEqual(canonicalValue({}));
    expect(canonicalValue([1, 2])).not.toEqual(canonicalValue([2, 1]));
    expect(canonicalValue(1)).not.toEqual(canonicalValue(1n));
    expect(canonicalValue(-0)).not.toEqual(canonicalValue(0));
    expect(canonicalValue(Number.NaN)).not.toEqual(canonicalValue(null));
    expect(canonicalValue(new Uint8Array([0, 255]))).toEqual({
      type: 'bytes',
      value: [0, 255],
    });
  });

  test('records yields, public state, and failure-atomic registration', async () => {
    const queries = new Map<string, unknown>();
    let fail = false;
    const driver = {
      initialized: () => true,
      replicaVersion: 'v0',
      currentVersion: () => 'v0',
      queries: () => queries,
      rowSetSignature: (queryID: string) =>
        queries.has(queryID) ? 42n : undefined,
      totalHydrationTimeMs: () => 7,
      addQuery: async function* (transformationHash: string, queryID: string) {
        if (fail) {
          throw new TypeError('hydrate failed');
        }
        yield {
          type: 0 as const,
          queryID,
          table: 'issues',
          rowKey: {id: '1'},
          row: {id: '1', transformationHash},
        };
        yield 'yield' as const;
        queries.set(queryID, {transformationHash});
      },
      removeQuery: (queryID: string) => queries.delete(queryID),
      getRow: () => undefined,
      reset: () => queries.clear(),
      advance: () => ({version: 'v0', numChanges: 0, changes: []}),
    };
    const trace = new DriverParityTrace(driver);
    const timer = {elapsedLap: () => 0, totalElapsed: () => 0};

    await trace.addQuery('hash', 'q1', {table: 'issues'}, timer);
    fail = true;
    await trace.addQuery('hash', 'q2', {table: 'issues'}, timer);

    const [success, failure] = trace.events();
    expect(success.outcome).toEqual({
      status: 'ok',
      value: canonicalValue([
        {
          kind: 'change',
          change: canonicalValue({
            type: 0,
            queryID: 'q1',
            table: 'issues',
            rowKey: {id: '1'},
            row: {id: '1', transformationHash: 'hash'},
          }),
        },
        {kind: 'yield'},
      ]),
    });
    expect(success.state).toMatchObject({
      queries: [
        {queryID: 'q1', rowSetSignature: {type: 'bigint', value: '42'}},
      ],
    });
    expect(failure.outcome).toMatchObject({
      status: 'error',
      error: {
        className: 'TypeError',
        name: 'TypeError',
        message: 'hydrate failed',
      },
    });
    expect(failure.state).toMatchObject({queries: [{queryID: 'q1'}]});
  });

  test('error trace retains class, message, code, reason, and cause', () => {
    const cause = new Error('sqlite busy');
    const error = new Error('advance failed', {cause}) as Error & {
      code: string;
      reason: string;
    };
    error.code = 'SQLITE_BUSY';
    error.reason = 'schema-change';

    expect(errorTrace(error)).toMatchObject({
      className: 'Error',
      name: 'Error',
      message: 'advance failed',
      code: {type: 'string', value: 'SQLITE_BUSY'},
      reason: {type: 'string', value: 'schema-change'},
    });
  });

  test('reports the first exact sequence difference', () => {
    expect(firstTraceDifference([1, {value: 2}], [1, {value: 3}])).toEqual({
      path: '$[1].value',
      rust: 2,
      ts: 3,
    });
  });
});
