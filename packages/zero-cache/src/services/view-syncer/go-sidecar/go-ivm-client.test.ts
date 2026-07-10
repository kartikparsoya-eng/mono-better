// Unit tests for the streaming accumulators used by GoIVMClient.
//
// These cover the defensive throw paths (chunk-order violation,
// orphan queries, missing-final) that the integration soak can't
// reliably exercise — the soak validates the happy path, but the
// throws only fire if the Go side bugs, which it didn't during
// soak. Without these tests the throw code would be dead until
// the first wire-level regression.
//
// We test the factory functions directly rather than spinning up
// a fake Unix socket — the accumulator is a pure state machine
// that's already isolated from network/transport concerns.

import {describe, expect, test, vi} from 'vitest';
import {
  createAdvanceToHeadStreamChunkAccumulator,
  createHydrateStreamAccumulator,
  GoIVMClient,
  type AdvanceToHeadStreamChunk,
  type RowChange,
  unpack,
} from './go-ivm-client.ts';

const row = (id: string, queryID = 'q1'): RowChange => ({
  type: 0,
  queryID,
  table: 't',
  rowKey: {id},
  row: {id},
});

// Positional (rev-9) INPUT frame carrying ADDs for `ids` — the wire twin of
// `ids.map(id => row(id, queryID))`; extractChanges decodes it back to exactly
// that. The legacy map-keyed `changes` INPUT shape is gone from both the wire
// (rev-9 Go emits positional only) and the client (fallback deleted in the
// RPC-surface cleanup) — these fixtures now exercise the real decode.
const posFrame = (ids: string[], queryID = 'q1') => ({
  d: [{q: queryID, t: 't', c: ['id'], k: ['id']}],
  r: ids.map(id => [0, 0, id] as unknown[]),
});

describe('createHydrateStreamAccumulator', () => {
  test('single-chunk fast path: one frame with final=true delivers result', () => {
    const onResult = vi.fn();
    const h = createHydrateStreamAccumulator(onResult);

    h.onFrame({
      queryID: 'q1',
      ...posFrame(['a', 'b']),
      chunkIndex: 0,
      final: true,
      timingMs: 5,
    });
    h.finish();

    expect(onResult).toHaveBeenCalledTimes(1);
    expect(onResult).toHaveBeenCalledWith({
      queryID: 'q1',
      changes: [row('a'), row('b')],
      timingMs: 5,
    });
  });

  test('final frame carries Go row-set signature delta', () => {
    const onResult = vi.fn();
    const h = createHydrateStreamAccumulator(onResult);

    h.onFrame({
      queryID: 'q1',
      ...posFrame(['a']),
      chunkIndex: 0,
      final: true,
      timingMs: 5,
      sigDelta: '1577071b99a74425',
    });
    h.finish();

    expect(onResult).toHaveBeenCalledWith({
      queryID: 'q1',
      changes: [row('a')],
      timingMs: 5,
      sigDelta: '1577071b99a74425',
    });
  });

  test('multi-chunk: accumulates rows in order, fires onResult only on final', () => {
    const onResult = vi.fn();
    const h = createHydrateStreamAccumulator(onResult);

    h.onFrame({queryID: 'q1', ...posFrame(['a']), chunkIndex: 0, final: false});
    h.onFrame({queryID: 'q1', ...posFrame(['b']), chunkIndex: 1, final: false});

    // Should not have fired yet — final hasn't arrived
    expect(onResult).not.toHaveBeenCalled();

    h.onFrame({
      queryID: 'q1',
      ...posFrame(['c']),
      chunkIndex: 2,
      final: true,
      timingMs: 12,
    });
    h.finish();

    expect(onResult).toHaveBeenCalledTimes(1);
    expect(onResult).toHaveBeenCalledWith({
      queryID: 'q1',
      changes: [row('a'), row('b'), row('c')],
      timingMs: 12,
    });
  });

  test('multiple queries: independent per-queryID accumulation', () => {
    const onResult = vi.fn();
    const h = createHydrateStreamAccumulator(onResult);

    // Interleave q1 and q2 frames — completion order may differ from input order
    h.onFrame({queryID: 'q1', ...posFrame(['a']), chunkIndex: 0, final: false});
    h.onFrame({
      queryID: 'q2',
      ...posFrame(['x'], 'q2'),
      chunkIndex: 0,
      final: true,
      timingMs: 3,
    });
    h.onFrame({
      queryID: 'q1',
      ...posFrame(['b']),
      chunkIndex: 1,
      final: true,
      timingMs: 7,
    });
    h.finish();

    expect(onResult).toHaveBeenCalledTimes(2);
    // q2 finalized first
    expect(onResult).toHaveBeenNthCalledWith(1, {
      queryID: 'q2',
      changes: [row('x', 'q2')],
      timingMs: 3,
    });
    expect(onResult).toHaveBeenNthCalledWith(2, {
      queryID: 'q1',
      changes: [row('a'), row('b')],
      timingMs: 7,
    });
  });

  test('chunk-order gap: skipped index throws immediately', () => {
    const onResult = vi.fn();
    const h = createHydrateStreamAccumulator(onResult);

    h.onFrame({queryID: 'q1', ...posFrame(['a']), chunkIndex: 0, final: false});
    // Skip chunkIndex=1, jump to 2 — should throw
    expect(() =>
      h.onFrame({
        queryID: 'q1',
        ...posFrame(['b']),
        chunkIndex: 2,
        final: true,
      }),
    ).toThrow(
      /addQueriesStream chunk order violation for queryID=q1: expected chunkIndex=1, got 2/,
    );
    expect(onResult).not.toHaveBeenCalled();
  });

  test('chunk-order duplicate: same index twice throws', () => {
    const onResult = vi.fn();
    const h = createHydrateStreamAccumulator(onResult);

    h.onFrame({queryID: 'q1', ...posFrame(['a']), chunkIndex: 0, final: false});
    expect(() =>
      h.onFrame({
        queryID: 'q1',
        ...posFrame(['b']),
        chunkIndex: 0,
        final: false,
      }),
    ).toThrow(/expected chunkIndex=1, got 0/);
  });

  test('orphan query: finish() throws if a queryID never received final=true', () => {
    const onResult = vi.fn();
    const h = createHydrateStreamAccumulator(onResult);

    // q1 finalizes, q2 doesn't
    h.onFrame({queryID: 'q1', chunkIndex: 0, final: true, timingMs: 1});
    h.onFrame({
      queryID: 'q2',
      ...posFrame(['x'], 'q2'),
      chunkIndex: 0,
      final: false,
    });

    expect(() => h.finish()).toThrow(
      /addQueriesStream finished but 1 queries never received a final chunk: q2/,
    );
    expect(onResult).toHaveBeenCalledTimes(1);
    expect(onResult).toHaveBeenCalledWith(
      expect.objectContaining({queryID: 'q1'}),
    );
  });

  test('empty query: zero rows + final=true still delivers empty changes', () => {
    const onResult = vi.fn();
    const h = createHydrateStreamAccumulator(onResult);

    h.onFrame({queryID: 'q1', chunkIndex: 0, final: true, timingMs: 0});
    h.finish();

    expect(onResult).toHaveBeenCalledWith({
      queryID: 'q1',
      changes: [],
      timingMs: 0,
    });
  });

  test('legacy sidecar compat: frame without final defaults to final=true', () => {
    // Pre-protocol-rev-3 sidecars sent partial frames without `final` —
    // accumulator treats them as single-frame and delivers immediately.
    const onResult = vi.fn();
    const h = createHydrateStreamAccumulator(onResult);

    h.onFrame({queryID: 'q1', ...posFrame(['a']), timingMs: 4}); // no chunkIndex, no final
    h.finish();

    expect(onResult).toHaveBeenCalledWith({
      queryID: 'q1',
      changes: [row('a')],
      timingMs: 4,
    });
  });
});

function advanceChunkHarness(opts?: {rowMode?: boolean}) {
  const chunks: AdvanceToHeadStreamChunk[] = [];
  const h = createAdvanceToHeadStreamChunkAccumulator(
    chunk => chunks.push(chunk),
    opts,
  );
  return {h, chunks};
}

describe('createAdvanceToHeadStreamChunkAccumulator', () => {
  test('single-chunk: one final frame yields one final chunk', () => {
    const {h, chunks} = advanceChunkHarness();
    h.onFrame({
      ...posFrame(['a', 'b']),
      chunkIndex: 0,
      final: true,
      timings: [{table: 't', type: 0, ms: 5}],
      version: '0000000009',
      numChanges: 2,
    });
    h.finish();
    expect(chunks).toEqual([
      {
        changes: [row('a'), row('b')],
        final: true,
        chunkIndex: 0,
        version: '0000000009',
        numChanges: 2,
        timings: [{table: 't', type: 0, ms: 5}],
        reset: undefined,
        sigDeltas: undefined,
      },
    ]);
  });

  test('final frame carries Go row-set signature deltas', () => {
    const {h, chunks} = advanceChunkHarness();
    h.onFrame({
      ...posFrame(['a']),
      chunkIndex: 0,
      final: true,
      version: '0000000009',
      numChanges: 1,
      sigDeltas: {q1: '1577071b99a74425'},
    });
    h.finish();

    expect(chunks).toEqual([
      expect.objectContaining({
        changes: [row('a')],
        final: true,
        sigDeltas: {q1: '1577071b99a74425'},
      }),
    ]);
  });

  test('multi-chunk: rows are emitted before final metadata', () => {
    const {h, chunks} = advanceChunkHarness();
    h.onFrame({...posFrame(['a']), chunkIndex: 0, final: false});
    expect(chunks).toEqual([
      {
        changes: [row('a')],
        final: false,
        chunkIndex: 0,
      },
    ]);

    h.onFrame({...posFrame(['b']), chunkIndex: 1, final: false});
    expect(chunks.at(-1)).toEqual({
      changes: [row('b')],
      final: false,
      chunkIndex: 1,
    });

    h.onFrame({
      ...posFrame(['c']),
      chunkIndex: 2,
      final: true,
      timings: [{table: 't', type: 1, ms: 2}],
      version: '0000000010',
      numChanges: 3,
    });
    h.finish();
    expect(chunks).toEqual([
      {changes: [row('a')], final: false, chunkIndex: 0},
      {changes: [row('b')], final: false, chunkIndex: 1},
      {
        changes: [row('c')],
        final: true,
        chunkIndex: 2,
        version: '0000000010',
        numChanges: 3,
        timings: [{table: 't', type: 1, ms: 2}],
        reset: undefined,
        sigDeltas: undefined,
      },
    ]);
  });

  test('version/numChanges on a non-final frame are ignored — only final counts', () => {
    // A buggy sidecar that stamped metadata on a non-final frame must not
    // corrupt the watermark: the final frame is authoritative.
    const {h, chunks} = advanceChunkHarness();
    h.onFrame({
      ...posFrame(['a']),
      chunkIndex: 0,
      final: false,
      version: 'WRONG',
      numChanges: 999,
    });
    h.onFrame({
      ...posFrame(['b']),
      chunkIndex: 1,
      final: true,
      version: '0000000011',
      numChanges: 2,
    });
    h.finish();
    expect(chunks[0]).toEqual({
      changes: [row('a')],
      final: false,
      chunkIndex: 0,
    });
    expect(chunks[1]).toMatchObject({
      changes: [row('b')],
      final: true,
      version: '0000000011',
      numChanges: 2,
    });
  });

  test('reset frame: single final frame with reset + version, no rows', () => {
    const {h, chunks} = advanceChunkHarness();
    h.onFrame({
      chunkIndex: 0,
      final: true,
      version: '0000000012',
      reset: {reason: 'truncation', msg: 'table issue has been truncated'},
    });
    h.finish();
    expect(chunks).toHaveLength(1);
    expect(chunks[0].changes).toEqual([]);
    expect(chunks[0].reset).toEqual({
      reason: 'truncation',
      msg: 'table issue has been truncated',
    });
    expect(chunks[0].version).toBe('0000000012');
  });

  test('empty advance: one final frame with no changes', () => {
    const {h, chunks} = advanceChunkHarness();
    h.onFrame({chunkIndex: 0, final: true, version: '0000000013'});
    h.finish();
    expect(chunks).toEqual([
      {
        changes: [],
        final: true,
        chunkIndex: 0,
        version: '0000000013',
        numChanges: 0,
        timings: undefined,
        reset: undefined,
        sigDeltas: undefined,
      },
    ]);
  });

  test('chunk-order gap throws immediately', () => {
    const {h} = advanceChunkHarness();
    h.onFrame({...posFrame(['a']), chunkIndex: 0, final: false});
    expect(() =>
      h.onFrame({...posFrame(['b']), chunkIndex: 2, final: true}),
    ).toThrow(
      /advanceToHeadStream chunk order violation: expected chunkIndex=1, got 2/,
    );
  });

  test('missing final: finish() throws if no frame had final=true', () => {
    const {h} = advanceChunkHarness();
    h.onFrame({...posFrame(['a']), chunkIndex: 0, final: false});
    h.onFrame({...posFrame(['b']), chunkIndex: 1, final: false});
    expect(() => h.finish()).toThrow(
      /advanceToHeadStream finished without a final chunk/,
    );
  });

  test('post-final frame throws (audit fix #18)', () => {
    // Same as advanceStream: a chunk after the terminal frame must throw.
    const {h} = advanceChunkHarness();
    h.onFrame({
      ...posFrame(['a']),
      chunkIndex: 0,
      final: true,
      version: '0000000005',
    });
    expect(() =>
      h.onFrame({...posFrame(['b']), chunkIndex: 1, final: false}),
    ).toThrow(
      /advanceToHeadStream received chunk \(index=1\) after final frame/,
    );
  });

  // --- rowMode (NAPI transport, two-plane delivery) ---

  test('rowMode: rows via onRow emit chunks before final frame', () => {
    const {h, chunks} = advanceChunkHarness({rowMode: true});
    // Rows arrive individually as records; row-bearing partials produce NO
    // frame at all, so the final frame's chunkIndex has a gap.
    h.onRow(row('a'));
    expect(chunks).toEqual([{changes: [row('a')], final: false}]);
    h.onRow(row('b'));
    expect(chunks.at(-1)).toEqual({changes: [row('b')], final: false});
    h.onFrame({
      chunkIndex: 2,
      final: true,
      timings: [{table: 't', type: 0, ms: 3}],
      version: '0000000011',
      numChanges: 2,
    });
    h.finish();
    expect(chunks.at(-1)).toEqual({
      changes: [],
      final: true,
      chunkIndex: 2,
      version: '0000000011',
      numChanges: 2,
      timings: [{table: 't', type: 0, ms: 3}],
      reset: undefined,
      sigDeltas: undefined,
    });
  });

  test('rowMode: fallback rows in frames interleave with records in queue order', () => {
    const {h, chunks} = advanceChunkHarness({rowMode: true});
    h.onRow(row('a'));
    // A non-encodable change rides a fallback frame between records.
    h.onFrame({...posFrame(['b']), chunkIndex: 1, final: false});
    h.onRow(row('c'));
    h.onFrame({
      chunkIndex: 3,
      final: true,
      version: '0000000012',
      numChanges: 3,
    });
    h.finish();
    expect(chunks.flatMap(c => c.changes)).toEqual([
      row('a'),
      row('b'),
      row('c'),
    ]);
    expect(chunks.at(-1)).toMatchObject({
      changes: [],
      final: true,
      version: '0000000012',
      numChanges: 3,
    });
  });

  test('rowMode: chunkIndex gaps allowed, backwards still throws', () => {
    const {h} = advanceChunkHarness({rowMode: true});
    h.onFrame({...posFrame(['a']), chunkIndex: 3, final: false});
    expect(() =>
      h.onFrame({...posFrame(['b']), chunkIndex: 2, final: false}),
    ).toThrow(/advanceToHeadStream chunk order violation/);
  });

  test('rowMode: row record after final frame throws', () => {
    const {h} = advanceChunkHarness({rowMode: true});
    h.onFrame({chunkIndex: 0, final: true, version: '0000000013'});
    expect(() => h.onRow(row('x'))).toThrow(
      /advanceToHeadStream received row record after final frame/,
    );
  });
});

// Cross-language contract: a positional (protocolRev 9) frame — as produced by
// the Go side's toPositional (positional.go) — must decode through the
// accumulator into the same RowChange[] the legacy map-keyed frame produced.
// Column order in `c` is sorted (matching Go's sortedSetKeys), so the value
// arrays follow that order; rowKey is derived from `k`.
describe('positional (rev 9) frame decoding', () => {
  test('hydrate accumulator decodes a positional frame (add/remove, 2 tables)', () => {
    const onResult = vi.fn();
    const h = createHydrateStreamAccumulator(onResult);

    h.onFrame({
      queryID: 'q1',
      d: [
        {
          q: 'q1',
          t: 'conversations',
          c: ['_0_version', 'channelId', 'conversationId', 'createdAt'],
          k: ['conversationId'],
        },
        {
          q: 'q1',
          t: 'channel_user_status',
          c: ['_0_version', 'channelId', 'userId'],
          k: ['channelId', 'userId'],
        },
      ],
      r: [
        [0, 0, '0abc', 'ch1', 'c1', 1779813865070], // add conversations c1
        [1, 0, '0abe', 'ch1', 'u1'], // add channel_user_status
        [0, 1, 'c3'], // remove conversations c3 (PK only)
      ],
      chunkIndex: 0,
      final: true,
    });

    expect(onResult).toHaveBeenCalledTimes(1);
    const {changes} = onResult.mock.calls[0][0] as {changes: RowChange[]};
    expect(changes).toEqual([
      {
        type: 0,
        queryID: 'q1',
        table: 'conversations',
        rowKey: {conversationId: 'c1'},
        row: {
          _0_version: '0abc',
          channelId: 'ch1',
          conversationId: 'c1',
          createdAt: 1779813865070,
        },
      },
      {
        type: 0,
        queryID: 'q1',
        table: 'channel_user_status',
        rowKey: {channelId: 'ch1', userId: 'u1'},
        row: {_0_version: '0abe', channelId: 'ch1', userId: 'u1'},
      },
      // remove carries no row (rowKey only)
      {
        type: 1,
        queryID: 'q1',
        table: 'conversations',
        rowKey: {conversationId: 'c3'},
      },
    ]);
  });

  test('empty positional frame (no r) yields no changes', () => {
    const {h, chunks} = advanceChunkHarness();
    h.onFrame({chunkIndex: 0, final: true}); // neither `changes` nor `r`
    h.finish();
    expect(chunks[0].changes).toEqual([]);
  });

  test('positional frame with r: null (msgpack-null) yields no changes', () => {
    // Simulates a Go encoder that dropped `omitempty` on the Rows field: a nil
    // slice then encodes as msgpack-null, so the decoded frame carries
    // r === null rather than r omitted. Before extractChanges guarded null
    // (not just undefined) this reached decodePositionalChanges(…, null) ->
    // null.length -> TypeError, orphaning the advance RPC. Must be inert.
    const {h, chunks} = advanceChunkHarness();
    h.onFrame({chunkIndex: 0, final: true, r: null});
    h.finish();
    expect(chunks[0].changes).toEqual([]);
  });

  // Wire-level contract: the bytes below are EXACTLY what the Go sidecar's
  // production mpMarshal (vmihailenco/msgpack, UseCompactInts, json tags)
  // emits for the canonical hydrate frame — captured from positionalWireFixture
  // in positional_wire_test.go. Decoding them here through the PRODUCTION
  // msgpackr `unpack` crosses the real vmihailenco-encode → msgpackr-decode
  // boundary that the object-level tests above (which start from hand-built
  // {d, r}) never touch; until now only the ephemeral shadow soak exercised it.
  //
  // The expected RowChange[] is identical to the hand-built test above — that's
  // the point: it proves the hand-built objects faithfully model Go's real bytes.
  //
  // To regenerate after an intentional wire-format change, run:
  //   cd go-ivm && go test ./cmd/sidecar -run TestPositionalWireFixture -v
  // and copy the printed FIXTURE_BASE64 here AND into goldenHydrateFrameB64.
  const GO_WIRE_FIXTURE_B64 =
    'hqdxdWVyeUlEonExoWSShKFxonExoXStY29udmVyc2F0aW9uc6FjlKpfMF92ZXJzaW9uqWNoYW5uZWxJZK5jb252ZXJzYXRpb25JZKljcmVhdGVkQXSha5GuY29udmVyc2F0aW9uSWSEoXGicTGhdLNjaGFubmVsX3VzZXJfc3RhdHVzoWOTql8wX3ZlcnNpb26pY2hhbm5lbElkpnVzZXJJZKFrkqljaGFubmVsSWSmdXNlcklkoXKTlgAApDBhYmOjY2gxomMxy0J55lLFZuAAlQEApDBhYmWjY2gxonUxkwABomMzqmNodW5rSW5kZXgApWZpbmFsw6h0aW1pbmdNc8s/+AAAAAAAAA==';

  test('decodes REAL Go-encoded wire bytes through the production unpack', () => {
    const frame = unpack(Buffer.from(GO_WIRE_FIXTURE_B64, 'base64')) as {
      queryID: string;
      final: boolean;
    };
    // Sanity: the envelope round-tripped (drives the accumulator's final path).
    expect(frame.queryID).toBe('q1');
    expect(frame.final).toBe(true);

    const onResult = vi.fn();
    const h = createHydrateStreamAccumulator(onResult);
    h.onFrame(frame);

    expect(onResult).toHaveBeenCalledTimes(1);
    const {changes} = onResult.mock.calls[0][0] as {changes: RowChange[]};
    expect(changes).toEqual([
      {
        type: 0,
        queryID: 'q1',
        table: 'conversations',
        rowKey: {conversationId: 'c1'},
        row: {
          _0_version: '0abc',
          channelId: 'ch1',
          conversationId: 'c1',
          createdAt: 1779813865070,
        },
      },
      {
        type: 0,
        queryID: 'q1',
        table: 'channel_user_status',
        rowKey: {channelId: 'ch1', userId: 'u1'},
        row: {_0_version: '0abe', channelId: 'ch1', userId: 'u1'},
      },
      {
        type: 1,
        queryID: 'q1',
        table: 'conversations',
        rowKey: {conversationId: 'c3'},
      },
    ]);
  });
});

describe('addQueryStream (single-query streaming hydrate)', () => {
  test('routes one query through addQueriesStream and returns its result', async () => {
    const client = new GoIVMClient();
    const changes = [row('r1')];
    const spy = vi
      .spyOn(client, 'addQueriesStream')
      .mockImplementation((_cg, queries, _epoch, onResult) => {
        // The single query is forwarded as a one-element batch.
        expect(queries).toEqual([{queryID: 'q1', ast: {table: 't'}}]);
        onResult({queryID: queries[0].queryID, changes, timingMs: 12});
        return Promise.resolve();
      });

    const res = await client.addQueryStream('cg1', 'q1', {table: 't'}, 3);

    expect(spy).toHaveBeenCalledOnce();
    expect(res).toEqual({changes, timingMs: 12});
  });

  test('throws if the stream completes without a result frame', async () => {
    const client = new GoIVMClient();
    // onResult never fires — defensive guard must reject.
    vi.spyOn(client, 'addQueriesStream').mockImplementation(() =>
      Promise.resolve(),
    );

    await expect(client.addQueryStream('cg1', 'qX', {}, 1)).rejects.toThrow(
      /no result frame for query qX/,
    );
  });
});
