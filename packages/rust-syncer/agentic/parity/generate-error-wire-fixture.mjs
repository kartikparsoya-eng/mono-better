#!/usr/bin/env node
/**
 * Layer-2 wire-shape golden for `protocol/error.rs::ErrorBody` — one valid TS
 * wire body per member of `errorBodySchema` (zero-protocol/src/error.ts), with
 * every optional field exercised BOTH present and absent.
 *
 * Every body is validated against the REAL `errorBodySchema` (valita) before it
 * is emitted, so a fixture entry is by construction something zero-client's
 * `downstreamSchema` parse accepts. The Rust test parses each body, checks it
 * bound to the mirrored union member, re-serializes, and requires JSON
 * equality with this golden — pinning field names (`bodyPreview`, `mutationIDs`,
 * `queryIDs`, `minBackoffMs`, …), enum strings, AND absence-vs-null of
 * optional fields (valita `.optional()` rejects `null`; a `"minBackoffMs":
 * null` makes zero-client disconnect with InvalidMessage).
 *
 * Usage (from packages/rust-syncer):
 *   npx tsx agentic/parity/generate-error-wire-fixture.mjs > agentic/parity/error-wire-fixture.json
 */
import * as v from '../../../shared/src/valita.ts';
import {errorBodySchema} from '../../../zero-protocol/src/error.ts';
import * as ErrorKind from '../../../zero-protocol/src/error-kind-enum.ts';
import * as ErrorOrigin from '../../../zero-protocol/src/error-origin-enum.ts';
import * as ErrorReason from '../../../zero-protocol/src/error-reason-enum.ts';

const mid = {clientID: 'c1', id: 7};

const cases = [
  // basicErrorBodySchema — origin optional.
  {variant: 'Basic', body: {kind: ErrorKind.Internal, message: 'boom'}},
  {
    variant: 'Basic',
    body: {
      kind: ErrorKind.SchemaVersionNotSupported,
      message: 'schema',
      origin: ErrorOrigin.ZeroCache,
    },
  },
  {
    variant: 'Basic',
    body: {kind: ErrorKind.Unauthorized, message: 'no', origin: ErrorOrigin.Server},
  },
  // backoffBodySchema — every field beyond kind/message optional.
  {variant: 'Backoff', body: {kind: ErrorKind.Rehome, message: 'shed'}},
  {
    variant: 'Backoff',
    body: {
      kind: ErrorKind.Rebalance,
      message: 'move',
      minBackoffMs: 100,
      maxBackoffMs: 5000,
      reconnectParams: {a: '1', b: 'two'},
      origin: ErrorOrigin.ZeroCache,
    },
  },
  {
    variant: 'Backoff',
    body: {kind: ErrorKind.ServerOverloaded, message: 'busy', maxBackoffMs: 30000},
  },
  // pushFailedBodySchema — origin server (details optional).
  {
    variant: 'PushFailedServer',
    body: {
      kind: ErrorKind.PushFailed,
      mutationIDs: [mid],
      message: 'db',
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Database,
    },
  },
  {
    variant: 'PushFailedServer',
    body: {
      kind: ErrorKind.PushFailed,
      details: {code: 'ooo', n: null},
      mutationIDs: [mid, {clientID: 'c2', id: 8}],
      message: 'mutation was out of order',
      origin: ErrorOrigin.Server,
      reason: ErrorReason.OutOfOrderMutation,
    },
  },
  // pushFailedBodySchema — zeroCache http (bodyPreview optional).
  {
    variant: 'PushFailedHttp',
    body: {
      kind: ErrorKind.PushFailed,
      mutationIDs: [mid],
      message: 'Failed to push: 401',
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.HTTP,
      status: 401,
    },
  },
  {
    variant: 'PushFailedHttp',
    body: {
      kind: ErrorKind.PushFailed,
      details: 'x',
      mutationIDs: [],
      message: 'Failed to push: 500',
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.HTTP,
      status: 500,
      bodyPreview: '<html>',
    },
  },
  // pushFailedBodySchema — zeroCache timeout/parse/internal.
  {
    variant: 'PushFailedZeroCache',
    body: {
      kind: ErrorKind.PushFailed,
      mutationIDs: [mid],
      message: 'timed out',
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.Timeout,
    },
  },
  {
    variant: 'PushFailedZeroCache',
    body: {
      kind: ErrorKind.PushFailed,
      details: [1, 'two'],
      mutationIDs: [mid],
      message: 'Failed to parse response from API server',
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.Parse,
    },
  },
  // transformFailedBodySchema — the same three shapes.
  {
    variant: 'TransformFailedServer',
    body: {
      kind: ErrorKind.TransformFailed,
      queryIDs: ['q1'],
      message: 'parse',
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Parse,
    },
  },
  {
    variant: 'TransformFailedHttp',
    body: {
      kind: ErrorKind.TransformFailed,
      queryIDs: ['q1', 'q2'],
      message: 'Failed to transform queries: 503',
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.HTTP,
      status: 503,
    },
  },
  {
    variant: 'TransformFailedHttp',
    body: {
      kind: ErrorKind.TransformFailed,
      details: {retry: true},
      queryIDs: [],
      message: 'Failed to transform queries: 401',
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.HTTP,
      status: 401,
      bodyPreview: 'Unauthorized',
    },
  },
  {
    variant: 'TransformFailedZeroCache',
    body: {
      kind: ErrorKind.TransformFailed,
      queryIDs: ['q1'],
      message: 'internal',
      origin: ErrorOrigin.ZeroCache,
      reason: ErrorReason.Internal,
    },
  },
];

for (const c of cases) {
  // Throws (with the valita path) if a case is not a valid wire body.
  v.parse(c.body, errorBodySchema);
}
console.log(JSON.stringify(cases, null, 2));
