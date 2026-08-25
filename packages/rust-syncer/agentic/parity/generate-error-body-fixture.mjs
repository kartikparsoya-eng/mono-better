#!/usr/bin/env node
/**
 * Layer-2 differential golden for `CCMError::to_error_body`
 * (connection_context_manager.rs). Builds the wire error body for each CCMError
 * kind exactly as the TS throw sites do — connection-context-manager.ts
 * (Unauthorized / InvalidConnectionRequest) and auth.ts (AuthInvalidated) — using
 * the REAL zero-protocol `ErrorKind` / `ErrorOrigin` enums, so a rename of any
 * kind string or of `ErrorOrigin.ZeroCache` ('zeroCache') moves the golden and
 * the Rust serialization must follow. Pins kind string + origin string + the
 * flat {kind,message,origin} field names.
 *
 * Usage:
 *   npx tsx agentic/parity/generate-error-body-fixture.mjs > agentic/parity/error-body-fixture.json
 */
import * as ErrorKind from '../../../zero-protocol/src/error-kind-enum.ts';
import * as ErrorOrigin from '../../../zero-protocol/src/error-origin-enum.ts';

const cases = [
  {
    variant: 'InvalidConnectionRequest',
    message: 'No validated connection is available for shared query work.',
    body: {
      kind: ErrorKind.InvalidConnectionRequest,
      message: 'No validated connection is available for shared query work.',
      origin: ErrorOrigin.ZeroCache,
    },
  },
  {
    variant: 'Unauthorized',
    message: 'Connection userID does not match validated server userID.',
    body: {
      kind: ErrorKind.Unauthorized,
      message: 'Connection userID does not match validated server userID.',
      origin: ErrorOrigin.ZeroCache,
    },
  },
  {
    variant: 'AuthInvalidated',
    message: 'Failed to decode auth token: bad',
    body: {
      kind: ErrorKind.AuthInvalidated,
      message: 'Failed to decode auth token: bad',
      origin: ErrorOrigin.ZeroCache,
    },
  },
];

console.log(JSON.stringify(cases, null, 2));
