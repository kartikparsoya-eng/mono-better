#!/usr/bin/env node
/**
 * Generates the TS-vs-Rust JWT parity fixture.
 *
 * Signs HS256 tokens over a fixed battery of claim shapes, runs each through
 * the REAL TS `verifyToken` (jose) with the same verifyOptions the production
 * syncer passes ({subject, issuer?, audience?}), and records whether TS accepts
 * or rejects. The Rust `JwtAuthValidator` must reach the identical accept/reject
 * decision — this pins the claim-validation contract (exp/nbf/sub/iss/aud, and
 * crucially whether exp is REQUIRED) to TS behavior rather than the porter's.
 *
 * Usage:
 *   npx tsx packages/rust-syncer/agentic/parity/generate-auth-fixture.mjs \
 *     > packages/rust-syncer/agentic/parity/auth-fixture.json
 */

import crypto from 'node:crypto';
import {generateKeyPair, exportJWK, SignJWT} from 'jose';
import {verifyToken} from '../../../zero-cache/src/auth/jwt.ts';

// jose enforces a 256-bit minimum key for HS256, so use a 32-byte secret.
const SECRET = '0123456789abcdef0123456789abcdef';

// Fixed, absolute timestamps so the fixture is time-independent: PAST is always
// expired / already-valid; FUTURE never expires / not-yet-valid.
const PAST = 1_000_000_000; // 2001-09-09
const FUTURE = 4_102_444_800; // 2100-01-01

const b64 = o => Buffer.from(JSON.stringify(o)).toString('base64url');
function signHS256(payload, secret = SECRET) {
  const data = `${b64({alg: 'HS256', typ: 'JWT'})}.${b64(payload)}`;
  const sig = crypto.createHmac('sha256', secret).update(data).digest('base64url');
  return `${data}.${sig}`;
}

// Each case: the signed token + the validator config the Rust side rebuilds.
// `issuer`/`audience` null => not configured (claim not validated).
const CASES = [
  {desc: 'valid: sub matches, exp future', userID: 'user1',
   payload: {sub: 'user1', exp: FUTURE}},
  {desc: 'expired: exp in the past', userID: 'user1',
   payload: {sub: 'user1', exp: PAST}},
  {desc: 'not-yet-valid: nbf in the future', userID: 'user1',
   payload: {sub: 'user1', nbf: FUTURE, exp: FUTURE}},
  {desc: 'NO exp claim (jose accepts; does the port require it?)', userID: 'user1',
   payload: {sub: 'user1'}},
  {desc: 'no exp but nbf already valid', userID: 'user1',
   payload: {sub: 'user1', nbf: PAST}},
  {desc: 'wrong sub', userID: 'user1',
   payload: {sub: 'user2', exp: FUTURE}},
  {desc: 'issuer configured + matches', userID: 'user1', issuer: 'iss-A',
   payload: {sub: 'user1', iss: 'iss-A', exp: FUTURE}},
  {desc: 'issuer configured + mismatch', userID: 'user1', issuer: 'iss-A',
   payload: {sub: 'user1', iss: 'iss-B', exp: FUTURE}},
  {desc: 'issuer configured + missing in token', userID: 'user1', issuer: 'iss-A',
   payload: {sub: 'user1', exp: FUTURE}},
  {desc: 'audience configured + matches', userID: 'user1', audience: 'app-1',
   payload: {sub: 'user1', aud: 'app-1', exp: FUTURE}},
  {desc: 'audience configured + mismatch', userID: 'user1', audience: 'app-1',
   payload: {sub: 'user1', aud: 'app-2', exp: FUTURE}},
  {desc: 'audience configured + missing in token', userID: 'user1', audience: 'app-1',
   payload: {sub: 'user1', exp: FUTURE}},
  {desc: 'no sub claim at all (subject option requires it)', userID: 'user1',
   payload: {exp: FUTURE}},
  {desc: 'issuer/aud present in token but NOT configured -> ignored', userID: 'user1',
   payload: {sub: 'user1', iss: 'whatever', aud: 'whatever', exp: FUTURE}},
  {desc: 'wrong signature (bad secret)', userID: 'user1',
   token: signHS256({sub: 'user1', exp: FUTURE}, 'the-wrong-secret-32-bytes-long!!')},
];

// verifyOptions the production syncer passes; `config` selects secret vs jwk.
async function tsAccepts(config, token, userID, issuer, audience) {
  const opts = {
    subject: userID,
    ...(issuer ? {issuer} : {}),
    ...(audience ? {audience} : {}),
  };
  try {
    await verifyToken(config, token, opts);
    return true;
  } catch {
    return false;
  }
}

const secretCases = await Promise.all(
  CASES.map(async c => {
    const token = c.token ?? signHS256(c.payload);
    const issuer = c.issuer ?? null;
    const audience = c.audience ?? null;
    return {
      desc: c.desc,
      secret: SECRET,
      token,
      userID: c.userID,
      issuer,
      audience,
      tsAccept: await tsAccepts({secret: SECRET}, token, c.userID, issuer, audience),
    };
  }),
);

// ── Asymmetric JWK path (config.jwk = single public JWK) ──────────────────
// ECDSA signatures are non-deterministic, so regenerating rotates these
// tokens/keys — that's fine, the committed fixture is a self-consistent
// snapshot (public JWK matches the signing key).
async function jwkSetup(alg, kid) {
  const {publicKey, privateKey} = await generateKeyPair(alg, {extractable: true});
  const publicJwk = await exportJWK(publicKey);
  publicJwk.alg = alg;
  publicJwk.use = 'sig';
  publicJwk.kid = kid;
  return {privateKey, publicJwk};
}
async function signAsym(privateKey, alg, kid, payload) {
  return new SignJWT(payload).setProtectedHeader({alg, kid}).sign(privateKey);
}

const es = await jwkSetup('ES256', 'es-1');
const rs = await jwkSetup('RS256', 'rs-1');
const esOther = await jwkSetup('ES256', 'es-1'); // different key, same kid

const JWK_CASES = [
  {desc: 'JWK ES256 valid', jwk: es.publicJwk,
   token: await signAsym(es.privateKey, 'ES256', 'es-1', {sub: 'user1', exp: FUTURE})},
  {desc: 'JWK ES256 expired', jwk: es.publicJwk,
   token: await signAsym(es.privateKey, 'ES256', 'es-1', {sub: 'user1', exp: PAST})},
  {desc: 'JWK ES256 not-yet-valid (nbf future)', jwk: es.publicJwk,
   token: await signAsym(es.privateKey, 'ES256', 'es-1', {sub: 'user1', nbf: FUTURE, exp: FUTURE})},
  {desc: 'JWK ES256 signed by a DIFFERENT key -> reject', jwk: es.publicJwk,
   token: await signAsym(esOther.privateKey, 'ES256', 'es-1', {sub: 'user1', exp: FUTURE})},
  {desc: 'JWK ES256 no-exp (jose accepts)', jwk: es.publicJwk,
   token: await signAsym(es.privateKey, 'ES256', 'es-1', {sub: 'user1'})},
  {desc: 'JWK ES256 wrong sub', jwk: es.publicJwk,
   token: await signAsym(es.privateKey, 'ES256', 'es-1', {sub: 'user2', exp: FUTURE})},
  {desc: 'JWK RS256 valid', jwk: rs.publicJwk,
   token: await signAsym(rs.privateKey, 'RS256', 'rs-1', {sub: 'user1', exp: FUTURE})},
];

const jwkCases = await Promise.all(
  JWK_CASES.map(async c => {
    const jwkStr = JSON.stringify(c.jwk);
    return {
      desc: c.desc,
      jwk: jwkStr,
      token: c.token,
      userID: 'user1',
      issuer: null,
      audience: null,
      tsAccept: await tsAccepts({jwk: jwkStr}, c.token, 'user1', null, null),
    };
  }),
);

console.log(JSON.stringify({secret: SECRET, cases: [...secretCases, ...jwkCases]}, null, 2));
