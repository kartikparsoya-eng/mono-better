#!/usr/bin/env node
/**
 * Generates the TS-vs-Rust permission-transform parity fixture.
 *
 * Runs the REAL TS `transformQuery` (read-authorizer) over a battery of
 * (ast, permissions, authData) triples and captures the transformed AST. The
 * Rust `transform_query` must produce byte-identical JSON — pinning the
 * fail-closed deny-by-default, allow-rule merging into WHERE, and static-param
 * binding to TS rather than the porter's reading of the authorizer.
 *
 * Usage:
 *   npx tsx packages/rust-syncer/agentic/parity/generate-perms-fixture.mjs \
 *     > packages/rust-syncer/agentic/parity/perms-fixture.json
 */

import {transformQuery} from '../../../zero-cache/src/auth/read-authorizer.ts';

// transformQueryInternal only calls lc.warn?.(); a minimal stub suffices.
const lc = {warn() {}, debug() {}, info() {}, error() {}};

// An always-true allow rule (ANYONE_CAN-style: an empty AND is vacuously true).
const ANYONE = ['allow', {type: 'and', conditions: []}];
// A simple auth-gated rule: issue.ownerId == $authData.sub (static param).
const OWNER_RULE = [
  'allow',
  {
    type: 'simple',
    left: {type: 'column', name: 'ownerId'},
    op: '=',
    right: {type: 'static', anchor: 'authData', field: 'sub'},
  },
];

const CASES = [
  {
    desc: 'deny-by-default: no rules for table -> fail closed',
    query: {table: 'issue', orderBy: [['id', 'asc']]},
    permissions: {tables: {}},
    authData: {},
  },
  {
    desc: 'anyone-can: always-true allow rule',
    query: {table: 'issue', orderBy: [['id', 'asc']]},
    permissions: {tables: {issue: {row: {select: [ANYONE]}}}},
    authData: {},
  },
  {
    desc: 'owner rule binds authData.sub into where',
    query: {table: 'issue', orderBy: [['id', 'asc']]},
    permissions: {tables: {issue: {row: {select: [OWNER_RULE]}}}},
    authData: {sub: 'user-123'},
  },
  {
    desc: 'existing where AND-merged with allow rule',
    query: {
      table: 'issue',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'closed'},
        op: '=',
        right: {type: 'literal', value: false},
      },
      orderBy: [['id', 'asc']],
    },
    permissions: {tables: {issue: {row: {select: [OWNER_RULE]}}}},
    authData: {sub: 'user-123'},
  },
];

const cases = CASES.map(c => ({
  desc: c.desc,
  query: c.query,
  permissions: c.permissions,
  authData: c.authData,
  expected: transformQuery(lc, c.query, c.permissions, {decoded: c.authData}),
}));

console.log(JSON.stringify({cases}, null, 2));
