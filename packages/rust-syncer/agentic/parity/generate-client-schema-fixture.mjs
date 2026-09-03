#!/usr/bin/env node
/**
 * Layer-2 differential golden for `services/view_syncer/client_schema.rs::
 * check_client_schema` — runs the REAL TS `checkClientSchema`
 * (zero-cache/src/services/view-syncer/client-schema.ts) over a battery of
 * (clientSchema, tableSpecs, fullTables) inputs and records the thrown
 * ProtocolError `{kind, message}` (or null). Inputs are hand-built minimal
 * `LiteAndZqlSpec` / `LiteTableSpec` shapes (only the fields the check reads).
 *
 * Usage (from packages/rust-syncer):
 *   npx tsx agentic/parity/generate-client-schema-fixture.mjs > agentic/parity/client-schema-fixture.json
 */
import {checkClientSchema} from '../../../zero-cache/src/services/view-syncer/client-schema.ts';
import {isProtocolError} from '../../../zero-protocol/src/error.ts';

const shard = {appID: 'zero', shardNum: 0};

const ts = (zql, keys) => ({
  zqlSpec: Object.fromEntries(Object.entries(zql).map(([c, t]) => [c, {type: t}])),
  tableSpec: {allPotentialPrimaryKeys: keys},
});
const ft = (columns, primaryKey) => ({
  columns: Object.fromEntries(Object.entries(columns).map(([c, d]) => [c, {dataType: d}])),
  ...(primaryKey ? {primaryKey} : {}),
});

const usersZql = {id: 'string', name: 'string', email: 'string', org: 'string', _0_version: 'string'};
const usersKeys = [['id'], ['email', 'org']];
const usersFull = {
  id: 'text|NOT_NULL', name: 'varchar', email: 'text|NOT_NULL', org: 'text|NOT_NULL',
  blob: 'bytea', _0_version: 'text',
};

const tableSpecs = {
  empty: {},
  // dotted synced table present → the tip never applies
  withDotted: {
    users: ts(usersZql, usersKeys),
    'hr.people': ts({id: 'string'}, [['id']]),
    'zero_0.clients': ts({clientGroupID: 'string'}, [['clientGroupID']]),
    'zero.permissions': ts({lock: 'boolean'}, [['lock']]),
  },
  // only public tables synced (prefixed ones filtered from the listing)
  publicOnly: {
    users: ts(usersZql, usersKeys),
    'zero_0.clients': ts({clientGroupID: 'string'}, [['clientGroupID']]),
    'zero.permissions': ts({lock: 'boolean'}, [['lock']]),
  },
};
const fullTables = {
  empty: {},
  all: {
    users: ft(usersFull),
    'hr.people': ft({id: 'text|NOT_NULL'}),
    'zero_0.clients': ft({clientGroupID: 'text|NOT_NULL'}),
    'zero.permissions': ft({lock: 'bool|NOT_NULL'}),
    keyless: ft({a: 'text|NOT_NULL'}),
    badpk: ft({k: 'bytea|NOT_NULL', v: 'text'}, ['k']),
    twobad: ft({k: 'bytea|NOT_NULL', j: 'jsonb|NOT_NULL', z: 'xml'}, ['k', 'z']),
  },
  publicOnly: {
    users: ft(usersFull),
    'zero_0.clients': ft({clientGroupID: 'text|NOT_NULL'}),
    'zero.permissions': ft({lock: 'bool|NOT_NULL'}),
  },
};

const t = (columns, primaryKey) => ({
  columns: Object.fromEntries(Object.entries(columns).map(([c, ty]) => [c, {type: ty}])),
  ...(primaryKey !== undefined ? {primaryKey} : {}),
});

const cases = [
  {name: 'nothing synced → Internal', tableSpecs: 'empty', fullTables: 'empty',
   clientSchema: {tables: {users: t({id: 'string'}, ['id'])}}},
  {name: 'valid', tableSpecs: 'withDotted', fullTables: 'all',
   clientSchema: {tables: {users: t({id: 'string', name: 'string'}, ['id'])}}},
  {name: 'valid alternate key, any order', tableSpecs: 'withDotted', fullTables: 'all',
   clientSchema: {tables: {users: t({email: 'string', org: 'string'}, ['org', 'email'])}}},
  {name: 'valid empty client schema', tableSpecs: 'withDotted', fullTables: 'all',
   clientSchema: {tables: {}}},
  {name: 'missing table, unknown everywhere, no dot', tableSpecs: 'withDotted', fullTables: 'all',
   clientSchema: {tables: {ghost: t({id: 'string'}, ['id'])}}},
  {name: 'missing dotted table, synced list has a dot → no tip', tableSpecs: 'withDotted', fullTables: 'all',
   clientSchema: {tables: {'hr.ghost': t({id: 'string'}, ['id'])}}},
  {name: 'missing dotted table, only public synced → tip', tableSpecs: 'publicOnly', fullTables: 'publicOnly',
   clientSchema: {tables: {'hr.ghost': t({id: 'string'}, ['id'])}}},
  {name: 'missing table known to replica, unsupported pk column', tableSpecs: 'withDotted', fullTables: 'all',
   clientSchema: {tables: {badpk: t({v: 'string'}, ['k'])}}},
  {name: 'missing table known to replica, two unsupported pk columns', tableSpecs: 'withDotted', fullTables: 'all',
   clientSchema: {tables: {twobad: t({j: 'json'}, ['k', 'z'])}}},
  {name: 'missing table known to replica, no key', tableSpecs: 'withDotted', fullTables: 'all',
   clientSchema: {tables: {keyless: t({a: 'string'}, ['a'])}}},
  {name: 'missing column of unsupported type', tableSpecs: 'withDotted', fullTables: 'all',
   clientSchema: {tables: {users: t({id: 'string', blob: 'string'}, ['id'])}}},
  {name: 'missing column, unknown', tableSpecs: 'withDotted', fullTables: 'all',
   clientSchema: {tables: {users: t({id: 'string', nope: 'string'}, ['id'])}}},
  {name: 'type mismatch', tableSpecs: 'withDotted', fullTables: 'all',
   clientSchema: {tables: {users: t({id: 'number'}, ['id'])}}},
  {name: 'no client primary key', tableSpecs: 'withDotted', fullTables: 'all',
   clientSchema: {tables: {users: t({id: 'string'})}}},
  {name: 'client primary key not a unique index', tableSpecs: 'withDotted', fullTables: 'all',
   clientSchema: {tables: {users: t({id: 'string', name: 'string'}, ['name'])}}},
  {name: 'client primary key superset of a unique index', tableSpecs: 'withDotted', fullTables: 'all',
   clientSchema: {tables: {users: t({id: 'string', name: 'string'}, ['id', 'name'])}}},
  {name: 'many errors: missing tables sorted, then tables sorted, columns in client order',
   tableSpecs: 'withDotted', fullTables: 'all',
   clientSchema: {tables: {
     zzz: t({id: 'string'}, ['id']),
     users: t({name: 'number', id: 'number', nope: 'string', blob: 'json'}, ['name']),
     aaa: t({id: 'string'}, ['id']),
     'hr.people': t({id: 'number'}),
   }}},
];

const out = {tableSpecs, fullTables, cases: []};
for (const c of cases) {
  const specs = new Map(Object.entries(tableSpecs[c.tableSpecs]));
  const full = new Map(
    Object.entries(fullTables[c.fullTables]).map(([name, spec]) => [name, {name, ...spec}]),
  );
  let expected = null;
  try {
    checkClientSchema(shard, c.clientSchema, specs, full);
  } catch (e) {
    if (!isProtocolError(e)) throw e;
    expected = {kind: e.errorBody.kind, message: e.errorBody.message};
  }
  out.cases.push({...c, shard, expected});
}
console.log(JSON.stringify(out, null, 2));
