// capture-loader.mjs — ESM loader hooks for the fixture capture harness.
//
// Intercepts two module specifiers when a *.push.test.ts / *.fetch.test.ts is
// imported by capture.mjs:
//   1. `vitest`                         -> a stub that runs describe/test bodies
//                                          immediately and no-ops expect().
//   2. `.../test/fetch-and-push-tests`  -> a wrapper whose runPushTest /
//                                          runFetchTest serialize their resolved
//                                          arguments to a <name>.input.json and
//                                          return a dummy result.
//
// Both synthetic modules execute in the MAIN thread, so they read/write the
// shared globalThis.__CAP__ that capture.mjs sets up. The loader hooks
// themselves only supply source text.

const VITEST_STUB = `
const CAP = globalThis.__CAP__;
function push(name){ CAP.stack.push(String(name)); }
function pop(){ CAP.stack.pop(); }
export function describe(name, fn){ push(name); try { fn && fn(); } finally { pop(); } }
describe.each = (rows) => (name, fn) => { for (const r of rows) { push(String(name)); try { fn && fn(Array.isArray(r) ? r[0] : r); } catch(e){} finally { pop(); } } };
describe.for = (rows) => (name, fn) => { for (const r of rows) { push(String(name)); try { fn && fn(r, {}); } catch(e){} finally { pop(); } } };
describe.skip = () => {};
describe.only = describe;
export function test(name, fn){ push(name); try { fn && fn(); } catch(e) { /* throwing tests are not fixture-able */ } finally { pop(); } }
test.each = (rows) => (name, fn) => { for (const r of rows) { push(String(name)); try { fn && fn(...(Array.isArray(r) ? r : [r])); } catch(e){} finally { pop(); } } };
test.for = (rows) => (name, fn) => { for (const r of rows) { push(String(name)); try { fn && fn(r, {}); } catch(e){} finally { pop(); } } };
test.skip = () => {};
test.only = test;
test.todo = () => {};
export const it = test;
export function beforeEach(){} export function afterEach(){}
export function beforeAll(){} export function afterAll(){}
function noopProxy(){ const p = new Proxy(function(){ return p; }, { get(_t,k){ if (k==='then') return undefined; return () => p; }, apply(){ return p; } }); return p; }
export function expect(){ return noopProxy(); }
expect.soft = expect; expect.assertions = () => {}; expect.hasAssertions = () => {};
expect.any = () => ({}); expect.anything = () => ({});
expect.objectContaining = x => x; expect.arrayContaining = x => x; expect.stringContaining = x => x;
export const suite = describe;
export const vi = new Proxy(function(){}, { get(){ return () => {}; }, apply(){ return undefined; } });
export const assert = new Proxy(function(){}, { get(){ return () => {}; }, apply(){ return undefined; } });
export default { describe, test, it, expect, vi, beforeEach, afterEach, beforeAll, afterAll };
`;

const HARNESS_WRAPPER = `
import { writeFileSync, mkdirSync } from 'node:fs';
const CAP = globalThis.__CAP__;
const TYPE = { 0: 'add', 1: 'remove', 2: 'edit', 3: 'child' };

function colToStr(v){
  if (typeof v === 'string') return v;
  const t = (v && v.type) || 'string';
  return (v && v.optional) ? ('null|' + t) : t;
}
function changeToPush(entry){
  // entry = [sourceName, SourceChange]; SourceChange = [ChangeType, row, oldRow]
  const [table, ch] = entry;
  const type = TYPE[ch[0]];
  const out = { table, type };
  if (ch[1] != null) out.row = ch[1];
  if (type === 'edit' && ch[2] != null) out.oldRow = ch[2];
  return out;
}
function toInput(t){
  const tables = {};
  for (const [name, spec] of Object.entries(t.sources)){
    const columns = {};
    for (const [c, sv] of Object.entries(spec.columns)) columns[c] = colToStr(sv);
    tables[name] = {
      columns,
      primaryKey: spec.primaryKeys,
      rows: (t.sourceContents && t.sourceContents[name]) || [],
    };
  }
  const pushes = (t.pushes || []).map(changeToPush);
  const input = { tables, ast: t.ast, pushes };
  if (t.enableNotExists) input.enableNotExists = true;
  return input;
}
function slugify(s){ return s.toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-+|-+$/g,''); }
function emit(t){
  const base = CAP.stack.length ? CAP.stack.join(' ') : 'unnamed';
  let name = slugify(CAP.prefix + '-' + base);
  CAP.counter[name] = (CAP.counter[name] || 0) + 1;
  if (CAP.counter[name] > 1) name = name + '-' + CAP.counter[name];
  const input = toInput(t);
  input.name = name;
  mkdirSync(CAP.outDir, { recursive: true });
  writeFileSync(CAP.outDir + '/' + name + '.input.json', JSON.stringify(input));
  CAP.written.push(name);
  return { data: [], pushes: [], actualStorage: {}, log: [], logWithFetch: [], pushesWithFetch: [] };
}
export function runPushTest(t){ return emit(t); }
export function runFetchTest(t){ return emit(t); }
`;

export async function resolve(specifier, context, nextResolve) {
  if (specifier === 'vitest') {
    return { url: 'virtual:vitest', shortCircuit: true };
  }
  return nextResolve(specifier, context);
}

export async function load(url, context, nextLoad) {
  if (url === 'virtual:vitest') {
    return { format: 'module', source: VITEST_STUB, shortCircuit: true };
  }
  if (url.endsWith('/test/fetch-and-push-tests.ts')) {
    return { format: 'module', source: HARNESS_WRAPPER, shortCircuit: true };
  }
  return nextLoad(url, context);
}
