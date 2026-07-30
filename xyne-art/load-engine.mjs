#!/usr/bin/env node
// load-engine.mjs — lightweight NAPI engine loader for ART scripts.
//
// Usage: node load-engine.mjs <cmd> [...args]
//   ping     — engine.ping() (sanity check) → "ok" or "bad"
//   hydrate  — in-memory hydrate of a simple table → "hydrate:N"
//   reset    — engine.reset() → "reset:ok"
//   cgs <N>  — create N concurrent engines → "N CGs created"
//   destroy  — create+destroy engine → "destroy:ok"

import { createRequire } from 'node:module';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { existsSync } from 'node:fs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

const NAPI = resolve(__dirname, '..', 'packages', 'rust-ivm', 'napi');
const candidates = process.platform === 'darwin'
  ? [resolve(NAPI, 'target/release/librust_ivm_napi.dylib'), resolve(NAPI, 'rust-ivm.node')]
  : [resolve(NAPI, 'rust-ivm.node'), resolve(NAPI, 'target/release/librust_ivm_napi.so')];

let addonPath = null;
for (const p of candidates) {
  if (existsSync(p)) { addonPath = p; break; }
}
if (!addonPath) {
  console.error(`no NAPI addon found at: ${candidates.join(', ')}`);
  process.exit(1);
}

const { RustIvmEngine } = require(addonPath);
const cmd = process.argv[2];

switch (cmd) {
  case 'ping': {
    const eng = new RustIvmEngine();
    const result = eng.ping();
    console.log(result === 'pong' ? 'ok' : 'bad');
    process.exit(0);
  }
  case 'hydrate': {
    const eng = new RustIvmEngine();
    eng.setDatabasePath(':memory:');
    const ast = JSON.stringify({table: 't0', orderBy: [['id', 'asc']]});
    eng.addQueriesStreaming([{queryId: 'q1', astJson: ast}])
      .then(rows => {
        console.log(`hydrate:${rows.length}`);
        return eng.destroy();
      })
      .then(() => process.exit(0))
      .catch(e => {
        console.error('ERR', e.message || e);
        process.exit(1);
      });
    break;
  }
  case 'reset': {
    const eng = new RustIvmEngine();
    eng.reset();
    console.log('reset:ok');
    process.exit(0);
  }
RustIvmEngine();
    eng.setDatabasePath(':memory:');
    eng.init([], ':memory:');
    eng.destroy().then(() => {
      console.log('destroy:ok');
      process.exit(0);
    }).catch(e => {
      console.error('destroy:fail', e.message || e);
      process.exit(1);
    });
    break;
  }
  case 'cgs': {
    const N = parseInt(process.argv[3] || '5', 10);
    const cgs = [];
    for (let i = 0; i < N; i++) cgs.push(new RustIvmEngine());
    console.log(`${cgs.length} CGs created`);
    for (const cg of cgs) cg.destroy().catch(() => {});
    process.exit(0);
  }
  default:
    console.error(`unknown cmd: ${cmd}`);
    process.exit(1);
}
