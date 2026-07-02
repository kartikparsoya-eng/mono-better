// Loader for the goivm_napi addon (see addon.c). The addon is built
// OUT-OF-BAND (not on npm install) via:
//
//	cd packages/zero-cache/src/services/view-syncer/go-sidecar/napi
//	npx node-gyp rebuild
//
// and the Go engine library via (in the go-ivm repo):
//
//	go build -tags napilib -buildmode=c-shared -o libgoivm.dylib ./cmd/sidecar
//	  (linux prod builds add -tags libsqlite3 — see build-wal2.sh)
//
// Missing artifacts surface as a descriptive throw from loadGoNapiBridge;
// callers feature-detect with isGoNapiAddonAvailable().

import {existsSync} from 'node:fs';
import {createRequire} from 'node:module';
import {join} from 'node:path';

export type GoNapiAddon = {
  /** dlopen + goivm_start. Throws on ABI mismatch / missing symbols. */
  start(libPath: string, onDelivery: (kind: number, payload: Buffer) => void): void;
  /** Forward one request frame (no length prefix). Returns goivm rc (0=ok). */
  send(payload: Buffer): number;
  /** Tear down the Go host. The Go runtime stays resident (cannot unload). */
  shutdown(): void;
  /** ABI version of the loaded library; -1 before start. */
  abiVersion(): number;
};

const require = createRequire(import.meta.url);

const ADDON_PATH = join(
  new URL('.', import.meta.url).pathname,
  'build',
  'Release',
  'goivm_napi.node',
);

export function isGoNapiAddonAvailable(): boolean {
  return existsSync(ADDON_PATH);
}

export function loadGoNapiAddon(): GoNapiAddon {
  if (!isGoNapiAddonAvailable()) {
    throw new Error(
      `goivm_napi addon not built (expected ${ADDON_PATH}). ` +
        `Build it with: npx node-gyp rebuild (in the napi/ directory).`,
    );
  }
  return require(ADDON_PATH) as GoNapiAddon;
}
