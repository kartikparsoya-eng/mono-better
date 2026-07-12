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

import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";

export type GoNapiAddon = {
  /** dlopen + goivm_start. Throws on ABI mismatch / missing symbols. */
  start(
    libPath: string,
    onDelivery: (kind: number, payload: Buffer) => void,
  ): void;
  /** Forward one request frame (no length prefix). Returns goivm rc (0=ok). */
  send(payload: Buffer): number;
  /** ABI version of the loaded library; -1 before start. */
  abiVersion(): number;
  /**
   * Grant `n` pull credits to the in-flight pullMode RPC `reqID` (ABI v3,
   * Direct synchronous call into the Go library
   * — O(1) leaf-mutex registry op, never blocks the JS thread. Unknown
   * reqID is a silent no-op (RPC already settled).
   */
  streamCredit(reqID: number, n: number): void;
  /**
   * Cancel the pull gate for `reqID` — the AsyncIterator's
   * .return()/.throw() crossing the boundary. Go unwinds the producer
   * (cursor close, pool-reader release) and settles the RPC with a
   * terminal error frame. Idempotent; unknown reqID is a no-op.
   */
  streamCancel(reqID: number): void;
  // NOTE: no shutdown() — calling goivm_shutdown on the JS thread deadlocks
  // against TSFN backpressure and racing deliveries are a use-after-free; the
  // Go host lives until process exit (see addon.c).
};

const require = createRequire(import.meta.url);

const ADDON_PATH = join(
  new URL(".", import.meta.url).pathname,
  "build",
  "Release",
  "goivm_napi.node",
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
