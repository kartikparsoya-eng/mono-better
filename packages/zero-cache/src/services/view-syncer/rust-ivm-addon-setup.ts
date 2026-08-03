// Test setup that GUARANTEES the rust-ivm napi addon under test is the wal2
// build — never the system SQLite. Import this FIRST (before RustIVMDriver) in
// every rust-ivm-driver*.test.ts.
//
// WHY: the addon links SQLite via rusqlite. The default system SQLite has no
// wal2, so the snapshotter's `journal_mode = wal2` replica fails to open and
// every snapshotter/driver test either errors at init or — worse — silently
// falls back and produces WRONG results that look green. That masked the exact
// driver/snapshotter-seam bugs we care about (rowKey/PK). This module removes
// the footgun: it points the addon path at the wal2 build (auto-building it
// locally if missing) and LOUDLY fails if the loaded addon is not wal2.
import {execFileSync} from 'node:child_process';
import {existsSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
// packages/zero-cache/src/services/view-syncer -> packages/rust-ivm
const rustIvmDir = join(here, '../../../../rust-ivm');
const addonPath = join(rustIvmDir, 'napi', 'rust-ivm.node');
const buildScript = join(rustIvmDir, 'scripts', 'build-local-wal2.sh');

/**
 * Returns true if `path` does NOT dynamically link a system libsqlite3 — i.e.
 * SQLite is statically linked in, which is how the wal2 fork is embedded.
 * Best-effort: if the platform link tool is unavailable we don't block.
 */
function looksStaticallyLinked(path: string): boolean {
  try {
    const [tool, args] =
      process.platform === 'darwin' ? ['otool', ['-L', path]] : ['ldd', [path]];
    const out = execFileSync(tool, args, {encoding: 'utf8'});
    return !/libsqlite3/i.test(out);
  } catch {
    return true; // tool missing / not analyzable — don't hard-block on this
  }
}

if (!process.env['RUST_IVM_ADDON_PATH']) {
  if (!existsSync(addonPath)) {
    if (process.env['CI']) {
      throw new Error(
        `rust-ivm wal2 addon missing at ${addonPath} and RUST_IVM_ADDON_PATH is ` +
          `unset. CI must build it (Dockerfile stage 1/3) or run ` +
          `packages/rust-ivm/scripts/build-local-wal2.sh before these tests.`,
      );
    }
    // Local dev: build the wal2 addon once (~15s). Prevents the silent
    // "ran against system SQLite" failure mode entirely.
    // eslint-disable-next-line no-console
    console.error(
      '[rust-ivm-test] wal2 addon not found — building via build-local-wal2.sh …',
    );
    execFileSync('bash', [buildScript], {stdio: 'inherit'});
  }
  process.env['RUST_IVM_ADDON_PATH'] = addonPath;
}

// Loud guard: whatever addon we ended up pointing at MUST be wal2-capable.
const resolved = process.env['RUST_IVM_ADDON_PATH'];
if (resolved && existsSync(resolved) && !looksStaticallyLinked(resolved)) {
  throw new Error(
    `rust-ivm addon at ${resolved} dynamically links system libsqlite3 — it is ` +
      `NOT the wal2 build. Snapshotter tests would run against wal2-less SQLite ` +
      `and silently produce wrong results. Rebuild with ` +
      `packages/rust-ivm/scripts/build-local-wal2.sh.`,
  );
}
