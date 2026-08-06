import {createRequire} from 'node:module';

/**
 * Single entry point for loading the rust-cvr napi addon.
 * Gated by the `RUST_CVR=1` environment variable.
 *
 * All CVR components (signature, row-cache, updaters, client-handler, store)
 * check this single flag. When enabled, the addon is loaded once and cached.
 * When disabled or load fails, all components fall back to their TS implementations.
 */

// `undefined` = not yet attempted; `null` = attempted and unavailable.
let cachedAddon: Record<string, unknown> | null | undefined;

export function isRustCvrEnabled(): boolean {
  return process.env['RUST_CVR'] === '1';
}

export function getRustCvrAddon<T = Record<string, unknown>>(): T | null {
  if (cachedAddon !== undefined) {
    return cachedAddon as T | null;
  }

  if (!isRustCvrEnabled()) {
    cachedAddon = null;
    return null;
  }

  try {
    const nodeRequire = createRequire(import.meta.url);
    const addonPath =
      process.env['RUST_CVR_ADDON_PATH'] ??
      '../../../../packages/rust-cvr/napi/rust-cvr.node';
    cachedAddon = nodeRequire(addonPath) as Record<string, unknown>;
  } catch (e) {
    console.error('[rust-cvr] Failed to load addon:', (e as Error).message);
    cachedAddon = null;
  }

  return cachedAddon as T | null;
}
