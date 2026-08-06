import {h64} from '../../../../shared/src/hash.ts';
import {rowIDString, type RowID} from '../../types/row-key.ts';
import {createRequire} from 'node:module';

/**
 * A 64-bit fingerprint of the rows of a result set, computed by XOR-ing
 * the 64-bit signatures of each row.
 */
export type RowSetSignature = bigint;

/**
 * A 64-bit (rowID-agnostic) signature unit representing a row in some set.
 */
export function rowIDSignatureUnit(id: RowID): bigint {
  const fns = tryLoadRustSignatureFns();
  return fns ? fns.rustCvrRowIdSignatureUnit(id) : rowIDSignatureUnitTs(id);
}

/**
 * Parses a signature from its hex string representation.
 */
export function parseSignature(hex: string | undefined | null): bigint {
  const fns = tryLoadRustSignatureFns();
  return fns ? fns.rustCvrParseSignature(hex) : parseSignatureTs(hex);
}

/**
 * Formats a signature into a hex string.
 */
export function formatSignature(sig: RowSetSignature): string {
  const fns = tryLoadRustSignatureFns();
  return fns ? fns.rustCvrFormatSignature(sig) : formatSignatureTs(sig);
}

function rowIDSignatureUnitTs(id: RowID): bigint {
  return h64(rowIDString(id));
}

function parseSignatureTs(hex: string | undefined | null): bigint {
  return hex ? BigInt(`0x${hex}`) : BigInt(0);
}

function formatSignatureTs(sig: RowSetSignature): string {
  return sig.toString(16);
}

type RustSignatureUnit = (id: RowID) => bigint;
type RustParseSignature = (hex: string | undefined | null) => bigint;
type RustFormatSignature = (sig: bigint) => string;

type RustSignatureFns = {
  rustCvrRowIdSignatureUnit: RustSignatureUnit;
  rustCvrParseSignature: RustParseSignature;
  rustCvrFormatSignature: RustFormatSignature;
};

// `undefined` = not yet attempted; `null` = attempted and unavailable.
let cachedRustSignatureFns: RustSignatureFns | null | undefined;

function tryLoadRustSignatureFns(): RustSignatureFns | null {
  if (cachedRustSignatureFns !== undefined) {
    return cachedRustSignatureFns;
  }

  if (process.env['USE_RUST_CVR_SIGNATURE'] !== '1') {
    cachedRustSignatureFns = null;
    return null;
  }

  try {
    const nodeRequire = createRequire(import.meta.url);
    const addonPath =
      process.env['RUST_CVR_ADDON_PATH'] ??
      '../../../../packages/rust-cvr/napi/rust-cvr.node';
    const addon = nodeRequire(addonPath) as {
      rustCvrRowIdSignatureUnit?: RustSignatureUnit;
      rustCvrParseSignature?: RustParseSignature;
      rustCvrFormatSignature?: RustFormatSignature;
    };
    if (
      typeof addon.rustCvrRowIdSignatureUnit === 'function' &&
      typeof addon.rustCvrParseSignature === 'function' &&
      typeof addon.rustCvrFormatSignature === 'function'
    ) {
      cachedRustSignatureFns = {
        rustCvrRowIdSignatureUnit: addon.rustCvrRowIdSignatureUnit,
        rustCvrParseSignature: addon.rustCvrParseSignature,
        rustCvrFormatSignature: addon.rustCvrFormatSignature,
      };
    } else {
      cachedRustSignatureFns = null;
    }
  } catch (e) {
    console.error(
      '[rust-cvr-signature] Failed to load addon:',
      (e as Error).message,
    );
    cachedRustSignatureFns = null;
  }

  return cachedRustSignatureFns;
}

export type {RowID};
