/**
 * Background for using `json-custom-numbers`:
 *
 * https://neon.tech/blog/parsing-json-from-postgres-in-js
 */
import {
  parse as customParse,
  stringify as customStringify,
} from 'json-custom-numbers';
import * as v from './valita.ts';

function numberParser(_: unknown, v: string) {
  const n = +v;
  if (n >= Number.MIN_SAFE_INTEGER && n <= Number.MAX_SAFE_INTEGER) return n;
  if (v.includes('.') || v.includes('e') || v.includes('E')) {
    return n;
  }
  return BigInt(v);
}

// Variant of postgres.JSONValue adapted to include bigints
export type JSONValue =
  | null
  | string
  | number
  | bigint
  | boolean
  | readonly JSONValue[]
  | JSONObject;

export type JSONObject = {readonly [prop: string]: JSONValue | undefined};

export const jsonValueSchema: v.Type<JSONValue> = v.lazy(() => {
  const jsonObjectSchema = v.readonly(v.record(jsonValueSchema));

  return v.union(
    v.null(),
    v.string(),
    v.number(),
    v.bigint(),
    v.boolean(),
    v.readonly(v.array(jsonValueSchema)),
    jsonObjectSchema,
  );
});

export const jsonObjectSchema = v.readonly(v.record(jsonValueSchema));

/**
 * Parses JSON strings that may contain arbitrarily large integers. Integers
 * larger than {@link Number.MAX_SAFE_INTEGER} are deserialized as a `bigint`.
 */
export function parse(
  str: string,
  reviver?: (k: string, v: unknown) => unknown,
): JSONValue {
  return customParse(str, reviver, numberParser);
}

/**
 * A string that is ALREADY valid JSON and should be spliced into the output of
 * {@link stringify} verbatim, rather than being escaped as a JSON string.
 *
 * This exists so that JSON produced somewhere else — notably rows serialized by
 * the Rust IVM engine — can reach the wire without a parse/re-encode round
 * trip. Without it, handing JS a JSON string and then stringifying it again
 * costs a `JSON.parse` to turn it into objects and a full re-serialization to
 * turn it back, per row.
 *
 * The contents are NOT validated. Splicing invalid JSON produces a malformed
 * document, so only wrap output from a serializer you trust.
 */
export class RawJSON {
  readonly json: string;

  constructor(json: string) {
    this.json = json;
  }

  toString(): string {
    return this.json;
  }
}

// oxlint-disable-next-line @typescript-eslint/no-explicit-any
function customSerializer(_: string, v: any, type: string) {
  if (type === 'bigint') return v.toString();
  // A serializer that returns a string has its return value emitted as raw
  // JSON text (unquoted) — the same mechanism the bigint case above relies on
  // to emit `10` rather than `"10"`.
  if (v instanceof RawJSON) return v.json;
}

/**
 * Stringifies objects to JSON, supporting objects containing bigint values.
 * Note that the resulting JSON string may not be deserializable by
 * all environments, but it is supported by Postgres. The string should be
 * deserialized with the corresponding {@link parse} method that will represent
 * large numbers as bigints. From there it is up to the application to suitably
 * handle bigints passed to downstream logic.
 */
export function stringify(
  obj: unknown,
  replacer?:
    | (string | number)[]
    | ((key: string, value: unknown) => unknown)
    | null,
  indent?: string | number,
) {
  return customStringify(obj, replacer, indent, customSerializer);
}

export const BigIntJSON = {
  parse,
  stringify,
} as const;
