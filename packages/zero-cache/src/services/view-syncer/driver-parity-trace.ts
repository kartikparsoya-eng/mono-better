import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {RowKey} from '../../types/row-key.ts';
import type {Timer} from './pipeline-driver.ts';

type Driver = {
  initialized(): boolean;
  readonly replicaVersion: string;
  currentVersion(): string;
  queries(): ReadonlyMap<string, unknown>;
  rowSetSignature(queryID: string): bigint | undefined;
  totalHydrationTimeMs(): number;
  addQuery(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
    queryName?: string,
  ): Iterable<unknown> | AsyncIterable<unknown>;
  removeQuery(queryID: string): void;
  getRow(table: string, pk: RowKey): Row | undefined;
  reset(clientSchema: ClientSchema): void;
  advance(
    timer: Timer,
  ): DriverAdvance | DriverReset | Promise<DriverAdvance | DriverReset>;
};

type DriverAdvance = {
  version: string;
  numChanges: number;
  changes: Iterable<unknown> | AsyncIterable<unknown>;
};

type DriverReset = Error & {reason: string};

export type CanonicalValue =
  | {type: 'undefined' | 'null'}
  | {type: 'boolean'; value: boolean}
  | {type: 'string'; value: string}
  | {type: 'number'; value: number | 'NaN' | 'Infinity' | '-Infinity' | '-0'}
  | {type: 'bigint'; value: string}
  | {type: 'bytes'; value: number[]}
  | {type: 'array'; value: CanonicalValue[]}
  | {type: 'object'; value: Array<[string, CanonicalValue]>}
  | {type: 'symbol' | 'function'; value: string};

export type ErrorTrace = {
  className: string;
  name: string;
  message: string;
  code: CanonicalValue;
  reason: CanonicalValue;
  cause: CanonicalValue;
};

export type StreamTraceEvent =
  | {kind: 'yield'}
  | {kind: 'change'; change: CanonicalValue};

export type DriverStateTrace = {
  initialized: boolean;
  replicaVersion: CanonicalValue;
  currentVersion: CanonicalValue;
  queries: Array<{
    queryID: string;
    info: CanonicalValue;
    rowSetSignature: CanonicalValue;
  }>;
};

type Outcome =
  | {status: 'ok'; value: CanonicalValue}
  | {status: 'error'; error: ErrorTrace};

export type DriverTraceEvent = {
  operation: string;
  input: CanonicalValue;
  outcome: Outcome;
  state: DriverStateTrace | {error: ErrorTrace};
};

/**
 * Canonicalizes values without applying semantic coercions. Object key order is
 * the only normalized property: array/stream order, missing keys, undefined,
 * bigint, -0, non-finite numbers, and bytes all remain distinguishable.
 */
export function canonicalValue(value: unknown): CanonicalValue {
  if (value === undefined) {
    return {type: 'undefined'};
  }
  if (value === null) {
    return {type: 'null'};
  }
  if (typeof value === 'boolean') {
    return {type: 'boolean', value};
  }
  if (typeof value === 'string') {
    return {type: 'string', value};
  }
  if (typeof value === 'number') {
    let canonical: number | 'NaN' | 'Infinity' | '-Infinity' | '-0';
    if (Number.isNaN(value)) {
      canonical = 'NaN';
    } else if (value === Infinity) {
      canonical = 'Infinity';
    } else if (value === -Infinity) {
      canonical = '-Infinity';
    } else if (Object.is(value, -0)) {
      canonical = '-0';
    } else {
      canonical = value;
    }
    return {type: 'number', value: canonical};
  }
  if (typeof value === 'bigint') {
    return {type: 'bigint', value: value.toString()};
  }
  if (typeof value === 'symbol') {
    return {type: 'symbol', value: String(value)};
  }
  if (typeof value === 'function') {
    return {type: 'function', value: String(value)};
  }
  if (ArrayBuffer.isView(value)) {
    const bytes = new Uint8Array(
      value.buffer,
      value.byteOffset,
      value.byteLength,
    );
    return {type: 'bytes', value: [...bytes]};
  }
  if (Array.isArray(value)) {
    return {type: 'array', value: value.map(canonicalValue)};
  }
  return {
    type: 'object',
    value: Object.keys(value as object)
      .sort()
      .map(key => [
        key,
        canonicalValue((value as Record<string, unknown>)[key]),
      ]),
  };
}

export function errorTrace(error: unknown): ErrorTrace {
  const record =
    error !== null && typeof error === 'object'
      ? (error as Record<string, unknown>)
      : undefined;
  const constructor = record?.constructor as
    | {name?: string | undefined}
    | undefined;
  return {
    className: constructor?.name ?? typeof error,
    name:
      typeof record?.name === 'string'
        ? record.name
        : (constructor?.name ?? typeof error),
    message:
      typeof record?.message === 'string' ? record.message : String(error),
    code: canonicalValue(record?.code),
    reason: canonicalValue(record?.reason),
    cause: canonicalValue(record?.cause),
  };
}

export type TraceDifference = {
  path: string;
  rust: unknown;
  ts: unknown;
};

/** Returns the first exact structural difference in two canonical traces. */
export function firstTraceDifference(
  rust: unknown,
  ts: unknown,
  path = '$',
): TraceDifference | undefined {
  if (Object.is(rust, ts)) {
    return undefined;
  }
  if (Array.isArray(rust) && Array.isArray(ts)) {
    if (rust.length !== ts.length) {
      return {path: `${path}.length`, rust: rust.length, ts: ts.length};
    }
    for (let i = 0; i < rust.length; i++) {
      const difference = firstTraceDifference(rust[i], ts[i], `${path}[${i}]`);
      if (difference) {
        return difference;
      }
    }
    return undefined;
  }
  if (
    rust !== null &&
    ts !== null &&
    typeof rust === 'object' &&
    typeof ts === 'object'
  ) {
    const rustRecord = rust as Record<string, unknown>;
    const tsRecord = ts as Record<string, unknown>;
    const keys = [
      ...new Set([...Object.keys(rustRecord), ...Object.keys(tsRecord)]),
    ].sort();
    for (const key of keys) {
      if (!(key in rustRecord) || !(key in tsRecord)) {
        return {
          path: `${path}.${key}`,
          rust: rustRecord[key],
          ts: tsRecord[key],
        };
      }
      const difference = firstTraceDifference(
        rustRecord[key],
        tsRecord[key],
        `${path}.${key}`,
      );
      if (difference) {
        return difference;
      }
    }
    return undefined;
  }
  return {path, rust, ts};
}

function isReset(value: unknown): value is DriverReset {
  return (
    value instanceof Error &&
    value.name === 'ResetPipelinesSignal' &&
    typeof (value as Partial<DriverReset>).reason === 'string'
  );
}

async function streamTrace(
  stream: Iterable<unknown> | AsyncIterable<unknown>,
): Promise<StreamTraceEvent[]> {
  const events: StreamTraceEvent[] = [];
  for await (const item of stream as AsyncIterable<unknown>) {
    events.push(
      item === 'yield'
        ? {kind: 'yield'}
        : {kind: 'change', change: canonicalValue(item)},
    );
  }
  return events;
}

/** Records complete public-driver operations for an exact TS-vs-Rust diff. */
export class DriverParityTrace {
  readonly #events: DriverTraceEvent[] = [];
  readonly #driver: Driver;

  constructor(driver: Driver) {
    this.#driver = driver;
  }

  events(): readonly DriverTraceEvent[] {
    return this.#events;
  }

  recordState(label: string): void {
    this.#events.push({
      operation: 'state',
      input: canonicalValue({label}),
      outcome: {status: 'ok', value: canonicalValue(undefined)},
      state: this.#state(),
    });
  }

  async addQuery(
    transformationHash: string,
    queryID: string,
    query: AST,
    timer: Timer,
    queryName?: string,
  ): Promise<StreamTraceEvent[] | undefined> {
    let events: StreamTraceEvent[] | undefined;
    await this.#record(
      'addQuery',
      {transformationHash, queryID, query, queryName},
      async () => {
        events = await streamTrace(
          this.#driver.addQuery(
            transformationHash,
            queryID,
            query,
            timer,
            queryName,
          ),
        );
        return events;
      },
    );
    return events;
  }

  async advance(timer: Timer): Promise<StreamTraceEvent[] | undefined> {
    let events: StreamTraceEvent[] | undefined;
    await this.#record('advance', {}, async () => {
      const result = await this.#driver.advance(timer);
      if (isReset(result)) {
        return {kind: 'reset', reset: errorTrace(result)};
      }
      events = await streamTrace(result.changes);
      return {
        kind: 'changes',
        version: result.version,
        numChanges: result.numChanges,
        events,
      };
    });
    return events;
  }

  async getRow(table: string, pk: RowKey): Promise<void> {
    await this.#record('getRow', {table, pk}, () =>
      this.#driver.getRow(table, pk),
    );
  }

  async removeQuery(queryID: string): Promise<void> {
    await this.#record('removeQuery', {queryID}, () =>
      this.#driver.removeQuery(queryID),
    );
  }

  async reset(clientSchema: ClientSchema): Promise<void> {
    await this.#record('reset', {clientSchema}, () =>
      this.#driver.reset(clientSchema),
    );
  }

  hydrationTimeMs(): number {
    return this.#driver.totalHydrationTimeMs();
  }

  async #record(
    operation: string,
    input: unknown,
    run: () => unknown | Promise<unknown>,
  ): Promise<void> {
    let outcome: Outcome;
    try {
      outcome = {status: 'ok', value: canonicalValue(await run())};
    } catch (error) {
      outcome = {status: 'error', error: errorTrace(error)};
    }
    this.#events.push({
      operation,
      input: canonicalValue(input),
      outcome,
      state: this.#state(),
    });
  }

  #state(): DriverStateTrace | {error: ErrorTrace} {
    try {
      const initialized = this.#driver.initialized();
      if (!initialized) {
        return {
          initialized,
          replicaVersion: canonicalValue(undefined),
          currentVersion: canonicalValue(undefined),
          queries: [],
        };
      }
      const queries = [...this.#driver.queries()].map(([queryID, info]) => {
        const queryInfo = info as Record<string, unknown>;
        return {
          queryID,
          // `PipelineDriver` stores private implementation fields on the same
          // object, but the public `QueryInfo` contract exposes only these.
          info: canonicalValue({
            transformedAst: queryInfo.transformedAst,
            transformationHash: queryInfo.transformationHash,
            queryName: queryInfo.queryName,
          }),
          rowSetSignature: canonicalValue(
            this.#driver.rowSetSignature(queryID),
          ),
        };
      });
      return {
        initialized,
        replicaVersion: canonicalValue(this.#driver.replicaVersion),
        currentVersion: canonicalValue(this.#driver.currentVersion()),
        queries,
      };
    } catch (error) {
      return {error: errorTrace(error)};
    }
  }
}
