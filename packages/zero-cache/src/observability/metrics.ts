import type {
  Attributes,
  Counter,
  Histogram,
  Meter,
  MetricOptions,
  ObservableGauge,
  UpDownCounter,
} from '@opentelemetry/api';
import {metrics} from '@opentelemetry/api';

// intentional lazy initialization so it is not started before the SDK is started.

// Per-process worker identity, attached as a metric ATTRIBUTE (data-point
// dimension) to every histogram measurement — see setMetricWorkerAttributes.
let workerAttributes: Attributes = {};

/**
 * Stamp the calling process's worker identity onto every metric it records.
 *
 * The worker name + index are ALSO set as OTel *resource* attributes in
 * `startOtelAuto` (process.worker / process.worker_index), but resource
 * attributes only become per-series labels via the collector's
 * `resource_to_telemetry_conversion`, which is environment-dependent and was
 * observed dropping them for the syncer histograms in the sandbox. When two
 * `fork()`ed syncer workers emit `zero.sync.*` histograms with no
 * worker-distinguishing label, their series share one identity and collide
 * (last-write-wins) in the metric store — the merged cumulative counter steps
 * DOWN, which makes every `histogram_quantile` percentile garbage.
 *
 * A *data-point* attribute is part of the series identity in EVERY export/scrape
 * path unconditionally (it becomes a Prometheus label directly, no `target_info`
 * join), so attaching the worker here guarantees worker 0 and worker 1 are
 * distinct, monotonic series regardless of collector config. Called once per
 * process from `startOtelAuto` with the same name/index used for the resource.
 */
export function setMetricWorkerAttributes(
  workerName: string,
  workerIndex: number,
): void {
  workerAttributes = {worker: workerName, worker_index: String(workerIndex)};
}

export type Category =
  | 'replication' // postgres to replica
  | 'replica' // health of replica and litestream backup
  | 'sync' // replica to client
  | 'mutation'
  | 'server';

let meter: Meter | undefined;

type Options = MetricOptions & {description: string};

function getMeter() {
  if (!meter) {
    meter = metrics.getMeter('zero');
  }
  return meter;
}

function cache<TRet>(): (
  name: string,
  creator: (name: string) => TRet,
) => TRet {
  const instruments = new Map<string, TRet>();
  return (name: string, creator: (name: string) => TRet) => {
    const existing = instruments.get(name);
    if (existing) {
      return existing;
    }

    const ret = creator(name);
    instruments.set(name, ret);
    return ret;
  };
}

const upDownCounters = cache<UpDownCounter>();

export function getOrCreateUpDownCounter(
  category: Category,
  name: string,
  description: string,
): UpDownCounter;
export function getOrCreateUpDownCounter(
  category: Category,
  name: string,
  opts: Options,
): UpDownCounter;
export function getOrCreateUpDownCounter(
  category: Category,
  name: string,
  opts: string | Options,
): UpDownCounter {
  const raw = upDownCounters(name, name =>
    getMeter().createUpDownCounter(
      `zero.${category}.${name}`,
      typeof opts === 'string' ? {description: opts} : opts,
    ),
  );
  // Wrap to merge workerAttributes so forked syncer workers produce distinct
  // series — same fix the histograms got. Without this, counters like
  // row-set-signature-drifts and reset-class counters collide at multi-worker
  // scale (last-write-wins → non-monotonic).
  return {
    add: (value: number, attributes?: Attributes) =>
      raw.add(value, {...workerAttributes, ...attributes}),
  };
}

/**
 * A latency histogram whose {@link recordMs} method accepts raw millisecond
 * durations and converts them to seconds internally.
 *
 * Use {@link getOrCreateLatencyHistogram} to create one — the unit (`'s'`),
 * bucket boundaries, and ms→s conversion are all baked in
 */
export type LatencyHistogram = {
  /**
   * Record a duration. Pass the raw elapsed milliseconds — the conversion to
   * seconds (required by the `unit: 's'` OTel histogram) is handled internally.
   *
   * @param durationMs  Elapsed time in **milliseconds** (do NOT pre-divide).
   * @param attributes  Optional OTel attributes to attach to the observation.
   */
  recordMs(
    durationMs: number,
    attributes?: Parameters<Histogram['record']>[1],
  ): void;
};

/**
 * Bucket boundaries (in seconds) for zero's latency histograms.
 *
 * The operational range is 1 ms – 5,000 ms (including customers actively
 * tuning queries). ~2× logarithmic steps give proportionally consistent
 * `histogram_quantile` accuracy regardless of where values cluster within
 * that range. 10,000 ms and 30,000 ms are overflow catchers for truly broken
 * states.
 *
 *   1 ms, 2 ms, 5 ms, 10 ms, 20 ms, 50 ms, 100 ms, 200 ms, 500 ms,
 *   1 s, 2 s, 5 s, 10 s, 30 s
 */
const LATENCY_HISTOGRAM_BOUNDARIES_S = [
  0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30,
];

const latencyHistograms = cache<Histogram>();

/**
 * Creates (or retrieves) a latency histogram for the given metric.
 *
 * - `unit` is always `'s'` (seconds), matching the OTel convention.
 * - Bucket boundaries are pre-set for zero's typical operation range
 *   (1 ms – 5 s); see {@link LATENCY_HISTOGRAM_BOUNDARIES_S}.
 * - The returned {@link LatencyHistogram} accepts **milliseconds** via
 *   `recordMs()`, so callers never need to divide by 1000.
 *
 * @example
 * ```ts
 * readonly #hydrationTime = getOrCreateLatencyHistogram(
 *   'sync', 'hydration-time', 'Time to hydrate a query.',
 * );
 * // ...
 * this.#hydrationTime.recordMs(performance.now() - start);
 * ```
 */
export function getOrCreateLatencyHistogram(
  category: Category,
  name: string,
  description: string,
): LatencyHistogram {
  const h = latencyHistograms(name, name =>
    getMeter().createHistogram(`zero.${category}.${name}`, {
      description,
      unit: 's',
      advice: {
        explicitBucketBoundaries: LATENCY_HISTOGRAM_BOUNDARIES_S,
      },
    }),
  );
  return {
    // Merge the per-process worker identity into every observation so two
    // forked syncer workers produce DISTINCT series instead of colliding into
    // one flickering series (caller-supplied attributes win on key conflict).
    recordMs: (durationMs, attributes) =>
      h.record(durationMs / 1000, {...workerAttributes, ...attributes}),
  };
}

const counters = cache<Counter>();

export function getOrCreateCounter(
  category: Category,
  name: string,
  description: string,
): Counter;
export function getOrCreateCounter(
  category: Category,
  name: string,
  opts: Options,
): Counter;
export function getOrCreateCounter(
  category: Category,
  name: string,
  opts: string | Options,
): Counter {
  const raw = counters(name, name =>
    getMeter().createCounter(
      `zero.${category}.${name}`,
      typeof opts === 'string' ? {description: opts} : opts,
    ),
  );
  // Wrap to merge workerAttributes — same fix the histograms got.
  return {
    add: (value: number, attributes?: Attributes) =>
      raw.add(value, {...workerAttributes, ...attributes}),
  };
}

const gauges = cache<ObservableGauge>();

export function getOrCreateGauge(
  category: Category,
  name: string,
  description: string,
): ObservableGauge;
export function getOrCreateGauge(
  category: Category,
  name: string,
  opts: Options,
): ObservableGauge;
export function getOrCreateGauge(
  category: Category,
  name: string,
  opts: string | Options,
): ObservableGauge {
  // Gauges use addCallback — the callback is invoked by the meter, so we
  // can't wrap the add path. Callers that need worker-distinguished series
  // should use observeWithWorker() below, or merge workerAttributes in
  // their callback.
  return gauges(name, name =>
    getMeter().createObservableGauge(
      `zero.${category}.${name}`,
      typeof opts === 'string' ? {description: opts} : opts,
    ),
  );
}

/**
 * Observe a gauge value with the per-process worker attributes merged in.
 * Use this in gauge callbacks instead of result.observe() when the gauge
 * is emitted from a forked syncer worker to ensure distinct series.
 */
export function observeWithWorker(
  result: {observe: (value: number, attributes?: Attributes) => void},
  value: number,
  attributes?: Attributes,
): void {
  result.observe(value, {...workerAttributes, ...attributes});
}
