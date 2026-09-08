//! TDigest — port of `shared/src/tdigest.ts` (+ its two tiny helper modules
//! `shared/src/centroid.ts` and `shared/src/binary-search.ts`, folded here per
//! the AGENTS rule-3 exception: rust has no `shared` crate twin, and these are
//! single-purpose utilities whose only rust consumer is the inspector's
//! `InspectorDelegate` server-metrics store — see `server/inspector_delegate.rs`).
//!
//! Upstream: Apache License 2.0, https://github.com/influxdata/tdigest.
//!
//! A TDigest accumulates rank-based statistics (quantiles / trimmed means)
//! on-line. The inspector uses only `new` + `add` + `to_json` on the server
//! side; `quantile`/`cdf`/`merge`/`count`/`from_json` are ported for a faithful
//! 1:1 module (the client renders quantiles from the same JSON shape).
//!
//! `to_json` returns the exact TS `TDigestJSON` wire shape
//! (`[compression, mean0, weight0, mean1, weight1, …]`, tdigest-schema.ts):
//! `to_json_value` renders it as a JSON array, emitting integer-valued numbers
//! without a fractional part so the bytes match TS `JSON.stringify` for the
//! common case (compression `1000`, whole weights).

use serde_json::Value;

/// Port of `Centroid` (centroid.ts:6) — the average position of all points in a
/// shape.
#[derive(Clone, Debug)]
pub struct Centroid {
    pub mean: f64,
    pub weight: f64,
}

impl Centroid {
    pub fn new(mean: f64, weight: f64) -> Self {
        Self { mean, weight }
    }

    /// Port of `Centroid.add` (centroid.ts:14).
    pub fn add(&mut self, r: &Centroid) {
        // TS: `if (r.weight < 0) throw new Error('centroid weight cannot be less
        // than zero');`. Only reachable with a negative weight, which
        // `addCentroid` already filters, so this stays a loud invariant.
        assert!(r.weight >= 0.0, "centroid weight cannot be less than zero");
        if self.weight != 0.0 {
            self.weight += r.weight;
            self.mean += (r.weight * (r.mean - self.mean)) / self.weight;
        } else {
            self.weight = r.weight;
            self.mean = r.mean;
        }
    }
}

/// Port of `sortCentroidList` (centroid.ts:31): sort by mean ascending. TS
/// `Array.prototype.sort` is stable (V8); rust `sort_by` is stable too, so ties
/// keep insertion order identically.
fn sort_centroid_list(centroids: &mut [Centroid]) {
    centroids.sort_by(|a, b| {
        a.mean
            .partial_cmp(&b.mean)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
}

/// Port of `binarySearch` (binary-search.ts:16): index of the first element
/// `>=` the needle, expressed through the sign of `compare(i)`.
fn binary_search(high: usize, compare: impl Fn(usize) -> f64) -> usize {
    let mut low = 0usize;
    let mut high = high;
    while low < high {
        let mid = low + ((high - low) >> 1);
        let i = compare(mid);
        if i == 0.0 {
            return mid;
        }
        if i > 0.0 {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}

/// Port of `TDigest` (tdigest.ts:16).
pub struct TDigest {
    pub compression: f64,
    max_processed: usize,
    max_unprocessed: usize,
    processed: Vec<Centroid>,
    unprocessed: Vec<Centroid>,
    cumulative: Vec<f64>,
    processed_weight: f64,
    unprocessed_weight: f64,
    min: f64,
    max: f64,
}

impl Default for TDigest {
    fn default() -> Self {
        Self::new(1000.0)
    }
}

impl TDigest {
    /// Port of the constructor (tdigest.ts:29): default compression `1000`.
    pub fn new(compression: f64) -> Self {
        let mut d = Self {
            compression,
            max_processed: processed_size(0, compression),
            max_unprocessed: unprocessed_size(0, compression),
            processed: Vec::new(),
            unprocessed: Vec::new(),
            cumulative: Vec::new(),
            processed_weight: 0.0,
            unprocessed_weight: 0.0,
            min: f64::MAX,
            max: -f64::MAX,
        };
        d.reset();
        d
    }

    /// Port of `TDigest.fromJSON` (tdigest.ts:40).
    pub fn from_json(data: &[f64]) -> Result<Self, String> {
        let mut digest = Self::new(*data.first().unwrap_or(&1000.0));
        if data.len() % 2 != 1 {
            return Err("Invalid centroids array".to_string());
        }
        let mut i = 1;
        while i < data.len() {
            digest.add(data[i], data[i + 1]);
            i += 2;
        }
        Ok(digest)
    }

    /// Port of `reset` (tdigest.ts:51).
    pub fn reset(&mut self) {
        self.processed.clear();
        self.unprocessed.clear();
        self.cumulative.clear();
        self.processed_weight = 0.0;
        self.unprocessed_weight = 0.0;
        self.min = f64::MAX;
        self.max = -f64::MAX;
    }

    /// Port of `add` (tdigest.ts:61): default weight `1`.
    pub fn add(&mut self, mean: f64, weight: f64) {
        self.add_centroid(Centroid::new(mean, weight));
    }

    /// Port of `addCentroidList` (tdigest.ts:66).
    pub fn add_centroid_list(&mut self, centroid_list: Vec<Centroid>) {
        for c in centroid_list {
            self.add_centroid(c);
        }
    }

    /// Port of `addCentroid` (tdigest.ts:76): NaN means and non-finite / `<= 0`
    /// weights are ignored.
    pub fn add_centroid(&mut self, c: Centroid) {
        if c.mean.is_nan() || c.weight <= 0.0 || c.weight.is_nan() || !c.weight.is_finite() {
            return;
        }

        self.unprocessed.push(Centroid::new(c.mean, c.weight));
        self.unprocessed_weight += c.weight;

        if self.processed.len() > self.max_processed
            || self.unprocessed.len() > self.max_unprocessed
        {
            self.process();
        }
    }

    /// Port of `merge` (tdigest.ts:102).
    pub fn merge(&mut self, t2: &mut TDigest) {
        t2.process();
        self.add_centroid_list(t2.processed.clone());
    }

    /// Port of `#process` (tdigest.ts:107).
    fn process(&mut self) {
        if self.unprocessed.is_empty() && self.processed.len() <= self.max_processed {
            return;
        }

        // Append all processed centroids to the unprocessed list and sort.
        self.unprocessed.append(&mut self.processed.clone());
        self.processed.clear();
        sort_centroid_list(&mut self.unprocessed);

        // Reset processed list with first centroid.
        self.processed.push(self.unprocessed[0].clone());

        self.processed_weight += self.unprocessed_weight;
        self.unprocessed_weight = 0.0;
        let mut so_far = self.unprocessed[0].weight;
        let mut limit = self.processed_weight * self.integrated_q(1.0);
        for i in 1..self.unprocessed.len() {
            let centroid = self.unprocessed[i].clone();
            let projected = so_far + centroid.weight;
            if projected <= limit {
                so_far = projected;
                let last = self.processed.len() - 1;
                self.processed[last].add(&centroid);
            } else {
                let k1 = self.integrated_location(so_far / self.processed_weight);
                limit = self.processed_weight * self.integrated_q(k1 + 1.0);
                so_far += centroid.weight;
                self.processed.push(centroid);
            }
        }
        self.min = self.min.min(self.processed[0].mean);
        self.max = self.max.max(self.processed[self.processed.len() - 1].mean);
        self.unprocessed.clear();
    }

    /// Port of `centroids` (tdigest.ts:152): a copy of the processed centroids.
    pub fn centroids(&mut self) -> Vec<Centroid> {
        self.process();
        self.processed.clone()
    }

    /// Port of `count` (tdigest.ts:157).
    pub fn count(&mut self) -> f64 {
        self.process();
        self.processed_weight
    }

    /// Port of `toJSON` (tdigest.ts:169): `[compression, mean0, weight0, …]`.
    pub fn to_json(&mut self) -> Vec<f64> {
        self.process();
        let mut data = Vec::with_capacity(1 + self.processed.len() * 2);
        data.push(self.compression);
        for centroid in &self.processed {
            data.push(centroid.mean);
            data.push(centroid.weight);
        }
        data
    }

    /// Render `to_json` as a JSON array, emitting integer-valued numbers without
    /// a fractional part so the bytes match TS `JSON.stringify(digest.toJSON())`
    /// (e.g. `[1000, 12.5, 1]`, not `[1000.0, 12.5, 1.0]`).
    pub fn to_json_value(&mut self) -> Value {
        Value::Array(self.to_json().into_iter().map(number_to_value).collect())
    }

    /// Port of `#updateCumulative` (tdigest.ts:178).
    fn update_cumulative(&mut self) {
        // Weight can only increase, so if the last cumulative equals the total
        // weight nothing has changed.
        if self
            .cumulative
            .last()
            .is_some_and(|&last| last == self.processed_weight)
        {
            return;
        }
        let n = self.processed.len() + 1;
        if self.cumulative.len() > n {
            self.cumulative.truncate(n);
        }
        // Grow to hold indices 0..=processed.len().
        if self.cumulative.len() < n {
            self.cumulative.resize(n, 0.0);
        }

        let mut prev = 0.0;
        for i in 0..self.processed.len() {
            let cur = self.processed[i].weight;
            self.cumulative[i] = prev + cur / 2.0;
            prev += cur;
        }
        self.cumulative[self.processed.len()] = prev;
    }

    /// Port of `quantile` (tdigest.ts:206). `q` in `[0, 1]`; `NaN` when the
    /// digest is empty or the input is out of range.
    pub fn quantile(&mut self, q: f64) -> f64 {
        self.process();
        self.update_cumulative();
        // Kept as the TS `q < 0 || q > 1` shape for 1:1 readability
        // (tdigest.ts:209) rather than a range-contains rewrite.
        #[allow(clippy::manual_range_contains)]
        if q < 0.0 || q > 1.0 || self.processed.is_empty() {
            return f64::NAN;
        }
        if self.processed.len() == 1 {
            return self.processed[0].mean;
        }
        let index = q * self.processed_weight;
        if index <= self.processed[0].weight / 2.0 {
            return self.min
                + ((2.0 * index) / self.processed[0].weight) * (self.processed[0].mean - self.min);
        }

        let cumulative = &self.cumulative;
        let lower = binary_search(cumulative.len(), |i| -cumulative[i] + index);

        if lower + 1 != self.cumulative.len() {
            let z1 = index - self.cumulative[lower - 1];
            let z2 = self.cumulative[lower] - index;
            return weighted_average(
                self.processed[lower - 1].mean,
                z2,
                self.processed[lower].mean,
                z1,
            );
        }

        let z1 = index - self.processed_weight - self.processed[lower - 1].weight / 2.0;
        let z2 = self.processed[lower - 1].weight / 2.0 - z1;
        weighted_average(
            self.processed[self.processed.len() - 1].mean,
            z1,
            self.max,
            z2,
        )
    }

    /// Port of `cdf` (tdigest.ts:250).
    pub fn cdf(&mut self, x: f64) -> f64 {
        self.process();
        self.update_cumulative();
        match self.processed.len() {
            0 => return 0.0,
            1 => {
                let width = self.max - self.min;
                if x <= self.min {
                    return 0.0;
                }
                if x >= self.max {
                    return 1.0;
                }
                if x - self.min <= width {
                    return 0.5;
                }
                return (x - self.min) / width;
            }
            _ => {}
        }

        if x <= self.min {
            return 0.0;
        }
        if x >= self.max {
            return 1.0;
        }
        let m0 = self.processed[0].mean;
        // Left tail.
        if x <= m0 {
            if m0 - self.min > 0.0 {
                return (((x - self.min) / (m0 - self.min)) * self.processed[0].weight)
                    / self.processed_weight
                    / 2.0;
            }
            return 0.0;
        }
        // Right tail.
        let last = self.processed.len() - 1;
        let mn = self.processed[last].mean;
        if x >= mn {
            if self.max - mn > 0.0 {
                return 1.0
                    - (((self.max - x) / (self.max - mn)) * self.processed[last].weight)
                        / self.processed_weight
                        / 2.0;
            }
            return 1.0;
        }

        let processed = &self.processed;
        let upper = binary_search(processed.len(), |i| {
            // Treat equals as greater than so we use the upper index.
            let d = x - processed[i].mean;
            if d == 0.0 { 1.0 } else { d }
        });

        let z1 = x - self.processed[upper - 1].mean;
        let z2 = self.processed[upper].mean - x;
        weighted_average(self.cumulative[upper - 1], z2, self.cumulative[upper], z1)
            / self.processed_weight
    }

    /// Port of `#integratedQ` (tdigest.ts:327).
    fn integrated_q(&self, k: f64) -> f64 {
        ((k.min(self.compression) * std::f64::consts::PI / self.compression
            - std::f64::consts::PI / 2.0)
            .sin()
            + 1.0)
            / 2.0
    }

    /// Port of `#integratedLocation` (tdigest.ts:338).
    fn integrated_location(&self, q: f64) -> f64 {
        (self.compression * ((2.0 * q - 1.0).asin() + std::f64::consts::PI / 2.0))
            / std::f64::consts::PI
    }
}

/// Port of `byteSizeForCompression` (tdigest.ts:345).
pub fn byte_size_for_compression(comp: f64) -> f64 {
    // TS `comp | 0` truncates toward zero into an int32.
    let c = (comp as i64 as i32) as f64;
    c * 40.0
}

/// Port of `weightedAverage` (tdigest.ts:367).
fn weighted_average(x1: f64, w1: f64, x2: f64, w2: f64) -> f64 {
    if x1 <= x2 {
        weighted_average_sorted(x1, w1, x2, w2)
    } else {
        weighted_average_sorted(x2, w2, x1, w1)
    }
}

/// Port of `weightedAverageSorted` (tdigest.ts:379).
fn weighted_average_sorted(x1: f64, w1: f64, x2: f64, w2: f64) -> f64 {
    let x = (x1 * w1 + x2 * w2) / (w1 + w2);
    x1.max(x.min(x2))
}

/// Port of `processedSize` (tdigest.ts:389).
fn processed_size(size: usize, compression: f64) -> usize {
    if size == 0 {
        return (compression.ceil() as usize) * 2;
    }
    size
}

/// Port of `unprocessedSize` (tdigest.ts:396).
fn unprocessed_size(size: usize, compression: f64) -> usize {
    if size == 0 {
        return (compression.ceil() as usize) * 8;
    }
    size
}

/// Emit an integer-valued `f64` as a JSON integer (`1000`, not `1000.0`) and a
/// fractional one as a JSON float, matching how JS `JSON.stringify` renders the
/// numbers in a TDigest's `toJSON()` array.
fn number_to_value(n: f64) -> Value {
    if n.is_finite() && n.fract() == 0.0 && n.abs() < 9.007_199_254_740_992e15 {
        Value::Number((n as i64).into())
    } else {
        serde_json::Number::from_f64(n).map_or(Value::Null, Value::Number)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_digest_to_json_is_just_compression() {
        // `new TDigest().toJSON()` === `[1000]` (no centroids).
        let mut d = TDigest::default();
        assert_eq!(d.to_json(), vec![1000.0]);
        assert_eq!(d.to_json_value(), serde_json::json!([1000]));
    }

    #[test]
    fn single_point_digest_to_json_value_uses_integer_weight() {
        // One `add(12.5)` → `[1000, 12.5, 1]` (weight 1 rendered as an integer).
        let mut d = TDigest::default();
        d.add(12.5, 1.0);
        assert_eq!(d.to_json_value(), serde_json::json!([1000, 12.5, 1]));
    }

    /// TS-golden: drive the exact same `add` sequence the real
    /// `shared/src/tdigest.ts` was driven with (via tsx) and pin `to_json` +
    /// `quantile` + `cdf` to its output. NON-VACUOUS: the golden exercises the
    /// `#process` sort + the `integratedQ`-driven merge `limit` (a wrong
    /// `integratedQ` merges centroids that TS keeps separate → the array's length
    /// and contents change) and the `quantile`/`cdf` interpolation
    /// (`binarySearch` + `updateCumulative`). Reverting `sort_centroid_list` to a
    /// no-op, or `integrated_q` to a constant, fails the exact-array assertion.
    #[test]
    fn ts_golden_matches_real_tdigest() {
        // vals driven through TS `new TDigest()` then `toJSON()` /
        // `quantile(0.5)` / `cdf(7)`.
        let vals = [
            5.0, 1.0, 3.0, 3.0, 2.0, 100.0, 7.0, 7.0, 7.0, 42.0, 0.5, 88.0, 12.0, 12.0, 3.0,
        ];
        let mut d = TDigest::default();
        for v in vals {
            d.add(v, 1.0);
        }
        // Byte-exact TS golden (all weights 1, sorted by mean, no merge).
        let golden = serde_json::json!([
            1000, 0.5, 1, 1, 1, 2, 1, 3, 1, 3, 1, 3, 1, 5, 1, 7, 1, 7, 1, 7, 1, 12, 1, 12, 1, 42,
            1, 88, 1, 100, 1
        ]);
        assert_eq!(
            d.to_json_value(),
            golden,
            "to_json must match TS byte-for-byte"
        );
        assert_eq!(d.quantile(0.5), 7.0, "quantile(0.5) TS golden");
        assert!(
            (d.cdf(7.0) - 0.633_333_333_333_333_3).abs() < 1e-12,
            "cdf(7) TS golden ≈ 0.6333…; got {}",
            d.cdf(7.0)
        );
    }
}
