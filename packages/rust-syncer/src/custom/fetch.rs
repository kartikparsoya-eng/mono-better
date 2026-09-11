//! Port of `zero-cache/src/custom/fetch.ts` — the API-server fetch helpers:
//! URL-pattern allowlist matching, retry backoff, and the error body preview
//!.

use serde::Deserialize;
use serde_json::Value;
use urlpattern::{UrlPattern, UrlPatternInit, UrlPatternMatchInput, UrlPatternOptions};

use crate::custom::metrics::ApiErrorAttrs;
use crate::protocol::error::ErrorBody;
use crate::protocol::error_reason_enum::ErrorReason;
use crate::protocol::push::PushError;

/// WHATWG URLPattern match of `url` against `pattern` (TS `urlMatch` /
/// `compileUrlPattern`), backed by the `urlpattern` crate for true parity with
/// TS's `urlpattern-polyfill` — full component-aware matching (protocol / host /
/// port / path), not a flat glob.
pub fn url_match(pattern: &str, url: &str) -> bool {
    // Port of TS `urlMatch(url, [compileUrlPattern(pattern)])` (custom/fetch.ts):
    // compile the pattern as a WHATWG URLPattern and `.test(url)`. Backed by the
    // `urlpattern` crate — the same spec as TS's `urlpattern-polyfill` — so `*`
    // matches WITHIN a component (crosses `.` in the host, `/` in the path) but
    // NEVER across component boundaries (scheme / host / port / path). The old
    // flat glob let `*` cross the host→path boundary, so an attacker host
    // `https://evil.com/x.example.com/q` matched `https://*.example.com/q` — an
    // allowlist bypass (F-FETCH-1). Unspecified pattern components default to `*`
    // (constructor-string parsing), so query/hash are ignored exactly like TS.
    //
    // A pattern that fails to compile (TS throws at config time via
    // `compileUrlPattern`) or a URL that fails to parse → no match (fail-closed).
    let Ok(init) = UrlPatternInit::parse_constructor_string::<regex::Regex>(pattern, None) else {
        return false;
    };
    let Ok(compiled) = UrlPattern::<regex::Regex>::parse(init, UrlPatternOptions::default()) else {
        return false;
    };
    let Ok(target) = ::url::Url::parse(url) else {
        return false;
    };
    compiled
        .test(UrlPatternMatchInput::Url(target))
        .unwrap_or(false)
}

pub(crate) fn get_backoff_delay_ms(attempt: u32) -> u64 {
    let jitter = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0)
        % 100) as u64;
    (100u64 * 2u64.pow(attempt.saturating_sub(1)) + jitter).min(1000)
}

/// Max bytes of a failing relay response body echoed back in a `PushFailed`
/// frame (TS `bodyPreview` parity — never buffer an unbounded error body).
pub(crate) const BODY_PREVIEW_CAP: usize = 1024;

/// Read up to `cap` bytes of a response body as a lossy UTF-8 preview. Bounded
/// so a huge error page can't be buffered into the `PushFailed` frame.
pub(crate) async fn read_body_preview(resp: reqwest::Response, cap: usize) -> Option<String> {
    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            // TS fetch.ts:73 `lc.warn?.('failed to get body preview', …)`.
            tracing::warn!(error = %e, "failed to get body preview");
            return None;
        }
    };
    if bytes.is_empty() {
        return None;
    }
    let end = bytes.len().min(cap);
    Some(String::from_utf8_lossy(&bytes[..end]).into_owned())
}

/// Port of `apiErrorFromResult` (custom/fetch.ts:462-486): a 2xx whose
/// parsed body is itself an error — an `errorBodySchema` body, the legacy
/// `['transformFailed', body]` tuple, or a legacy `pushErrorSchema` body — is
/// recorded as `api_error` rather than `success`, labelled by kind/reason.
/// Every `try` is valita `passthrough`: unknown keys do not disqualify a body.
pub fn api_error_from_result(result: &Value) -> Option<ApiErrorAttrs> {
    if ErrorBody::deserialize(result).is_ok() {
        return ApiErrorAttrs::from_value(result);
    }

    if let Some(arr) = result.as_array()
        && arr.first().and_then(Value::as_str) == Some("transformFailed")
    {
        let legacy_transform_failed = arr.get(1).unwrap_or(&Value::Null);
        return if ErrorBody::deserialize(legacy_transform_failed).is_ok() {
            ApiErrorAttrs::from_value(legacy_transform_failed)
        } else {
            None
        };
    }

    let legacy_push_error = PushError::deserialize(result).ok()?;
    let reason = serde_json::to_value(legacy_push_error_reason(&legacy_push_error))
        .ok()
        .and_then(|v| v.as_str().map(str::to_string));
    Some(ApiErrorAttrs {
        kind: "PushFailed".to_string(),
        reason,
    })
}

/// Port of `legacyPushErrorReason` (custom/fetch.ts:488-499).
fn legacy_push_error_reason(error: &PushError) -> ErrorReason {
    match error {
        PushError::Http { .. } => ErrorReason::Http,
        PushError::UnsupportedPushVersion { .. } => ErrorReason::UnsupportedPushVersion,
        PushError::UnsupportedSchemaVersion { .. } | PushError::ZeroPusher { .. } => {
            ErrorReason::Internal
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Port of TS `fetchFromAPIServer` backoff (custom/fetch.ts, #6315):
    /// `min(1000, 100·2^(attempt-1) + jitter(0..100))`. Pins the exponential
    /// base, the jitter range, and the 1000ms cap.
    #[test]
    fn get_backoff_delay_ms_matches_ts_bounds() {
        for attempt in 1..=4u32 {
            let base = 100u64 * 2u64.pow(attempt - 1);
            for _ in 0..8 {
                let d = get_backoff_delay_ms(attempt);
                assert!(
                    d >= base.min(1000) && d <= (base + 100).min(1000),
                    "attempt {attempt}: delay {d} outside [{}, {}]",
                    base.min(1000),
                    (base + 100).min(1000)
                );
            }
        }
        // Past the cap the delay clamps to exactly 1000 (jitter included).
        assert_eq!(get_backoff_delay_ms(5), 1000, "base 1600 must cap at 1000");
    }

    /// Layer-2 body-differential: `url_match` (the custom-query URL allowlist)
    /// must return the same bool as the REAL TS `urlMatch`+`compileUrlPattern`
    /// (native WHATWG `URLPattern`) for every (pattern, url) in
    /// `url-match-fixture.json` (generated by `generate-url-match-fixture.mjs`).
    /// Security-relevant: a divergence would mean the Rust syncer POSTs custom
    /// queries to a URL set the TS syncer would not (or vice versa). Patterns
    /// stay within the ported literal/`*`/`:name` subset.
    #[test]
    fn url_match_parity_against_ts() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/agentic/parity/url-match-fixture.json"
        );
        let bytes = std::fs::read(path)
            .unwrap_or_else(|e| panic!("failed to read url-match fixture {path}: {e}"));
        let fixture: serde_json::Value =
            serde_json::from_slice(&bytes).expect("url-match fixture is not valid JSON");
        let cases = fixture["cases"].as_array().expect("fixture.cases missing");
        assert!(!cases.is_empty());
        for case in cases {
            let pattern = case["pattern"].as_str().unwrap();
            let url = case["url"].as_str().unwrap();
            // Skip compile-error cases (TS URLPattern rejected the pattern; the
            // Rust glob does not validate — out of the differential's scope).
            let Some(want) = case["matched"].as_bool() else {
                continue;
            };
            assert_eq!(
                url_match(pattern, url),
                want,
                "url_match divergence: pattern={pattern:?} url={url:?}"
            );
        }
    }
}
