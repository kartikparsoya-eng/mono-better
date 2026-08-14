//! JWT auth validation (Stage D). Port of the verify contract in
//! `zero-cache/src/auth/jwt.ts` + `auth.ts`: verify the token against the
//! configured key and require `sub == userID`.
//!
//! Config precedence matches TS `verifyToken`: `jwk`, then `secret`, then
//! `jwksUrl`. When NONE is configured the token is treated as opaque and passes
//! unverified (TS `resolveAuth` opaque path).
//!
//! All three key modes are verified in-process:
//! - `secret` — HMAC (HS256/384/512).
//! - `jwk` — a single inline asymmetric public JWK (RSA/EC/OKP); the algorithm
//!   is taken from the JWK, not the (attacker-controlled) token header.
//! - `jwksUrl` — a remote JWKS document, fetched once and cached per URL (TS
//!   `createRemoteJWKSet` module singleton), with the signing key selected by
//!   the token's `kid` header.

use crate::protocol::ErrorBody;
use crate::router::AuthValidator;
use jsonwebtoken::jwk::{Jwk, JwkSet};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex as StdMutex};
use std::time::{Duration, Instant};

/// Cached remote JWKS documents, keyed by URL. Mirrors the module-level
/// `remoteKeyset` singleton in TS `jwt.ts` (jose's `createRemoteJWKSet`), which
/// fetches once and refreshes in the background. We refresh on a fixed TTL.
static JWKS_CACHE: LazyLock<StdMutex<HashMap<String, CachedJwks>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

/// How long a fetched JWKS is reused on the fast path before it is considered
/// stale and a `kid`-miss is allowed to refetch.
const JWKS_TTL: Duration = Duration::from_secs(300);

/// Minimum time between refetches of the same URL, regardless of `kid` misses.
/// Without this, an attacker spamming tokens with random `kid` headers forces
/// one outbound JWKS fetch per request — hammering the syncer and the identity
/// provider (which may then rate-limit real traffic). jose's `createRemoteJWKSet`
/// enforces the same cooldown for exactly this reason.
const JWKS_REFETCH_COOLDOWN: Duration = Duration::from_secs(30);

struct CachedJwks {
    fetched_at: Instant,
    set: JwkSet,
}

/// Decode a JWT's claims (payload) WITHOUT verifying the signature. Safe only
/// for tokens already verified upstream (see `AuthValidator::validate_auth`,
/// which runs before a connection reaches the CG thread). Returns `{}` for a
/// non-JWT/opaque token or on any decode error. The claims become the
/// `authData` bound into read-permission rules.
pub fn decode_jwt_claims(token: &str) -> Value {
    use base64::Engine;
    let mut parts = token.split('.');
    let (_header, payload) = (parts.next(), parts.next());
    let Some(payload) = payload else {
        return json!({});
    };
    match base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload) {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|_| json!({})),
        Err(_) => json!({}),
    }
}

/// JWT auth config, mirroring `SyncerConfig`'s auth fields.
pub struct JwtAuthValidator {
    pub jwk: Option<String>,
    pub secret: Option<String>,
    pub jwks_url: Option<String>,
    /// Expected `iss` claim. When set, tokens with a different/missing issuer are
    /// rejected; when `None`, issuer is not validated (matches TS, which only
    /// passes the `issuer` option when `config.auth.issuer` is set).
    pub issuer: Option<String>,
    /// Expected `aud` claim, same conditional semantics as `issuer`.
    pub audience: Option<String>,
}

/// Apply the claim checks TS performs in `verifyToken`: `subject` always, plus
/// `issuer`/`audience` only when configured. The audience default is flipped off
/// when unconfigured so a token bearing an `aud` claim isn't rejected for a
/// server that never opted into audience validation.
fn apply_claim_validation(
    validation: &mut Validation,
    user_id: &str,
    issuer: Option<&str>,
    audience: Option<&str>,
) {
    validation.sub = Some(user_id.to_string());
    // jose validates `nbf` by default; jsonwebtoken's `validate_nbf` defaults to
    // false. Without this, a not-yet-valid token (nbf in the future) is honored
    // early by the Rust path but rejected by TS — a real auth divergence.
    validation.validate_nbf = true;
    // jose's `clockTolerance` defaults to 0 and the syncer never sets it, but
    // jsonwebtoken's `leeway` defaults to 60s — so without this, Rust would
    // accept a token up to 60s past `exp` (or 60s before `nbf`) that TS rejects,
    // partially re-opening the nbf/exp gap. Zero it to match jose.
    validation.leeway = 0;
    // Required-claim presence must mirror jose: it never requires `exp` (only
    // validates it when present), always requires `sub` (the subject option),
    // and requires `iss`/`aud` to be present when those options are configured.
    // jsonwebtoken defaults `required_spec_claims` to {"exp"}, which both
    // over-requires exp and under-requires sub/iss/aud — so set it explicitly.
    let mut required = std::collections::HashSet::new();
    required.insert("sub".to_string());
    if let Some(iss) = issuer {
        validation.set_issuer(&[iss]);
        required.insert("iss".to_string());
    }
    match audience {
        Some(aud) => {
            validation.set_audience(&[aud]);
            required.insert("aud".to_string());
        }
        None => validation.validate_aud = false,
    }
    validation.required_spec_claims = required;
}

#[derive(Debug, Deserialize)]
struct Claims {
    #[serde(default)]
    #[allow(dead_code)]
    sub: Option<String>,
}

impl JwtAuthValidator {
    fn has_config(&self) -> bool {
        self.jwk.is_some() || self.secret.is_some() || self.jwks_url.is_some()
    }

    /// Verify a token against the configured key. `jwksUrl` is handled in the
    /// async `validate_auth` (it may fetch); this covers the synchronous `jwk`
    /// and `secret` modes. Precedence matches TS `verifyToken`.
    fn verify_sync(&self, token: &str, user_id: &str) -> Result<(), String> {
        if let Some(jwk_json) = &self.jwk {
            let jwk: Jwk = serde_json::from_str(jwk_json)
                .map_err(|e| format!("invalid AUTH_JWK json: {e}"))?;
            return verify_with_jwk(
                token,
                &jwk,
                user_id,
                self.issuer.as_deref(),
                self.audience.as_deref(),
            );
        }
        if let Some(secret) = &self.secret {
            let key = DecodingKey::from_secret(secret.as_bytes());
            let mut validation = Validation::new(Algorithm::HS256);
            validation.algorithms = vec![Algorithm::HS256, Algorithm::HS384, Algorithm::HS512];
            apply_claim_validation(
                &mut validation,
                user_id,
                self.issuer.as_deref(),
                self.audience.as_deref(),
            );
            decode::<Claims>(token, &key, &validation).map_err(|e| e.to_string())?;
            return Ok(());
        }
        Err("no auth key configured".to_string())
    }

    /// Verify a token against the remote JWKS at `jwks_url`, fetching + caching
    /// the document by URL. Port of the `createRemoteJWKSet` + `jwtVerify` path.
    async fn verify_with_jwks(
        &self,
        jwks_url: &str,
        token: &str,
        user_id: &str,
    ) -> Result<(), String> {
        // The signing key is selected by the token's `kid` header.
        let header = decode_header(token).map_err(|e| format!("invalid JWT header: {e}"))?;

        // Fast path: a fresh cached JWKS that contains the key.
        if let Some(jwk) = lookup_cached_jwk(jwks_url, header.kid.as_deref()) {
            return verify_with_jwk(
                token,
                &jwk,
                user_id,
                self.issuer.as_deref(),
                self.audience.as_deref(),
            );
        }

        // Cache miss (no entry, stale, or the token's `kid` is not in the cached
        // set). Refetching on every miss is a DoS amplifier: a storm of tokens
        // with random `kid`s would trigger one outbound fetch each. Gate the
        // refetch behind a cooldown — within the window, fail closed against the
        // cached set instead of hitting the IdP again.
        if within_refetch_cooldown(jwks_url) {
            return Err("no matching key in JWKS (refetch on cooldown)".to_string());
        }

        // Fetch and cache the set BEFORE selecting the key, so `fetched_at` is
        // recorded even when the fetched set does not contain the requested
        // `kid` — otherwise repeated unknown-`kid` requests would each refetch.
        let set: JwkSet = reqwest::get(jwks_url)
            .await
            .map_err(|e| format!("JWKS fetch failed: {e}"))?
            .json()
            .await
            .map_err(|e| format!("JWKS parse failed: {e}"))?;
        if let Ok(mut cache) = JWKS_CACHE.lock() {
            cache.insert(
                jwks_url.to_string(),
                CachedJwks {
                    fetched_at: Instant::now(),
                    set,
                },
            );
        }
        let jwk = lookup_cached_jwk(jwks_url, header.kid.as_deref())
            .ok_or_else(|| "no matching key in JWKS".to_string())?;
        verify_with_jwk(
            token,
            &jwk,
            user_id,
            self.issuer.as_deref(),
            self.audience.as_deref(),
        )
    }
}

/// Map a JWK `KeyAlgorithm` to a jsonwebtoken signature `Algorithm`, rejecting
/// encryption algorithms (`RSA1_5`, `RSA-OAEP*`) which cannot sign a JWT.
/// `jsonwebtoken`'s own converter is private, so we map the shared names here.
fn key_algorithm_to_signature_alg(
    ka: jsonwebtoken::jwk::KeyAlgorithm,
) -> Result<Algorithm, String> {
    use jsonwebtoken::jwk::KeyAlgorithm as K;
    Ok(match ka {
        K::HS256 => Algorithm::HS256,
        K::HS384 => Algorithm::HS384,
        K::HS512 => Algorithm::HS512,
        K::ES256 => Algorithm::ES256,
        K::ES384 => Algorithm::ES384,
        K::RS256 => Algorithm::RS256,
        K::RS384 => Algorithm::RS384,
        K::RS512 => Algorithm::RS512,
        K::PS256 => Algorithm::PS256,
        K::PS384 => Algorithm::PS384,
        K::PS512 => Algorithm::PS512,
        K::EdDSA => Algorithm::EdDSA,
        other => {
            return Err(format!(
                "JWK alg {other:?} is not a JWT signature algorithm"
            ));
        }
    })
}

/// Select a JWK from a set by `kid`. With no `kid` on the token, a single-key
/// set is unambiguous, so use its only key (jose behaves the same).
fn select_jwk<'a>(set: &'a JwkSet, kid: Option<&str>) -> Option<&'a Jwk> {
    match kid {
        Some(kid) => set.find(kid),
        None if set.keys.len() == 1 => set.keys.first(),
        None => None,
    }
}

/// Look up a key in the cached JWKS for `url`, if the cache entry is still
/// fresh. Returns an owned clone so the cache lock isn't held during verify.
fn lookup_cached_jwk(url: &str, kid: Option<&str>) -> Option<Jwk> {
    let cache = JWKS_CACHE.lock().ok()?;
    let entry = cache.get(url)?;
    if entry.fetched_at.elapsed() >= JWKS_TTL {
        return None;
    }
    select_jwk(&entry.set, kid).cloned()
}

/// True if `url` was fetched within the refetch cooldown, so a `kid`-miss must
/// NOT trigger another fetch (DoS protection). False when there is no cache
/// entry (first fetch) or the last fetch is older than the cooldown.
fn within_refetch_cooldown(url: &str) -> bool {
    JWKS_CACHE
        .lock()
        .ok()
        .and_then(|cache| {
            cache
                .get(url)
                .map(|entry| entry.fetched_at.elapsed() < JWKS_REFETCH_COOLDOWN)
        })
        .unwrap_or(false)
}

/// Verify a token against a single asymmetric JWK. The algorithm is taken from
/// the JWK (`alg`), NOT the token header, to prevent algorithm-confusion. The
/// `sub` claim is required to equal `user_id`.
fn verify_with_jwk(
    token: &str,
    jwk: &Jwk,
    user_id: &str,
    issuer: Option<&str>,
    audience: Option<&str>,
) -> Result<(), String> {
    let alg = match jwk.common.key_algorithm {
        Some(ka) => key_algorithm_to_signature_alg(ka)?,
        None => {
            // RFC 7517 makes `alg` OPTIONAL and some identity providers — notably
            // Microsoft / Azure AD — omit it in their JWKS. jose falls back to the
            // token header's alg (constrained by the key type), so a hard reject
            // here would fail-closed on legitimate Azure AD tokens. Match jose:
            // use the header alg, but reject any HMAC alg so an asymmetric public
            // key can NEVER verify an HS token (the algorithm-confusion attack).
            // jsonwebtoken additionally rejects a key-family mismatch at verify
            // time (e.g. an EC key against an RS token), so RS/ES/PS confusion is
            // caught by the signature check.
            let header = decode_header(token).map_err(|e| format!("invalid JWT header: {e}"))?;
            if matches!(
                header.alg,
                Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512
            ) {
                return Err("token uses an HMAC alg but the JWK is asymmetric".to_string());
            }
            header.alg
        }
    };
    let key = DecodingKey::from_jwk(jwk).map_err(|e| format!("invalid JWK: {e}"))?;
    let mut validation = Validation::new(alg);
    validation.algorithms = vec![alg];
    apply_claim_validation(&mut validation, user_id, issuer, audience);
    decode::<Claims>(token, &key, &validation).map_err(|e| e.to_string())?;
    Ok(())
}

#[async_trait::async_trait]
impl AuthValidator for JwtAuthValidator {
    async fn validate_auth(
        &self,
        _client_group_id: &str,
        _client_id: &str,
        user_id: Option<&str>,
        auth: Option<&str>,
    ) -> Result<(), ErrorBody> {
        // No token → nothing to verify (the caller only calls us for non-empty
        // tokens, but stay defensive).
        let Some(token) = auth.filter(|t| !t.is_empty()) else {
            return Ok(());
        };
        // No JWT key configured → opaque token, pass unverified (TS opaque path).
        if !self.has_config() {
            return Ok(());
        }
        // JWT configured → a userID is required and must match `sub`.
        let Some(user_id) = user_id else {
            return Err(ErrorBody::unauthorized(
                "Authenticated connections require a userID.",
            ));
        };
        // Precedence: jwk, secret, jwksUrl (matches TS verifyToken). The first
        // two are synchronous; jwksUrl may fetch, so it's handled here.
        let result = if self.jwk.is_some() || self.secret.is_some() {
            self.verify_sync(token, user_id)
        } else if let Some(jwks_url) = &self.jwks_url {
            self.verify_with_jwks(jwks_url, token, user_id).await
        } else {
            Err("no auth key configured".to_string())
        };
        result.map_err(|e| ErrorBody::unauthorized(format!("JWT verification failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde::Serialize;

    #[derive(Serialize)]
    struct TestClaims {
        sub: String,
        exp: usize,
    }

    fn token(secret: &str, sub: &str) -> String {
        let claims = TestClaims {
            sub: sub.to_string(),
            exp: 9_999_999_999, // far future
        };
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap()
    }

    fn validator(secret: &str) -> JwtAuthValidator {
        JwtAuthValidator {
            jwk: None,
            secret: Some(secret.to_string()),
            jwks_url: None,
            issuer: None,
            audience: None,
        }
    }

    #[test]
    fn jwks_refetch_cooldown_gates_fetches() {
        use jsonwebtoken::jwk::JwkSet;
        let url = "https://example.test/jwks-cooldown-unit";
        JWKS_CACHE.lock().unwrap().remove(url);

        // No cache entry -> not on cooldown, so the first fetch is allowed.
        assert!(!within_refetch_cooldown(url));

        // A fresh fetch puts the URL on cooldown: a subsequent kid-miss must NOT
        // refetch (this is what stops an unknown-kid storm from hammering the IdP).
        JWKS_CACHE.lock().unwrap().insert(
            url.to_string(),
            CachedJwks {
                fetched_at: Instant::now(),
                set: JwkSet { keys: vec![] },
            },
        );
        assert!(within_refetch_cooldown(url));

        // A fetch older than the cooldown is allowed to refetch (key rotation).
        if let Some(old) =
            Instant::now().checked_sub(JWKS_REFETCH_COOLDOWN + Duration::from_secs(5))
        {
            JWKS_CACHE.lock().unwrap().insert(
                url.to_string(),
                CachedJwks {
                    fetched_at: old,
                    set: JwkSet { keys: vec![] },
                },
            );
            assert!(!within_refetch_cooldown(url));
        }
        JWKS_CACHE.lock().unwrap().remove(url);
    }

    /// TS-vs-Rust JWT parity: every token's accept/reject decision must match
    /// the real TS `verifyToken` captured in `agentic/parity/auth-fixture.json`
    /// (generated by `generate-auth-fixture.mjs`). Pins the claim-validation
    /// contract — exp/nbf/sub/iss/aud and whether exp is REQUIRED — to TS.
    #[tokio::test]
    async fn jwt_parity_against_ts() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/agentic/parity/auth-fixture.json"
        );
        let bytes = std::fs::read(path)
            .unwrap_or_else(|e| panic!("failed to read auth fixture {}: {}", path, e));
        let fixture: serde_json::Value =
            serde_json::from_slice(&bytes).expect("auth fixture is not valid JSON");

        for case in fixture
            .get("cases")
            .and_then(serde_json::Value::as_array)
            .expect("fixture.cases missing")
        {
            let desc = case.get("desc").and_then(|v| v.as_str()).unwrap_or("");
            // A case carries either a symmetric `secret` or an asymmetric `jwk`.
            let secret = case.get("secret").and_then(|v| v.as_str());
            let jwk = case.get("jwk").and_then(|v| v.as_str());
            let token = case.get("token").and_then(|v| v.as_str()).expect("token");
            let user_id = case.get("userID").and_then(|v| v.as_str()).expect("userID");
            let issuer = case.get("issuer").and_then(|v| v.as_str());
            let audience = case.get("audience").and_then(|v| v.as_str());
            let ts_accept = case
                .get("tsAccept")
                .and_then(serde_json::Value::as_bool)
                .expect("tsAccept");

            let v = JwtAuthValidator {
                jwk: jwk.map(str::to_string),
                secret: secret.map(str::to_string),
                jwks_url: None,
                issuer: issuer.map(str::to_string),
                audience: audience.map(str::to_string),
            };
            let rust_accept = v
                .validate_auth("cg", "c", Some(user_id), Some(token))
                .await
                .is_ok();
            assert_eq!(
                rust_accept, ts_accept,
                "JWT decision mismatch [{}]: rust_accept={} ts_accept={}",
                desc, rust_accept, ts_accept
            );
        }
    }

    #[tokio::test]
    async fn accepts_valid_token_with_matching_sub() {
        let v = validator("s3cret");
        let t = token("s3cret", "user1");
        assert!(
            v.validate_auth("cg", "c", Some("user1"), Some(&t))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn rejects_sub_mismatch() {
        let v = validator("s3cret");
        let t = token("s3cret", "user1");
        let err = v
            .validate_auth("cg", "c", Some("user2"), Some(&t))
            .await
            .unwrap_err();
        assert_eq!(*err.kind(), crate::protocol::ErrorKind::Unauthorized);
    }

    #[tokio::test]
    async fn validates_issuer_and_audience_only_when_configured() {
        #[derive(Serialize)]
        struct ClaimsIssAud {
            sub: String,
            exp: usize,
            iss: String,
            aud: String,
        }
        let mk = |iss: &str, aud: &str| {
            encode(
                &Header::new(Algorithm::HS256),
                &ClaimsIssAud {
                    sub: "user1".to_string(),
                    exp: 9_999_999_999,
                    iss: iss.to_string(),
                    aud: aud.to_string(),
                },
                &EncodingKey::from_secret(b"s3cret"),
            )
            .unwrap()
        };

        let v = JwtAuthValidator {
            jwk: None,
            secret: Some("s3cret".to_string()),
            jwks_url: None,
            issuer: Some("iss-A".to_string()),
            audience: Some("aud-A".to_string()),
        };
        // Matching iss + aud → accepted.
        assert!(
            v.validate_auth("cg", "c", Some("user1"), Some(&mk("iss-A", "aud-A")))
                .await
                .is_ok()
        );
        // Wrong issuer or audience → rejected.
        assert!(
            v.validate_auth("cg", "c", Some("user1"), Some(&mk("WRONG", "aud-A")))
                .await
                .is_err()
        );
        assert!(
            v.validate_auth("cg", "c", Some("user1"), Some(&mk("iss-A", "WRONG")))
                .await
                .is_err()
        );
        // With neither configured, a token that HAPPENS to carry iss/aud still
        // passes — we don't validate claims we didn't opt into (matches TS).
        let open = validator("s3cret");
        assert!(
            open.validate_auth("cg", "c", Some("user1"), Some(&mk("any", "any")))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn rejects_wrong_secret() {
        let v = validator("s3cret");
        let t = token("different", "user1");
        assert!(
            v.validate_auth("cg", "c", Some("user1"), Some(&t))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn requires_user_id_when_jwt_configured() {
        let v = validator("s3cret");
        let t = token("s3cret", "user1");
        assert!(v.validate_auth("cg", "c", None, Some(&t)).await.is_err());
    }

    #[test]
    fn decode_jwt_claims_extracts_payload() {
        let t = token("s3cret", "user1");
        let claims = decode_jwt_claims(&t);
        assert_eq!(claims["sub"], "user1");
        // A non-JWT string yields an empty object.
        assert_eq!(decode_jwt_claims("not-a-jwt"), json!({}));
    }

    #[tokio::test]
    async fn opaque_token_passes_without_config() {
        let v = JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        };
        // No JWT config → opaque token, passes.
        assert!(
            v.validate_auth("cg", "c", None, Some("opaque-token"))
                .await
                .is_ok()
        );
    }

    #[test]
    fn key_algorithm_mapping_covers_signature_algs_and_rejects_encryption() {
        use jsonwebtoken::jwk::KeyAlgorithm as K;
        assert_eq!(
            key_algorithm_to_signature_alg(K::RS256).unwrap(),
            Algorithm::RS256
        );
        assert_eq!(
            key_algorithm_to_signature_alg(K::ES256).unwrap(),
            Algorithm::ES256
        );
        assert_eq!(
            key_algorithm_to_signature_alg(K::PS512).unwrap(),
            Algorithm::PS512
        );
        assert_eq!(
            key_algorithm_to_signature_alg(K::EdDSA).unwrap(),
            Algorithm::EdDSA
        );
        // Encryption algorithms are not valid for signing a JWT.
        assert!(key_algorithm_to_signature_alg(K::RSA_OAEP).is_err());
        assert!(key_algorithm_to_signature_alg(K::RSA1_5).is_err());
    }

    fn jwks_two_keys() -> JwkSet {
        // Minimal (structurally valid) EC JWKs — enough for `find`-by-kid, which
        // matches on `kid` only.
        serde_json::from_value(json!({
            "keys": [
                {"kty":"EC","crv":"P-256","x":"AAAA","y":"BBBB","kid":"a","alg":"ES256"},
                {"kty":"EC","crv":"P-256","x":"CCCC","y":"DDDD","kid":"b","alg":"ES256"}
            ]
        }))
        .unwrap()
    }

    #[test]
    fn select_jwk_matches_by_kid_and_single_key_fallback() {
        let set = jwks_two_keys();
        assert_eq!(
            select_jwk(&set, Some("b"))
                .unwrap()
                .common
                .key_id
                .as_deref(),
            Some("b")
        );
        // Unknown kid → none.
        assert!(select_jwk(&set, Some("zzz")).is_none());
        // No kid with a multi-key set is ambiguous → none.
        assert!(select_jwk(&set, None).is_none());

        // No kid with a single-key set → the only key.
        let single: JwkSet = serde_json::from_value(json!({
            "keys": [{"kty":"EC","crv":"P-256","x":"AAAA","y":"BBBB","kid":"solo","alg":"ES256"}]
        }))
        .unwrap();
        assert_eq!(
            select_jwk(&single, None).unwrap().common.key_id.as_deref(),
            Some("solo")
        );
    }

    #[tokio::test]
    async fn malformed_jwk_config_fails_closed() {
        // A `jwk` that isn't valid JSON must reject (never silently pass).
        let v = JwtAuthValidator {
            jwk: Some("{not json".to_string()),
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        };
        let t = token("s3cret", "user1");
        let err = v
            .validate_auth("cg", "c", Some("user1"), Some(&t))
            .await
            .unwrap_err();
        assert_eq!(*err.kind(), crate::protocol::ErrorKind::Unauthorized);
    }
}
