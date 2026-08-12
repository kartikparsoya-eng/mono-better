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

/// How long a fetched JWKS is reused before re-fetching. jose refreshes lazily;
/// a 5-minute TTL matches its default cooldown/cache window closely enough.
const JWKS_TTL: Duration = Duration::from_secs(300);

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
            return verify_with_jwk(token, &jwk, user_id);
        }
        if let Some(secret) = &self.secret {
            let key = DecodingKey::from_secret(secret.as_bytes());
            let mut validation = Validation::new(Algorithm::HS256);
            validation.algorithms = vec![Algorithm::HS256, Algorithm::HS384, Algorithm::HS512];
            // Verify `sub` equals the connection's claimed userID.
            validation.sub = Some(user_id.to_string());
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
            return verify_with_jwk(token, &jwk, user_id);
        }

        // Miss/stale → fetch, cache, then look up. A concurrent refresh just
        // re-fetches; the last writer wins (matches jose's lazy refresh).
        let set: JwkSet = reqwest::get(jwks_url)
            .await
            .map_err(|e| format!("JWKS fetch failed: {e}"))?
            .json()
            .await
            .map_err(|e| format!("JWKS parse failed: {e}"))?;
        let jwk = select_jwk(&set, header.kid.as_deref())
            .ok_or_else(|| "no matching key in JWKS".to_string())?
            .clone();
        if let Ok(mut cache) = JWKS_CACHE.lock() {
            cache.insert(
                jwks_url.to_string(),
                CachedJwks {
                    fetched_at: Instant::now(),
                    set,
                },
            );
        }
        verify_with_jwk(token, &jwk, user_id)
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

/// Verify a token against a single asymmetric JWK. The algorithm is taken from
/// the JWK (`alg`), NOT the token header, to prevent algorithm-confusion. The
/// `sub` claim is required to equal `user_id`.
fn verify_with_jwk(token: &str, jwk: &Jwk, user_id: &str) -> Result<(), String> {
    let alg = jwk
        .common
        .key_algorithm
        .ok_or_else(|| "JWK missing `alg`".to_string())
        .and_then(key_algorithm_to_signature_alg)?;
    let key = DecodingKey::from_jwk(jwk).map_err(|e| format!("invalid JWK: {e}"))?;
    let mut validation = Validation::new(alg);
    validation.algorithms = vec![alg];
    validation.sub = Some(user_id.to_string());
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
        };
        let t = token("s3cret", "user1");
        let err = v
            .validate_auth("cg", "c", Some("user1"), Some(&t))
            .await
            .unwrap_err();
        assert_eq!(*err.kind(), crate::protocol::ErrorKind::Unauthorized);
    }
}
