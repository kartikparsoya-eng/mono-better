use napi::bindgen_prelude::*;
use napi_derive::napi;

use rust_cvr::hash::{h128, h32, h64};
use rust_cvr::row_key::{row_id_hash, row_id_string, RowID};
use rust_cvr::row_set_signature::{format_signature, parse_signature, signature_unit};

#[napi]
pub fn rust_cvr_h32(s: String) -> u32 {
    h32(&s)
}

#[napi]
pub fn rust_cvr_h64(s: String) -> BigInt {
    BigInt::from(h64(&s))
}

#[napi]
pub fn rust_cvr_h128(s: String) -> BigInt {
    BigInt::from(h128(&s))
}

#[napi]
pub fn rust_cvr_row_id_string(id: serde_json::Value) -> Result<String> {
    let id: RowID = serde_json::from_value(id).map_err(|e| {
        Error::new(
            Status::InvalidArg,
            format!("failed to deserialize RowID: {}", e),
        )
    })?;
    Ok(row_id_string(&id))
}

#[napi]
pub fn rust_cvr_row_id_hash(id: serde_json::Value) -> Result<String> {
    let id: RowID = serde_json::from_value(id).map_err(|e| {
        Error::new(
            Status::InvalidArg,
            format!("failed to deserialize RowID: {}", e),
        )
    })?;
    Ok(row_id_hash(&id))
}

#[napi]
pub fn rust_cvr_row_id_signature_unit(id: serde_json::Value) -> Result<BigInt> {
    let id: RowID = serde_json::from_value(id).map_err(|e| {
        Error::new(
            Status::InvalidArg,
            format!("failed to deserialize RowID: {}", e),
        )
    })?;
    Ok(BigInt::from(signature_unit(&id)))
}

#[napi]
pub fn rust_cvr_parse_signature(hex: Option<String>) -> Result<BigInt> {
    let s = hex.as_deref();
    let n = parse_signature(s).map_err(|e| {
        Error::new(
            Status::InvalidArg,
            format!("failed to parse signature hex {:?}: {}", s, e),
        )
    })?;
    Ok(BigInt::from(n))
}

#[napi]
pub fn rust_cvr_format_signature(sig: BigInt) -> Result<String> {
    let (_sign, v, _lossless): (bool, u64, bool) = sig.get_u64();
    Ok(format_signature(v))
}
