//! Fuzz the CVR version-string (cookie) parser — client-supplied on every
//! connect; a panic here would kill a CG task hosting every client of the
//! group (the exact class the maybe_version_string design note warns about).
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = rust_cvr::schema::types::maybe_version_string(s);
    }
});
