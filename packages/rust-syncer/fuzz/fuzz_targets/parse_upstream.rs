//! Fuzz the ws upstream message parser — the exact surface G36 probes with
//! hand-written adversarial cases; the fuzzer explores the rest. Must never
//! panic: every malformed input is an Err (→ InvalidMessage), never an abort.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = rust_syncer::protocol::parse_upstream(text);
    }
});
