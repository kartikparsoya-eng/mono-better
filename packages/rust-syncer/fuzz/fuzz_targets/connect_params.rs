//! Fuzz the connect-URL/param parser (client-controlled query string).
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let url = format!("http://localhost/sync/v51/connect?{s}");
        let _ = rust_syncer::workers::connect_params::get_connect_params(
            51,
            &url,
            None,
            None,
            None,
            Default::default(),
        );
    }
});
