//! Compile and statically link the WAL2-patched SQLite amalgamation.
//!
//! The zero-cache replicator writes the SQLite replica in `wal2` journal mode
//! (rocicorp's custom SQLite). Vanilla/system SQLite rejects such a file with
//! "file is not a database", so the rust-syncer binary must link the same WAL2
//! amalgamation. The canonical vendored amalgamation lives in
//! `rust-ivm/wal2-sqlite` (also consumed by the Dockerfile and rust-ivm itself);
//! we compile it directly rather than keeping a second copy. (It formerly also
//! existed under `rust-ivm/napi`, which was the removed NAPI hybrid.) Compiling
//! it here produces `libsqlite3.a` in OUT_DIR and points the linker at it, so
//! `libsqlite3-sys`'s `-lsqlite3` resolves to the WAL2 build.
fn main() {
    let src = "../rust-ivm/wal2-sqlite/sqlite3.c";
    println!("cargo:rerun-if-changed={src}");
    println!("cargo:rerun-if-changed=../rust-ivm/wal2-sqlite/sqlite3.h");

    cc::Build::new()
        .file(src)
        .include("../rust-ivm/wal2-sqlite")
        .flag_if_supported("-O2")
        .warnings(false)
        // Compile-define PARITY with the SQLite the TS zero-cache links
        // (@rocicorp/zero-sqlite3 deps/defines.gypi, mirrored 1:1 and enforced
        // by scripts/local-rust-ci.sh + tests/global_allocator_test.rs
        // `sqlite_compile_options_match_the_zero_sqlite3_build`). Same source
        // (the source-id CI step) is not enough: these flags set SQLite's
        // RUNTIME behavior, and two of them decide throughput under many
        // threads —
        //   SQLITE_DEFAULT_MEMSTATUS=0: no global `mem0.mutex` around every
        //     sqlite3Malloc/free. Missing until 2026-09-07: 110 CG threads
        //     preparing/planning at once serialized on it (perf: 30 %
        //     inclusive under pthread_mutex_lock, 74 % kernel futex spin) —
        //     rust's ART capacity knee was 110 connections vs TS 230.
        //   SQLITE_THREADSAFE=2 (multi-thread, TS) vs 1 (serialized): rusqlite
        //     opens every connection with SQLITE_OPEN_NO_MUTEX anyway; 2 is
        //     what TS runs.
        // STMT_SCANSTATUS + STAT4 (+ sample count) are what make the planner's
        // scanstatus cost model (rust-ivm sqlite_cost_model.rs, port of TS
        // createSQLiteCostModel) read the same SQLITE_SCANSTAT_EST TS reads;
        // without them it silently degraded to the filter-blind COUNT(*) model
        // (the 2026-08-29 prod 144s flipped-join tickets hydrate).
        // WAL2 support is baked into the amalgamation and selected at runtime
        // via `PRAGMA journal_mode=wal2`.
        .define("SQLITE_DEFAULT_CACHE_SIZE", "-16000")
        .define("SQLITE_DEFAULT_FOREIGN_KEYS", "1")
        .define("SQLITE_DEFAULT_MEMSTATUS", "0")
        .define("SQLITE_DEFAULT_WAL_SYNCHRONOUS", "1")
        .define("SQLITE_DQS", "0")
        .define("SQLITE_ENABLE_COLUMN_METADATA", None)
        .define("SQLITE_ENABLE_DBSTAT_VTAB", None)
        .define("SQLITE_ENABLE_DESERIALIZE", None)
        .define("SQLITE_ENABLE_FTS3", None)
        .define("SQLITE_ENABLE_FTS3_PARENTHESIS", None)
        .define("SQLITE_ENABLE_FTS4", None)
        .define("SQLITE_ENABLE_FTS5", None)
        .define("SQLITE_ENABLE_GEOPOLY", None)
        .define("SQLITE_ENABLE_JSON1", None)
        .define("SQLITE_ENABLE_MATH_FUNCTIONS", None)
        .define("SQLITE_ENABLE_PERCENTILE", None)
        .define("SQLITE_ENABLE_RTREE", None)
        .define("SQLITE_ENABLE_STAT4", None)
        .define("SQLITE_ENABLE_STMT_SCANSTATUS", None)
        .define("SQLITE_ENABLE_UPDATE_DELETE_LIMIT", None)
        .define("SQLITE_LIKE_DOESNT_MATCH_BLOBS", None)
        .define("SQLITE_OMIT_DEPRECATED", None)
        .define("SQLITE_OMIT_PROGRESS_CALLBACK", None)
        .define("SQLITE_OMIT_SHARED_CACHE", None)
        .define("SQLITE_OMIT_TCL_VARIABLE", None)
        .define("SQLITE_SOUNDEX", None)
        .define("SQLITE_STAT4_SAMPLES", "128")
        .define("SQLITE_THREADSAFE", "2")
        .define("SQLITE_TRACE_SIZE_LIMIT", "32")
        .define("SQLITE_USE_URI", "1")
        // Emits `cargo:rustc-link-lib=static=sqlite3` + the OUT_DIR search path,
        // so it satisfies libsqlite3-sys's `-lsqlite3` with the WAL2 static lib.
        .compile("sqlite3");
}
