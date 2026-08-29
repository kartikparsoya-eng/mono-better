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
        // Match a standard threadsafe build; WAL2 support is baked into the
        // amalgamation and selected at runtime via `PRAGMA journal_mode=wal2`.
        .define("SQLITE_THREADSAFE", "1")
        .define("SQLITE_ENABLE_JSON1", None)
        .define("SQLITE_ENABLE_FTS5", None)
        .define("SQLITE_ENABLE_RTREE", None)
        .define("SQLITE_ENABLE_COLUMN_METADATA", None)
        // Cost-estimator parity with the zero-sqlite3 build the TS syncer runs
        // on (Dockerfile stage 1 defines): the query planner's scanstatus cost
        // model (rust-ivm sqlite_cost_model.rs, port of TS
        // createSQLiteCostModel) reads SQLITE_SCANSTAT_EST through
        // `sqlite3_stmt_scanstatus_v2`, which only EXISTS when the amalgamation
        // is compiled with STMT_SCANSTATUS; STAT4 (+ the same sample count)
        // makes this build's estimates use the same histogram stats TS's does.
        // Without STMT_SCANSTATUS the planner silently degraded to the
        // filter-blind COUNT(*) model — the 2026-08-29 prod 144s flipped-join
        // tickets hydrate.
        .define("SQLITE_ENABLE_STMT_SCANSTATUS", None)
        .define("SQLITE_ENABLE_STAT4", None)
        .define("SQLITE_STAT4_SAMPLES", "128")
        // Emits `cargo:rustc-link-lib=static=sqlite3` + the OUT_DIR search path,
        // so it satisfies libsqlite3-sys's `-lsqlite3` with the WAL2 static lib.
        .compile("sqlite3");
}
