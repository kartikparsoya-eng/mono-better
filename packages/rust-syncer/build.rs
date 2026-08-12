//! Compile and statically link the WAL2-patched SQLite amalgamation.
//!
//! The zero-cache replicator writes the SQLite replica in `wal2` journal mode
//! (rocicorp's custom SQLite). Vanilla/system SQLite rejects such a file with
//! "file is not a database", so the rust-syncer binary must link the same WAL2
//! amalgamation the production NAPI build uses (see `rust-ivm`'s build notes:
//! the Dockerfile installs WAL2 SQLite as the system library). Compiling the
//! amalgamation here produces `libsqlite3.a` in OUT_DIR and points the linker at
//! it, so `libsqlite3-sys`'s `-lsqlite3` resolves to the WAL2 build.
fn main() {
    let src = "../rust-ivm/napi/wal2-sqlite/sqlite3.c";
    println!("cargo:rerun-if-changed={src}");
    println!("cargo:rerun-if-changed=../rust-ivm/napi/wal2-sqlite/sqlite3.h");

    cc::Build::new()
        .file(src)
        .include("../rust-ivm/napi/wal2-sqlite")
        .flag_if_supported("-O2")
        .warnings(false)
        // Match a standard threadsafe build; WAL2 support is baked into the
        // amalgamation and selected at runtime via `PRAGMA journal_mode=wal2`.
        .define("SQLITE_THREADSAFE", "1")
        .define("SQLITE_ENABLE_JSON1", None)
        .define("SQLITE_ENABLE_FTS5", None)
        .define("SQLITE_ENABLE_RTREE", None)
        .define("SQLITE_ENABLE_COLUMN_METADATA", None)
        // Emits `cargo:rustc-link-lib=static=sqlite3` + the OUT_DIR search path,
        // so it satisfies libsqlite3-sys's `-lsqlite3` with the WAL2 static lib.
        .compile("sqlite3");
}
