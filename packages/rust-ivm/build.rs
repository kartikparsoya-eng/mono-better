fn main() {
    // WAL2 SQLite is compiled in the NAPI crate's build.rs (the final artifact).
    // For local tests, rusqlite links against system SQLite (macOS has it built-in).
    // The Dockerfile installs WAL2 SQLite as system SQLite before building.
}
