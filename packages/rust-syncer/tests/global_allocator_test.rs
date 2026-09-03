//! Pins invention I-13 (parity/INVENTIONS.md): the process-wide Rust allocator
//! is mimalloc (per-thread heaps), installed by `rust_syncer`'s
//! `GLOBAL_ALLOCATOR` in `lib.rs`. A `#[global_allocator]` applies to every
//! binary that links the crate, so this integration test observes the same
//! allocator the production binary runs on.
//!
//! Non-vacuous: with the `#[global_allocator]` line removed, Rust allocations
//! come from glibc / the system allocator, `mi_is_in_heap_region` is false for
//! both sizes, and the test fails (proven on revert, see the I-13 commit).

use std::ffi::c_void;

// Ensure the library (and its `#[global_allocator]`) is linked into this test
// binary even though no symbol of it is otherwise used.
#[allow(unused_imports)]
use rust_syncer as _;

fn in_mimalloc_heap<T>(p: *const T) -> bool {
    // SAFETY: mi_is_in_heap_region only inspects mimalloc's region map; any
    // pointer value is a valid argument.
    unsafe { libmimalloc_sys::mi_is_in_heap_region(p as *const c_void) }
}

#[test]
fn rust_allocations_come_from_mimalloc_heaps_for_small_and_large_sizes() {
    // Small object: glibc would serve this from an arena.
    let small = Box::new(0xC0FFEE_u64);
    assert!(
        in_mimalloc_heap(&*small),
        "small Box must live in a mimalloc heap region (global allocator not installed?)"
    );

    // 1 MiB row buffer: far above glibc's default 128 KiB mmap threshold, so
    // glibc would mmap it and free it with munmap under the mmap lock — the
    // contention I-13 removes.
    let large: Vec<u8> = vec![7u8; 1 << 20];
    assert!(
        in_mimalloc_heap(large.as_ptr()),
        "1 MiB Vec must live in a mimalloc heap region, not an mmap'd glibc chunk"
    );
    assert_eq!(large[(1 << 20) - 1], 7);
}

#[test]
fn allocations_made_on_other_threads_also_come_from_mimalloc() {
    // Per-CG executor threads are where the contended allocations happen.
    let ok = std::thread::spawn(|| {
        let buf: Vec<u64> = vec![1; 64 << 10]; // 512 KiB
        in_mimalloc_heap(buf.as_ptr())
    })
    .join()
    .unwrap();
    assert!(
        ok,
        "allocations on a spawned thread must come from mimalloc too"
    );
}

/// SQLite is C and never sees Rust's global allocator; I-13 also installs
/// mimalloc as SQLite's `sqlite3_mem_methods`. This test runs in its own
/// process (integration test binary) so the hook precedes SQLite's
/// initialization exactly as in `main`. Non-vacuous: with the hook a no-op,
/// `sqlite3_malloc` returns a glibc/system pointer and the assertion fails.
#[test]
fn sqlite_allocations_come_from_mimalloc_after_the_config_hook() {
    rust_syncer::alloc::route_sqlite_malloc_through_mimalloc()
        .expect("SQLITE_CONFIG_MALLOC must be accepted before SQLite initializes");

    // Initialize SQLite the way production does (first Connection::open) and
    // do real work through it, so the hook is exercised, not just installed.
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, s TEXT); INSERT INTO t(s) VALUES ('x');",
    )
    .unwrap();
    let s: String = conn.query_row("SELECT s FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(s, "x");

    // SAFETY: plain FFI allocation/free pair.
    let p = unsafe { rusqlite::ffi::sqlite3_malloc(4096) };
    assert!(!p.is_null());
    assert!(
        in_mimalloc_heap(p),
        "sqlite3_malloc must return mimalloc memory once the hook is installed"
    );
    unsafe { rusqlite::ffi::sqlite3_free(p) };
}
