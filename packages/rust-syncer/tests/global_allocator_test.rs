//! Pins invention I-13 (parity/INVENTIONS.md): the process-wide Rust allocator
//! is mimalloc (per-thread heaps), installed by `rust_syncer`'s
//! `GLOBAL_ALLOCATOR` in `lib.rs`. A `#[global_allocator]` applies to every
//! binary that links the crate, so this integration test observes the same
//! allocator the production binary runs on.
//!
//! Mutation test: with the `#[global_allocator]` line removed, Rust allocations
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

/// Install the SQLite→mimalloc hook exactly once per test process (the tests
/// below share one SQLite library; `SQLITE_CONFIG_MALLOC` is refused after the
/// first `sqlite3_initialize`, so whichever test runs first installs it).
fn install_sqlite_hook_once() {
    static HOOK: std::sync::Once = std::sync::Once::new();
    HOOK.call_once(|| {
        rust_syncer::alloc::route_sqlite_malloc_through_mimalloc()
            .expect("SQLITE_CONFIG_MALLOC must be accepted before SQLite initializes");
    });
}

/// libtest runs the tests below on PARALLEL threads (`cargo test`'s default;
/// the CI coverage leg passes no `--test-threads=1`), and they share ONE
/// process-global SQLite. If `sqlite_compile_options_...` wins the race to the
/// first `Connection::open`, SQLite initializes WITHOUT the hook and the later
/// `SQLITE_CONFIG_MALLOC` returns SQLITE_MISUSE (21) — the hook test panics,
/// the poisoning Once takes the memstatus test down with it (observed 1/100
/// locally, ~always under the instrumented coverage build). The mutex plus
/// hook-before-connect in EVERY SQLite-touching test makes that ordering
/// deterministic under any --test-threads count.
static SQLITE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn lock_sqlite_test() -> std::sync::MutexGuard<'static, ()> {
    // A sibling test's panic must not turn the lock's poisoning into a
    // cascade that masks the first failure's diagnostics.
    SQLITE_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// SQLite is C and never sees Rust's global allocator; I-13 also installs
/// mimalloc as SQLite's `sqlite3_mem_methods`. This test runs in its own
/// process (integration test binary) so the hook precedes SQLite's
/// initialization exactly as in `main`. Mutation test: with the hook a no-op,
/// `sqlite3_malloc` returns a glibc/system pointer and the assertion fails.
#[test]
fn sqlite_allocations_come_from_mimalloc_after_the_config_hook() {
    let _guard = lock_sqlite_test();
    install_sqlite_hook_once();

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

/// SQLite parity with the build the TS zero-cache runs (`@rocicorp/zero-sqlite3`
/// deps/defines.gypi: `SQLITE_DEFAULT_MEMSTATUS=0`): memory statistics OFF, so
/// `sqlite3Malloc`/`sqlite3_free` never take SQLite's global `mem0.mutex`.
///
/// Mutation test: with memstatus on (the earlier rust build), SQLite keeps
/// `SQLITE_STATUS_MEMORY_USED` current/highwater counters and this reports the
/// live bytes of the connection + the 1 MiB block below (> 0) — under a 110-
/// connection replay burst that mutex was 30 % of the process's inclusive CPU
/// (`pthread_mutex_lock` under `sqlite3Prepare`, per `perf`) and the futex
/// storm behind rust's 110-vs-230 capacity knee. With memstatus off the
/// counters are never maintained and both stay 0.
#[test]
fn sqlite_memory_statistics_are_off_like_zero_sqlite3() {
    let _guard = lock_sqlite_test();
    install_sqlite_hook_once();
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, s TEXT); INSERT INTO t(s) VALUES ('x');",
    )
    .unwrap();
    // SAFETY: plain FFI allocation/free pair around a status read.
    let p = unsafe { rusqlite::ffi::sqlite3_malloc(1 << 20) };
    assert!(!p.is_null());
    let (mut current, mut highwater) = (0i64, 0i64);
    let rc = unsafe {
        rusqlite::ffi::sqlite3_status64(
            rusqlite::ffi::SQLITE_STATUS_MEMORY_USED,
            &mut current,
            &mut highwater,
            0,
        )
    };
    unsafe { rusqlite::ffi::sqlite3_free(p) };
    assert_eq!(rc, rusqlite::ffi::SQLITE_OK);
    assert_eq!(
        (current, highwater),
        (0, 0),
        "SQLite memory statistics must be compiled OFF (SQLITE_DEFAULT_MEMSTATUS=0, \
         like zero-sqlite3): with them on every SQLite malloc/free takes the global \
         mem0 mutex"
    );
}

/// The vendored SQLite must be compiled with the same options as the
/// `@rocicorp/zero-sqlite3` build the TS zero-cache links (deps/defines.gypi),
/// as SQLite itself reports them (`PRAGMA compile_options`, `SQLITE_` prefix
/// dropped). The planner's cost model IS SQLite's own estimate, and its
/// runtime behavior (memstatus mutex, threading mode, page cache, DQS,
/// LIKE-on-blob, shared cache) follows these flags — so a flag drift is a
/// silent divergence. `scripts/local-rust-ci.sh` cross-checks this list
/// against defines.gypi so it cannot rot independently.
#[test]
fn sqlite_compile_options_match_the_zero_sqlite3_build() {
    // Hook first: this test opens a connection, so unguarded it could be the
    // one to initialize SQLite and break the hook install (see SQLITE_TEST_LOCK).
    let _guard = lock_sqlite_test();
    install_sqlite_hook_once();
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    let mut stmt = conn.prepare("PRAGMA compile_options").unwrap();
    let opts: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let want = [
        "DEFAULT_MEMSTATUS=0",
        "THREADSAFE=2",
        "DEFAULT_CACHE_SIZE=-16000",
        "DEFAULT_FOREIGN_KEYS",
        "DEFAULT_WAL_SYNCHRONOUS=1",
        "DQS=0",
        "LIKE_DOESNT_MATCH_BLOBS",
        "OMIT_SHARED_CACHE",
        "OMIT_DEPRECATED",
        "OMIT_PROGRESS_CALLBACK",
        "USE_URI",
        "ENABLE_STMT_SCANSTATUS",
        "ENABLE_STAT4",
        "ENABLE_COLUMN_METADATA",
        "ENABLE_FTS5",
        "ENABLE_RTREE",
        "ENABLE_MATH_FUNCTIONS",
    ];
    let missing: Vec<&str> = want
        .iter()
        .copied()
        .filter(|w| !opts.iter().any(|o| o == w))
        .collect();
    assert!(
        missing.is_empty(),
        "vendored SQLite compile options differ from zero-sqlite3 (missing: {missing:?}); \
         reported: {opts:?}"
    );
}
