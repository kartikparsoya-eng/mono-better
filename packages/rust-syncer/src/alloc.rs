//! Rust-only invention I-13 (parity/INVENTIONS.md; AGENTS.md rule 5: memory
//! management). No TS twin: one TS syncer worker is one PROCESS (one V8 heap),
//! so its allocations never contend with another client group's. rust runs
//! ~1000 client-group threads in ONE process and glibc malloc's arena locks
//! became the bottleneck under a connect storm — `perf` on image 7f38dffd6
//! (a 5-minute production-trace replay): 48% of the process's samples inside glibc
//! malloc/free (`__lll_lock_wait_private`, `pthread_mutex_lock`), 31% in the
//! kernel futex/wakeup paths those locks take, 21% doing work. The same
//! 20K-row query hydrated in 1.2-1.4s alone (TS: 1.25-1.57s, parity) and in
//! 9-18s when five ran at once.
//!
//! Two allocators feed that contention and both are moved to mimalloc
//! (per-thread heaps, lock-free fast path):
//! 1. Rust's global allocator (`GLOBAL_ALLOCATOR`).
//! 2. SQLite's own malloc — libsqlite3 is C and never sees Rust's global
//!    allocator; `route_sqlite_malloc_through_mimalloc` installs mimalloc as
//!    SQLite's `sqlite3_mem_methods` (`SQLITE_CONFIG_MALLOC`), which must run
//!    before the first `sqlite3_initialize` in the process (i.e. before any
//!    `rusqlite::Connection::open`).
//!
//! Client-observable behaviour is unchanged (allocation is not TS-visible);
//! contract + tests: INVENTIONS.md I-13, `tests/global_allocator_test.rs`.
//! The `dhat-heap` profiling build installs dhat's allocator instead
//! (`main.rs`) and leaves SQLite on glibc so the heap profile stays Rust-only.

use std::os::raw::{c_int, c_void};

#[cfg(not(feature = "dhat-heap"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

// SQLite `sqlite3_mem_methods` callbacks over mimalloc. SQLite never calls
// xMalloc/xRealloc with a size of 0 and never xFree/xRealloc/xSize with NULL
// (malloc.c guards those before dispatching), so no extra checks here.
unsafe extern "C" fn x_malloc(n: c_int) -> *mut c_void {
    // SAFETY: plain allocation; `n` is > 0 (SQLite invariant) and fits usize.
    unsafe { libmimalloc_sys::mi_malloc(n as usize) }
}
unsafe extern "C" fn x_free(p: *mut c_void) {
    // SAFETY: `p` came from x_malloc/x_realloc (SQLite only frees what it got).
    unsafe { libmimalloc_sys::mi_free(p) }
}
unsafe extern "C" fn x_realloc(p: *mut c_void, n: c_int) -> *mut c_void {
    // SAFETY: as above; mi_realloc preserves the old contents up to min(old, n).
    unsafe { libmimalloc_sys::mi_realloc(p, n as usize) }
}
/// Clamp a mimalloc byte count into SQLite's `c_int` size domain.
///
/// SQLite's `sqlite3_mem_methods` reports sizes as `int`, while mimalloc
/// reports `size_t`. A bare `as c_int` TRUNCATES: SQLite caps a single
/// allocation near `0x7fffff00`, and mimalloc's usable/good size for one that
/// large rounds ABOVE `i32::MAX`, where the cast wraps NEGATIVE. SQLite would
/// then carry a negative size through its memory accounting
/// (`sqlite3MallocSize` → `sqlite3_memory_used`) or use it as the xRoundup
/// result — undefined behaviour at the C level, and the failure mode is memory
/// corruption rather than an error.
///
/// Saturating is legal for both callers: SQLite documents xSize as returning
/// "the size of the allocation, which may be larger" than the request, so
/// under-reporting at the extreme is conservative; and xRoundup must return a
/// value `>= n`, which `i32::MAX` always is because `n` is itself a `c_int`.
fn saturating_c_int(n: usize) -> c_int {
    n.min(c_int::MAX as usize) as c_int
}

unsafe extern "C" fn x_size(p: *mut c_void) -> c_int {
    // SQLite asks for the size of an allocation it owns (memory accounting +
    // `sqlite3MallocSize`); the usable size is >= the request, which SQLite
    // permits ("the size of the allocation, which may be larger").
    // SAFETY: `p` is a live allocation from this allocator.
    saturating_c_int(unsafe { libmimalloc_sys::mi_usable_size(p) })
}
unsafe extern "C" fn x_roundup(n: c_int) -> c_int {
    // SAFETY: pure function of `n`.
    saturating_c_int(unsafe { libmimalloc_sys::mi_good_size(n as usize) })
}
unsafe extern "C" fn x_init(_app: *mut c_void) -> c_int {
    0 // SQLITE_OK — mimalloc needs no per-process init.
}
unsafe extern "C" fn x_shutdown(_app: *mut c_void) {}

/// `sqlite3_mem_methods` holds a raw `pAppData` pointer, so the struct is not
/// `Sync`; we never dereference it (always NULL), hence the manual impl.
struct SqliteMemMethods(rusqlite::ffi::sqlite3_mem_methods);
// SAFETY: the struct is immutable after construction and `pAppData` is NULL.
unsafe impl Sync for SqliteMemMethods {}

static SQLITE_MIMALLOC: SqliteMemMethods = SqliteMemMethods(rusqlite::ffi::sqlite3_mem_methods {
    xMalloc: Some(x_malloc),
    xFree: Some(x_free),
    xRealloc: Some(x_realloc),
    xSize: Some(x_size),
    xRoundup: Some(x_roundup),
    xInit: Some(x_init),
    xShutdown: Some(x_shutdown),
    pAppData: std::ptr::null_mut(),
});

/// Make SQLite allocate through mimalloc for the rest of the process.
///
/// Must be called before the first `sqlite3_initialize()` — in practice as the
/// first statement of `main`, before any `rusqlite::Connection::open`. Returns
/// the SQLite result code on rejection (`SQLITE_MISUSE` = 21 when SQLite was
/// already initialized); the caller then keeps running on glibc malloc, which
/// is slower under contention but correct.
pub fn route_sqlite_malloc_through_mimalloc() -> Result<(), c_int> {
    // SAFETY: `sqlite3_config` is variadic; SQLITE_CONFIG_MALLOC takes exactly
    // one `const sqlite3_mem_methods*`, which SQLite copies (malloc.c:
    // `sqlite3GlobalConfig.m = *pMethods`), so the static outlives the copy.
    let rc = unsafe {
        rusqlite::ffi::sqlite3_config(
            rusqlite::ffi::SQLITE_CONFIG_MALLOC,
            &SQLITE_MIMALLOC.0 as *const rusqlite::ffi::sqlite3_mem_methods,
        )
    };
    if rc == rusqlite::ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(rc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `usize -> c_int` casts in `x_size` / `x_roundup` truncated.
    ///
    /// SQLite caps a single allocation near `0x7fffff00`, and mimalloc's
    /// usable/good size for an allocation that large rounds ABOVE `i32::MAX`,
    /// where `as c_int` wraps NEGATIVE. SQLite would then use a negative size
    /// for memory accounting or as its xRoundup result — undefined at the C
    /// level, with memory corruption rather than an error as the failure mode.
    ///
    /// This tests the clamp directly rather than through a >2GiB allocation,
    /// which is why the clamp is a named function.
    ///
    /// Mutation test: change `saturating_c_int` back to `n as c_int` and every
    /// assertion below the first two fails — each of these inputs wraps to a
    /// negative `c_int`.
    #[test]
    fn saturating_c_int_never_wraps_negative() {
        // Below the boundary: exact passthrough, so ordinary page/pcache sizes
        // are unaffected.
        assert_eq!(saturating_c_int(0), 0);
        assert_eq!(saturating_c_int(4096), 4096);
        assert_eq!(saturating_c_int(c_int::MAX as usize - 1), c_int::MAX - 1);
        assert_eq!(saturating_c_int(c_int::MAX as usize), c_int::MAX);

        // At and above the boundary: saturate, never wrap.
        for n in [
            c_int::MAX as usize + 1, // wraps to i32::MIN
            2usize << 31,            // 2^32, wraps to 0
            0x7fff_ff00usize + 4096, // SQLite's cap, page-rounded up
            usize::MAX,              // wraps to -1
        ] {
            let clamped = saturating_c_int(n);
            assert!(
                clamped > 0,
                "saturating_c_int({n}) must stay positive; a negative size is \
                 undefined for SQLite's xSize / xRoundup, got {clamped}"
            );
            assert_eq!(
                clamped,
                c_int::MAX,
                "an out-of-domain size must clamp to c_int::MAX, not wrap"
            );
        }
    }

    /// The xRoundup C contract is `result >= n`. Saturation preserves it
    /// because `n` is itself a `c_int`, so `i32::MAX >= n` always holds — the
    /// truncating version could return a NEGATIVE result for a large `n`,
    /// violating it outright.
    #[test]
    fn x_roundup_returns_at_least_its_argument() {
        for n in [1i32, 8, 4096, 65536, 0x7fff_ff00, c_int::MAX] {
            // SAFETY: `x_roundup` is a pure function of `n`.
            let rounded = unsafe { x_roundup(n) };
            assert!(rounded >= n, "xRoundup({n}) must round UP, got {rounded}");
        }
    }
}
