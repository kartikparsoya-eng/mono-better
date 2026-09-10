//! F-26: the PK strings `Cap` copies on the per-row removal path must be
//! SHARED (`Rc<str>` refcount bumps), because TS shares them by reference.
//!
//! Same shape as F-05 and F-07: a JS array or Set of strings copies POINTERS,
//! so TS pays nothing per element, while the Rust `Vec<String>` equivalent
//! deep-cloned every element. TS's removal path does
//! `const pks = [...capState.pks]` (cap.ts:210), `new Set(pks)` (:216) and
//! `#storage.set(key, {size, pks})` (:241) — three handles on the SAME
//! strings. Rust did `pks.clone()`, a `HashSet<String>` collect, and
//! `pks.clone()` again: ~3N `String` allocations per removed row on every
//! capped query, scaling with the limit.
//!
//! `Rc::ptr_eq` is the observable a `String` cannot satisfy, so this is
//! NON-VACUOUS: revert `CapState.pks` to `Vec<String>` and the
//! pointer-identity assertions cannot compile. Behaviour is unchanged by
//! design — ordering and membership stay pinned by the existing `cap` suite,
//! which must remain green alongside this.
//!
//! F-30 (the `Streamer` frame path) is NOT tested here: unlike `CapState`, the
//! path has no TS twin at all — TS's `#streamNodes` recurses with the resolved
//! `childSchema` and never builds a path (pipeline-driver.ts:1385) — so the
//! invariant is that the path RESOLVES to that schema, which is pinned next to
//! the code in `streamer/mod.rs`
//! (`a_frame_path_resolves_to_the_schema_ts_would_pass_as_child_schema`).

use std::rc::Rc;

use rust_ivm::ivm::cap::CapState;

#[test]
fn cap_state_pks_are_shared_not_deep_copied() {
    let a: Rc<str> = Rc::from("[\"i1\"]");
    let b: Rc<str> = Rc::from("[\"i2\"]");
    let state = CapState {
        size: 2,
        pks: vec![Rc::clone(&a), Rc::clone(&b)],
    };

    // TS's `[...capState.pks]` copies references; the Rust twin is a Vec clone
    // of `Rc<str>`, i.e. refcount bumps.
    let copied = state.pks.clone();
    assert!(
        Rc::ptr_eq(&copied[0], &a) && Rc::ptr_eq(&copied[1], &b),
        "copying the pk vector must share the strings, not reallocate them — \
         this is TS's `[...capState.pks]`"
    );

    // TS's `new Set(pks)` also shares. The Rust twin collects `Rc<str>` clones.
    let set: std::collections::HashSet<Rc<str>> = state.pks.iter().cloned().collect();
    let from_set = set
        .get(&*a)
        .expect("membership must be by string value, so a &str looks the entry up");
    assert!(
        Rc::ptr_eq(from_set, &a),
        "the membership set must hold the SAME allocation, matching TS's \
         `new Set(pks)`"
    );

    // Storing the state again (TS `#storage.set(key, {size, pks})`) shares too.
    let stored = state.clone();
    assert!(
        Rc::ptr_eq(&stored.pks[0], &a),
        "re-storing the state must not deep-copy the pk vector"
    );

    // And the whole point: one allocation per distinct pk, however many
    // handles exist.
    assert_eq!(
        Rc::strong_count(&a),
        // `a` + state + copied + set + stored
        5,
        "every handle on `a` must be a refcount bump on ONE allocation"
    );
}
