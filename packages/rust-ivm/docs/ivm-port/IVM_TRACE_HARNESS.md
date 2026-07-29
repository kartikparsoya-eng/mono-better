# IVM Trace Harness — debugging push routing

A permanent, env-gated pipeline tracer for the open regressions in
`agentic/fixtures/regressions/`. Zero cost when off.

## Usage
```bash
FIX=seed-159916 IVM_TRACE=1 cargo test --test trace_fixture -- --nocapture
```
`FIX` defaults to `seed-159916`; it resolves against both `agentic/fixtures/`
and `agentic/fixtures/regressions/`. Output is one line per push a routing
operator receives:
```
[ivm-trace] <op>#<n>   recv  <change>
```
where `<op>` is the operator file (`join`, `fan_in`, `exists`, `catch`, …) and
`#n` distinguishes adapters (e.g. `join#1` = ParentOutput, `join#2` =
ChildOutput). The emit→recv chain reconstructs the flow top-to-bottom.

## Mechanics
- `src/ivm/trace.rs` — `enabled()` (cached `IVM_TRACE` check), `recv`/`emit`/
  `note`, and `describe(&Change)` (recurses into CHILD).
- Instrumentation: `crate::ivm::trace::recv("<file>#<n>", &change)` at the top of
  each routing operator's `Output::push`. Add `emit(...)` calls at
  `output.push(...)` sites if you need to see emits too.

## Finding so far (the open regression cluster)
The 9 open regressions are all **OR-with-EXISTS child-routing**. Tracing
`seed-159916` (a `WHERE (c1>=X OR c3!=Y OR EXISTS(zsubq_t1))`, child add of
`t1-p1000`):
```
join#2   recv  ADD(t1-p1000)
catch#1  recv  CHILD(t0-r24 rel=zsubq_t1_0 -> ADD(t1-p1000))
```
The child add flows **Join → Catch directly** — **no `Exists`, no `FanIn` in the
push path.** Expected (TS): the child add flips `t0-r24`'s EXISTS 0→1 and the
row enters the result as a parent **ADD**. Rust emits a bare **CHILD** because
the `Exists` operator (which would convert CHILD→ADD on the 0↔1 boundary) is not
wired between the exists-subquery `Join` and the output on the push path.

**Conclusion: this is a builder wiring issue for `OR`-with-`EXISTS`, not operator
logic.** Ruled out earlier: push_accumulated (faithful port), Exists 0→1
conversion (correct in isolation), uniquify rel-name mismatch (names match).

## Next step
Inspect the builder path for `applyWhere` on an `OR` that contains a
(non-flipped) EXISTS correlated subquery: confirm it builds
`FanOut → [filters | Join+Exists] → FanIn` and that the exists branch's `Join`
output is wired to `Exists` (not straight to the fan-in/output). Compare against
TS `builder.ts` OR-expansion. The trace above is the reproduction; re-run it
after each wiring change and watch for an `exists#1 recv` / `fan_in#1 recv`
appearing between `join#2` and `catch#1`.

## Open: seed-68178 (last regression) — re-entrant borrow in push+refetch

`seed-68178` (nested EXISTS + OR + IN + LIKE + Cap + Take) panics with a
`RefCell already-borrowed` — NOT a wrong answer. Root cause (traced): the
NodeFilter OR-with-EXISTS fix correctly emits an ADD (not CHILD) on a boundary
flip; that ADD makes the downstream **Take re-fetch its input during the push**
(take.rs:276), and the re-fetch's EXISTS-size count re-enters an upstream
operator whose `output.borrow_mut()` is still held from the outer push
(cap.rs:302 → join → NodeFilter → Take). Chain:
`cap.push → join.push → NodeFilter.push(emit ADD) → Take.push → Take re-fetch
→ NodeFilter.fetch(eval EXISTS) → borrow collision`.

This is a general borrow-management gap: `output.borrow_mut().push(change)` holds
the borrow across the whole synchronous downstream cascade, so any re-entrant
fetch that touches a mid-push operator panics. The safe fix is a push-deferral
refactor (don't hold operator borrows across downstream pushes) applied across
operators — deliberately not attempted blind here, as it touches every operator
and risks the 1705 passing fixtures. seed-68178 is the sole pathological case
that hits it; all real/simple shapes pass.
