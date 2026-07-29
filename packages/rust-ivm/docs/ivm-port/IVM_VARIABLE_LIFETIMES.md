# IVM Variable Lifetimes — TS → Rust Porting Guide

Source: `mono-v1.7/packages/zql/src/ivm/` (TS IVM, tag `zero/v1.7.0`).
Companion: `RUST-IVM-GOAL.md` (mechanical port, behavior-identical, not idiomatic).

This document catalogs **when each variable is born, used, and dropped** in the
TS IVM, then maps that to Rust ownership/borrow decisions. It is organized so
you can port operator-by-operator and know exactly which fields are
(pipeline-lifetime) vs (push-scoped) vs (single-use-stream-scoped).

> Legend for the "Lifetime" column:
> - **PIPELINE** = lives as long as the operator instance (created in `buildPipeline`, destroyed in `destroy()`).
> - **CONN** = lives as long as a source connection (created in `connect()`, removed in `destroy()`).
> - **PUSH** = transient, set during one `push()`/`genPush()` call, cleared in `finally`.
> - **FETCH** = transient, lives for one `fetch()` call (one stream iteration).
> - **SINGLE-USE** = a stream/iterator; consumed exactly once, then dropped.
> - **IMMUTABLE** = logically never mutated after creation (shared by reference).
> - **COW** = copy-on-write; mutated only if uniquely owned / owned-by-current-txn.

---

## Part 1 · The Six Lifetime Classes (read this first)

Every variable in IVM falls into one of six classes (A–F). Porting is mostly
about not confusing them.

### Class A — Pipeline-lifetime owned state
Operator fields created in the constructor, never moved, destroyed when the
pipeline is torn down. **Rust: just struct fields, owned.**

Examples: `Take.#limit`, `Join.#parentKey`, `MemorySource.#indexes`,
`Exists.#relationshipName`, `Cap.#primaryKey`.

### Class B — Shared, immutable data flowing through the pipeline
`Row` and `Node.row`. These are treated as immutable — never written after
creation. Operators spread `relationships` into a new record but reuse the
same `row` reference. **Rust: `Cow<Row>` / `Arc<Row>` / `&Row` where the borrow
is contained.** The catch is the relationship thunks (Class E).

### Class C — Single-use lazy streams
`Stream<T> = Iterable<T>`. Every `fetch()` returns a *fresh* generator.
Generators are consumed exactly once; `yield*` delegates; `break`/`.return()`
runs `finally` cleanup (SQLite cursor close, overlay clear). **Rust: this is
the hard one — see Part 6.** Maps to `impl Iterator` / a custom `Stream` trait,
but the self-referential thunk problem (Class E) forces `Rc<RefCell<>>` for
mechanical fidelity.

### Class D — Push-scoped transient overlay state
Fields set at the start of a push and cleared in `finally`. They exist so that
*lazy fetches triggered during a push* see the in-flight change. The push is
synchronous and depth-first, so "during the push" is well-defined. **Rust:
`RefCell<Option<...>>` set/cleared within the push scope; or `Cell` for the
epoch counter.** Single-threaded so no locking needed.

### Class E — Self-referential / escaping thunks
`Node.relationships[name]` is a `() => Stream<Node>` closure that captures
`&self` (the operator) and the parent row. The closure is stored *inside* the
Node, which is *yielded out* of the generator — i.e. the borrow escapes the
generator. TS handles this via GC. **Rust: this is THE porting crux.** Options
in Part 6; recommended = `Rc<RefCell<Operator>>` shared handles (matches TS
GC semantics, keeps the port mechanical).

### Class F — COW view tree
`ArrayView.#root` and the `MetaEntry` tree. Path-copied on every change for
cross-transaction reference stability; within a transaction mutated in place
via a `WeakSet` ownership test. **Rust: `Rc<Entry>` + `Rc::make_mut` is the
exact COW primitive** — `make_mut` clones iff refcount > 1, else mutates.
Replaces the WeakSet trick cleanly.

---

## Part 2 · Core Data Types — Per-Field Lifetime

### `Row` (`zero-protocol/src/data.ts`) — `Record<string, Value>`
| Aspect | TS | Rust |
|---|---|---|
| Creation | `fromSQLiteTypes` (TableSource) / read from `BTreeSet` (MemorySource) / spread from another Row | owned `Row` (a `HashMap<String, Value>` or a typed column struct) |
| Mutation | **never** (treated immutable) | `&Row` or `Arc<Row>` |
| Sharing | same Row object reused across Nodes, Changes, view entries | clone cheaply only when truly mutating |

### `Node` (`data.ts:1`) — `{row, relationships}`
```ts
type Node = {
  row: Row;                                                    // IMMUTABLE (Class B)
  relationships: Record<string, () => Stream<Node | 'yield'>>; // Class E (thunks)
};
```
- `row`: shared immutable. **Rust: `Arc<Row>` or `&Row`** depending on escape.
- `relationships`: a map of thunks. Each thunk, *when called*, returns a
  **fresh** single-use stream (it re-issues `child.fetch({constraint})`).
  Calling the same thunk twice yields two independent streams. **Rust: a
  boxed closure `Box<dyn Fn() -> Stream<Item>>`** — but the closure captures
  the child operator and the constraint. See Part 6.

### `Change` (`change.ts:11`) — tagged tuple
```ts
type Change =
  | [ADD, node, null]
  | [REMOVE, node, null]
  | [CHILD, node, {relationshipName, change: Change}]
  | [EDIT, node, oldNode];
```
- Created by `makeAddChange` etc. The `node` inside is **shared** (same Node
  reference the caller already holds). **Rust: `enum Change { Add(Node),
  Remove(Node), Child(Node, ChildData), Edit(Node, Node) }`** — Node owned or
  `Arc<Node>` if multiple changes reference it (they don't usually, so owned
  is fine; `mergeRelationships` in `push-accumulated.ts` does share).

### `Stream<T>` (`stream.ts:1`) — `Iterable<T>`
- Single-use. `Symbol.iterator` creates the state machine. `for...of`
  consumes. `yield*` delegates. `.return()` triggers `finally`.
- **Rust: a custom trait**
  ```rust
  enum Item<T> { Data(T), Yield /* cooperative pause */ }
  trait Stream { fn next(&mut self) -> Option<Item<Node>>; }
  ```
  OR model `'yield'` as a separate side-channel. The `'yield'` sentinel is
  *not* a generator yield — it is an in-band "pump the event loop" hint.

---

## Part 3 · Per-Operator Variable Lifetime Tables

### `Filter` (`filter.ts`) — STATELESS
| Field | Lifetime | Notes |
|---|---|---|
| `#input` | PIPELINE | owned downstream input |
| `#predicate` | PIPELINE | pure `Fn(&Row) -> bool` |
| `#output` | PIPELINE | set once via `setOutput` (init = `throwOutput`) |

No state. `push` delegates to `filterPush`. **Rust: trivial — owns `Box<dyn
Input>`, a `Box<dyn Fn(&Row)->bool>`, `Box<dyn Output>`.**

### `Skip` (`skip.ts`) — STATELESS except bound
| Field | Lifetime | Notes |
|---|---|---|
| `#input` | PIPELINE | |
| `#bound: {row, exclusive}` | PIPELINE | immutable from constructor |
| `#comparator` | PIPELINE | from `input.getSchema().compareRows` |
| `#output` | PIPELINE | |

**Rust: owned bound + comparator. `push` uses
`maybeSplitAndPushEditChange`.** No transient state.

### `Take` (`take.ts`) — STATEFUL, the canonical state machine
| Field | Lifetime | Class | Notes |
|---|---|---|---|
| `#input` | PIPELINE | A | |
| `#storage` | PIPELINE | A | delegate-provided `Storage` |
| `#limit` | PIPELINE | A | immutable |
| `#partitionKey` | PIPELINE | A | |
| `#partitionKeyComparator` | PIPELINE | A | |
| `#output` | PIPELINE | A | init = `throwOutput` |
| `#rowHiddenFromFetch` | **PUSH** | D | set in `#pushWithRowHiddenFromFetch`, cleared in `finally`. Hides the row being removed from re-fetches during the same push. |
| storage entries `{size, bound}` | PIPELINE | A | keyed by `getTakeStateKey(...)`; persists across pushes |
| storage `MAX_BOUND_KEY` | PIPELINE | A | global max bound across partitions |

**Rust:** storage = `HashMap<String, TakeState>`. The
`#rowHiddenFromFetch` field is the tricky one: it's read by `fetch` (which
can be called *during* the push by downstream operators) and set/cleared by
`push`. **`Cell<Option<Row>>` or `RefCell<Option<Row>>`** — set in push,
checked in fetch. Single-threaded so `Cell` for an owned `Row` works.

Key invariant to preserve in Rust: **output size ≤ limit at all times,
even mid-push.** Take does "remove before add" to maintain this. The
`#initialFetch` `finally` asserts no downstream early return — preserve
this assert (it's a correctness guard, not just a test).

### `Cap` (`cap.ts`) — STATEFUL, count-based limiter (no ordering)
| Field | Lifetime | Class | Notes |
|---|---|---|---|
| `#input` | PIPELINE | A | |
| `#storage` | PIPELINE | A | |
| `#limit` | PIPELINE | A | |
| `#partitionKey`, `#partitionKeyComparator` | PIPELINE | A | |
| `#primaryKey` | PIPELINE | A | from `input.getSchema().primaryKey` |
| `#output` | PIPELINE | A | |
| storage `{size, pks: Vec<String>}` | PIPELINE | A | PK-set membership (no comparator) |

**Rust:** like Take but simpler — no `#rowHiddenFromFetch` overlay (Cap
defers adding to the pk set). Uses PK serialization for membership. The
`#initialFetch` early-return assert also applies.

### `Join` (`join.ts`) — STATEFUL (schema + push overlay)
| Field | Lifetime | Class | Notes |
|---|---|---|---|
| `#parent`, `#child` | PIPELINE | A | two owned inputs |
| `#parentKey`, `#childKey`, `#relationshipName` | PIPELINE | A | |
| `#schema` | PIPELINE | A | computed once: spreads parent's + adds the relationship |
| `#output` | PIPELINE | A | |
| `#inprogressChildChange` | **PUSH** | D | set in `#pushChildChange`, cleared in `finally`. The in-flight child change being pushed to parents. |
| `#inprogressChildChangePosition` | **PUSH** | D | the last parent row position pushed to; used to decide overlay direction |

**Critical pattern (Class D + E combined):** `#processParentNode` builds a
Node whose relationship thunk *reads* `#inprogressChildChange` when called
*lazily* by downstream. So the thunk's correctness depends on the overlay
field staying set for the entire duration of the push. In TS this is
guaranteed because push is synchronous depth-first and `finally` clears only
after all downstream pushes complete. **Rust: `RefCell<Option<Change>>` for
the overlay; the thunk holds an `Rc<RefCell<Join>>` (or shared handle) so it
can read the overlay when invoked.** Do NOT clear the overlay before the
thunk has been consumed — the synchronous push ordering guarantees this.

`#pushChildChange` also re-fetches the parent (`this.#parent.fetch
({constraint})`) inside the push — that fetch may itself trigger lazy
relationship streams that read the overlay. So the overlay lifetime = entire
push call, not just the immediate push to `#output`.

### `FlippedJoin` (`flipped-join.ts`) — STATEFUL (same overlay pattern)
| Field | Lifetime | Class | Notes |
|---|---|---|---|
| `#parent`, `#child` | PIPELINE | A | |
| keys, rel name, schema | PIPELINE | A | |
| `#output` | PIPELINE | A | |
| `#inprogressChildChange` | **PUSH** | D | same as Join |
| `#inprogressChildChangePosition` | **PUSH** | D | |

Same overlay lifetime as `Join`. The `#fetchBatched` path also reads
`#inprogressChildChange` (splicing the removed child back into childNodes for
the fetch). Chunk size 256 — a module-level mutable `let
multiConstraintChunkSize` test seam; **Rust: make it a const or a field, drop
the global mutable.**

### `Exists` (`exists.ts`) — STATEFUL (size cache + push flag)
| Field | Lifetime | Class | Notes |
|---|---|---|---|
| `#input` | PIPELINE | A | FilterInput (filter-pipeline, not normal Input) |
| `#relationshipName`, `#not`, `#parentJoinKey`, `#noSizeReuse` | PIPELINE | A | |
| `#cache: Map<string, boolean>` | **per-fetch-batch** | D-ish | cleared in `endFilter()`. Lives for one `beginFilter`→`endFilter` cycle (one fetch pass). |
| `#cacheHitCountsForTesting` | PIPELINE | A | test only |
| `#output` | PIPELINE | A | |
| `#inPush` | **PUSH** | D | set `true` at push start, `false` in `finally`. Disables cache reuse during push because relationships are inconsistent mid-push. |

**Rust:** `#cache` = `HashMap<String, bool>` cleared on `endFilter`.
`#inPush` = `Cell<bool>`. The cache is *not* push-scoped — it's
filter-batch-scoped (one hydration fetch's worth). On push, `#inPush` bypasses
the cache entirely and re-fetches size every time.

### `FanOut` (`fan-out.ts`) — STATEFUL (multi-output + refcount destroy)
| Field | Lifetime | Class | Notes |
|---|---|---|---|
| `#input` | PIPELINE | A | |
| `#outputs: Vec<FilterOutput>` | PIPELINE | A | multiple downstreams |
| `#fanIn` | PIPELINE | A | set after construction via `setFanIn` |
| `#destroyCount` | PIPELINE | A | counts destroys; only destroys `#input` when count == outputs.len() |

**Rust:** the refcount-destroy is important — FanOut is destroyed once per
output. Only the *last* destroy propagates to `#input`. Mirror this exactly;
do not double-destroy the upstream.

### `FanIn` (`fan-in.ts`) — STATEFUL (accumulated pushes)
| Field | Lifetime | Class | Notes |
|---|---|---|---|
| `#inputs: Vec<FilterInput>` | PIPELINE | A | |
| `#schema` | PIPELINE | A | |
| `#output` | PIPELINE | A | |
| `#accumulatedPushes: Vec<Change>` | **PUSH** | D | drained in `fanOutDonePushingToAllBranches`. Cleared (`length = 0`) at end of push cycle. |

**Rust:** `RefCell<Vec<Change>>` — pushed to by each branch, drained by
`fanOutDonePushingToAllBranches` via `pushAccumulatedChanges`. The
`pushAccumulatedChanges` fn takes the vec *by mutable ref* and clears it
(`accumulatedPushes.length = 0`). **Rust: `&mut Vec` and `.clear()`.**

### `UnionFanOut` / `UnionFanIn` (`union-fan-*.ts`) — STATEFUL (similar to FanOut/FanIn)
UnionFanIn additions vs FanIn:
| Field | Lifetime | Class | Notes |
|---|---|---|---|
| `#fanOutPushStarted` | **PUSH** | D | bool toggle around a fan-out push cycle |
| `#accumulatedPushes` | **PUSH** | D | same as FanIn |

UnionFanIn also has `#pushInternalChange` — a path for changes that originate
*inside* the ufo/ufi sub-graph (from flip-join children). It re-fetches other
branches to decide whether to forward. **Rust: same `RefCell` pattern.**

### `MemorySource` (`memory-source.ts`) — STATEFUL, the in-memory source
| Field | Lifetime | Class | Notes |
|---|---|---|---|
| `#tableName`, `#columns`, `#primaryKey` | PIPELINE | A | |
| `#primaryIndexSort` | PIPELINE | A | |
| `#indexes: Map<String, Index>` | PIPELINE | A | shared across connections; `Index.usedBy: Set<Connection>` tracks users |
| `#connections: Vec<Connection>` | CONN-managed | A | connections added on `connect`, spliced on `destroy` |
| `#overlay: Option<Overlay>` | **PUSH** | D | set in `genPush`, cleared at end. Splices in-flight change into fetches. |
| `#pushEpoch: number` | PIPELINE (monotonic) | A | increments per push; never resets |
| `Connection.lastPushedEpoch` | CONN | A-ish | set per-connection during push; used by overlay logic |
| `Connection.{sort, splitEditKeys, compareRows, filters, output}` | CONN | A | per-connection config |

**Rust:**
- `#indexes` is shared state read by `fetch` and written by
  `#getOrCreateIndex`. `Rc<RefCell<Index>>` or store indexes in the source
  with `&self` borrows for fetch. The `usedBy` set lets us know when an index
  is safe to drop (TS keeps them forever — comment says LRU was tried and
  rejected; **port the "keep forever" behavior**).
- `#overlay` is `RefCell<Option<Overlay>>` — set/cleared in `genPush`'s
  scope. `fetch` reads it via `&self` borrow... but fetch is called *during*
  push, so `genPush` holds a `&mut`-ish borrow. **Use `RefCell` to break the
  borrow cycle.** The overlay is read by `generateWithOverlay` (a free fn)
  which receives the overlay *value* (it's passed in, not read from self
  mid-iteration) — check: in `#fetch`, `this.#overlay` is read at the start
  and passed to `generateWithOverlay`. So actually the borrow is fine: read
  overlay once at fetch entry, pass it down. ✅ **Good news: the overlay is
  snapshotted at fetch-entry time, not read mid-stream.** So `&self` fetch +
  `RefCell<Option<Overlay>>` works; you take a snapshot.
  - ⚠️ BUT: a fetch triggered *during* a push (Take re-fetch) must see the
    *current* overlay. Since fetch reads overlay at entry and the push is
    synchronous, the overlay is stable for the duration of that fetch. ✅

### `TableSource` (`zqlite/src/table-source.ts`) — STATEFUL, SQLite-backed
| Field | Lifetime | Class | Notes |
|---|---|---|---|
| `#dbCache: WeakMap<Database, Statements>` | PIPELINE | A | per-db statement cache (weak) |
| `#connections` | CONN-managed | A | |
| `#table`, `#columns`, `#uniqueIndexes`, `#primaryKey` | PIPELINE | A | |
| `#stmts: Statements` | PIPELINE (swappable) | A | replaced on `setDB` (snapshotter leapfrog) |
| `#overlay: Option<Overlay>` | **PUSH** | D | same as MemorySource |
| `#pushEpoch` | PIPELINE (monotonic) | A | |
| `#getRowStmtCache: Map<String, String>` | PIPELINE | A | for `getRow` (non-IVM path) |
| SQLite cursor (in `#fetch`) | **FETCH** | C | opened in `#fetch`, closed in `finally` — must close on early return! |

**Rust:** the SQLite cursor lifetime is the critical correctness item. The
`finally` block calls `rowIterator.return?.()` to close the iterator even on
early return (`break`, Take's limit). **In Rust this is the `Drop`
implementation of the iterator struct** — or an explicit `with`/scope guard.
Do NOT leave cursors open; the comment in `mergeSortedStreams` warns that
open cursors cause "database connection is busy executing a query" on the
next write. **Use RAII: the cursor's `Drop` closes it.** This is one place
Rust is *better* than TS (no forgotten `finally`).

`setDB` swaps the statement set — used by the Snapshotter leapfrog algorithm
for concurrent historical traversal. **Rust: `Statements` behind
`RefCell` or swapped atomically.** The `#dbCache` WeakMap → in Rust just a
`HashMap<DatabaseKey, Rc<Statements>>` (no Weak needed; lifetime is the
source's).

### `ArrayView` (`array-view.ts`) — STATEFUL, materialized view (Class F)
| Field | Lifetime | Class | Notes |
|---|---|---|---|
| `#input` | PIPELINE | A | |
| `#listeners: Set<Listener>` | PIPELINE | A | UI subscriptions |
| `#schema`, `#format` | PIPELINE | A | |
| `#root: Entry` | PIPELINE (reassigned) | F | the view tree; reassigned on every change |
| `onDestroy` | PIPELINE | A | callback |
| `#dirty: bool` | PIPELINE | A | set on push, cleared on flush |
| `#resultType`, `#error` | PIPELINE | A | hydration async state |
| `#txnDirty: WeakSet<object>` | **per-txn** | F | COW ownership set; replaced (fresh WeakSet) at `flush()` |

**Rust:** the view tree is `Rc<MetaEntry>` (entries) + `Rc<Vec<...>>`
(arrays). `applyChange` is immutable path-copy. The WeakSet COW trick maps to
**`Rc::make_mut`**: `make_mut(&mut rc)` clones iff refcount > 1, else gives
`&mut` in place. This is *exactly* the "owned by this txn → mutate; else copy"
semantic. Replace `owns(o)` checks with `Rc::strong_count == 1` (or just call
`make_mut` and let it decide). The `flush()` "fresh WeakSet" becomes a no-op
in Rust — once you flush, the committed `Rc`s have refcount > 1 (listeners
hold them), so the next `make_mut` auto-clones. ✅ **Cleaner than TS.**

The `refCountSymbol` (the app-level refcount for duplicate rows reachable via
multiple relationship edges) is a *separate* count from `Rc::strong_count`.
Keep it as an explicit field on `MetaEntry`. It tracks "how many query paths
reach this row," not memory refs.

### `Streamer` (`pipeline-driver.ts`) — per-push accumulator
| Field | Lifetime | Class | Notes |
|---|---|---|---|
| `#changes: Vec<(queryID, schema, changes)>` | **PUSH** | D | one Streamer per `#push` call; created in `#startAccumulating`, drained in `#stopAccumulating().stream()` |

**Rust:** local to the push scope. `Streamer` is constructed, accumulates
`(queryID, schema, Box<dyn Iterator>)` tuples, then `stream()` drains them.
PipelineDriver keeps it as `Option<Streamer>` (`#streamer`) — `RefCell` /
`Cell` since start/stop bracket each push batch.

---

## Part 4 · The Overlay/Epoch Mechanism (Class D deep-dive)

This pattern appears in `MemorySource`, `TableSource`, `Join`,
`FlippedJoin`, and `Take` (`#rowHiddenFromFetch`). It is the most
lifetime-subtle thing in IVM and you must get it right.

### What it does
During a push, downstream operators may *re-fetch* from upstream (e.g. Take
fetches the next row after the bound to find a replacement). That fetch
should see the world *as the push is making it*, not the stale pre-push state.
The overlay splices the in-flight change into fetch results.

### Lifetime invariant
```
push(change) starts
  ├─ overlay = Some({epoch, change})     // set
  ├─ for each connection:
  │    ├─ conn.lastPushedEpoch = epoch
  │    ├─ output.push(outputChange)     // may trigger downstream re-fetches
  │    │    └─ those fetches read overlay (snapshot at fetch entry)
  │    └─ yield undefined
  └─ overlay = None                     // cleared in finally
push returns
```
The overlay is **valid for the entire synchronous duration of the push**.
Re-entrancy is not a concern (the code asserts no re-entrancy in `Exists`).

### Rust mapping
- `overlay: RefCell<Option<Overlay>>` on the source.
- `pushEpoch: Cell<u64>` (monotonic).
- `lastPushedEpoch: Cell<u64>` per connection.
- In `genPush`: `*overlay.borrow_mut() = Some(...)` at start, `= None` in a
  guard's `Drop`. **Use an RAII guard** so the clear happens even on panic
  (TS `finally` ↔ Rust `Drop`).
- In `fetch`: `let overlay = overlay.borrow().clone();` at entry (snapshot),
  pass the owned `Option<Overlay>` into the generator fns. This avoids
  holding the `Ref` across yields.

### Join's variant
`#inprogressChildChange` is the *child* change being pushed; the thunk in
`#processParentNode` reads it to overlay the in-flight child onto fetched
child streams. The thunk is **lazy and consumed during the push** (downstream
pushes synchronously consume relationship streams). So clearing in `finally`
(after the push loop) is correct. **Rust: same RAII guard; the thunk holds a
shared handle (`Rc<RefCell<Join>>`) to read the overlay when invoked.**

---

## Part 5 · Pipeline & Connection Lifetime (the ownership graph)

### Build (`builder/builder.ts:buildPipelineInternal`)
```
source.connect(...) → SourceInput (conn)
  → [Skip?] → [correlated subqueries: Join/FlippedJoin] → [Where: Filter/FanOut/FanIn/UnionFanOut/UnionFanIn]
    → [limit: Take | Cap] → [related: Join/FlippedJoin ...]
```
Each operator constructor calls `input.setOutput(this)` (or
`setFilterOutput(this)`), registering itself as the downstream's output.
`delegate.addEdge(a, b)` records the edge (for debug/decoration).

### Destroy (cascade)
`pipeline.input.destroy()` → each operator's `destroy()` → calls
`input.destroy()` on its upstream → ... → reaches `SourceInput.destroy()` →
removes the connection from `source.#connections`.

**FanOut/UnionFanOut** destroy is refcounted: destroyed once per output; only
the last destroy propagates upstream.

### Rust ownership shape
```
PipelineDriver
  ├─ tables: HashMap<String, Rc<RefCell<TableSource>>>     // shared across pipelines
  ├─ pipelines: HashMap<String, Pipeline>
  │     └─ input: Box<dyn Input>   // owns the operator tree
  │           └─ operators own their children (Box<dyn Input>)
  │                 └─ ... leaves: SourceInput adapter
  │                       └─ holds Rc<RefCell<Connection>> (or back-ref to source)
  └─ rowSetSignatures: HashMap<String, u128>
```
The **shared source** is the only cross-pipeline shared mutable state. Each
pipeline's leaf is a `SourceInput` adapter that:
- borrows/holds a handle to the `Connection` (registered in the source's
  `connections` vec),
- on `destroy()` removes itself from that vec.

**Rust: `Rc<RefCell<Connection>>`** held by both the source's vec and the
adapter; `destroy` does `connections.retain(|c| !Rc::ptr_eq(c, &self.conn))`.
Or give each connection an id and remove by id (cheaper than ptr_eq on a
`RefCell`).

### Reset
`PipelineDriver.reset()` destroys all pipelines + clears `#tables`. Sources
are dropped. New sources are created on next `addQuery`.

---

## Part 6 · The Hard Problem: Lazy Relationship Thunks (Class E)

This is the single biggest Rust porting decision. Understand it before
writing any operator code.

### The TS pattern
```ts
// join.ts #processParentNode
const childStream = () => {
  const constraint = buildJoinConstraint(parentNodeRow, parentKey, childKey);
  const stream = constraint ? this.#child.fetch({constraint}) : [];
  // ... overlay logic reading this.#inprogressChildChange ...
  return stream;
};
return {
  row: parentNodeRow,                         // shared immutable
  relationships: {
    ...parentNodeRelations,
    [this.#relationshipName]: childStream,    // closure escapes via the Node
  },
};
```
The `childStream` closure:
1. captures `this` (the Join operator — a mutable, shared thing),
2. captures `parentNodeRow` (shared),
3. is stored *inside* the returned Node,
4. the Node is *yielded out* of `fetch`/`push`,
5. the closure is called *later*, lazily, by downstream (or by the Streamer
   when materializing the view),
6. when called, it invokes `this.#child.fetch(...)` which returns a *fresh*
  single-use stream.

### Why this is hard in Rust
The closure, stored in the yielded Node, holds `&self` (or `&mut self`) of
the Join operator. But the Node escapes the borrow scope of the fetch/push
that created it. A closure with `&'a self` cannot be stored in a value whose
lifetime outlives `'a`. The borrow checker will reject it.

Additionally: `this.#child.fetch(...)` returns a stream that borrows from
`#child` (another operator). So the stream's lifetime is bounded by the
child's borrow — and the stream is consumed lazily, possibly after the
parent fetch has returned.

### Three options (ranked by fidelity to RUST-IVM-GOAL)

**Option 1 — Shared ownership via `Rc<RefCell<>>` (RECOMMENDED for mechanical port).**
Make operators `Rc<RefCell<Join>>` etc. The thunk holds `Rc<RefCell<Join>>`
+ `Arc<Row>` (parentNodeRow) + the constraint. When called, it does
`join.borrow().child.fetch(constraint)`. The returned stream borrows from
the `Rc` (kept alive by the thunk's own `Rc` clone), so lifetimes are
decoupled. **This matches TS GC semantics exactly.** Cost: refcount
overhead + runtime borrow checks (but single-threaded, so cheap). This is
the path the Go port effectively took (Go GCs too).

**Option 2 — Typed relationship descriptors (more idiomatic, more work).**
Replace the closure with an enum:
```rust
enum Relationship {
    Join { child: Rc<RefCell<dyn Input>>, constraint: Constraint },
    FlippedJoin { ... },
    Empty,
}
```
and have the *consumer* (the Streamer / ArrayView / a downstream fetch)
explicitly drive it: `match rel { Relationship::Join{child, constraint} =>
child.borrow().fetch(constraint) }`. This removes closures but pushes the
"how to read this relationship" knowledge to every consumer. **More
invasive — deviates from the mechanical-port goal.** Only do this if Rc
overhead shows up in benchmarks.

**Option 3 — `Box<dyn Fn()>` with `'static` + `Rc`** — variant of Option 1;
same tradeoffs, just spelled differently.

**Recommendation: Option 1.** The goal doc explicitly says mechanical
fidelity wins every tie. Rc<RefCell<>> is the closest thing to TS GC
semantics. Profile later; if refcount is hot, revisit Option 2 for the
hottest paths (Join relationships).

### The `'yield'` sentinel
Don't conflate with `yield`. Model as:
```rust
enum StreamItem<T> { Node(T), Yield }
trait IvmStream { fn next(&mut self) -> Option<StreamItem<Node>>; }
```
or keep `Node | Yield` as `Option<Result<Node, Yield>>`. The consumer
(`view-syncer`) pumps the event loop on `Yield`. **Single-threaded — no
async needed.** If you later go multicore (Phase 3), each CG's driver stays
single-threaded; cross-CG parallelism doesn't change the per-stream model.

---

## Part 7 · Lifetime Cheat Sheet by Variable Class

| Class | TS field pattern | Lifetime | Rust |
|---|---|---|---|
| A — pipeline state | `#input`, `#limit`, `#schema` | operator instance | owned struct fields |
| B — immutable shared data | `Node.row`, `Row` | forever (logically) | `Arc<Row>` or `&Row` in contained scope |
| C — single-use stream | `Stream<T>`, generators | one iteration | `impl IvmStream` (Drop = cleanup) |
| D — push-scoped overlay | `#overlay`, `#inprogressChildChange`, `#rowHiddenFromFetch`, `#inPush` | one push call | `RefCell<Option<T>>` + RAII guard for clear |
| E — escaping thunk | `Node.relationships[name]: () => Stream` | until consumer drains | `Rc<RefCell<Op>>` in closure (Option 1) |
| F — COW view tree | `ArrayView.#root`, `MetaEntry` | reassigned per change | `Rc<Entry>` + `Rc::make_mut` |

---

## Part 8 · Porting Order (suggested, by lifetime complexity)

1. **Data types**: `Value`, `Row`, `Node`, `Change`, `Constraint`,
   `SourceSchema`, `Comparator`. All owned/Clone. (Class A/B)
2. **Stream trait + `StreamItem`**. Define the `IvmStream` trait and the
   `'yield'` sentinel. (Class C)
3. **`Filter`, `Skip`** — stateless. Validate the stream plumbing.
4. **`MemoryStorage`** (the `Storage` impl) — HashMap-backed.
5. **`Take`, `Cap`** — first stateful operators; introduces Class D
   (`#rowHiddenFromFetch`). Get the RAII overlay guard pattern right here.
6. **`MemorySource`** — shared source, `RefCell<Option<Overlay>>`, epoch,
   `Rc<RefCell<Connection>>`. Nail the overlay-snapshot-at-fetch-entry.
7. **`Join`** — first Class E (escaping thunks). Commit to Option 1
   (`Rc<RefCell<Join>>`).
8. **`FlippedJoin`** — same patterns, plus chunking + `mergeSortedStreams`
   (heap merge with `.return()`-on-early-return cleanup → Rust `Drop`).
9. **`Exists`** — filter-pipeline variant; `#inPush` Cell + per-batch cache.
10. **`FanOut`/`FanIn`/`UnionFanOut`/`UnionFanIn`** — accumulated-push
    `RefCell<Vec<Change>>` + refcounted destroy.
11. **`TableSource`** — SQLite cursor as RAII iterator (Drop closes cursor);
    `setDB` swap; statement cache.
12. **`ArrayView` + `view-apply-change`** — `Rc<MetaEntry>` + `make_mut` COW.
    Verify reference stability (unchanged subtrees keep identity).
13. **`pipeline-driver` + `builder`** — wire it together; `Rc<RefCell<Source>>`
    shared across pipelines; destroy cascade.

---

## Part 9 · Invariants You Must Preserve (lifetime-related)

From the source comments — these are not suggestions:

1. **Output size ≤ limit at all times** (Take, Cap). Take does
   *remove-before-add*; Cap defers pk-set add. Preserve ordering of pushes.
2. **No downstream early return during `#initialFetch`** (Take, Cap assert
   this). If you support early return, you must drain the input to the limit
   before returning so state is hydrated. The TS code asserts; Rust should
   assert too (and document why).
3. **Overlay cleared even on panic** — TS `finally` ↔ Rust `Drop` guard.
   Never leave `#overlay` / `#inprogressChildChange` set after a push.
4. **SQLite cursors closed on early return** — `mergeSortedStreams` and
   `TableSource.#fetch` both rely on `finally`/`.return()`. In Rust this is
   `Drop` on the iterator. **Do not bypass with `mem::forget`.**
5. **`mergeSortedStreams` propagates `.return()` to non-exhausted
   sub-iterators** on early termination. If you port the heap merge, the
   `finally` loop ↔ a `Drop` that calls `.return()` (or drops) each active
   sub-iterator.
6. **Join/FlippedJoin assert `parent !== child`** and
   `parentKey.length == childKey.length`. Preserve.
7. **`rowEqualsForCompoundKey` uses `compareValues` (null=null), but
   `valuesEqual` treats null ≠ null** (for join matching). Do NOT unify
   these in Rust — port both semantics separately. This affects which rows
   match in joins.
8. **FanOut/UnionFanOut destroy is refcounted** — only the last destroy
   propagates upstream. Do not destroy the upstream N times.
9. **`ArrayView` reference stability** — unchanged subtrees keep the same
   `Rc` pointer across pushes (for React.memo). `Rc::make_mut` preserves
   this automatically (clone-on-write). Verify with the oracle.
10. **`Streamer` is single-use per push batch** — created in
    `#startAccumulating`, drained in `#stopAccumulating`. The
    PipelineDriver's `#streamer: Option<Streamer>` field must be `None`
    between batches. Port as `RefCell<Option<Streamer>>`.

---

## Appendix · File → Class Cross-Reference

| File | Class A | Class B | Class C | Class D | Class E | Class F |
|---|---|---|---|---|---|---|
| `data.ts` | — | Row, Node, Comparator | — | — | Node.relationships | — |
| `change.ts` | — | Change | — | — | — | — |
| `operator.ts` | interfaces | — | Stream contract | — | — | — |
| `source.ts` | Source | — | — | — | — | — |
| `filter.ts` | ✓ | — | — | — | — | — |
| `skip.ts` | ✓ | — | — | — | — | — |
| `take.ts` | ✓ | — | fetch streams | `#rowHiddenFromFetch` | — | — |
| `cap.ts` | ✓ | — | — | — | — | — |
| `join.ts` | ✓ | row | — | `#inprogressChildChange` | `#processParentNode` thunks | — |
| `flipped-join.ts` | ✓ | row | chunked streams | `#inprogressChildChange` | `#yieldParentWithOverlay` | — |
| `exists.ts` | ✓ | — | — | `#inPush`, `#cache` | — | — |
| `fan-out.ts` | ✓ (refcount destroy) | — | — | — | — | — |
| `fan-in.ts` | ✓ | — | — | `#accumulatedPushes` | — | — |
| `union-fan-*.ts` | ✓ | — | `mergeFetches` | `#accumulatedPushes`, `#fanOutPushStarted` | — | — |
| `memory-source.ts` | ✓ (indexes, connections) | rows in BTreeSet | fetch streams, `mergeSortedStreams` | `#overlay`, epoch | — | — |
| `table-source.ts` | ✓ (stmts, cache) | rows | SQLite cursor (RAII!) | `#overlay`, epoch | — | — |
| `view-apply-change.ts` | — | — | — | `activeDirty` (module!) | — | MetaEntry tree, refCount |
| `array-view.ts` | ✓ | — | hydrate stream | `#txnDirty`, `#dirty` | — | `#root` |
| `push-accumulated.ts` | — | — | — | accumulated vec (borrowed) | — | — |
| `pipeline-driver.ts` | ✓ (tables, pipelines) | — | — | `#streamer` | — | — |

---

## TL;DR for the Rust porter

- **80% of fields are Class A** (owned, pipeline-lifetime) — boring, just
  struct fields.
- **`Row`/`Node.row` are Class B** (immutable, shared) — `Arc<Row>`.
- **Streams are Class C** (single-use, RAII cleanup) — custom trait, `Drop`
  closes cursors/overlays.
- **The push-scoped (Class D) overlay fields** are: `MemorySource.#overlay`,
  `TableSource.#overlay`, `Join.#inprogressChildChange`,
  `FlippedJoin.#inprogressChildChange`, `Take.#rowHiddenFromFetch`, and
  `Exists.#inPush`. Use `RefCell<Option<T>>` + an RAII guard whose `Drop`
  clears it (mirrors TS `finally`).
- **`Node.relationships` thunks are Class E** (escape the borrow) — this is
  the crux. Use `Rc<RefCell<Op>>` shared handles so thunks can be stored in
  yielded Nodes and invoke `child.fetch()` later. Matches TS GC semantics;
  mandated by the mechanical-fidelity goal.
- **`ArrayView` tree is Class F** (COW) — `Rc<MetaEntry>` + `Rc::make_mut`
  replaces the `WeakSet` trick and *automatically* preserves reference
  stability (the invariant that lets React.memo skip unchanged subtrees).
- **Shared sources** (`TableSource`/`MemorySource`) are `Rc<RefCell<>>` held
  by `PipelineDriver` and referenced by each pipeline's connection.
- **Destroy is a cascade** with two refcount exceptions: `FanOut` and
  `UnionFanOut` only propagate upstream on the *last* destroy.
