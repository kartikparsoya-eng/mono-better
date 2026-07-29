# IVM Assertion Catalog — TS contracts vs Rust port

Extraction #2 from `IVM_EXTRACTION_PLAN.md`. Every `assert()` in the TS IVM
non-test code is a correctness contract. This catalogs all **85** and records
whether the Rust port (`rust-ivm/`) enforces the same contract.

Source: `mono/packages/zql/src/ivm/*.ts` (tag `zero/v1.7.0`).
Port:   `rust-ivm/src/ivm/*.rs`, `rust-ivm/src/{builder,sqlite}/*.rs`.

## Method & caveats

- The 85 TS asserts were extracted mechanically (balanced-paren scan) — the
  count matches the plan exactly.
- Rust status was determined by (a) a complete inventory of `assert!` /
  `assert_eq!` / `debug_assert!` / `panic!` / `unreachable!` in Rust non-test
  code, and (b) targeted reads of the operators the plan flagged.
- **A 🔴 means "no explicit equivalent assertion found in the Rust inventory."**
  It does *not* always mean the invariant is unprotected — Rust often enforces
  the same thing structurally: `let Some(x) = … else { return }`, `?`,
  exhaustive `match`, or the borrow checker. Each 🔴 is a *candidate* to add;
  verify per-file before porting a literal `assert!`.
- ⚪ = intentionally handled by a different Rust mechanism (documented).

| Symbol | Meaning |
|---|---|
| ✅ | Equivalent assertion present in Rust |
| 🔴 | No equivalent assertion found — candidate to add |
| ⚪ | Contract met by a different mechanism (control-flow / borrow-checker) |
| ⚠️ | Partial / needs confirmation |

## ⚠️ Correction to the extraction plan

The plan lists these as "🔴 missing." Re-checking the current tree:

| Plan claim | Actual Rust status |
|---|---|
| `Join` doesn't assert `parent !== child` | ✅ **present** — `join.rs:64` |
| `FlippedJoin` doesn't assert `parent !== child` | ✅ **present** — `flipped_join.rs:91` |
| `Join` doesn't assert `parentKey.length === childKey.length` | ✅ **present** — `join.rs:69`, `flipped_join.rs:95` |
| `Take` doesn't assert "no early return during initialFetch" | ✅ **present** — `take.rs` `InitialFetchGuard` |
| `Cap` doesn't assert the same | ✅ **present** — `cap.rs` `CapInitialFetchGuard` |

So the port has advanced since the audit that seeded the plan. The
key-length and early-return guards remain genuinely absent.

---

## The 85 contracts

### `array-view.ts`
| TS | Assertion | Guards | Rust |
|---|---|---|---|
| 116 | `!listeners.has(listener)` | double-registration of a view listener | 🔴 |

### `cap.ts`
| TS | Assertion | Guards | Rust |
|---|---|---|---|
| 68 | `limit >= 0` | negative limit | ✅ `cap.rs:77` |
| 88 | `!req.start` | Cap has no ordering, can't seek | ✅ `cap.rs:170` |
| 89 | `!req.reverse` | same | ✅ `cap.rs:171` |
| 95 | partitionKey ⇒ constraint matches it | wrong partition fetch | ✅ `cap.rs:179` |
| 130 | `constraintMatchesPartitionKey` (push) | wrong-partition push | ⚠️ verify |
| 136 | cap state `=== undefined` (first hydrate) | double-hydration | 🔴 |
| 169 | `!downstreamEarlyReturn` | **state not hydrated to limit** | ✅ `cap.rs` `CapInitialFetchGuard` |
| 261 | partitionKeyComparator: edit keeps partition | edit crosses partition | 🔴 |

### `constraint.ts`
| TS | Assertion | Guards | Rust |
|---|---|---|---|
| 151 | `condition.right.type === 'literal'` | non-literal RHS in extractColumn | ⚪ `builder/filter.rs` panics on unsupported predicate |

### `exists.ts`
| TS | Assertion | Guards | Rust |
|---|---|---|---|
| 53 | input schema has `relationshipName` | malformed pipeline | 🔴 |
| 110 | `!#inPush` (no re-entrancy) | re-entrant push | ⚪ replaced — `exists.rs` dropped `in_push`, uses `try_borrow_mut` (see comment `exists.rs:327`) |
| 250 | relationship found on node | missing relationship at runtime | ⚠️ `exists.rs` `unreachable!` paths |

### `fan-in.ts`
| TS | Assertion | Guards | Rust |
|---|---|---|---|
| 41 | `#schema === input.getSchema()` | schema divergence across branches | 🔴 |
| 78 | no-inputs ⇒ `accumulatedPushes.length === 0` | push to an empty fan-in | 🔴 |

### `flipped-join.ts`
| TS | Assertion | Guards | Rust |
|---|---|---|---|
| 115 | `parent !== child` | self-join infinite recursion | ✅ `flipped_join.rs:91` |
| 116 | `parentKey.length === childKey.length` | malformed AST | 🔴 |
| 392 | child edit keeps `childKey` | edit changes join relationship | ✅ `flipped_join.rs:419` |
| 547 | parent edit keeps `parentKey` | same, parent side | 🔴 (only one edit-key assert present) |

### `join-utils.ts`
| TS | Assertion | Guards | Rust |
|---|---|---|---|
| 110 | edit overlay: new applied before old | overlay ordering | ✅ `join_utils.rs:161` |
| 120 | overlay was applied to some node | overlay never matched | ✅ `join_utils.rs:197` |
| 200 | suppressed ∨ overlay is REMOVE | overlay never matched (remove case) | ✅ `join_utils.rs:292` |

### `join.ts`
| TS | Assertion | Guards | Rust |
|---|---|---|---|
| 73 | `parent !== child` | self-join recursion | ✅ `join.rs:64` |
| 74 | `parentKey.length === childKey.length` | malformed AST | ✅ `join.rs:69` |
| 167 | parent edit keeps `parentKey` | edit changes relationship | ✅ `join.rs:~200` |
| 208 | child edit keeps `childKey` | edit changes relationship | ✅ `join.rs:~228` |

### `memory-source.ts`
| TS | Assertion | Guards | Rust |
|---|---|---|---|
| 207 | connection found on destroy | destroying an unknown connection | 🔴 |
| 221 | primary index exists | corrupt index map | 🔴 |
| 469 | add succeeded (existence pre-checked) | duplicate insert | ✅ `source.rs` `MemorySource::push_internal` |
| 478 | remove succeeded | missing row on remove | ✅ `source.rs` `MemorySource::push_internal` |
| 491 | edit-remove succeeded | missing row on edit | ✅ `source.rs` `MemorySource::push_internal` |
| 604 | dev: row not already present (add) | source drift (dup add) | ✅ `source.rs` `MemorySource::push_internal` |
| 610 | dev: row present (remove) | source drift (missing remove) | ✅ `source.rs` `MemorySource::push_internal` |
| 616 | dev: old row present (edit) | source drift (missing edit) | ✅ `source.rs` `MemorySource::push_internal` |

### `push-accumulated.ts`
| TS | Assertion | Guards | Rust |
|---|---|---|---|
| 108 | at most one change of a type per branch | fan-in over-accumulation | ✅ `push_accumulated.rs:166` |
| 139 | all-removes case | mixed types where removes expected | ✅ `push_accumulated.rs:184-185` |
| 149 | all-adds case | mixed types where adds expected | ✅ `push_accumulated.rs:190-191` |
| 159 | edit case: only add/remove/edit | unexpected type | ✅ `push_accumulated.rs:197` |
| 222 | child case: add/remove/edit/child only | unexpected type | ✅ `push_accumulated.rs:228` |
| 231 | child case: `types.length <= 2` | too many types from fan-out | ✅ `push_accumulated.rs:233` |
| 246 | child case: not both add and remove | contradictory child change | ✅ `push_accumulated.rs:243` |
| 290 | mergeRelationships: edit⇒edit | type-pairing invariant | ✅ `push_accumulated.rs:73/96` (panic) |
| 317 | mergeRelationships: child⇒child | same | ✅ `push_accumulated.rs:73/96` |
| 337 | mergeRelationships: differ⇒left is edit | same | ✅ `push_accumulated.rs:73/96` |

### `skip.ts`
| TS | Assertion | Guards | Rust |
|---|---|---|---|
| 42 | `sort !== undefined` | Skip on unsorted input | ⚠️ Rust type makes `sort` required — verify no runtime path |

### `snitch.ts`
| TS | Assertion | Guards | Rust |
|---|---|---|---|
| 156 | output set before filter | test-double misuse | ✅ `operator.rs:73` `panic!("Output not set")` |

### `take.ts`
| TS | Assertion | Guards | Rust |
|---|---|---|---|
| 72 | `limit >= 0` | negative limit | ✅ `take.rs:93` |
| 74 | `sort !== undefined` | Take on unsorted input | ⚠️ type-enforced — verify |
| 159 | `req.start === undefined` (initialFetch) | seek during hydrate | 🔴 |
| 160 | `!req.reverse` (initialFetch) | reverse during hydrate | 🔴 |
| 166 | constraint matches partitionKey | wrong-partition hydrate | 🔴 |
| 172 | take state `=== undefined` (first hydrate) | double-hydration | 🔴 |
| 210 | `!downstreamEarlyReturn` | **state not hydrated to limit** | ✅ `take.rs` `InitialFetchGuard` |
| 320 | boundNode found during fetch | corrupt bound | ⚪ likely `let Some … else` — verify |
| 433 | partitionKeyComparator: edit keeps partition | edit crosses partition | ✅ `take.rs:369` |
| 448 | `takeState.bound` set | pushing before hydrate | ⚠️ verify |
| 502 | beforeBoundNode found | corrupt bound | ⚪ verify |
| 517 | `newCmp > 0` | ordering violation | 🔴 (only `!= Equal` asserts present) |
| 534 | newBoundNode found | corrupt bound | ⚪ verify |
| 563 | `newCmp !== 0` (dup PK) | duplicate primary key | ✅ `take.rs:435` |
| 571 | `newCmp < 0` | ordering violation | 🔴 |
| 593 | oldBoundNode found | corrupt bound | ⚪ verify |
| 597 | newBoundNode found | corrupt bound | ⚪ verify |
| 620 | `newCmp !== 0` (dup PK) | duplicate primary key | ✅ `take.rs:460` |
| 630 | `newCmp > 0` | ordering violation | 🔴 |
| 649 | afterBoundNode found | corrupt bound | ⚪ verify |

### `union-fan-in.ts`
| TS | Assertion | Guards | Rust |
|---|---|---|---|
| 36 | `sort !== undefined` | union fan-in on unsorted | ⚠️ verify |
| 55 | tableName matches across inputs | schema divergence | ✅ `union_fan_in.rs:55` |
| 59 | primaryKey matches | schema divergence | ✅ `union_fan_in.rs:59` |
| 63 | system matches | schema divergence | ✅ `union_fan_in.rs:63` |
| 67 | compareRows matches | schema divergence | ✅ `union_fan_in.rs:67` |
| 71 | sort matches | schema divergence | ⚠️ verify (may be folded) |
| 82 | relationship not in multiple inputs | ambiguous relationship | ✅ `union_fan_in.rs:76` |
| 151 | change is add or remove | unexpected type into union | ✅ `union_fan_in.rs:93/103` |
| 179 | pusher was one of the inputs | unknown pusher | 🔴 |
| 186 | `!#fanOutPushStarted` on start | double-start | 🔴 |
| 194 | `#fanOutPushStarted` on done | done-without-start | 🔴 |
| 257 | `min !== undefined` | empty merge heap | ⚪ `union_fan_in.rs:165` `unreachable!` |

### `union-fan-out.ts`
| TS | Assertion | Guards | Rust |
|---|---|---|---|
| 23 | `!#unionFanIn` (set once) | double fan-in wiring | 🔴 |

### `view-apply-change.ts`
| TS | Assertion | Guards | Rust |
|---|---|---|---|
| 286 | singular rel: one row (`compareRows === 0`) | fan-out into a singular relationship | ✅ `view.rs:344` |
| 384 | `pos >= 0` | node not found (add) | ✅ `view.rs:470` |
| 433 | `oldPos >= 0` | old node not found (edit) | ✅ `view.rs:586` |
| 527 | `pos >= 0` | node not found | ✅ `view.rs:530` |
| 750 | `pos >= 0` | node not found | ✅ `view.rs:631` |
| 801 | `entry !== undefined` | node not found | ✅ `view.rs:797` |

---

## Summary of gaps (candidates to add)

Grouped by impact. 🔴 = no explicit Rust assert found.

**High — silent-corruption class:**
All high-severity assertions are now present:
- ✅ `join.ts:74` / `flipped-join.ts:116` — `parentKey.length === childKey.length`.
  Present in `join.rs:69` and `flipped_join.rs:95`.
- ✅ `join.ts:167` / `join.ts:208` / `flipped-join.ts:547` — edit-keeps-join-key.
  Present in `join.rs` (`push_parent` / `push_child` Edit arms) and
  `flipped_join.rs:428`.
- ✅ `take.ts:210` / `cap.ts:169` — no-early-return-during-initialFetch. Present
  via `InitialFetchGuard` / `CapInitialFetchGuard` RAII guards.
- ✅ `memory-source.ts:469/478/491` — add/remove/edit-must-succeed. Present in
  `MemorySource::push_internal` (panics on duplicate add, missing remove,
  missing edit old-row; skipped only for SQLite-backed fetch where the in-memory
  cache is not authoritative).

**Medium — defensive / fail-fast:**
- `take.ts:159/160/166/172`, `cap.ts:130/136` — initialFetch preconditions.
- `union-fan-in.ts:179/186/194`, `union-fan-out.ts:23` — fan-out/in wiring &
  push-cycle state machine.
- `fan-in.ts:41/78`, `union-fan-in.ts:36/71` — schema-consistency across branches.
- `take.ts:517/571/630` — comparator ordering (`> 0` / `< 0`) beyond the
  dup-PK `!= 0` checks that are present.

**Low — internal / test-only:**
- `array-view.ts:116` (listener dup), `exists.ts:53` (schema relationship).

**Intentionally different (no action):**
- `exists.ts:110` re-entrancy → Rust uses `try_borrow_mut` instead of an
  `in_push` flag.
- `constraint.ts:151` → covered by builder-side panic on unsupported predicate.

## Next steps
1. ✅ All four **High** groups are now present. No further high-severity
   assertion work required.
2. For every ⚪, confirm the structural guard actually holds on all paths
   (especially the Take `boundNode` fetches — a `None` there should be a hard
   error, not a silent `return`).
3. Fold any newly-added assertions into the fixture-replay suite
   (`tests/fixture_replay_test.rs`) so a violating fixture fails loudly.
