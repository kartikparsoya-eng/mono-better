/**
 * Mutation testing — verifies that the test suite actually has teeth.
 *
 * The suite here is strong (55 test binaries, proptest laws, agentic fuzz
 * corpora, fixture replay, planner differential). But none of that answers the
 * question this tool asks: **is the code the oracles claim to cover actually
 * being reached?**
 *
 * That distinction is not academic. On a much smaller Rust IVM port, after
 * 2000 fuzz seeds and a full differential harness went green, deleting an
 * entire 200-line module (the join overlay) changed *no* test result —
 * instrumentation showed the path was invoked 0 times. A green suite was
 * reporting confidence it had not earned.
 *
 * Each mutation below introduces a deliberate, semantically real bug. A
 * mutation that **survives** means nothing in the suite exercises that
 * behaviour, which is a coverage gap regardless of how many tests pass.
 *
 * Usage:
 *   node tools/mutate.mjs              # all mutations
 *   node tools/mutate.mjs null         # only mutations matching "null"
 *   FAST=1 node tools/mutate.mjs       # skip fixture-replay (quicker, weaker)
 *
 * Note: mutations are applied to the working tree and restored in a `finally`.
 * Run on a clean tree; if the process is killed mid-run, `git checkout` the
 * affected file.
 */
import {execSync} from 'node:child_process';
import {readFileSync, writeFileSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const FAST = process.env.FAST === '1';

const MUTATIONS = [
  {
    name: 'data/values-equal-null',
    why: 'NULL would compare equal to NULL, so NULL foreign keys would join instead of matching nothing',
    file: 'src/ivm/data.rs',
    find: '    if a.is_null() || b.is_null() {\n        return false;\n    }\n    a == b',
    repl: '    a == b',
  },
  {
    name: 'join/compound-key-null',
    why: 'Reintroduces the null-semantics bug fixed in b6f8cc871 — row_equals_for_compound_key must use compare_values (NULL==NULL), not join semantics',
    file: 'src/ivm/join_utils.rs',
    find: '        if compare_values(&av, &bv) != CmpOrdering::Equal {',
    repl: '        if !crate::ivm::data::values_equal(&av, &bv) {',
  },
  {
    // NOTE: an earlier version of this mutation targeted `Exists::filter`,
    // which is `#[allow(dead_code)]` and called by nothing. Mutating dead code
    // can never be killed, and a "surviving" mutation there says nothing about
    // coverage. Target the LIVE fetch-path filter instead.
    name: 'exists/fetch-always-true',
    why: 'EXISTS fetch filter always passes — parents with no children leak into results',
    file: 'src/ivm/exists.rs',
    find: '                size > 0\n            };\n            let keep = if not { !exists_result } else { exists_result };',
    repl: '                size >= 0\n            };\n            let keep = if not { !exists_result } else { exists_result };',
  },
  {
    name: 'exists/push-always-true',
    why: 'EXISTS push filter always passes — NOT-EXISTS rows leak through on advance',
    file: 'src/ivm/exists.rs',
    find: 'let passes = if not { size == 0 } else { size > 0 };',
    repl: 'let passes = if not { size == 0 } else { size >= 0 };',
  },
  {
    name: 'data/compare-null-ordering',
    why: 'NULL would no longer sort before all other values, corrupting index scan order',
    file: 'src/ivm/data.rs',
    find: '(Value::Null, _) => CmpOrdering::Less',
    repl: '(Value::Null, _) => CmpOrdering::Greater',
  },
];

const filter = process.argv[2];
const selected = MUTATIONS.filter(m => !filter || m.name.includes(filter));

function run(cmd) {
  try {
    execSync(cmd, {cwd: root, stdio: 'pipe', encoding: 'utf8', maxBuffer: 1 << 28});
    return true; // exit 0 => bug NOT detected
  } catch {
    return false; // non-zero => detected
  }
}

const survived = [];
let killed = 0;

for (const m of selected) {
  const path = join(root, m.file);
  const original = readFileSync(path, 'utf8');

  if (!original.includes(m.find)) {
    console.log(`⚠ ${m.name.padEnd(28)} SKIPPED — anchor not found (mutation is stale)`);
    survived.push({...m, reason: 'stale anchor — code moved; update or drop this mutation'});
    continue;
  }

  process.stdout.write(`· ${m.name.padEnd(28)} building… `);
  writeFileSync(path, original.replace(m.find, m.repl));
  try {
    if (!run('cargo build --release 2>&1')) {
      console.log('killed by the compiler');
      killed++;
      continue;
    }
    process.stdout.write('testing… ');
    const suite = FAST
      ? 'cargo test --release --no-fail-fast -- --test-threads=1 2>&1'
      : 'cargo test --release --no-fail-fast -- --test-threads=1 2>&1';
    if (!run(suite)) {
      console.log('KILLED ✓');
      killed++;
    } else {
      console.log('SURVIVED ✗');
      survived.push(m);
    }
  } finally {
    writeFileSync(path, original);
  }
}

run('cargo build --release 2>&1'); // leave the tree in a known-good state

console.log(`\n${killed}/${selected.length} mutations killed`);

if (survived.length) {
  console.log('\n--- COVERAGE GAPS ---');
  for (const m of survived) {
    console.log(`\n${m.name}\n  ${m.reason ?? m.why}\n  file: ${m.file}`);
  }
  console.log(
    '\nA surviving mutation means that behaviour is untested. Either add a test,\n' +
      'or record it in RUST-DRIFT-LEDGER.md as a knowingly-unverified path.',
  );
  process.exit(1);
}
