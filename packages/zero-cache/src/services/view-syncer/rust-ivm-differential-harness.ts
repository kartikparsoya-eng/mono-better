// Shared harness for the RustIVMDriver-vs-PipelineDriver differential tests
// (both the curated `rust-ivm-driver.differential.test.ts` and the fuzz-driven
// `rust-ivm-driver.fuzz.test.ts`). Kept framework-free so both import the SAME,
// already-validated comparison + translation logic.
//
// CORRECTNESS NOTE: the two drivers are fed the SAME replica + SAME client
// schema + SAME AST/pushes. Because the input pipeline is identical for both, a
// translation imperfection cannot manufacture a false divergence — it makes both
// drivers behave the same (or both error). A reported divergence therefore means
// the two engines genuinely disagree. (This is the structural advantage over the
// engine fuzzer, which compares two *different* input pipelines.)
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  json,
  number,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';

// --- change comparison -------------------------------------------------------

export type Change = {
  type: number;
  queryID: string;
  table: string;
  rowKey: unknown;
  row: unknown;
};

/** Deterministic JSON with sorted keys (rowKey/row column order may differ). */
export function stable(v: unknown): string {
  if (v === null || typeof v !== 'object') {
    return JSON.stringify(v);
  }
  const o = v as Record<string, unknown>;
  return `{${Object.keys(o)
    .sort()
    .map(k => `${JSON.stringify(k)}:${stable(o[k])}`)
    .join(',')}}`;
}

/** Drain a sync OR async iterable of `RowChange | 'yield'`, dropping sentinels. */
export async function drain(
  it: Iterable<unknown> | AsyncIterable<unknown>,
): Promise<Change[]> {
  const out: Change[] = [];
  for await (const c of it as AsyncIterable<unknown>) {
    if (c === 'yield') {
      continue;
    }
    out.push(c as Change);
  }
  return out;
}

/** Multiset keyed by (table, canonical rowKey) → sorted list of {type,row}. */
export function multiset(changes: Change[]): Map<string, string[]> {
  const m = new Map<string, string[]>();
  for (const c of changes) {
    const key = `${c.table} ${stable(c.rowKey)}`;
    const val = `${c.type} ${stable(c.row ?? null)}`;
    const arr = m.get(key);
    if (arr) {
      arr.push(val);
    } else {
      m.set(key, [val]);
    }
  }
  for (const arr of m.values()) {
    arr.sort();
  }
  return m;
}

/** Symmetric multiset diff. Empty arrays => the two streams are equal. */
export function diffChanges(
  rust: Change[],
  ts: Change[],
): {onlyInRust: string[]; onlyInTs: string[]} {
  const rm = multiset(rust);
  const tm = multiset(ts);
  const onlyInRust: string[] = [];
  const onlyInTs: string[] = [];
  for (const [k, rv] of rm) {
    const tv = tm.get(k);
    if (!tv || stable(rv) !== stable(tv)) {
      onlyInRust.push(
        `${k} => ${JSON.stringify(rv)} (ts: ${JSON.stringify(tv)})`,
      );
    }
  }
  for (const [k, tv] of tm) {
    if (!rm.has(k)) {
      onlyInTs.push(`${k} => ${JSON.stringify(tv)}`);
    }
  }
  return {onlyInRust, onlyInTs};
}

// --- fixture translation (agentic gen.mjs fixture → replica + client schema) --

type FixtureColType = string; // e.g. 'string' | 'number' | 'boolean' | 'json' | 'number|null'
type FixtureTable = {
  columns: Record<string, FixtureColType>;
  primaryKey: string[];
  replicaPrimaryKey?: string[];
  rows: Record<string, unknown>[];
};
export type Fixture = {
  tables: Record<string, FixtureTable>;
  ast: unknown;
  pushes?: Array<{
    type: 'add' | 'remove' | 'edit';
    table: string;
    row: Record<string, unknown>;
    oldRow?: Record<string, unknown>;
  }>;
};

/** base type of a fixture column, e.g. 'number' from 'number|null'. */
function baseType(colType: FixtureColType): string {
  const parts = colType.split('|');
  return parts.find(p => p !== 'null') ?? 'string';
}
function isNullable(colType: FixtureColType): boolean {
  return colType.split('|').includes('null');
}

/** ZQL base type -> lite/pg column type token (must map back to the same
 * ValueType via pg-data-type, else checkClientSchema rejects the client type). */
function litePgType(base: string): string {
  switch (base) {
    case 'number':
      return 'int';
    case 'boolean':
      return 'bool';
    case 'json':
      return 'json';
    default:
      return 'text';
  }
}

/** SQLite-storable value for a fixture value + its base type. */
function sqlValue(v: unknown, base: string): unknown {
  if (v === null || v === undefined) {
    return null;
  }
  switch (base) {
    case 'number':
      return Number(v);
    case 'boolean':
      return v ? 1 : 0;
    case 'json':
      return JSON.stringify(v);
    default:
      return String(v);
  }
}

export class UntranslatableFixture extends Error {}

/**
 * DDL + seed DML for the wal2 replica. Columns that participate in ANY primary
 * key (client or replica) get the `|NOT_NULL` lite marker so they qualify as
 * potential primary keys (see lite-tables notNullColumns / client-schema
 * checkClientSchema). The replica PRIMARY KEY is the replicaPrimaryKey; a
 * divergent client primaryKey gets its own non-null UNIQUE INDEX.
 */
export function fixtureReplicaDDL(
  tables: Record<string, FixtureTable>,
  baseVersion: string,
): string {
  const stmts: string[] = [];
  for (const [name, spec] of Object.entries(tables)) {
    const clientPK = spec.primaryKey;
    const replicaPK = spec.replicaPrimaryKey ?? spec.primaryKey;
    if (replicaPK.length === 0) {
      throw new UntranslatableFixture(
        `table ${name} has no replica primary key`,
      );
    }
    const pkCols = new Set([...clientPK, ...replicaPK]);
    const colDefs = Object.entries(spec.columns).map(([col, type]) => {
      const notNull = pkCols.has(col);
      // A PK column must be non-null AND non-nullable in the fixture.
      if (notNull && isNullable(type)) {
        throw new UntranslatableFixture(
          `table ${name} pk column ${col} is nullable`,
        );
      }
      const lite = `${litePgType(baseType(type))}${notNull ? '|NOT_NULL' : ''}`;
      return `"${col}" "${lite}"`;
    });
    colDefs.push(`_0_version "text|NOT_NULL"`);
    colDefs.push(`PRIMARY KEY (${replicaPK.map(c => `"${c}"`).join(', ')})`);
    stmts.push(`CREATE TABLE "${name}" (${colDefs.join(', ')});`);
    if (stable(clientPK) !== stable(replicaPK)) {
      stmts.push(
        `CREATE UNIQUE INDEX "${name}__client_pk" ON "${name}" ` +
          `(${clientPK.map(c => `"${c}"`).join(', ')});`,
      );
    }
    const cols = Object.keys(spec.columns);
    for (const row of spec.rows ?? []) {
      const vals = cols.map(c =>
        sqlLiteral(sqlValue(row[c], baseType(spec.columns[c]))),
      );
      vals.push(sqlLiteral(baseVersion));
      stmts.push(
        `INSERT INTO "${name}" (${cols
          .map(c => `"${c}"`)
          .concat('"_0_version"')
          .join(', ')}) VALUES (${vals.join(', ')});`,
      );
    }
  }
  return stmts.join('\n');
}

/** SQL applying fixture pushes as replica writes stamped at `version`. */
export function fixturePushesDML(
  tables: Record<string, FixtureTable>,
  pushes: Fixture['pushes'],
  version: string,
): string {
  const stmts: string[] = [];
  let position = 0;
  const log = (table: string, op: 's' | 'd', key: Record<string, unknown>) => {
    stmts.push(
      `INSERT OR REPLACE INTO "_zero.changeLog2" ` +
        `(stateVersion, pos, "table", rowKey, op) VALUES (` +
        `${sqlLiteral(version)}, ${position++}, ${sqlLiteral(table)}, ` +
        `JSON(${sqlLiteral(JSON.stringify(key))}), ${sqlLiteral(op)});`,
    );
  };
  for (const push of pushes ?? []) {
    const spec = tables[push.table];
    if (!spec) {
      continue;
    }
    const cols = Object.keys(spec.columns);
    const replicaPK = spec.replicaPrimaryKey ?? spec.primaryKey;
    const whereFor = (r: Record<string, unknown>) =>
      replicaPK
        .map(
          c =>
            `"${c}" = ${sqlLiteral(sqlValue(r[c], baseType(spec.columns[c])))}`,
        )
        .join(' AND ');
    const keyFor = (r: Record<string, unknown>) =>
      Object.fromEntries(replicaPK.map(column => [column, r[column]]));
    if (push.type === 'add') {
      const vals = cols.map(c =>
        sqlLiteral(sqlValue(push.row[c], baseType(spec.columns[c]))),
      );
      vals.push(sqlLiteral(version));
      stmts.push(
        `INSERT INTO "${push.table}" (${cols
          .map(c => `"${c}"`)
          .concat('"_0_version"')
          .join(', ')}) VALUES (${vals.join(', ')});`,
      );
      log(push.table, 's', keyFor(push.row));
    } else if (push.type === 'remove') {
      stmts.push(`DELETE FROM "${push.table}" WHERE ${whereFor(push.row)};`);
      log(push.table, 'd', keyFor(push.row));
    } else if (push.type === 'edit') {
      const set = cols
        .map(
          c =>
            `"${c}" = ${sqlLiteral(sqlValue(push.row[c], baseType(spec.columns[c])))}`,
        )
        .concat(`"_0_version" = ${sqlLiteral(version)}`)
        .join(', ');
      stmts.push(
        `UPDATE "${push.table}" SET ${set} WHERE ${whereFor(push.oldRow ?? push.row)};`,
      );
      const oldKey = keyFor(push.oldRow ?? push.row);
      const newKey = keyFor(push.row);
      if (stable(oldKey) !== stable(newKey)) {
        log(push.table, 'd', oldKey);
      }
      log(push.table, 's', newKey);
    }
  }
  return stmts.join('\n');
}

function sqlLiteral(v: unknown): string {
  if (v === null || v === undefined) {
    return 'NULL';
  }
  if (typeof v === 'number') {
    return String(v);
  }
  return `'${String(v).replace(/'/g, "''")}'`;
}

/** Build the client schema (createSchema) from the fixture tables. */
export function fixtureClientSchema(tables: Record<string, FixtureTable>) {
  const built = Object.entries(tables).map(([name, spec]) => {
    const cols: Record<string, ReturnType<typeof string>> = {};
    for (const [col, type] of Object.entries(spec.columns)) {
      const base = baseType(type);
      let b:
        | ReturnType<typeof string>
        | ReturnType<typeof number>
        | ReturnType<typeof boolean>
        | ReturnType<typeof json>;
      switch (base) {
        case 'number':
          b = number();
          break;
        case 'boolean':
          b = boolean();
          break;
        case 'json':
          b = json();
          break;
        default:
          b = string();
      }
      cols[col] = (isNullable(type) ? (b as any).optional() : b) as any;
    }
    return table(name)
      .columns(cols)
      .primaryKey(...(spec.primaryKey as [string, ...string[]]));
  });
  return createSchema({tables: built as any});
}
