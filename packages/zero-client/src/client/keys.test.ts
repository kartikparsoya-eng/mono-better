import fc from 'fast-check';
import {expect, test} from 'vitest';
import type {
  PrimaryKey,
  PrimaryKeyValueRecord,
} from '../../../zero-protocol/src/primary-key.ts';
import {
  toPrimaryKeyString as toPrimaryKeyStringImpl,
  toMutationResponseKey,
} from './keys.ts';

test('toPrimaryKeyString', () => {
  function toPrimaryKeyString(
    tableName: string,
    primaryKey: PrimaryKey,
    id: PrimaryKeyValueRecord,
  ) {
    return toPrimaryKeyStringImpl(tableName, primaryKey, id);
  }

  expect(
    toPrimaryKeyString('issue', ['id'], {id: 'issue1'}),
  ).toMatchInlineSnapshot(`"e/issue/issue1"`);

  expect(
    toPrimaryKeyString('issue_label', ['issueID', 'labelID'], {
      issueID: 'issue1',
      labelID: 'label1',
    }),
  ).toMatchInlineSnapshot(
    `"e/issue_label/15328927053344014787296837153592575662"`,
  );
  expect(
    toPrimaryKeyString('issue_label', ['issueID', 'labelID'], {
      labelID: 'label1',
      issueID: 'issue1',
    }),
  ).toMatchInlineSnapshot(
    `"e/issue_label/15328927053344014787296837153592575662"`,
  );

  // Order of the primary key fields matter.
  expect(
    toPrimaryKeyString('issue_label', ['labelID', 'issueID'], {
      issueID: 'issue1',
      labelID: 'label1',
    }),
  ).toMatchInlineSnapshot(
    `"e/issue_label/178908081844397207787976631973495261027"`,
  );

  // Extra columns are ignored
  expect(
    toPrimaryKeyString('issue_label', ['issueID', 'labelID'], {
      labelID: 'label1',
      issueID: 'issue1',
      more: 'data',
      ignore: 'bananas',
      me: true,
    }),
  ).toMatchInlineSnapshot(
    `"e/issue_label/15328927053344014787296837153592575662"`,
  );

  // Numeric value in the primary key.
  expect(
    toPrimaryKeyString('issue_label', ['id'], {
      id: Math.PI,
    }),
  ).toMatchInlineSnapshot(`"e/issue_label/3.141592653589793"`);

  // 1 is same as '1' but that's okay because the schema should not allow
  // incorrect types at a higher level.
  expect(
    toPrimaryKeyString('issue_label', ['id'], {
      id: 1,
    }),
  ).toMatchInlineSnapshot(`"e/issue_label/1"`);
  expect(
    toPrimaryKeyString('issue_label', ['id'], {
      id: '1',
    }),
  ).toMatchInlineSnapshot(`"e/issue_label/1"`);

  // Boolean value in the primary key.
  expect(
    toPrimaryKeyString('issue_label', ['id'], {
      id: true,
    }),
  ).toMatchInlineSnapshot(`"e/issue_label/true"`);
  expect(
    toPrimaryKeyString('issue_label', ['id'], {
      id: false,
    }),
  ).toMatchInlineSnapshot(`"e/issue_label/false"`);

  // true is same as 'true' but that's okay because the schema should not allow
  // incorrect types at a higher level.
  expect(
    toPrimaryKeyString('issue_label', ['id'], {
      id: 'true',
    }),
  ).toMatchInlineSnapshot(`"e/issue_label/true"`);
});

test('no clashes - single pk', () => {
  fc.assert(
    fc.property(
      fc.oneof(
        fc.tuple(fc.string(), fc.string()),
        fc.tuple(fc.double(), fc.double()),
        fc.tuple(fc.boolean(), fc.boolean()),
      ),
      ([a, b]) => {
        const keyA = toPrimaryKeyStringImpl('issue', ['id'], {id: a});
        const keyB = toPrimaryKeyStringImpl('issue', ['id'], {id: b});
        if (a === b) {
          expect(keyA).toBe(keyB);
        } else {
          expect(keyA).not.toBe(keyB);
        }
      },
    ),
  );
});

test('no clashes - multiple pk', () => {
  const primaryKey = ['id', 'name'] as const;
  fc.assert(
    fc.property(
      fc.tuple(
        fc.oneof(fc.string(), fc.double(), fc.boolean()),
        fc.oneof(fc.string(), fc.double(), fc.boolean()),
        fc.oneof(fc.string(), fc.double(), fc.boolean()),
        fc.oneof(fc.string(), fc.double(), fc.boolean()),
      ),
      ([a1, a2, b1, b2]) => {
        const keyA = toPrimaryKeyStringImpl('issue', primaryKey, {
          id: a1,
          name: a2,
        });
        const keyB = toPrimaryKeyStringImpl('issue', primaryKey, {
          id: b1,
          name: b2,
        });
        if (a1 === b1 && a2 === b2) {
          expect(keyA).toBe(keyB);
        } else {
          expect(keyA).not.toBe(keyB);
        }
      },
    ),
  );
});

// Root-cause pin: toPrimaryKeyString is (intentionally) strict. A rowKey that
// is missing a primary-key column throws `Expected string, number or boolean.
// Got undefined`. This is the exact prod crash that a poisoned/malformed
// historical CVR rowKey triggered inside rowsPatchOpToReplicachePatchOp. The
// defensive guard lives in the poke-handler (which can skip the op); this test
// documents WHY that guard is needed and that the function itself stays strict.
test('toPrimaryKeyString throws when a primary-key column is missing (single-col)', () => {
  expect(() =>
    // channel_participants client PK is ['id'] but a poisoned historical row
    // was keyed by {channelId,userId} with no id.
    toPrimaryKeyStringImpl('channel_participants', ['id'], {
      channelId: 'c1',
      userId: 'u1',
    } as unknown as PrimaryKeyValueRecord),
  ).toThrow('Expected string, number or boolean. Got undefined');
});

test('toPrimaryKeyString throws when a primary-key column is missing (compound)', () => {
  expect(() =>
    toPrimaryKeyStringImpl('issue_label', ['issueID', 'labelID'], {
      issueID: 'issue1',
      // labelID missing
    } as unknown as PrimaryKeyValueRecord),
  ).toThrow('Expected string, number or boolean. Got undefined');
});

test('toMutationResponseKey', () => {
  expect(
    toMutationResponseKey({
      clientID: 'cid',
      id: 1,
    }),
  ).toBe('m/cid/1');
  expect(
    toMutationResponseKey({
      clientID: 'cid',
      id: 2,
    }),
  ).toBe('m/cid/2');
  expect(
    toMutationResponseKey({
      clientID: 'cid2',
      id: 1,
    }),
  ).toBe('m/cid2/1');
});
