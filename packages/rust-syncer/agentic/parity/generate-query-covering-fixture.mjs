#!/usr/bin/env node
/**
 * Generates the TS-vs-Rust query-covering parity fixture.
 *
 * Drives the REAL TS `isQueryCoveredBy(covered, covering)` (the pure covered-query
 * detector in services/view-syncer/query-covering.ts) over a battery of AST pairs
 * lifted from query-covering.test.ts — self-cover, unfiltered⊇filtered, filter-
 * subset (both directions), IN / range implication, OR disjunct coverage, limit /
 * paging rules, recursive related-subquery coverage, NOT-EXISTS reversal, and the
 * EXISTS-flip invariant. The Rust `is_query_covered_by(&Value, &Value)` must return
 * the same bool for every pair.
 *
 * Usage:
 *   npx tsx packages/rust-syncer/agentic/parity/generate-query-covering-fixture.mjs \
 *     > packages/rust-syncer/agentic/parity/query-covering-fixture.json
 */

import {isQueryCoveredBy} from '../../../zero-cache/src/services/view-syncer/query-covering.ts';

// Builders mirrored from query-covering.test.ts.
const allIssues = {table: 'issues', orderBy: [['id', 'asc']]};
const allComments = {table: 'comments', orderBy: [['id', 'asc']]};
const where = condition => ({...allIssues, where: condition});
const eq = (column, value) => ({
  type: 'simple',
  left: {type: 'column', name: column},
  op: '=',
  right: {type: 'literal', value},
});
const gt = (column, value) => ({
  type: 'simple',
  left: {type: 'column', name: column},
  op: '>',
  right: {type: 'literal', value},
});
const and = (...conditions) => ({type: 'and', conditions});
const or = (...conditions) => ({type: 'or', conditions});
const inOp = (column, values) => ({
  type: 'simple',
  left: {type: 'column', name: column},
  op: 'IN',
  right: {type: 'literal', value: values},
});
const commentsRelated = subquery => ({
  system: 'client',
  correlation: {parentField: ['id'], childField: ['issueID']},
  subquery: {...subquery, alias: 'comments'},
});

const limitedOpen = {...where(eq('status', 'open')), limit: 10};
const pagedOpen = {...where(eq('status', 'open')), limit: 10, start: {row: {id: 'abc'}, exclusive: true}};
const commentsWithText = {...allComments, where: eq('text', 'hello')};
const notExists = sub => where({type: 'correlatedSubquery', op: 'NOT EXISTS', related: commentsRelated(sub)});
const exists = (sub, flip) =>
  where({type: 'correlatedSubquery', op: 'EXISTS', ...(flip ? {flip: true} : {}), related: commentsRelated(sub)});

// [desc, coveredAst, coveringAst]
const PAIRS = [
  ['same query covers itself', where(eq('id', '123')), where(eq('id', '123'))],
  ['unfiltered covers filtered', where(eq('id', '123')), allIssues],
  ['filtered does NOT cover unfiltered', allIssues, where(eq('id', '123'))],
  ['conjunction covered by subset', where(and(eq('status', 'open'), eq('owner', 'alice'))), where(eq('status', 'open'))],
  ['subset does NOT cover conjunction', where(eq('status', 'open')), where(and(eq('status', 'open'), eq('owner', 'alice')))],
  ['eq covered by IN', where(eq('id', '1')), where(inOp('id', ['1', '2']))],
  ['range implication >5 covered by >3', where(gt('priority', 5)), where(gt('priority', 3))],
  ['range NOT covered >3 by >5', where(gt('priority', 3)), where(gt('priority', 5))],
  ['disjunct covered by OR', where(eq('type', 'bug')), where(or(eq('type', 'bug'), eq('type', 'feature')))],
  ['OR NOT covered by disjunct', where(or(eq('type', 'bug'), eq('type', 'feature'))), where(eq('type', 'bug'))],
  ['unlimited covers limited+paged', pagedOpen, allIssues],
  ['same input larger limit covers', limitedOpen, {...where(eq('status', 'open')), limit: 20}],
  ['broader input same limit does NOT cover', limitedOpen, {...allIssues, limit: 10}],
  ['recursive related coverage', {...where(eq('status', 'open')), related: [commentsRelated(commentsWithText)]}, {...allIssues, related: [commentsRelated(allComments)]}],
  ['related NOT covered without the related', {...where(eq('status', 'open')), related: [commentsRelated(commentsWithText)]}, allIssues],
  ['NOT EXISTS reverses implication', notExists(allComments), notExists(commentsWithText)],
  ['NOT EXISTS reverse (other direction) does NOT cover', notExists(commentsWithText), notExists(allComments)],
  ['EXISTS flip invariant (unflipped by flipped)', exists(allComments, false), exists(allComments, true)],
  ['EXISTS flip invariant (flipped by unflipped)', exists(allComments, true), exists(allComments, false)],
  ['different tables do NOT cover', where(eq('id', '1')), allComments],
];

const cases = PAIRS.map(([desc, covered, covering]) => ({
  desc,
  covered,
  covering,
  covered_by: isQueryCoveredBy(covered, covering),
}));

process.stdout.write(JSON.stringify({cases}, null, 2) + '\n');
