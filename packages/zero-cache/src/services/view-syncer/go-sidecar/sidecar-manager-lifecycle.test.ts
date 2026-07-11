// Env-sanitization regressions for SidecarManager (scale review):
//
//  - Finding 11 (memory-limit env): sanitizeGoMemLimitEnv strips malformed
//    GO_IVM_GOMEMLIMIT / GOMEMLIMIT values with a loud log so the
//    per-worker percent fallback applies (Go-side, a malformed
//    GO_IVM_GOMEMLIMIT also disabled every fallback — fixed in go-ivm
//    tuneRuntime; a malformed GOMEMLIMIT fatals the Go runtime at dlopen).

import {describe, expect, test} from 'vitest';
import {sanitizeGoMemLimitEnv} from './sidecar-manager.ts';

describe('sanitizeGoMemLimitEnv (finding 11)', () => {
  function run(env: Record<string, string | undefined>) {
    const logs: string[] = [];
    sanitizeGoMemLimitEnv(env, (_lvl, msg) => logs.push(msg));
    return logs;
  }

  test('valid values are preserved (no log)', () => {
    const env = {GO_IVM_GOMEMLIMIT: '1073741824', GOMEMLIMIT: '4GiB'};
    expect(run(env)).toEqual([]);
    expect(env.GO_IVM_GOMEMLIMIT).toBe('1073741824');
    expect(env.GOMEMLIMIT).toBe('4GiB');
    const off = {GOMEMLIMIT: 'off'};
    expect(run(off)).toEqual([]);
    expect(off.GOMEMLIMIT).toBe('off');
  });

  test('malformed GO_IVM_GOMEMLIMIT is deleted with a loud log', () => {
    // Pre-fix this value skipped the per-worker percent fallback in
    // #startNapi AND failed Go-side parsing — no memory ceiling at all.
    const env: Record<string, string | undefined> = {
      GO_IVM_GOMEMLIMIT: '4G!garbage',
    };
    const logs = run(env);
    expect(logs).toHaveLength(1);
    expect(logs[0]).toMatch(/invalid GO_IVM_GOMEMLIMIT/);
    expect(env.GO_IVM_GOMEMLIMIT).toBeUndefined();
  });

  test('malformed GOMEMLIMIT is deleted (it would FATAL the Go runtime at dlopen)', () => {
    const env: Record<string, string | undefined> = {GOMEMLIMIT: 'lots'};
    const logs = run(env);
    expect(logs).toHaveLength(1);
    expect(logs[0]).toMatch(/invalid GOMEMLIMIT/);
    expect(env.GOMEMLIMIT).toBeUndefined();
  });
});
