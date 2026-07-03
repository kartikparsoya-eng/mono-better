// Lifecycle regressions for SidecarManager (scale review):
//
//  - Finding 10 (zombie spawn): #handleRestartTrigger armed an ANONYMOUS
//    setTimeout for the delayed respawn. A stop() landing during
//    'restarting' could not cancel it, so the timer fired after stop() and
//    #spawn'd a fresh sidecar NOBODY owned — a zombie process holding the
//    socket, and a manager resurrected to 'running' after its owner
//    stopped it. The timer is now stored (cleared in stop()) and re-checks
//    state at fire time.
//
//  - Finding 11 (memory-limit env): sanitizeGoMemLimitEnv strips malformed
//    GO_IVM_GOMEMLIMIT / GOMEMLIMIT values with a loud log so the
//    per-worker percent fallback applies (Go-side, a malformed
//    GO_IVM_GOMEMLIMIT also disabled every fallback — fixed in go-ivm
//    tuneRuntime; a malformed GOMEMLIMIT fatals the Go runtime at dlopen).

import {createServer, type Server, type Socket as NetSocket} from 'node:net';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Packr, Unpackr} from 'msgpackr';
import {afterEach, describe, expect, test} from 'vitest';
import {SidecarManager, sanitizeGoMemLimitEnv} from './sidecar-manager.ts';

// Match the client's wire codec (go-ivm-client.ts): useRecords:false —
// its Unpackr cannot decode msgpackr record extensions.
const packr = new Packr({useRecords: false});
const unpackr = new Unpackr({useRecords: false, mapsAsObjects: true});

// Minimal length-prefixed msgpack RPC server speaking just enough of the
// sidecar protocol for the manager's handshake (ping + version).
function fakeSidecarServer(sockPath: string): {
  server: Server;
  conns: NetSocket[];
  connectionCount: () => number;
  listening: Promise<void>;
} {
  const conns: NetSocket[] = [];
  let count = 0;
  const server = createServer(conn => {
    count++;
    conns.push(conn);
    let buf = Buffer.alloc(0);
    conn.on('data', chunk => {
      buf = Buffer.concat([buf, chunk]);
      while (buf.length >= 4) {
        const len = buf.readUInt32BE(0);
        if (buf.length < 4 + len) break;
        const payload = buf.subarray(4, 4 + len);
        buf = buf.subarray(4 + len);
        const req = unpackr.unpack(payload) as {id: unknown; method: string};
        let result: unknown;
        if (req.method === 'ping') result = 'pong';
        else if (req.method === 'version') {
          result = {version: 'fake', protocolRev: 9};
        } else result = null;
        const respPayload = packr.pack({jsonrpc: '2.0', id: req.id, result});
        const frame = Buffer.allocUnsafe(4 + respPayload.length);
        frame.writeUInt32BE(respPayload.length, 0);
        respPayload.copy(frame, 4);
        conn.write(frame);
      }
    });
    conn.on('error', () => {});
  });
  const listening = new Promise<void>(r => server.listen(sockPath, () => r()));
  return {server, conns, connectionCount: () => count, listening};
}

describe('stop() during restarting (finding 10)', () => {
  let manager: SidecarManager | null = null;
  let server: Server | null = null;

  afterEach(async () => {
    await manager?.stop().catch(() => {});
    await new Promise<void>(r => (server ? server.close(() => r()) : r()));
    manager = null;
    server = null;
  });

  test('a restart scheduled before stop() must not spawn after it', async () => {
    const sockPath = join(tmpdir(), `goivm-zombie-${process.pid}-${Date.now()}.sock`);
    const fake = fakeSidecarServer(sockPath);
    server = fake.server;
    await fake.listening;

    manager = new SidecarManager({
      binaryPath: '/nonexistent-not-used',
      socketPath: sockPath,
      externallyManaged: true, // no child process; connects to our fake
      restartDelayMs: 300,
      verbose: false,
      logger: () => {},
      fatalExit: () => {},
    });
    await manager.start();
    expect(manager.status).toBe('running');
    // start() may probe the socket before the real client connect — take
    // the post-start count as the baseline; the zombie check below is
    // about NEW connections after stop().
    const baseline = fake.connectionCount();

    // Kill the connection server-side; the 2s health tick notices the dead
    // socket and enters 'restarting' (respawn timer armed: 300ms).
    for (const c of fake.conns) c.destroy();
    const deadline = Date.now() + 5_000;
    while (manager.status !== 'restarting' && Date.now() < deadline) {
      await new Promise(r => setTimeout(r, 5));
    }
    expect(manager.status).toBe('restarting');

    // stop() lands INSIDE the respawn-delay window.
    await manager.stop();
    expect(manager.status).toBe('stopped');

    // Observe past the timer horizon: pre-fix the anonymous timer fired
    // anyway, #spawn reconnected (new connections appeared) and the manager
    // resurrected itself to 'running' — the zombie.
    await new Promise(r => setTimeout(r, 800));
    expect(fake.connectionCount()).toBe(baseline);
    expect(manager.status).toBe('stopped');
  });
});

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
    const env: Record<string, string | undefined> = {GO_IVM_GOMEMLIMIT: '4G!garbage'};
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
