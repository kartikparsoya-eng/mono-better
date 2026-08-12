import {describe, expect, test, vi} from 'vitest';
import type {PushBody} from '../../../zero-protocol/src/push.ts';
import {
  buildMutateHeaders,
  buildMutateURL,
  postDirectPush,
} from './direct-pusher.ts';

const server = {appID: 'myapp', shardNum: 2};

describe('buildMutateURL', () => {
  test('appends schema + appID, matching zero-cache fetchFromAPIServer', () => {
    expect(buildMutateURL('https://api.example.com/api/push', server)).toBe(
      'https://api.example.com/api/push?schema=myapp_2&appID=myapp',
    );
  });

  test('preserves existing query params', () => {
    expect(buildMutateURL('https://api.example.com/push?foo=bar', server)).toBe(
      'https://api.example.com/push?foo=bar&schema=myapp_2&appID=myapp',
    );
  });

  test('resolves a relative URL against the app origin base', () => {
    expect(buildMutateURL('/api/push', server, 'https://app.example.com')).toBe(
      'https://app.example.com/api/push?schema=myapp_2&appID=myapp',
    );
  });

  test('rejects reserved query params', () => {
    expect(() => buildMutateURL('https://x/push?schema=evil', server)).toThrow(
      /reserved query param "schema"/,
    );
    expect(() => buildMutateURL('https://x/push?appID=evil', server)).toThrow(
      /reserved query param "appID"/,
    );
  });
});

describe('buildMutateHeaders', () => {
  test('sets JSON content type + bearer token + custom headers', () => {
    expect(buildMutateHeaders('tok', {'X-Custom': 'v'})).toEqual({
      'Content-Type': 'application/json',
      'X-Custom': 'v',
      'Authorization': 'Bearer tok',
    });
  });

  test('omits Authorization when there is no auth', () => {
    expect(buildMutateHeaders(undefined, undefined)).toEqual({
      'Content-Type': 'application/json',
    });
  });
});

describe('postDirectPush', () => {
  const body: PushBody = {
    clientGroupID: 'cg1',
    mutations: [],
    pushVersion: 1,
    timestamp: 123,
    requestID: 'req1',
  };

  test('POSTs the body and returns status + parsed JSON', async () => {
    const fetchFn = vi.fn((url: string, init: RequestInit) => {
      expect(url).toContain('schema=myapp_2');
      expect(init.method).toBe('POST');
      expect(JSON.parse(init.body as string)).toEqual(body);
      return Promise.resolve(
        new Response('{"kind":"MutateResponse","mutations":[]}', {
          status: 200,
        }),
      );
    }) as unknown as typeof fetch;

    const res = await postDirectPush(
      buildMutateURL('https://x/push', server),
      buildMutateHeaders('tok', undefined),
      body,
      fetchFn,
    );
    expect(res.httpStatusCode).toBe(200);
    expect(res.errorMessage).toBe('');
    expect(res.body).toEqual({kind: 'MutateResponse', mutations: []});
  });

  test('reports a non-OK status without throwing', async () => {
    const fetchFn = vi.fn(() =>
      Promise.resolve(new Response('nope', {status: 401})),
    ) as unknown as typeof fetch;
    const res = await postDirectPush('https://x/push', {}, body, fetchFn);
    expect(res.httpStatusCode).toBe(401);
    expect(res.errorMessage).toBe('HTTP 401');
  });

  test('reports a transport failure as status 0', async () => {
    const fetchFn = vi.fn(() =>
      Promise.reject(new Error('network down')),
    ) as unknown as typeof fetch;
    const res = await postDirectPush('https://x/push', {}, body, fetchFn);
    expect(res.httpStatusCode).toBe(0);
    expect(res.errorMessage).toBe('network down');
    expect(res.body).toBeUndefined();
  });
});
