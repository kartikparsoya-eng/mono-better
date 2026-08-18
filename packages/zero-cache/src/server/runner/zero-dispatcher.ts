import type {LogContext} from '@rocicorp/logger';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import {handleHeapzRequest} from '../../services/heapz.ts';
import {HttpService, type Options} from '../../services/http-service.ts';
import {handleStatzRequest} from '../../services/statz.ts';
import type {IncomingMessageSubset} from '../../types/http.ts';
import type {Worker} from '../../types/processes.ts';
import {
  installWebSocketHandoff,
  type HandoffSpec,
} from '../../types/websocket-handoff.ts';

export class ZeroDispatcher extends HttpService {
  readonly id = 'zero-dispatcher';
  readonly #getWorker: () => Promise<Worker>;
  readonly #onStart: (() => void) | undefined;

  constructor(
    config: NormalizedZeroConfig,
    lc: LogContext,
    opts: Options,
    getWorker: () => Promise<Worker>,
    onStart?: () => void,
  ) {
    super(`zero-dispatcher`, lc, opts, fastify => {
      fastify.get('/statz', (req, res) =>
        handleStatzRequest(lc, config, req, res),
      );
      fastify.get('/heapz', (req, res) =>
        handleHeapzRequest(lc, config, req, res),
      );
      // Orchestrator probe contract. The dispatcher only begins listening
      // after `allWorkersReady()`, so a 200 from /healthz implies the process
      // is up; /readyz additionally awaits a live sync worker (which also
      // boots the cache under --lazy-startup), so it flips 200 only when a
      // connection could actually be served.
      fastify.get('/healthz', (_req, res) => res.send('OK'));
      fastify.get('/readyz', (_req, res) =>
        getWorker().then(
          () => res.send('OK'),
          err => res.code(503).send(String(err)),
        ),
      );
      installWebSocketHandoff(lc, this.#handoff, fastify.server);
    });
    this.#getWorker = getWorker;
    this.#onStart = onStart;
  }

  protected override _onStart() {
    this.#onStart?.();
  }

  readonly #handoff = (
    _req: IncomingMessageSubset,
    dispatch: (h: HandoffSpec<string>) => void,
    onError: (error: unknown) => void,
  ) => {
    void this.#getWorker().then(
      sender => dispatch({payload: 'unused', sender}),
      onError,
    );
  };
}
