import { beforeEach, describe, expect, it, mock } from 'bun:test';

mock.module('chalk', () => {
  const identity = (text: string) => text;
  return {
    default: {
      red: identity,
      green: identity,
      yellow: identity,
      blue: identity,
      cyan: identity,
      white: identity,
      gray: identity,
      dim: identity,
      bold: identity,
    },
  };
});

mock.module('ora', () => {
  class MockSpinner {
    text = '';
    start() {
      return this;
    }
    succeed() {
      return this;
    }
    fail() {
      return this;
    }
    warn() {
      return this;
    }
    stop() {
      return this;
    }
  }

  return {
    default: () => new MockSpinner(),
  };
});

import { cancelCommand } from './cancel.js';
import { resultsCommand } from './results.js';
import { runCommand } from './run.js';
import { statusCommand } from './status.js';

type CliCtx = {
  values: Record<string, unknown>;
  log: (message: string) => void;
};

const fetchMock = mock();
globalThis.fetch = fetchMock as unknown as typeof fetch;

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

function withResponses(responses: Response[]): void {
  let index = 0;
  fetchMock.mockImplementation(async () => {
    const response = responses[index];
    index += 1;
    if (!response) {
      throw new Error('Unexpected fetch call');
    }
    return response;
  });
}

function createCtx(values: Record<string, unknown>): CliCtx & { logs: string[] } {
  const logs: string[] = [];
  return {
    values,
    log: (message: string) => logs.push(message),
    logs,
  };
}

describe('attribution commands', () => {
  beforeEach(() => {
    fetchMock.mockClear();
  });

  it('run (no-wait/json) submits request and prints JSON', async () => {
    withResponses([jsonResponse({ job_id: 'attr-1', status: 'pending' })]);

    const ctx = createCtx({
      strategy: 'strategy.yml',
      wait: false,
      'shapley-top-n': '5',
      'shapley-permutations': '128',
      'random-seed': undefined,
      format: 'json',
      'bt-url': 'http://localhost:3002',
      debug: false,
    });

    await runCommand.run(ctx as unknown as Parameters<typeof runCommand.run>[0]);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const req = fetchMock.mock.calls[0]?.[1] as RequestInit;
    expect(req.method).toBe('POST');
    expect(req.body).toBe(
      JSON.stringify({
        strategy_name: 'strategy.yml',
        shapley_top_n: 5,
        shapley_permutations: 128,
        random_seed: null,
      })
    );
    expect(ctx.logs.some((line) => line.includes('"job_id": "attr-1"'))).toBe(true);
  });

  it('run (wait/table) polls and prints summary', async () => {
    withResponses([
      jsonResponse({ job_id: 'attr-2', status: 'pending' }),
      jsonResponse({ job_id: 'attr-2', status: 'running', progress: 0.5, message: 'running' }),
      jsonResponse({
        job_id: 'attr-2',
        status: 'completed',
        progress: 1,
        result_data: {
          baseline_metrics: { total_return: 0.2, sharpe_ratio: 1.1 },
          top_n_selection: {
            top_n_requested: 5,
            top_n_effective: 1,
            selected_signal_ids: ['entry.signal_a'],
            scores: [{ signal_id: 'entry.signal_a', score: 1 }],
          },
          shapley: { method: 'exact', sample_size: 2, error: null, evaluations: 2 },
          timing: { total_seconds: 1, baseline_seconds: 0.2, loo_seconds: 0.4, shapley_seconds: 0.4 },
          signals: [],
        },
      }),
    ]);

    const ctx = createCtx({
      strategy: 'strategy.yml',
      wait: true,
      'shapley-top-n': '5',
      'shapley-permutations': '128',
      'random-seed': undefined,
      format: 'table',
      'bt-url': 'http://localhost:3002',
      debug: false,
    });

    await runCommand.run(ctx as unknown as Parameters<typeof runCommand.run>[0]);

    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(ctx.logs.some((line) => line.includes('Signal Attribution Results'))).toBe(true);
  });

  it('status (json) prints payload', async () => {
    withResponses([
      jsonResponse({
        job_id: 'attr-1',
        status: 'completed',
        progress: 1,
        message: null,
        created_at: '2025-01-01T00:00:00Z',
        started_at: '2025-01-01T00:00:01Z',
        completed_at: '2025-01-01T00:00:02Z',
        error: null,
      }),
    ]);

    const ctx = createCtx({
      jobId: 'attr-1',
      'bt-url': 'http://localhost:3002',
      format: 'json',
      debug: false,
    });

    await statusCommand.run(ctx as unknown as Parameters<typeof statusCommand.run>[0]);

    expect(ctx.logs.some((line) => line.includes('"job_id": "attr-1"'))).toBe(true);
  });

  it('status (table) prints formatted fields', async () => {
    withResponses([
      jsonResponse({
        job_id: 'attr-3',
        status: 'completed',
        progress: 1,
        message: 'done',
        created_at: '2025-01-01T00:00:00Z',
        started_at: '2025-01-01T00:00:01Z',
        completed_at: '2025-01-01T00:00:02Z',
        error: 'none',
      }),
    ]);

    const ctx = createCtx({
      jobId: 'attr-3',
      'bt-url': 'http://localhost:3002',
      format: 'table',
      debug: false,
    });

    await statusCommand.run(ctx as unknown as Parameters<typeof statusCommand.run>[0]);

    expect(ctx.logs.some((line) => line.includes('Attribution Job Status'))).toBe(true);
    expect(ctx.logs.some((line) => line.includes('See results'))).toBe(true);
  });

  it('results (table) prints summary and signal rows', async () => {
    withResponses([
      jsonResponse({
        job_id: 'attr-1',
        strategy_name: 'strategy.yml',
        result: {
          baseline_metrics: { total_return: 0.2, sharpe_ratio: 1.1 },
          top_n_selection: {
            top_n_requested: 5,
            top_n_effective: 1,
            selected_signal_ids: ['entry.signal_a'],
            scores: [{ signal_id: 'entry.signal_a', score: 1 }],
          },
          shapley: { method: 'exact', sample_size: 2, error: null, evaluations: 2 },
          timing: { total_seconds: 1, baseline_seconds: 0.2, loo_seconds: 0.4, shapley_seconds: 0.4 },
          signals: [
            {
              signal_id: 'entry.signal_a',
              scope: 'entry',
              param_key: 'signal_a',
              signal_name: 'Signal A',
              loo: {
                status: 'ok',
                variant_metrics: { total_return: 0.1, sharpe_ratio: 1.0 },
                delta_total_return: 0.1,
                delta_sharpe_ratio: 0.1,
                error: null,
              },
              shapley: {
                status: 'ok',
                total_return: 0.1,
                sharpe_ratio: 0.05,
                method: 'exact',
                sample_size: 2,
                error: null,
              },
            },
          ],
        },
        created_at: '2025-01-01T00:00:00Z',
      }),
    ]);

    const ctx = createCtx({
      jobId: 'attr-1',
      'bt-url': 'http://localhost:3002',
      format: 'table',
      debug: false,
    });

    await resultsCommand.run(ctx as unknown as Parameters<typeof resultsCommand.run>[0]);

    expect(ctx.logs.some((line) => line.includes('Signal Attribution Result'))).toBe(true);
    expect(ctx.logs.some((line) => line.includes('entry.signal_a'))).toBe(true);
  });

  it('cancel succeeds for cancellable jobs', async () => {
    withResponses([jsonResponse({ job_id: 'attr-1', status: 'cancelled' })]);

    const ctx = createCtx({
      jobId: 'attr-1',
      'bt-url': 'http://localhost:3002',
      debug: false,
    });

    await cancelCommand.run(ctx as unknown as Parameters<typeof cancelCommand.run>[0]);

    expect(ctx.logs.some((line) => line.includes('Job ID: attr-1'))).toBe(true);
    expect(ctx.logs.some((line) => line.includes('Status: cancelled'))).toBe(true);
  });
});
