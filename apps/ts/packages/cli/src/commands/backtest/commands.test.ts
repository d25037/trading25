import { beforeEach, describe, expect, it, mock } from 'bun:test';

mock.module('chalk', () => {
  const identity = (text: string) => text;
  const bold = Object.assign((text: string) => text, {
    white: identity,
    blue: identity,
    green: identity,
    yellow: identity,
    red: identity,
    gray: identity,
    dim: identity,
    cyan: identity,
    magenta: identity,
  });
  return {
    default: {
      red: identity,
      green: identity,
      yellow: identity,
      blue: identity,
      cyan: Object.assign((text: string) => text, { bold: identity }),
      white: identity,
      gray: identity,
      dim: identity,
      magenta: identity,
      bold,
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
import { listCommand } from './list.js';
import { resultsCommand } from './results.js';
import { runCommand } from './run.js';
import { statusCommand } from './status.js';
import { validateCommand } from './validate.js';

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

describe('backtest commands', () => {
  beforeEach(() => {
    fetchMock.mockClear();
  });

  it('run command (no-wait) checks health and submits job', async () => {
    withResponses([
      jsonResponse({ status: 'ok', service: 'bt', version: '1.0.0' }),
      jsonResponse({ job_id: 'job-1', status: 'pending' }),
    ]);

    const ctx = createCtx({
      strategy: 'range_break_v5',
      wait: false,
      debug: false,
      btUrl: 'http://localhost:3002',
    });

    await runCommand.run(ctx as unknown as Parameters<typeof runCommand.run>[0]);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(String(fetchMock.mock.calls[0]?.[0] ?? '')).toContain('/api/health');
    expect(String(fetchMock.mock.calls[1]?.[0] ?? '')).toContain('/api/backtest/run');
    expect((fetchMock.mock.calls[1]?.[1] as RequestInit)?.method).toBe('POST');
    expect((fetchMock.mock.calls[1]?.[1] as RequestInit)?.body).toBe(JSON.stringify({ strategy_name: 'range_break_v5' }));
    expect(ctx.logs.some((line) => line.includes('Check status:'))).toBe(true);
  });

  it('run command (wait) completes end-to-end with polling and result output', async () => {
    withResponses([
      jsonResponse({ status: 'ok', service: 'bt', version: '1.0.0' }),
      jsonResponse({
        job_id: 'job-wait-1',
        status: 'pending',
        progress: 0,
        message: 'queued',
      }),
      jsonResponse({
        job_id: 'job-wait-1',
        status: 'completed',
        progress: 1,
        message: 'done',
        result: {
          total_return: 0.1234,
          sharpe_ratio: 1.25,
          sortino_ratio: 1.4,
          calmar_ratio: 0.9,
          max_drawdown: -0.056,
          win_rate: 0.62,
          trade_count: 15,
          html_path: '/tmp/job-wait-1.html',
        },
      }),
    ]);

    const ctx = createCtx({
      strategy: 'production/range_break_v5',
      wait: true,
      debug: false,
      btUrl: 'http://localhost:3002',
    });

    await runCommand.run(ctx as unknown as Parameters<typeof runCommand.run>[0]);

    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(String(fetchMock.mock.calls[0]?.[0] ?? '')).toContain('/api/health');
    expect(String(fetchMock.mock.calls[1]?.[0] ?? '')).toContain('/api/backtest/run');
    expect(String(fetchMock.mock.calls[2]?.[0] ?? '')).toContain('/api/backtest/jobs/job-wait-1');
    expect(ctx.logs.some((line) => line.includes('=== Results ==='))).toBe(true);
    expect(ctx.logs.some((line) => line.includes('Job ID: job-wait-1'))).toBe(true);
    expect(ctx.logs.some((line) => line.includes('Total Return:'))).toBe(true);
  });

  it('list command fetches strategies and applies category filter', async () => {
    withResponses([
      jsonResponse({
        strategies: [
          {
            name: 'production/range_break_v5',
            category: 'production',
            display_name: 'Range Break',
            description: null,
            last_modified: null,
          },
          {
            name: 'experimental/test_v1',
            category: 'experimental',
            display_name: null,
            description: null,
            last_modified: null,
          },
        ],
        total: 2,
      }),
    ]);

    const ctx = createCtx({
      category: 'production',
      btUrl: 'http://localhost:3002',
      format: 'table',
      debug: false,
    });

    await listCommand.run(ctx as unknown as Parameters<typeof listCommand.run>[0]);

    expect(String(fetchMock.mock.calls[0]?.[0] ?? '')).toContain('/api/strategies');
    expect(ctx.logs.some((line) => line.includes('Available Strategies (1 total)'))).toBe(true);
    expect(ctx.logs.some((line) => line.includes('range_break_v5'))).toBe(true);
  });

  it('results command without jobId lists jobs', async () => {
    withResponses([
      jsonResponse([
        {
          job_id: 'job-1',
          status: 'completed',
          progress: 1,
          message: null,
          created_at: '2026-02-01T00:00:00Z',
          started_at: '2026-02-01T00:00:01Z',
          completed_at: '2026-02-01T00:01:00Z',
          error: null,
          result: {
            total_return: 0.12,
            sharpe_ratio: 1.1,
            sortino_ratio: 1.2,
            calmar_ratio: 0.8,
            max_drawdown: -0.05,
            win_rate: 0.55,
            trade_count: 12,
            html_path: null,
          },
        },
      ]),
    ]);

    const ctx = createCtx({
      jobId: undefined,
      limit: '10',
      btUrl: 'http://localhost:3002',
      format: 'table',
      debug: false,
    });

    await resultsCommand.run(ctx as unknown as Parameters<typeof resultsCommand.run>[0]);

    expect(String(fetchMock.mock.calls[0]?.[0] ?? '')).toContain('/api/backtest/jobs?limit=10');
    expect(ctx.logs.some((line) => line.includes('Recent Backtest Jobs'))).toBe(true);
  });

  it('results command with jobId fetches job detail', async () => {
    withResponses([
      jsonResponse({
        job_id: 'job-42',
        status: 'completed',
        progress: 1,
        message: 'done',
        created_at: '2026-02-01T00:00:00Z',
        started_at: '2026-02-01T00:00:01Z',
        completed_at: '2026-02-01T00:01:00Z',
        error: null,
        result: {
          total_return: 0.2,
          sharpe_ratio: 1.3,
          sortino_ratio: 1.4,
          calmar_ratio: 1.0,
          max_drawdown: -0.04,
          win_rate: 0.6,
          trade_count: 20,
          html_path: '/tmp/report.html',
        },
      }),
    ]);

    const ctx = createCtx({
      jobId: 'job-42',
      limit: '10',
      btUrl: 'http://localhost:3002',
      format: 'table',
      debug: false,
    });

    await resultsCommand.run(ctx as unknown as Parameters<typeof resultsCommand.run>[0]);

    expect(String(fetchMock.mock.calls[0]?.[0] ?? '')).toContain('/api/backtest/jobs/job-42');
    expect(ctx.logs.some((line) => line.includes('Job Details'))).toBe(true);
    expect(ctx.logs.some((line) => line.includes('job-42'))).toBe(true);
  });

  it('status command fetches health status', async () => {
    withResponses([
      jsonResponse({
        status: 'ok',
        service: 'trading25-bt',
        version: '1.2.3',
      }),
    ]);

    const ctx = createCtx({
      btUrl: 'http://localhost:3002',
      debug: false,
    });

    await statusCommand.run(ctx as unknown as Parameters<typeof statusCommand.run>[0]);

    expect(String(fetchMock.mock.calls[0]?.[0] ?? '')).toContain('/api/health');
    expect(ctx.logs.some((line) => line.includes('Server Status'))).toBe(true);
    expect(ctx.logs.some((line) => line.includes('trading25-bt'))).toBe(true);
  });

  it('cancel command cancels job', async () => {
    withResponses([jsonResponse({ job_id: 'job-9', status: 'cancelled' })]);

    const ctx = createCtx({
      jobId: 'job-9',
      btUrl: 'http://localhost:3002',
      debug: false,
    });

    await cancelCommand.run(ctx as unknown as Parameters<typeof cancelCommand.run>[0]);

    expect(String(fetchMock.mock.calls[0]?.[0] ?? '')).toContain('/api/backtest/jobs/job-9/cancel');
    expect((fetchMock.mock.calls[0]?.[1] as RequestInit)?.method).toBe('POST');
    expect(ctx.logs.some((line) => line.includes('Job ID: job-9'))).toBe(true);
  });

  it('validate command validates strategy', async () => {
    withResponses([
      jsonResponse({
        valid: true,
        errors: [],
        warnings: [],
      }),
    ]);

    const ctx = createCtx({
      strategy: 'production/range_break_v5',
      btUrl: 'http://localhost:3002',
      debug: false,
    });

    await validateCommand.run(ctx as unknown as Parameters<typeof validateCommand.run>[0]);

    expect(String(fetchMock.mock.calls[0]?.[0] ?? '')).toContain('/api/strategies/production%2Frange_break_v5/validate');
    expect((fetchMock.mock.calls[0]?.[1] as RequestInit)?.method).toBe('POST');
    expect(ctx.logs.some((line) => line.includes('Strategy configuration is valid'))).toBe(true);
  });
});
