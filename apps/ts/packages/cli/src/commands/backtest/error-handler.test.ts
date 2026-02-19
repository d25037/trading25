import { beforeEach, describe, expect, it, mock } from 'bun:test';

class MockBacktestApiError extends Error {
  readonly status: number;
  readonly statusText: string;

  constructor(status: number, statusText: string, message: string) {
    super(message);
    this.name = 'BacktestApiError';
    this.status = status;
    this.statusText = statusText;
  }
}

mock.module('@trading25/clients-ts/backtest', () => ({
  BacktestApiError: MockBacktestApiError,
}));

import { handleBacktestError } from './error-handler.js';

type LoggerCtx = {
  logs: string[];
  log: (message: string) => void;
};

function createCtx(): LoggerCtx {
  const logs: string[] = [];
  return {
    logs,
    log: (message: string) => {
      logs.push(message);
    },
  };
}

describe('backtest error handler', () => {
  beforeEach(() => {
    mock.restore();
  });

  it('prints not found message for 404 BacktestApiError', () => {
    const ctx = createCtx();

    handleBacktestError(ctx, new MockBacktestApiError(404, 'Not Found', 'strategy missing'));

    expect(ctx.logs).toHaveLength(1);
    expect(ctx.logs[0]).toContain('Error: Not found - strategy missing');
  });

  it('prints generic API error for non-404 BacktestApiError', () => {
    const ctx = createCtx();

    handleBacktestError(ctx, new MockBacktestApiError(500, 'Internal Server Error', 'crashed'));

    expect(ctx.logs).toHaveLength(1);
    expect(ctx.logs[0]).toContain('API Error (500): crashed');
  });

  it('prints connectivity hint for connection failures', () => {
    const ctx = createCtx();

    handleBacktestError(ctx, new Error('fetch failed: ECONNREFUSED'));

    expect(ctx.logs).toHaveLength(2);
    expect(ctx.logs[0]).toContain('Error: Cannot connect to bt server');
    expect(ctx.logs[1]).toContain('Make sure bt server is running: uv run bt server --port 3002');
  });

  it('prints plain message for regular errors', () => {
    const ctx = createCtx();

    handleBacktestError(ctx, new Error('unexpected failure'));

    expect(ctx.logs).toHaveLength(1);
    expect(ctx.logs[0]).toContain('Error: unexpected failure');
  });

  it('ignores non-error values', () => {
    const ctx = createCtx();

    handleBacktestError(ctx, { kind: 'unknown' });

    expect(ctx.logs).toEqual([]);
  });
});
