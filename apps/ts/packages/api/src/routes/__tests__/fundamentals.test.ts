import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockComputeFundamentals = mock();

mock.module('@trading25/shared/clients/backtest', () => ({
  BacktestClient: class {
    computeFundamentals = mockComputeFundamentals;
  },
  BacktestApiError: class BacktestApiError extends Error {
    constructor(
      public status: number,
      public statusText: string,
      message: string
    ) {
      super(message);
      this.name = 'BacktestApiError';
    }
  },
}));

let fundamentalsApp: typeof import('../analytics/fundamentals').default;

describe('Fundamentals Routes', () => {
  beforeEach(async () => {
    mockComputeFundamentals.mockReset();
    fundamentalsApp = (await import('../analytics/fundamentals')).default;
  });

  it('returns 404 when no data is found', async () => {
    mockComputeFundamentals.mockResolvedValue({ data: [], lastUpdated: new Date().toISOString() });

    const res = await fundamentalsApp.request('/api/analytics/fundamentals/7203');

    expect(res.status).toBe(404);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe('Not Found');
  });

  it('returns 500 when service throws', async () => {
    mockComputeFundamentals.mockRejectedValue(new Error('apps/bt/ API error'));

    const res = await fundamentalsApp.request('/api/analytics/fundamentals/7203');

    expect(res.status).toBe(500);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe('Internal Server Error');
  });

  it('returns fundamentals data and passes parsed query to service', async () => {
    mockComputeFundamentals.mockResolvedValue({
      symbol: '7203',
      data: [{ date: '2024-03-31', roe: 12.5 }],
      lastUpdated: new Date().toISOString(),
    });

    const res = await fundamentalsApp.request(
      '/api/analytics/fundamentals/7203?from=2023-01-01&to=2024-12-31&periodType=FY&preferConsolidated=false'
    );

    expect(res.status).toBe(200);
    expect(mockComputeFundamentals).toHaveBeenCalledWith({
      symbol: '7203',
      from_date: '2023-01-01',
      to_date: '2024-12-31',
      period_type: 'FY',
      prefer_consolidated: false,
    });
  });
});
