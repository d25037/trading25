import { afterAll, beforeEach, describe, expect, it, mock } from 'bun:test';

const mockCalculateROE = mock();

mock.module('../../services/roe-data', () => ({
  ROEDataService: class {
    calculateROE = mockCalculateROE;
  },
}));
let roeApp: typeof import('../analytics/roe').default;

describe('ROE Routes', () => {
  beforeEach(async () => {
    mockCalculateROE.mockReset();
    roeApp = (await import('../analytics/roe')).default;
  });

  afterAll(() => {
    mock.restore();
  });

  it('returns 400 when required params are missing', async () => {
    const res = await roeApp.request('/api/analytics/roe');

    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string; message: string };
    expect(body.error).toBe('Bad Request');
    expect(body.message).toContain('Either code or date parameter is required');
  });

  it('returns 500 when service throws', async () => {
    mockCalculateROE.mockRejectedValue(new Error('DB failure'));

    const res = await roeApp.request('/api/analytics/roe?code=7203');

    expect(res.status).toBe(500);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe('Internal Server Error');
  });

  it('returns ROE data and passes parsed query to service', async () => {
    mockCalculateROE.mockResolvedValue({
      results: [
        {
          roe: 12.5,
          netProfit: 1000,
          equity: 8000,
          metadata: {
            code: '7203',
            periodType: 'FY',
            periodEnd: '2024-03-31',
            isConsolidated: true,
            accountingStandard: 'IFRS',
          },
        },
      ],
      summary: { averageROE: 12.5, maxROE: 12.5, minROE: 12.5, totalCompanies: 1 },
      lastUpdated: new Date().toISOString(),
    });

    const res = await roeApp.request(
      '/api/analytics/roe?code=7203&annualize=false&preferConsolidated=false&minEquity=2000&sortBy=code&limit=10'
    );

    expect(res.status).toBe(200);
    expect(mockCalculateROE).toHaveBeenCalledWith({
      code: '7203',
      date: undefined,
      annualize: false,
      preferConsolidated: false,
      minEquity: 2000,
      sortBy: 'code',
      limit: 10,
    });
  });
});
