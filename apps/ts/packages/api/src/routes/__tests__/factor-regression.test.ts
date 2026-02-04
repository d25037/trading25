import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockAnalyzeStock = mock();

mock.module('../../services/factor-regression-service', () => ({
  FactorRegressionService: class {
    analyzeStock = mockAnalyzeStock;
    close = () => {};
  },
}));

import factorRegressionApp from '../analytics/factor-regression';

describe('Factor Regression Routes', () => {
  beforeEach(() => {
    mockAnalyzeStock.mockReset();
  });

  it('returns factor regression results', async () => {
    mockAnalyzeStock.mockResolvedValue({
      symbol: '7203',
      lookbackDays: 250,
      results: [],
      lastUpdated: new Date().toISOString(),
    });

    const res = await factorRegressionApp.request('/api/analytics/factor-regression/7203?lookbackDays=250');

    expect(res.status).toBe(200);
    expect(mockAnalyzeStock).toHaveBeenCalledWith({
      symbol: '7203',
      lookbackDays: 250,
    });
  });

  it('maps insufficient data errors to 422', async () => {
    mockAnalyzeStock.mockRejectedValue(new Error('Insufficient data for regression'));

    const res = await factorRegressionApp.request('/api/analytics/factor-regression/7203?lookbackDays=250');

    expect(res.status).toBe(422);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe('Unprocessable Entity');
  });
});
