import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockAnalyzePortfolio = mock();

mock.module('../../services/portfolio-factor-regression-service', () => ({
  PortfolioFactorRegressionService: class {
    analyzePortfolio = mockAnalyzePortfolio;
    close = () => {};
  },
}));

import portfolioFactorRegressionApp from '../analytics/portfolio-factor-regression';

describe('Portfolio Factor Regression Routes', () => {
  beforeEach(() => {
    mockAnalyzePortfolio.mockReset();
  });

  it('returns regression results for valid portfolio', async () => {
    mockAnalyzePortfolio.mockResolvedValue({
      portfolioId: 1,
      marketRegression: { alpha: 0.001, beta: 1.05, rSquared: 0.85 },
      factorMatches: [],
    });

    const res = await portfolioFactorRegressionApp.request('/api/analytics/portfolio-factor-regression/1');
    expect(res.status).toBe(200);
    expect(mockAnalyzePortfolio).toHaveBeenCalled();
  });

  it('returns 500 for generic service errors', async () => {
    mockAnalyzePortfolio.mockRejectedValue(new Error('Unexpected failure'));

    const res = await portfolioFactorRegressionApp.request('/api/analytics/portfolio-factor-regression/1');
    expect(res.status).toBe(500);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.error).toBe('Internal Server Error');
  });
});
