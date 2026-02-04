import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockGetPortfolioPerformance = mock();
const mockPerfClose = mock();

mock.module('../../services/portfolio-performance-service', () => ({
  PortfolioPerformanceService: class {
    getPortfolioPerformance = mockGetPortfolioPerformance;
    close = mockPerfClose;
  },
}));

// portfolio-service is already mocked by portfolio-crud.test.ts since it uses the same module path
// But we need to make sure it's mocked here too for the performance route's singleton
mock.module('../../services/portfolio-service', () => ({
  PortfolioService: class {
    listPortfolios = mock().mockResolvedValue([]);
    createPortfolio = mock();
    getPortfolioWithItems = mock();
    updatePortfolio = mock();
    deletePortfolio = mock();
    addItem = mock();
    updateItem = mock();
    deleteItem = mock();
    getPortfolioByName = mock();
    updateItemByPortfolioNameAndCode = mock();
    deleteItemByPortfolioNameAndCode = mock();
    close = mock();
  },
}));

let performanceApp: typeof import('../portfolio/performance').default;

describe('Portfolio Performance Routes', () => {
  beforeEach(async () => {
    mockGetPortfolioPerformance.mockReset();
    performanceApp = (await import('../portfolio/performance')).default;
  });

  describe('GET /api/portfolio/{id}/performance', () => {
    it('returns portfolio performance', async () => {
      mockGetPortfolioPerformance.mockResolvedValue({
        portfolioId: 1,
        portfolioName: 'Test Portfolio',
        portfolioDescription: null,
        summary: {
          totalCost: 250000,
          totalMarketValue: 280000,
          totalPnl: 30000,
          totalReturnRate: 12,
          holdingCount: 1,
        },
        holdings: [
          {
            code: '7203',
            companyName: 'トヨタ自動車',
            quantity: 100,
            purchasePrice: 2500,
            currentPrice: 2800,
            cost: 250000,
            marketValue: 280000,
            pnl: 30000,
            returnRate: 12,
            weight: 100,
            purchaseDate: '2024-06-01',
            account: null,
          },
        ],
        timeSeries: [{ date: '2025-01-01', dailyReturn: 0.5, cumulativeReturn: 12 }],
        benchmark: { annualizedReturn: 8, totalReturn: 4, sharpeRatio: 0.8, maxDrawdown: -5 },
        benchmarkTimeSeries: [{ date: '2025-01-01', cumulativeReturn: 4 }],
        analysisDate: '2025-01-01',
        dateRange: { from: '2024-06-01', to: '2025-01-01' },
        dataPoints: 150,
        warnings: [],
      });

      const res = await performanceApp.request('/api/portfolio/1/performance');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { portfolioName: string; holdings: unknown[] };
      expect(body.portfolioName).toBe('Test Portfolio');
      expect(body.holdings).toHaveLength(1);
    });

    it('returns 404 when portfolio not found', async () => {
      const { PortfolioNotFoundError } = await import('@trading25/shared/portfolio');
      mockGetPortfolioPerformance.mockRejectedValue(new PortfolioNotFoundError(999));

      const res = await performanceApp.request('/api/portfolio/999/performance');

      expect(res.status).toBe(404);
    });

    it('returns 500 on error', async () => {
      mockGetPortfolioPerformance.mockRejectedValue(new Error('calculation failed'));

      const res = await performanceApp.request('/api/portfolio/1/performance');

      expect(res.status).toBe(500);
    });
  });
});
