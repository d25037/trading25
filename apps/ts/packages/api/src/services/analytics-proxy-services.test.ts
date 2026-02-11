import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockBtGet = mock();

mock.module('./bt-api-proxy', () => ({
  btGet: mockBtGet,
}));

async function loadFactorRegressionService() {
  const moduleUrl = new URL('./factor-regression-service.ts', import.meta.url);
  moduleUrl.searchParams.set('test', crypto.randomUUID());
  return (await import(moduleUrl.href)).FactorRegressionService as typeof import('./factor-regression-service').FactorRegressionService;
}

async function loadPortfolioFactorRegressionService() {
  const moduleUrl = new URL('./portfolio-factor-regression-service.ts', import.meta.url);
  moduleUrl.searchParams.set('test', crypto.randomUUID());
  return (await import(moduleUrl.href))
    .PortfolioFactorRegressionService as typeof import('./portfolio-factor-regression-service').PortfolioFactorRegressionService;
}

async function loadROEDataService() {
  const moduleUrl = new URL('./roe-data.ts', import.meta.url);
  moduleUrl.searchParams.set('test', crypto.randomUUID());
  return (await import(moduleUrl.href)).ROEDataService as typeof import('./roe-data').ROEDataService;
}

describe('analytics proxy services', () => {
  beforeEach(() => {
    mockBtGet.mockReset();
  });

  it('FactorRegressionService proxies path and query', async () => {
    const FactorRegressionService = await loadFactorRegressionService();
    mockBtGet.mockResolvedValue({ stockCode: '7203' });
    const service = new FactorRegressionService();

    await service.analyzeStock({ symbol: '72/3', lookbackDays: 250 });

    expect(mockBtGet).toHaveBeenCalledWith('/api/analytics/factor-regression/72%2F3', {
      lookbackDays: 250,
    });
  });

  it('PortfolioFactorRegressionService proxies path and query', async () => {
    const PortfolioFactorRegressionService = await loadPortfolioFactorRegressionService();
    mockBtGet.mockResolvedValue({ portfolioId: 1 });
    const service = new PortfolioFactorRegressionService();

    await service.analyzePortfolio({ portfolioId: 42, lookbackDays: 180 });

    expect(mockBtGet).toHaveBeenCalledWith('/api/analytics/portfolio-factor-regression/42', {
      lookbackDays: 180,
    });
  });

  it('ROEDataService proxies query options as-is', async () => {
    const ROEDataService = await loadROEDataService();
    mockBtGet.mockResolvedValue({ results: [], summary: {}, lastUpdated: '2025-01-01T00:00:00.000Z' });
    const service = new ROEDataService();

    await service.calculateROE({
      code: '7203',
      date: '2025-01-15',
      annualize: false,
      preferConsolidated: true,
      minEquity: 2000,
      sortBy: 'code',
      limit: 10,
    });

    expect(mockBtGet).toHaveBeenCalledWith('/api/analytics/roe', {
      code: '7203',
      date: '2025-01-15',
      annualize: false,
      preferConsolidated: true,
      minEquity: 2000,
      sortBy: 'code',
      limit: 10,
    });
  });
});
