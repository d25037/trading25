import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockBtGet = mock();

mock.module('../bt-api-proxy', () => ({
  btGet: mockBtGet,
}));

async function loadMarketRankingService() {
  const moduleUrl = new URL('./market-ranking-service.ts', import.meta.url);
  moduleUrl.searchParams.set('test', crypto.randomUUID());
  return (await import(moduleUrl.href)).MarketRankingService as typeof import('./market-ranking-service').MarketRankingService;
}

async function loadMarketScreeningService() {
  const moduleUrl = new URL('./market-screening-service.ts', import.meta.url);
  moduleUrl.searchParams.set('test', crypto.randomUUID());
  return (await import(moduleUrl.href))
    .MarketScreeningService as typeof import('./market-screening-service').MarketScreeningService;
}

async function loadSectorStocksService() {
  const moduleUrl = new URL('./sector-stocks-service.ts', import.meta.url);
  moduleUrl.searchParams.set('test', crypto.randomUUID());
  return (await import(moduleUrl.href)).SectorStocksService as typeof import('./sector-stocks-service').SectorStocksService;
}

describe('market analytics proxy services', () => {
  beforeEach(() => {
    mockBtGet.mockReset();
  });

  it('MarketRankingService proxies query options', async () => {
    const MarketRankingService = await loadMarketRankingService();
    mockBtGet.mockResolvedValue({ rankings: {} });
    const service = new MarketRankingService();

    await service.getRankings({
      date: '2025-01-15',
      limit: 5,
      markets: 'prime,standard',
      lookbackDays: 3,
      periodDays: 20,
    });

    expect(mockBtGet).toHaveBeenCalledWith('/api/analytics/ranking', {
      date: '2025-01-15',
      limit: 5,
      markets: 'prime,standard',
      lookbackDays: 3,
      periodDays: 20,
    });
  });

  it('MarketScreeningService maps referenceDate to date query', async () => {
    const MarketScreeningService = await loadMarketScreeningService();
    mockBtGet.mockResolvedValue({ results: [] });
    const service = new MarketScreeningService();

    await service.runScreening({
      markets: 'prime',
      rangeBreakFast: true,
      rangeBreakSlow: false,
      recentDays: 15,
      referenceDate: '2025-01-10',
      minBreakPercentage: 2.5,
      minVolumeRatio: 1.2,
      sortBy: 'breakPercentage',
      order: 'desc',
      limit: 50,
    });

    expect(mockBtGet).toHaveBeenCalledWith('/api/analytics/screening', {
      markets: 'prime',
      rangeBreakFast: true,
      rangeBreakSlow: false,
      recentDays: 15,
      date: '2025-01-10',
      minBreakPercentage: 2.5,
      minVolumeRatio: 1.2,
      sortBy: 'breakPercentage',
      order: 'desc',
      limit: 50,
    });
  });

  it('SectorStocksService proxies sector filters and sort options', async () => {
    const SectorStocksService = await loadSectorStocksService();
    mockBtGet.mockResolvedValue({ stocks: [] });
    const service = new SectorStocksService();

    await service.getStocks({
      sector33Name: '電気機器',
      sector17Name: undefined,
      markets: 'prime,growth',
      lookbackDays: 5,
      sortBy: 'tradingValue',
      sortOrder: 'desc',
      limit: 30,
    });

    expect(mockBtGet).toHaveBeenCalledWith('/api/analytics/sector-stocks', {
      sector33Name: '電気機器',
      sector17Name: undefined,
      markets: 'prime,growth',
      lookbackDays: 5,
      sortBy: 'tradingValue',
      sortOrder: 'desc',
      limit: 30,
    });
  });
});
