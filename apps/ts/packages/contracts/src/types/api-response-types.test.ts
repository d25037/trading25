import { describe, expect, it } from 'bun:test';
import type {
  DeleteResponse,
  ListPortfoliosResponse,
  ListWatchlistsResponse,
  PortfolioBenchmarkMetrics,
  PortfolioBenchmarkPoint,
  PortfolioCreateRequest,
  PortfolioHoldingPerformance,
  PortfolioItemCreateRequest,
  PortfolioItemResponse,
  PortfolioItemUpdateRequest,
  PortfolioPerformanceDateRange,
  PortfolioPerformancePoint,
  PortfolioPerformanceResponse,
  PortfolioPerformanceSummary,
  PortfolioResponse,
  PortfolioSummaryResponse,
  PortfolioUpdateRequest,
  PortfolioWithItemsResponse,
  StockInfoResponse,
  StockSearchResponse,
  StockSearchResultItem,
  WatchlistCreateRequest,
  WatchlistDeleteResponse,
  WatchlistItemCreateRequest,
  WatchlistItemResponse,
  WatchlistPricesResponse,
  WatchlistResponse,
  WatchlistStockPrice,
  WatchlistSummaryResponse,
  WatchlistUpdateRequest,
  WatchlistWithItemsResponse,
} from './api-response-types';

describe('api-response-types portfolio/watchlist contracts', () => {
  it('keeps portfolio request contracts aligned with web usage', () => {
    const createPortfolio: PortfolioCreateRequest = {
      name: 'core',
      description: null,
    };
    const updatePortfolio: PortfolioUpdateRequest = {
      name: 'core updated',
      description: null,
    };
    const createItem: PortfolioItemCreateRequest = {
      code: '7203',
      companyName: 'Toyota',
      quantity: 100,
      purchasePrice: 3000,
      purchaseDate: '2026-01-01',
      account: null,
      notes: null,
    };
    const updateItem: PortfolioItemUpdateRequest = {
      quantity: 120,
      purchasePrice: null,
      purchaseDate: null,
      account: null,
      notes: null,
    };

    expect(createPortfolio.name).toBe('core');
    expect(updatePortfolio.description).toBeNull();
    expect(createItem.code).toBe('7203');
    expect(updateItem.quantity).toBe(120);
  });

  it('keeps portfolio response contracts aligned with web usage', () => {
    const summary: PortfolioSummaryResponse = {
      id: 1,
      name: 'core',
      description: 'main portfolio',
      stockCount: 12,
      totalShares: 1200,
      createdAt: '2026-03-04T00:00:00Z',
      updatedAt: '2026-03-04T00:00:00Z',
    };

    const item: PortfolioItemResponse = {
      id: 10,
      portfolioId: 1,
      code: '7203',
      companyName: 'Toyota',
      quantity: 100,
      purchasePrice: 3000,
      purchaseDate: '2026-01-01',
      createdAt: '2026-03-04T00:00:00Z',
      updatedAt: '2026-03-04T00:00:00Z',
    };

    const portfolio: PortfolioResponse = {
      id: 1,
      name: 'core',
      createdAt: '2026-03-04T00:00:00Z',
      updatedAt: '2026-03-04T00:00:00Z',
    };

    const detail: PortfolioWithItemsResponse = {
      ...portfolio,
      items: [item],
    };

    const list: ListPortfoliosResponse = {
      portfolios: [summary],
    };

    expect(list.portfolios[0]?.name).toBe('core');
    expect(detail.items[0]?.code).toBe('7203');
  });

  it('keeps portfolio performance contracts aligned with web usage', () => {
    const summary: PortfolioPerformanceSummary = {
      currentValue: 330000,
      returnRate: 0.1,
      totalCost: 300000,
      totalPnL: 30000,
    };
    const holding: PortfolioHoldingPerformance = {
      code: '7203',
      companyName: 'Toyota',
      quantity: 100,
      purchasePrice: 3000,
      purchaseDate: '2026-01-01',
      currentPrice: 3300,
      cost: 300000,
      marketValue: 330000,
      pnl: 30000,
      returnRate: 0.1,
      weight: 1,
      account: null,
    };
    const point: PortfolioPerformancePoint = {
      date: '2026-03-04',
      dailyReturn: 0.01,
      cumulativeReturn: 0.1,
    };
    const dateRange: PortfolioPerformanceDateRange = {
      from: '2026-01-01',
      to: '2026-03-04',
    };
    const benchmark: PortfolioBenchmarkMetrics = {
      code: '0000',
      name: 'TOPIX',
      benchmarkReturn: 0.02,
      relativeReturn: 0.08,
      beta: 1,
      alpha: 0.01,
      correlation: 0.8,
      rSquared: 0.64,
    };
    const benchmarkPoint: PortfolioBenchmarkPoint = {
      date: '2026-03-04',
      portfolioReturn: 0.1,
      benchmarkReturn: 0.02,
    };
    const response: PortfolioPerformanceResponse = {
      portfolioId: 1,
      portfolioName: 'core',
      dateRange,
      dataPoints: 1,
      summary,
      holdings: [holding],
      timeSeries: [point],
      benchmark,
      benchmarkTimeSeries: [benchmarkPoint],
      warnings: [],
    };

    expect(response.summary.totalPnL).toBe(30000);
    expect(response.holdings[0]?.code).toBe('7203');
    expect(response.benchmark?.code).toBe('0000');
  });

  it('keeps watchlist response contracts aligned with web usage', () => {
    const createWatchlist: WatchlistCreateRequest = {
      name: 'focus',
      description: null,
    };
    const updateWatchlist: WatchlistUpdateRequest = {
      name: 'focus updated',
      description: null,
    };
    const createItem: WatchlistItemCreateRequest = {
      code: '6758',
      companyName: 'Sony',
      memo: null,
    };
    const deleteResponse: DeleteResponse = {
      success: true,
      message: 'ok',
    };
    const watchlistDeleteResponse: WatchlistDeleteResponse = deleteResponse;

    const summary: WatchlistSummaryResponse = {
      id: 1,
      name: 'focus',
      stockCount: 4,
      createdAt: '2026-03-04T00:00:00Z',
      updatedAt: '2026-03-04T00:00:00Z',
    };

    const item: WatchlistItemResponse = {
      id: 20,
      watchlistId: 1,
      code: '6758',
      companyName: 'Sony',
      createdAt: '2026-03-04T00:00:00Z',
    };

    const watchlist: WatchlistResponse = {
      id: 1,
      name: 'focus',
      createdAt: '2026-03-04T00:00:00Z',
      updatedAt: '2026-03-04T00:00:00Z',
    };

    const detail: WatchlistWithItemsResponse = {
      ...watchlist,
      items: [item],
    };

    const prices: WatchlistStockPrice = {
      code: '6758',
      close: 1000,
      prevClose: 980,
      changePercent: 2.04,
      volume: 100000,
      date: '2026-03-04',
    };

    const priceResponse: WatchlistPricesResponse = {
      prices: [prices],
    };

    const list: ListWatchlistsResponse = {
      watchlists: [summary],
    };

    expect(createWatchlist.name).toBe('focus');
    expect(updateWatchlist.description).toBeNull();
    expect(createItem.code).toBe('6758');
    expect(watchlistDeleteResponse.success).toBe(true);
    expect(list.watchlists[0]?.stockCount).toBe(4);
    expect(detail.items[0]?.companyName).toBe('Sony');
    expect(priceResponse.prices[0]?.changePercent).toBe(2.04);
  });
});

describe('api-response-types stock lookup contracts', () => {
  it('keeps stock info and search contracts aligned with web usage', () => {
    const stockInfo: StockInfoResponse = {
      code: '7203',
      companyName: 'Toyota Motor',
      companyNameEnglish: '',
      listedDate: '',
      marketCode: '0111',
      marketName: 'Prime',
      scaleCategory: '',
      sector17Code: '',
      sector17Name: 'Automobiles',
      sector33Code: '',
      sector33Name: 'Transportation Equipment',
    };
    const result: StockSearchResultItem = {
      code: '7203',
      companyName: 'Toyota Motor',
      companyNameEnglish: null,
      marketCode: '0111',
      marketName: 'Prime',
      sector33Name: 'Transportation Equipment',
    };
    const response: StockSearchResponse = {
      count: 1,
      query: 'toyota',
      results: [result],
    };

    expect(stockInfo.code).toBe('7203');
    expect(response.results[0]?.companyName).toBe('Toyota Motor');
  });
});
