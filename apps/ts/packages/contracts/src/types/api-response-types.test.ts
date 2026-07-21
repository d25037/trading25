import { describe, expect, it } from 'bun:test';
import type { components as BtApiComponents } from '../clients/backtest/generated/bt-api-types';
import type { ApiFundamentalsResponse } from './api-types';
import type {
  DeleteResponse,
  FactorRegressionIndexMatch,
  FactorRegressionResponse,
  FundamentalRankings,
  IndicatorComputeRequest,
  IndicatorComputeResponse,
  IndicatorSpec,
  ListPortfoliosResponse,
  ListWatchlistsResponse,
  MarginIndicatorRequest,
  MarginIndicatorResponse,
  MarketBubbleFootprintLatestResponseContract,
  MarketRankingResponse,
  MarketValidationResponse,
  PortfolioBenchmarkMetrics,
  PortfolioBenchmarkPoint,
  PortfolioCreateRequest,
  PortfolioFactorRegressionIndexMatch,
  PortfolioFactorRegressionResponse,
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
  PublishedResearchSummaryContract,
  ResearchCatalogItemContract,
  ResearchCatalogResponseContract,
  ResearchDecisionStatus,
  ResearchDetailResponseContract,
  ResearchHighlightContract,
  ResearchRunReferenceContract,
  SignalComputeRequest,
  SignalComputeResponse,
  SignalResult,
  StockInfoResponse,
  StockSearchResponse,
  StockSearchResultItem,
  ValueCompositeRankingResponse,
  ValueCompositeScoreResponse,
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

type GeneratedProviderVintage = BtApiComponents['schemas']['ProviderVintageStats'];

const fundamentalRankings: FundamentalRankings = {};
const valueCompositeRanking: ValueCompositeRankingResponse = {
  date: '2026-07-14',
  markets: ['0112'],
  metricKey: 'standard_value_composite',
  scoreMethod: 'standard_pbr_tilt',
  forwardEpsMode: 'latest',
  applyLiquidityFilter: true,
  scorePolicy: 'rank percentile',
  weights: {},
  itemCount: 0,
  lastUpdated: '2026-07-14T00:00:00Z',
};
const valueCompositeScore: ValueCompositeScoreResponse = {
  date: '2026-07-14',
  code: '7203',
  metricKey: 'standard_value_composite',
  forwardEpsMode: 'latest',
  universeCount: 0,
  scoreAvailable: false,
  lastUpdated: '2026-07-14T00:00:00Z',
};

// @ts-expect-error asOfDate is required for every fundamentals response.
const fundamentalsWithoutAsOfDate: ApiFundamentalsResponse = {
  symbol: '7203',
  data: [],
  tradingValuePeriod: 15,
  forecastEpsLookbackFyCount: 3,
  lastUpdated: '2024-06-28T15:00:00+09:00',
  provenance: { source_kind: 'market' },
  diagnostics: {},
};

void fundamentalRankings;
void valueCompositeRanking;
void valueCompositeScore;
void fundamentalsWithoutAsOfDate;

describe('api-response-types ranking contracts', () => {
  it('preserves optional ranking collections from bt OpenAPI', () => {
    const response: MarketRankingResponse = {
      date: '2026-07-13',
      markets: ['0111'],
      lookbackDays: 20,
      periodDays: 20,
      rankings: {},
      indexPerformance: [],
      lastUpdated: '2026-07-13T15:00:00+09:00',
      sectorStrengthFamily: 'balanced_sector_strength',
    };

    expect(response.rankings.gainers).toBeUndefined();
  });
});

describe('api fundamentals contracts', () => {
  it('requires the fundamentals market as-of date', () => {
    const response: ApiFundamentalsResponse = {
      symbol: '7203',
      data: [],
      asOfDate: '2024-06-28',
      tradingValuePeriod: 15,
      forecastEpsLookbackFyCount: 3,
      lastUpdated: '2024-06-28T15:00:00+09:00',
      provenance: { source_kind: 'market' },
      diagnostics: {},
    };

    expect(response.asOfDate).toBe('2024-06-28');
  });
});

describe('api-response-types market validation contracts', () => {
  it('exposes the generated provider-vintage diagnostics shape', () => {
    const providerVintage: GeneratedProviderVintage = {
      providerPlan: 'standard',
      providerAsOf: '2026-07-20',
      effectiveCoverage: { min: '2016-07-20', max: '2026-07-18' },
      sourceFingerprint: 'provider-sha256',
      providerWindowCoherent: true,
      providerWindowFingerprintCount: 2,
      invalidProviderWindowCount: 0,
      adjustmentEventCount: 3,
      adjustmentEventFingerprintCount: 3,
      invalidAdjustmentEventCount: 0,
      providerAdjustedMismatchCount: 0,
      currentBasisStateCount: 2,
      currentBasisStatementCount: 16,
      expectedAdjustedStatementRows: 7,
      extraAdjustedStatementRows: 8,
      invalidCurrentBasisStateCount: 0,
      missingAdjustedStatementRows: 10,
      orphanAdjustedStatementRows: 12,
      pendingCurrentBasisCodeCount: 1,
      providerWindowCount: 2,
      readyProviderWindowCount: 1,
      sourceStatementKeyCount: 14,
      staleAdjustedStatementRows: 15,
      status: 'pending',
      recoveryStage: 'market_db_sync',
      wrongBasisAdjustedStatementRows: 17,
    };
    const response: Pick<MarketValidationResponse, 'providerVintage'> = {
      providerVintage,
    };

    expect(response.providerVintage?.sourceStatementKeyCount).toBe(14);
    expect(response.providerVintage?.recoveryStage).toBe('market_db_sync');
  });
});

describe('api-response-types factor regression contracts', () => {
  it('preserves generated stock optionality and distinct portfolio match fields', () => {
    const stockMatch: FactorRegressionIndexMatch = {
      indexCode: '0085',
      indexName: 'TOPIX-17',
      category: 'sector17',
      rSquared: 0.8,
      beta: 1.1,
    };
    const stockResponse: FactorRegressionResponse = {
      analysisDate: '2026-07-14',
      dataPoints: 252,
      dateRange: { from: '2025-07-14', to: '2026-07-14' },
      marketBeta: 1.05,
      marketRSquared: 0.7,
      sector17Matches: [stockMatch],
      sector33Matches: [],
      stockCode: '7203',
      topixStyleMatches: [],
    };
    const portfolioMatch: PortfolioFactorRegressionIndexMatch = {
      code: '0085',
      name: 'TOPIX-17',
      rSquared: 0.8,
    };
    const portfolioResponse: PortfolioFactorRegressionResponse = {
      analysisDate: '2026-07-14',
      dataPoints: 252,
      dateRange: { from: '2025-07-14', to: '2026-07-14' },
      excludedStocks: [],
      includedStockCount: 1,
      marketBeta: 1.05,
      marketRSquared: 0.7,
      portfolioId: 1,
      portfolioName: 'Core',
      sector17Matches: [portfolioMatch],
      sector33Matches: [],
      stockCount: 1,
      topixStyleMatches: [],
      totalValue: 100000,
      weights: [],
    };

    expect(stockResponse.companyName).toBeUndefined();
    expect(stockResponse.sector17Matches[0]?.indexCode).toBe('0085');
    expect(portfolioResponse.sector17Matches[0]?.code).toBe('0085');
  });
});

describe('api-response-types bt signal and indicator contracts', () => {
  it('keeps indicator compute request and response contracts aligned with bt OpenAPI', () => {
    const indicator: IndicatorSpec = {
      type: 'sma',
      params: { period: 25 },
    };
    const request: IndicatorComputeRequest = {
      stock_code: '7203',
      source: 'market',
      timeframe: 'daily',
      indicators: [indicator],
      nan_handling: 'include',
      output: 'indicators',
    };
    const response: IndicatorComputeResponse = {
      stock_code: '7203',
      timeframe: 'daily',
      indicators: {
        sma_25: [{ date: '2026-05-25', value: 3000 }],
      },
      provenance: { source_kind: 'market' },
    };

    expect(request.indicators?.[0]?.type).toBe('sma');
    expect(response.indicators?.sma_25?.[0]?.value).toBe(3000);
  });

  it('keeps margin indicator request and response contracts aligned with bt OpenAPI', () => {
    const request: MarginIndicatorRequest = {
      stock_code: '7203',
      source: 'market',
      indicators: ['margin_long_pressure', 'margin_flow_pressure', 'margin_turnover_days'],
      average_period: 15,
    };
    const response: MarginIndicatorResponse = {
      stock_code: '7203',
      indicators: {
        margin_long_pressure: [{ date: '2026-05-25', pressure: 0.4 }],
      },
      provenance: { source_kind: 'market' },
    };

    expect(request.indicators).toContain('margin_flow_pressure');
    expect(response.indicators.margin_long_pressure?.[0]?.pressure).toBe(0.4);
  });

  it('keeps signal compute request and response contracts aligned with bt OpenAPI', () => {
    const result: SignalResult = {
      mode: 'entry',
      label: 'breakout',
      trigger_dates: ['2026-05-25'],
      count: 1,
    };
    const request: SignalComputeRequest = {
      stock_code: '7203',
      source: 'market',
      timeframe: 'daily',
      signals: [{ type: 'breakout', mode: 'entry', params: {} }],
    };
    const response: SignalComputeResponse = {
      stock_code: '7203',
      timeframe: 'daily',
      signals: { breakout: result },
      provenance: { source_kind: 'market' },
    };

    expect(request.signals?.[0]?.mode).toBe('entry');
    expect(response.signals.breakout?.trigger_dates).toEqual(['2026-05-25']);
  });
});

describe('api-response-types research contracts', () => {
  it('keeps research API contracts separate from web normalized models', () => {
    const status: ResearchDecisionStatus = 'observed';
    const highlight: ResearchHighlightContract = {
      label: 'CAGR',
      value: '12%',
      tone: 'success',
    };
    const summary: PublishedResearchSummaryContract = {
      title: 'Research readout',
      status,
      tags: ['research'],
      selectedParameters: [{ label: 'lookback', value: '120' }],
      highlights: [highlight],
      tableHighlights: [],
      riskFlags: [],
      relatedExperiments: [],
      readoutSections: [{ title: 'Decision', items: ['Keep'] }],
    };
    const item: ResearchCatalogItemContract = {
      createdAt: '2026-05-25T00:00:00Z',
      experimentId: 'annual/value',
      family: 'annual',
      hasStructuredSummary: true,
      runId: 'run-1',
      status,
      title: 'Value research',
    };
    const run: ResearchRunReferenceContract = {
      createdAt: '2026-05-25T00:00:00Z',
      runId: 'run-1',
      isLatest: true,
    };
    const catalog: ResearchCatalogResponseContract = {
      lastUpdated: '2026-05-25T00:00:00Z',
      items: [item],
    };
    const detail: ResearchDetailResponseContract = {
      item,
      summary,
      summaryMarkdown: '# Research readout',
      outputTables: [],
      availableRuns: [run],
      resultMetadata: {},
    };

    expect(catalog.items?.[0]?.status).toBe('observed');
    expect(detail.summary?.highlights?.[0]?.tone).toBe('success');
  });
});

describe('api-response-types market regime contracts', () => {
  it('keeps market bubble footprint response contract available', () => {
    const response: MarketBubbleFootprintLatestResponseContract = {
      date: '2026-05-29',
      markets: ['prime', 'standard', 'growth'],
      overallRegime: 'blowoff_watch',
      overallScore: 4,
      nearBlowoff: true,
      researchExperimentId: 'market-behavior/market-bubble-footprint',
      reratingExperimentId: 'market-behavior/rerating-bubble-regime-forward-response',
      horizons: [
        {
          horizon: 60,
          score: 3,
          regime: 'crowded',
          nearBlowoff: true,
          breadthUpPct: 24.77,
          pctAboveSma50: 38.17,
          pctAboveSma200: 43.37,
          expensiveMcapSharePct: 24.06,
          returnP90P10SpreadPct: 39.9,
          returnDispersionPercentile: 0.8974,
          capWeightLeadershipPct: 6.55,
          activeFlags: ['breadth_narrowing', 'valuation_pressure', 'cap_weight_leadership'],
        },
      ],
    };

    expect(response.horizons?.[0]?.nearBlowoff).toBe(true);
  });
});

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

    expect(list.portfolios?.[0]?.name).toBe('core');
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
      analysisDate: '2026-03-04',
      warnings: [],
    };

    expect(response.summary.totalPnL).toBe(30000);
    expect(response.holdings[0]?.code).toBe('7203');
    expect(response.benchmark?.code).toBe('0000');
    expect(response.analysisDate).toBe('2026-03-04');
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
    const priceWithoutComparison: WatchlistStockPrice = {
      code: '6758',
      close: 1000,
      volume: 100000,
      date: '2026-03-04',
    };

    const priceResponse: WatchlistPricesResponse = {
      prices: [prices, priceWithoutComparison],
    };

    const list: ListWatchlistsResponse = {
      watchlists: [summary],
    };

    expect(createWatchlist.name).toBe('focus');
    expect(updateWatchlist.description).toBeNull();
    expect(createItem.code).toBe('6758');
    expect(watchlistDeleteResponse.success).toBe(true);
    expect(list.watchlists?.[0]?.stockCount).toBe(4);
    expect(detail.items[0]?.companyName).toBe('Sony');
    expect(priceResponse.prices[0]?.changePercent).toBe(2.04);
    expect(priceResponse.prices[1]?.prevClose).toBeUndefined();
    expect(priceResponse.prices[1]?.changePercent).toBeUndefined();
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
