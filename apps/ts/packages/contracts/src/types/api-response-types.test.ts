import { describe, expect, it } from 'bun:test';
import type {
  DeleteResponse,
  IndicatorComputeRequest,
  IndicatorComputeResponse,
  IndicatorSpec,
  ListPortfoliosResponse,
  ListWatchlistsResponse,
  MarginIndicatorRequest,
  MarginIndicatorResponse,
  PortfolioItemResponse,
  PortfolioResponse,
  PortfolioSummaryResponse,
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
  WatchlistDeleteResponse,
  WatchlistItemResponse,
  WatchlistPricesResponse,
  WatchlistResponse,
  WatchlistStockPrice,
  WatchlistSummaryResponse,
  WatchlistWithItemsResponse,
} from './api-response-types';

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

describe('api-response-types portfolio/watchlist contracts', () => {
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

  it('keeps watchlist response contracts aligned with web usage', () => {
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

    expect(watchlistDeleteResponse.success).toBe(true);
    expect(list.watchlists[0]?.stockCount).toBe(4);
    expect(detail.items[0]?.companyName).toBe('Sony');
    expect(priceResponse.prices[0]?.changePercent).toBe(2.04);
  });
});
