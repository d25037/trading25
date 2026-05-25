import { describe, expect, it } from 'bun:test';
import type {
  DeleteResponse,
  ListPortfoliosResponse,
  ListWatchlistsResponse,
  PortfolioCreateRequest,
  PortfolioItemCreateRequest,
  PortfolioItemResponse,
  PortfolioItemUpdateRequest,
  PortfolioResponse,
  PortfolioSummaryResponse,
  PortfolioUpdateRequest,
  PortfolioWithItemsResponse,
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
