import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockListWatchlists = mock();
const mockCreateWatchlist = mock();
const mockGetWatchlistWithItems = mock();
const mockUpdateWatchlist = mock();
const mockDeleteWatchlist = mock();
const mockAddItem = mock();
const mockDeleteItem = mock();
const mockClose = mock();

mock.module('../../services/watchlist-service', () => ({
  WatchlistService: class {
    listWatchlists = mockListWatchlists;
    createWatchlist = mockCreateWatchlist;
    getWatchlistWithItems = mockGetWatchlistWithItems;
    updateWatchlist = mockUpdateWatchlist;
    deleteWatchlist = mockDeleteWatchlist;
    addItem = mockAddItem;
    deleteItem = mockDeleteItem;
    close = mockClose;
  },
}));

// Mock MarketDataReader for watchlist-prices (which is loaded via watchlist/index)
mock.module('@trading25/shared/market-sync', () => ({
  MarketDataReader: class {
    close() {}
  },
  DrizzleMarketDataReader: class {
    close() {}
  },
  InitialSyncStrategy: class {},
  IncrementalSyncStrategy: class {},
}));

let watchlistApp: typeof import('../watchlist/index').default;

const now = new Date('2025-01-01');

describe('Watchlist CRUD Routes', () => {
  beforeEach(async () => {
    mockListWatchlists.mockReset();
    mockCreateWatchlist.mockReset();
    mockGetWatchlistWithItems.mockReset();
    mockUpdateWatchlist.mockReset();
    mockDeleteWatchlist.mockReset();
    mockAddItem.mockReset();
    mockDeleteItem.mockReset();
    watchlistApp = (await import('../watchlist/index')).default;
  });

  describe('GET /api/watchlist', () => {
    it('returns list of watchlists', async () => {
      mockListWatchlists.mockResolvedValue([
        { id: 1, name: 'Tech', description: null, stockCount: 5, createdAt: now, updatedAt: now },
      ]);

      const res = await watchlistApp.request('/api/watchlist');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { watchlists: Array<{ name: string }> };
      expect(body.watchlists).toHaveLength(1);
      expect(body.watchlists[0]?.name).toBe('Tech');
    });

    it('returns 500 on error', async () => {
      mockListWatchlists.mockRejectedValue(new Error('fail'));

      const res = await watchlistApp.request('/api/watchlist');

      expect(res.status).toBe(500);
    });
  });

  describe('POST /api/watchlist', () => {
    it('creates watchlist', async () => {
      mockCreateWatchlist.mockResolvedValue({
        id: 1,
        name: 'New WL',
        description: 'desc',
        createdAt: now,
        updatedAt: now,
      });

      const res = await watchlistApp.request('/api/watchlist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'New WL', description: 'desc' }),
      });

      expect(res.status).toBe(201);
      const body = (await res.json()) as { name: string };
      expect(body.name).toBe('New WL');
    });

    it('returns 409 on duplicate name', async () => {
      const { DuplicateWatchlistNameError } = await import('@trading25/shared/watchlist');
      mockCreateWatchlist.mockRejectedValue(new DuplicateWatchlistNameError('Dup'));

      const res = await watchlistApp.request('/api/watchlist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Dup' }),
      });

      expect(res.status).toBe(409);
    });
  });

  describe('GET /api/watchlist/{id}', () => {
    it('returns watchlist with items', async () => {
      mockGetWatchlistWithItems.mockResolvedValue({
        id: 1,
        name: 'Tech',
        description: null,
        createdAt: now,
        updatedAt: now,
        items: [{ id: 10, watchlistId: 1, code: '7203', companyName: 'トヨタ', memo: null, createdAt: now }],
      });

      const res = await watchlistApp.request('/api/watchlist/1');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { items: Array<{ code: string }> };
      expect(body.items).toHaveLength(1);
    });

    it('returns 404 when not found', async () => {
      const { WatchlistNotFoundError } = await import('@trading25/shared/watchlist');
      mockGetWatchlistWithItems.mockRejectedValue(new WatchlistNotFoundError(999));

      const res = await watchlistApp.request('/api/watchlist/999');

      expect(res.status).toBe(404);
    });
  });

  describe('PUT /api/watchlist/{id}', () => {
    it('updates watchlist', async () => {
      mockUpdateWatchlist.mockResolvedValue({
        id: 1,
        name: 'Updated',
        description: null,
        createdAt: now,
        updatedAt: now,
      });

      const res = await watchlistApp.request('/api/watchlist/1', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Updated' }),
      });

      expect(res.status).toBe(200);
      const body = (await res.json()) as { name: string };
      expect(body.name).toBe('Updated');
    });
  });

  describe('DELETE /api/watchlist/{id}', () => {
    it('deletes watchlist', async () => {
      mockDeleteWatchlist.mockResolvedValue(undefined);

      const res = await watchlistApp.request('/api/watchlist/1', { method: 'DELETE' });

      expect(res.status).toBe(200);
      const body = (await res.json()) as { success: boolean };
      expect(body.success).toBe(true);
    });

    it('returns 404 when not found', async () => {
      const { WatchlistNotFoundError } = await import('@trading25/shared/watchlist');
      mockDeleteWatchlist.mockRejectedValue(new WatchlistNotFoundError(999));

      const res = await watchlistApp.request('/api/watchlist/999', { method: 'DELETE' });

      expect(res.status).toBe(404);
    });
  });

  describe('POST /api/watchlist/{id}/items', () => {
    it('adds item to watchlist', async () => {
      mockAddItem.mockResolvedValue({
        id: 10,
        watchlistId: 1,
        code: '7203',
        companyName: 'トヨタ',
        memo: null,
        createdAt: now,
      });

      const res = await watchlistApp.request('/api/watchlist/1/items', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ code: '7203' }),
      });

      expect(res.status).toBe(201);
    });
  });

  describe('DELETE /api/watchlist/{id}/items/{itemId}', () => {
    it('deletes item from watchlist', async () => {
      mockDeleteItem.mockResolvedValue(undefined);

      const res = await watchlistApp.request('/api/watchlist/1/items/10', { method: 'DELETE' });

      expect(res.status).toBe(200);
    });

    it('returns 404 when item not found', async () => {
      const { WatchlistItemNotFoundError } = await import('@trading25/shared/watchlist');
      mockDeleteItem.mockRejectedValue(new WatchlistItemNotFoundError(999));

      const res = await watchlistApp.request('/api/watchlist/1/items/999', { method: 'DELETE' });

      expect(res.status).toBe(404);
    });
  });
});
