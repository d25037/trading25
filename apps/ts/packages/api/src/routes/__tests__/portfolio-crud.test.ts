import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockListPortfolios = mock();
const mockCreatePortfolio = mock();
const mockGetPortfolioWithItems = mock();
const mockUpdatePortfolio = mock();
const mockDeletePortfolio = mock();
const mockAddItem = mock();
const mockUpdateItem = mock();
const mockDeleteItem = mock();
const mockGetPortfolioByName = mock();
const mockUpdateItemByPortfolioNameAndCode = mock();
const mockDeleteItemByPortfolioNameAndCode = mock();
const mockClose = mock();

mock.module('../../services/portfolio-service', () => ({
  PortfolioService: class {
    listPortfolios = mockListPortfolios;
    createPortfolio = mockCreatePortfolio;
    getPortfolioWithItems = mockGetPortfolioWithItems;
    updatePortfolio = mockUpdatePortfolio;
    deletePortfolio = mockDeletePortfolio;
    addItem = mockAddItem;
    updateItem = mockUpdateItem;
    deleteItem = mockDeleteItem;
    getPortfolioByName = mockGetPortfolioByName;
    updateItemByPortfolioNameAndCode = mockUpdateItemByPortfolioNameAndCode;
    deleteItemByPortfolioNameAndCode = mockDeleteItemByPortfolioNameAndCode;
    close = mockClose;
  },
}));

let portfolioApp: typeof import('../portfolio/index').default;

const makeItem = (overrides: Record<string, unknown> = {}) => ({
  id: 10,
  portfolioId: 1,
  code: '7203',
  companyName: 'トヨタ自動車',
  quantity: 100,
  purchasePrice: 2500,
  purchaseDate: new Date('2024-06-01'),
  account: null,
  notes: null,
  createdAt: new Date('2025-01-01'),
  updatedAt: new Date('2025-01-01'),
  ...overrides,
});

describe('Portfolio CRUD Routes', () => {
  beforeEach(async () => {
    mockListPortfolios.mockReset();
    mockCreatePortfolio.mockReset();
    mockGetPortfolioWithItems.mockReset();
    mockUpdatePortfolio.mockReset();
    mockDeletePortfolio.mockReset();
    mockAddItem.mockReset();
    mockUpdateItem.mockReset();
    mockDeleteItem.mockReset();
    mockGetPortfolioByName.mockReset();
    mockUpdateItemByPortfolioNameAndCode.mockReset();
    mockDeleteItemByPortfolioNameAndCode.mockReset();
    portfolioApp = (await import('../portfolio/index')).default;
  });

  // ===== CRUD =====
  describe('GET /api/portfolio', () => {
    it('returns list of portfolios', async () => {
      mockListPortfolios.mockResolvedValue([
        {
          id: 1,
          name: 'Test Portfolio',
          description: 'desc',
          stockCount: 3,
          totalShares: 500,
          createdAt: new Date('2025-01-01'),
          updatedAt: new Date('2025-01-02'),
        },
      ]);

      const res = await portfolioApp.request('/api/portfolio');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { portfolios: Array<{ name: string }> };
      expect(body.portfolios).toHaveLength(1);
      expect(body.portfolios[0]?.name).toBe('Test Portfolio');
    });

    it('returns 500 when service throws', async () => {
      mockListPortfolios.mockRejectedValue(new Error('DB failure'));

      const res = await portfolioApp.request('/api/portfolio');

      expect(res.status).toBe(500);
    });
  });

  describe('POST /api/portfolio', () => {
    it('creates a portfolio and returns 201', async () => {
      mockCreatePortfolio.mockResolvedValue({
        id: 1,
        name: 'New Portfolio',
        description: 'A new portfolio',
        createdAt: new Date('2025-01-01'),
        updatedAt: new Date('2025-01-01'),
      });

      const res = await portfolioApp.request('/api/portfolio', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'New Portfolio', description: 'A new portfolio' }),
      });

      expect(res.status).toBe(201);
      const body = (await res.json()) as { name: string };
      expect(body.name).toBe('New Portfolio');
    });

    it('returns 409 when duplicate name', async () => {
      const { DuplicatePortfolioNameError } = await import('@trading25/shared/portfolio');
      mockCreatePortfolio.mockRejectedValue(new DuplicatePortfolioNameError('Dup'));

      const res = await portfolioApp.request('/api/portfolio', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Dup' }),
      });

      expect(res.status).toBe(409);
    });
  });

  describe('GET /api/portfolio/{id}', () => {
    it('returns portfolio with items', async () => {
      mockGetPortfolioWithItems.mockResolvedValue({
        id: 1,
        name: 'My Portfolio',
        description: null,
        createdAt: new Date('2025-01-01'),
        updatedAt: new Date('2025-01-01'),
        items: [makeItem()],
      });

      const res = await portfolioApp.request('/api/portfolio/1');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { items: Array<{ code: string }> };
      expect(body.items).toHaveLength(1);
      expect(body.items[0]?.code).toBe('7203');
    });

    it('returns 404 when portfolio not found', async () => {
      const { PortfolioNotFoundError } = await import('@trading25/shared/portfolio');
      mockGetPortfolioWithItems.mockRejectedValue(new PortfolioNotFoundError(999));

      const res = await portfolioApp.request('/api/portfolio/999');

      expect(res.status).toBe(404);
    });
  });

  describe('PUT /api/portfolio/{id}', () => {
    it('updates portfolio', async () => {
      mockUpdatePortfolio.mockResolvedValue({
        id: 1,
        name: 'Updated',
        description: null,
        createdAt: new Date('2025-01-01'),
        updatedAt: new Date('2025-01-02'),
      });

      const res = await portfolioApp.request('/api/portfolio/1', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'Updated' }),
      });

      expect(res.status).toBe(200);
      const body = (await res.json()) as { name: string };
      expect(body.name).toBe('Updated');
    });
  });

  describe('DELETE /api/portfolio/{id}', () => {
    it('deletes portfolio', async () => {
      mockDeletePortfolio.mockResolvedValue(undefined);

      const res = await portfolioApp.request('/api/portfolio/1', { method: 'DELETE' });

      expect(res.status).toBe(200);
      const body = (await res.json()) as { success: boolean };
      expect(body.success).toBe(true);
    });

    it('returns 404 when portfolio not found', async () => {
      const { PortfolioNotFoundError } = await import('@trading25/shared/portfolio');
      mockDeletePortfolio.mockRejectedValue(new PortfolioNotFoundError(999));

      const res = await portfolioApp.request('/api/portfolio/999', { method: 'DELETE' });

      expect(res.status).toBe(404);
    });
  });

  // ===== Item Routes =====
  describe('POST /api/portfolio/{id}/items', () => {
    it('adds item to portfolio', async () => {
      mockAddItem.mockResolvedValue(makeItem());

      const res = await portfolioApp.request('/api/portfolio/1/items', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          code: '7203',
          quantity: 100,
          purchasePrice: 2500,
          purchaseDate: '2024-06-01',
        }),
      });

      expect(res.status).toBe(201);
      const body = (await res.json()) as { code: string };
      expect(body.code).toBe('7203');
    });

    it('returns 500 when service throws', async () => {
      mockAddItem.mockRejectedValue(new Error('DB failure'));

      const res = await portfolioApp.request('/api/portfolio/1/items', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          code: '7203',
          quantity: 100,
          purchasePrice: 2500,
          purchaseDate: '2024-06-01',
        }),
      });

      expect(res.status).toBe(500);
    });
  });

  describe('PUT /api/portfolio/{id}/items/{itemId}', () => {
    it('updates item', async () => {
      mockUpdateItem.mockResolvedValue(makeItem({ quantity: 200 }));

      const res = await portfolioApp.request('/api/portfolio/1/items/10', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ quantity: 200 }),
      });

      expect(res.status).toBe(200);
      const body = (await res.json()) as { quantity: number };
      expect(body.quantity).toBe(200);
    });
  });

  describe('DELETE /api/portfolio/{id}/items/{itemId}', () => {
    it('deletes item', async () => {
      mockDeleteItem.mockResolvedValue(undefined);

      const res = await portfolioApp.request('/api/portfolio/1/items/10', { method: 'DELETE' });

      expect(res.status).toBe(200);
      const body = (await res.json()) as { success: boolean };
      expect(body.success).toBe(true);
    });
  });

  // ===== Stock Routes (name+code based) =====
  describe('PUT /api/portfolio/{portfolioName}/stocks/{code}', () => {
    it('updates stock by name and code', async () => {
      mockUpdateItemByPortfolioNameAndCode.mockResolvedValue(makeItem({ quantity: 300 }));

      const res = await portfolioApp.request('/api/portfolio/MyPortfolio/stocks/7203', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ quantity: 300 }),
      });

      expect(res.status).toBe(200);
    });
  });

  describe('DELETE /api/portfolio/{portfolioName}/stocks/{code}', () => {
    it('deletes stock by name and code', async () => {
      mockDeleteItemByPortfolioNameAndCode.mockResolvedValue(makeItem());

      const res = await portfolioApp.request('/api/portfolio/MyPortfolio/stocks/7203', { method: 'DELETE' });

      expect(res.status).toBe(200);
      const body = (await res.json()) as { success: boolean };
      expect(body.success).toBe(true);
    });
  });

  describe('GET /api/portfolio/{name}/codes', () => {
    it('returns stock codes in portfolio', async () => {
      mockGetPortfolioByName.mockResolvedValue({
        id: 1,
        name: 'MyPortfolio',
        description: null,
        createdAt: new Date(),
        updatedAt: new Date(),
      });
      mockGetPortfolioWithItems.mockResolvedValue({
        id: 1,
        name: 'MyPortfolio',
        description: null,
        createdAt: new Date(),
        updatedAt: new Date(),
        items: [makeItem({ code: '7203' }), makeItem({ id: 11, code: '9984' })],
      });

      const res = await portfolioApp.request('/api/portfolio/MyPortfolio/codes');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { codes: string[] };
      expect(body.codes).toContain('7203');
      expect(body.codes).toContain('9984');
    });

    it('returns 404 when portfolio not found by name', async () => {
      const { PortfolioNameNotFoundError } = await import('@trading25/shared/portfolio');
      mockGetPortfolioByName.mockRejectedValue(new PortfolioNameNotFoundError('NotExist'));

      const res = await portfolioApp.request('/api/portfolio/NotExist/codes');

      expect(res.status).toBe(404);
    });
  });
});
