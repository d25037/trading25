import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockGetStats = mock();
const mockValidate = mock();

mock.module('../../services/market/market-stats-service', () => ({
  MarketStatsService: class {
    getStats = mockGetStats;
    close = () => {};
  },
}));

mock.module('../../services/market/market-validation-service', () => ({
  MarketValidationService: class {
    validate = mockValidate;
    close = () => {};
  },
}));

let marketStatsApp: typeof import('../db/stats').default;
let marketValidateApp: typeof import('../db/validate').default;

describe('DB Routes', () => {
  beforeEach(async () => {
    mockGetStats.mockReset();
    mockValidate.mockReset();
    marketStatsApp = (await import('../db/stats')).default;
    marketValidateApp = (await import('../db/validate')).default;
  });

  describe('GET /api/db/stats', () => {
    it('returns stats', async () => {
      mockGetStats.mockResolvedValue({
        topix: { count: 100, dateRange: { from: '2024-01-01', to: '2025-01-01' } },
        stocks: { count: 4000 },
      });

      const res = await marketStatsApp.request('/api/db/stats');

      expect(res.status).toBe(200);
    });

    it('returns 422 when database not found', async () => {
      mockGetStats.mockRejectedValue(new Error('no such table: topix'));

      const res = await marketStatsApp.request('/api/db/stats');

      expect(res.status).toBe(422);
    });

    it('returns 500 on unknown error', async () => {
      mockGetStats.mockRejectedValue(new Error('unexpected'));

      const res = await marketStatsApp.request('/api/db/stats');

      expect(res.status).toBe(500);
    });
  });

  describe('GET /api/db/validate', () => {
    it('returns validation report', async () => {
      mockValidate.mockResolvedValue({
        valid: true,
        issues: [],
        recommendations: [],
      });

      const res = await marketValidateApp.request('/api/db/validate');

      expect(res.status).toBe(200);
    });

    it('returns 422 when database not found', async () => {
      mockValidate.mockRejectedValue(new Error('no such table'));

      const res = await marketValidateApp.request('/api/db/validate');

      expect(res.status).toBe(422);
    });

    it('returns 500 on unknown error', async () => {
      mockValidate.mockRejectedValue(new Error('unexpected'));

      const res = await marketValidateApp.request('/api/db/validate');

      expect(res.status).toBe(500);
    });
  });
});
