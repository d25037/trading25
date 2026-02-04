import { describe, expect, it } from 'bun:test';
import healthApp from '../health';

describe('Health Routes', () => {
  describe('GET /health', () => {
    it('should return 200 with status ok', async () => {
      const res = await healthApp.request('/health');

      expect(res.status).toBe(200);
      expect(res.headers.get('content-type')).toContain('application/json');

      const data = (await res.json()) as { status: string; timestamp: string };
      expect(data).toHaveProperty('status', 'ok');
      expect(data).toHaveProperty('timestamp');
    });

    it('should return valid ISO timestamp', async () => {
      const res = await healthApp.request('/health');
      const data = (await res.json()) as { timestamp: string };

      // Validate timestamp is ISO 8601 format
      expect(() => new Date(data.timestamp)).not.toThrow();
      expect(new Date(data.timestamp).toISOString()).toBe(data.timestamp);
    });
  });
});
