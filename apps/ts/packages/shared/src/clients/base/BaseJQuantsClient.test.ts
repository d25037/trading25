import { afterEach, beforeEach, describe, expect, it, type Mock, spyOn } from 'bun:test';
import { createMockResponse } from '../../test-utils/fetch-mock';
import { mockJQuantsConfig } from '../../test-utils/fixtures';
import { JQuantsClient } from '../JQuantsClient';
import {
  calculatePlanConcurrency,
  getRequestsPerSecond,
  resetRateLimiter,
  validateJQuantsPlan,
} from './BaseJQuantsClient';

const mockLogger = {
  trace: () => {},
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
  fatal: () => {},
};

describe('BaseJQuantsClient helpers', () => {
  describe('validateJQuantsPlan', () => {
    it('throws when plan is missing', () => {
      expect(() => validateJQuantsPlan(undefined)).toThrow('JQUANTS_PLAN environment variable is required');
    });

    it('throws when plan is invalid', () => {
      expect(() => validateJQuantsPlan('invalid-plan')).toThrow('Invalid JQUANTS_PLAN');
    });

    it('returns a valid plan', () => {
      expect(validateJQuantsPlan('free')).toBe('free');
      expect(validateJQuantsPlan('light')).toBe('light');
      expect(validateJQuantsPlan('standard')).toBe('standard');
      expect(validateJQuantsPlan('premium')).toBe('premium');
    });
  });

  describe('getRequestsPerSecond', () => {
    it('converts plan limits to requests per second', () => {
      expect(getRequestsPerSecond('free')).toBeCloseTo(5 / 60, 4);
      expect(getRequestsPerSecond('light')).toBeCloseTo(1, 4);
      expect(getRequestsPerSecond('standard')).toBeCloseTo(2, 4);
      expect(getRequestsPerSecond('premium')).toBeCloseTo(500 / 60, 4);
    });
  });

  describe('calculatePlanConcurrency', () => {
    it('returns expected concurrency by plan', () => {
      expect(calculatePlanConcurrency('free')).toBe(1);
      expect(calculatePlanConcurrency('light')).toBe(2);
      expect(calculatePlanConcurrency('standard')).toBe(3);
      expect(calculatePlanConcurrency('premium')).toBe(3);
    });
  });
});

describe('BaseJQuantsClient api key helpers', () => {
  it('returns masked key when api key exists', () => {
    const client = new JQuantsClient({ apiKey: '1234567890', logger: mockLogger });
    expect(client.getApiKeyStatus()).toEqual({
      hasApiKey: true,
      maskedKey: '1234...7890',
    });
  });

  it('returns null when api key is missing', () => {
    const client = new JQuantsClient({ apiKey: '', logger: mockLogger });
    expect(client.getApiKeyStatus()).toEqual({
      hasApiKey: false,
      maskedKey: null,
    });
  });

  it('updates api key via updateApiKey', () => {
    const client = new JQuantsClient({ apiKey: '', logger: mockLogger });
    client.updateApiKey('abcd1234efgh');
    expect(client.getApiKeyStatus()).toEqual({
      hasApiKey: true,
      maskedKey: 'abcd...efgh',
    });
  });
});

describe('RateLimitQueue (via resetRateLimiter / concurrent requests)', () => {
  let fetchSpy: Mock<typeof fetch>;
  let savedPlan: string | undefined;

  beforeEach(() => {
    savedPlan = process.env.JQUANTS_PLAN;
    // Use 'free' plan for tight rate limiting (5 req/min → ~13.2s interval)
    process.env.JQUANTS_PLAN = 'premium';
    resetRateLimiter();
    fetchSpy = spyOn(globalThis, 'fetch').mockImplementation((() =>
      Promise.resolve(createMockResponse({ data: [] }))) as unknown as typeof fetch);
  });

  afterEach(() => {
    fetchSpy.mockRestore();
    resetRateLimiter({ disable: true });
    if (savedPlan !== undefined) {
      process.env.JQUANTS_PLAN = savedPlan;
    } else {
      // biome-ignore lint/performance/noDelete: Required to truly clear env vars
      delete process.env.JQUANTS_PLAN;
    }
  });

  it('serializes concurrent requests in FIFO order', async () => {
    const callOrder: number[] = [];
    const originalImpl = fetchSpy.getMockImplementation();

    fetchSpy.mockImplementation(((...args: Parameters<typeof fetch>) => {
      const url = typeof args[0] === 'string' ? args[0] : '';
      // Extract a marker from the URL to track call order
      const match = url.match(/code=(\d+)/);
      if (match?.[1]) {
        callOrder.push(Number(match[1]));
      }
      return originalImpl?.(...args);
    }) as typeof fetch);

    const client = new JQuantsClient({ ...mockJQuantsConfig, logger: mockLogger });

    // Fire 3 concurrent requests
    const results = await Promise.all([
      client.getDailyQuotes({ code: '1001' }),
      client.getDailyQuotes({ code: '1002' }),
      client.getDailyQuotes({ code: '1003' }),
    ]);

    expect(results).toHaveLength(3);
    // Requests should be processed in FIFO order
    expect(callOrder).toEqual([1001, 1002, 1003]);
  });

  it('enforces minimum interval between requests', async () => {
    // Use 'light' plan: 60 req/min → ~1.1s interval
    process.env.JQUANTS_PLAN = 'light';
    resetRateLimiter();

    const timestamps: number[] = [];
    fetchSpy.mockImplementation((() => {
      timestamps.push(Date.now());
      return Promise.resolve(createMockResponse({ data: [] }));
    }) as unknown as typeof fetch);

    const client = new JQuantsClient({ ...mockJQuantsConfig, logger: mockLogger });

    // Fire 2 sequential requests (second should wait for rate limit)
    await client.getListedInfo();
    await client.getListedInfo();

    expect(timestamps).toHaveLength(2);
    const interval = (timestamps[1] ?? 0) - (timestamps[0] ?? 0);
    // light plan: ceil((60000/60)*1.1) = 1100ms minimum interval
    expect(interval).toBeGreaterThanOrEqual(1000); // allow small timing tolerance
  });

  it('works correctly across multiple client instances', async () => {
    const callOrder: number[] = [];
    fetchSpy.mockImplementation(((...args: Parameters<typeof fetch>) => {
      const url = typeof args[0] === 'string' ? args[0] : '';
      const match = url.match(/code=(\d+)/);
      if (match?.[1]) {
        callOrder.push(Number(match[1]));
      }
      return Promise.resolve(createMockResponse({ data: [] }));
    }) as typeof fetch);

    const client1 = new JQuantsClient({ ...mockJQuantsConfig, logger: mockLogger });
    const client2 = new JQuantsClient({ ...mockJQuantsConfig, logger: mockLogger });

    // Concurrent requests from different instances share the same rate limiter
    await Promise.all([
      client1.getDailyQuotes({ code: '2001' }),
      client2.getDailyQuotes({ code: '2002' }),
      client1.getDailyQuotes({ code: '2003' }),
    ]);

    // All 3 should be processed in FIFO order despite different instances
    expect(callOrder).toEqual([2001, 2002, 2003]);
  });

  it('disabled rate limiter skips queue entirely', async () => {
    resetRateLimiter({ disable: true });

    const client = new JQuantsClient({ ...mockJQuantsConfig, logger: mockLogger });

    // Should complete without any rate limit delay
    const start = Date.now();
    await Promise.all([client.getDailyQuotes({ code: '3001' }), client.getDailyQuotes({ code: '3002' })]);
    const elapsed = Date.now() - start;

    // With disabled rate limiter, should be near-instant
    expect(elapsed).toBeLessThan(500);
  });
});
