import type { Mock } from 'bun:test';
import { afterEach, beforeEach, describe, expect, spyOn, test } from 'bun:test';
import { createMockResponse } from '../test-utils/fetch-mock.js';
import { AnalyticsClient } from './AnalyticsClient.js';

describe('AnalyticsClient', () => {
  let client: AnalyticsClient;
  let fetchSpy: Mock<typeof fetch>;

  beforeEach(() => {
    client = new AnalyticsClient('http://localhost:3002');
    fetchSpy = spyOn(globalThis, 'fetch').mockImplementation((() =>
      Promise.resolve(createMockResponse({ ok: true }))) as unknown as typeof fetch);
  });

  afterEach(() => {
    fetchSpy.mockRestore();
  });

  test('getMarketRanking builds query parameters', async () => {
    fetchSpy.mockResolvedValueOnce(createMockResponse({ rankings: {} }));

    await client.getMarketRanking({
      date: '2026-02-01',
      limit: 20,
      markets: 'prime,standard',
      lookbackDays: 10,
      periodDays: 30,
    });

    expect(fetchSpy.mock.calls.at(-1)?.[0]).toBe(
      'http://localhost:3002/api/analytics/ranking?date=2026-02-01&limit=20&markets=prime%2Cstandard&lookbackDays=10&periodDays=30'
    );
  });

  test('createScreeningJob sends POST request body', async () => {
    fetchSpy.mockResolvedValueOnce(createMockResponse({ job_id: 'job-1', status: 'pending' }));

    await client.createScreeningJob({
      markets: 'prime',
      strategies: 'production/range_break_v15',
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
      limit: 50,
    });

    const lastCall = fetchSpy.mock.calls.at(-1);
    expect(lastCall?.[0]).toBe('http://localhost:3002/api/analytics/screening/jobs');
    expect(lastCall?.[1]).toMatchObject({
      method: 'POST',
      body: JSON.stringify({
        markets: 'prime',
        strategies: 'production/range_break_v15',
        recentDays: 10,
        sortBy: 'matchedDate',
        order: 'desc',
        limit: 50,
      }),
    });
  });

  test('screening job endpoints encode job id', async () => {
    fetchSpy.mockImplementation((() => Promise.resolve(createMockResponse({ ok: true }))) as typeof fetch);

    await client.getScreeningJobStatus('job/1 alpha');
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toBe('http://localhost:3002/api/analytics/screening/jobs/job%2F1%20alpha');

    await client.cancelScreeningJob('job/1 alpha');
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toBe(
      'http://localhost:3002/api/analytics/screening/jobs/job%2F1%20alpha/cancel'
    );
    expect(fetchSpy.mock.calls.at(-1)?.[1]).toMatchObject({ method: 'POST' });

    await client.getScreeningResult('job/1 alpha');
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toBe(
      'http://localhost:3002/api/analytics/screening/result/job%2F1%20alpha'
    );
  });

  test('getFundamentalRanking sends normalized query payload', async () => {
    fetchSpy.mockResolvedValueOnce(createMockResponse({ rankings: {} }));

    await client.getFundamentalRanking({
      markets: 'prime',
      limit: 20,
      forecastAboveRecentFyActuals: true,
      forecastLookbackFyCount: 3,
    });

    expect(fetchSpy.mock.calls.at(-1)?.[0]).toBe(
      'http://localhost:3002/api/analytics/fundamental-ranking?limit=20&markets=prime&forecastAboveRecentFyActuals=true&forecastLookbackFyCount=3'
    );
  });

  test('factor regression endpoints use expected paths', async () => {
    fetchSpy.mockImplementation((() => Promise.resolve(createMockResponse({ ok: true }))) as typeof fetch);

    await client.getFactorRegression({ symbol: '7203', lookbackDays: 180 });
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toBe(
      'http://localhost:3002/api/analytics/factor-regression/7203?lookbackDays=180'
    );

    await client.getPortfolioFactorRegression({ portfolioId: 42, lookbackDays: 252 });
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toBe(
      'http://localhost:3002/api/analytics/portfolio-factor-regression/42?lookbackDays=252'
    );
  });

  test('getROE builds query parameters', async () => {
    fetchSpy.mockResolvedValueOnce(createMockResponse({ results: [], summary: {}, lastUpdated: '2026-02-01' }));

    await client.getROE({
      code: '7203',
      date: '2026-02-01',
      annualize: true,
      preferConsolidated: false,
      minEquity: 1000,
      sortBy: 'roe',
      limit: 30,
    });

    expect(fetchSpy.mock.calls.at(-1)?.[0]).toBe(
      'http://localhost:3002/api/analytics/roe?code=7203&date=2026-02-01&annualize=true&preferConsolidated=false&minEquity=1000&sortBy=roe&limit=30'
    );
  });

  test('encodes symbol in factor regression path', async () => {
    fetchSpy.mockResolvedValueOnce(createMockResponse({ ok: true }));

    await client.getFactorRegression({ symbol: '7203/TEST' });
    expect(fetchSpy.mock.calls.at(-1)?.[0]).toBe('http://localhost:3002/api/analytics/factor-regression/7203%2FTEST');
  });

  test('supports config object constructor', async () => {
    const configured = new AnalyticsClient({ baseUrl: 'http://localhost:3999', timeoutMs: 1234 });
    fetchSpy.mockResolvedValueOnce(createMockResponse({ rankings: {} }));

    await configured.getMarketRanking({ limit: 1 });

    const lastCall = fetchSpy.mock.calls.at(-1);
    expect(lastCall?.[0]).toBe('http://localhost:3999/api/analytics/ranking?limit=1');
    expect(lastCall?.[1]).toBeDefined();
    expect((lastCall?.[1] as RequestInit | undefined)?.signal).toBeDefined();
  });
});
