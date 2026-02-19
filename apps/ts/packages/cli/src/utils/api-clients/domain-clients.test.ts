import { beforeEach, describe, expect, it, mock } from 'bun:test';
import { ApiClient } from '../api-client.js';
import { AnalyticsClient } from './analytics-client.js';
import { AuthClient } from './auth-client.js';
import { DatabaseClient } from './database-client.js';
import { DatasetClient } from './dataset-client.js';
import { JQuantsClient } from './jquants-client.js';
import { PortfolioClient } from './portfolio-client.js';
import { WatchlistClient } from './watchlist-client.js';

const requestMock = mock(async () => ({ ok: true }));

function bindRequest(client: object): void {
  const mutableClient = client as unknown as {
    request: (endpoint: string, options?: RequestInit) => Promise<unknown>;
  };
  mutableClient.request = requestMock;
}

function getLastCall(): { endpoint: string; options?: RequestInit } {
  const calls = requestMock.mock.calls as unknown[][];
  const call = calls.at(-1);
  if (!call) {
    throw new Error('Expected request() to be called');
  }

  const [endpoint, options] = call;
  if (typeof endpoint !== 'string') {
    throw new Error('Expected endpoint argument to be a string');
  }

  return {
    endpoint,
    options: options as RequestInit | undefined,
  };
}

describe('ApiClient composition', () => {
  it('uses env API base URL by default and explicit URL when provided', () => {
    const previousApiBaseUrl = process.env.API_BASE_URL;
    process.env.API_BASE_URL = 'http://env-api:9999';

    try {
      const fromEnv = new ApiClient();
      expect(fromEnv.baseUrl).toBe('http://env-api:9999');

      const explicit = new ApiClient('http://explicit-api:3002');
      expect(explicit.baseUrl).toBe('http://explicit-api:3002');
    } finally {
      if (previousApiBaseUrl === undefined) {
        process.env.API_BASE_URL = undefined;
      } else {
        process.env.API_BASE_URL = previousApiBaseUrl;
      }
    }
  });
});

describe('Domain API clients', () => {
  beforeEach(() => {
    requestMock.mockClear();
  });

  it('AnalyticsClient builds request endpoints and payloads', async () => {
    const client = new AnalyticsClient('http://localhost:3002');
    bindRequest(client);

    await client.getMarketRanking({
      date: '2026-02-01',
      limit: 20,
      markets: 'prime,standard',
      lookbackDays: 15,
    });
    expect(getLastCall()).toEqual({
      endpoint: '/api/analytics/ranking?date=2026-02-01&limit=20&markets=prime%2Cstandard&lookbackDays=15',
      options: undefined,
    });

    await client.createScreeningJob({
      markets: 'prime',
      strategies: 'range_break_v15',
      recentDays: 10,
      date: '2026-02-01',
      sortBy: 'matchedDate',
      order: 'desc',
      limit: 50,
    });
    expect(getLastCall()).toEqual({
      endpoint: '/api/analytics/screening/jobs',
      options: {
        method: 'POST',
        body: JSON.stringify({
          markets: 'prime',
          strategies: 'range_break_v15',
          recentDays: 10,
          date: '2026-02-01',
          sortBy: 'matchedDate',
          order: 'desc',
          limit: 50,
        }),
      },
    });

    await client.getScreeningJobStatus('job/1 alpha');
    expect(getLastCall()).toEqual({
      endpoint: '/api/analytics/screening/jobs/job%2F1%20alpha',
      options: undefined,
    });

    await client.cancelScreeningJob('job/1 alpha');
    expect(getLastCall()).toEqual({
      endpoint: '/api/analytics/screening/jobs/job%2F1%20alpha/cancel',
      options: { method: 'POST' },
    });

    await client.getScreeningResult('job/1 alpha');
    expect(getLastCall()).toEqual({
      endpoint: '/api/analytics/screening/result/job%2F1%20alpha',
      options: undefined,
    });

    await client.getROE({
      code: '7203',
      date: '2026-01-31',
      annualize: true,
      preferConsolidated: true,
      minEquity: 1000,
      sortBy: 'roe',
      limit: 30,
    });
    expect(getLastCall()).toEqual({
      endpoint:
        '/api/analytics/roe?code=7203&date=2026-01-31&annualize=true&preferConsolidated=true&minEquity=1000&sortBy=roe&limit=30',
      options: undefined,
    });

    await client.getFactorRegression({ symbol: '7203' });
    expect(getLastCall()).toEqual({
      endpoint: '/api/analytics/factor-regression/7203',
      options: undefined,
    });

    await client.getPortfolioFactorRegression({ portfolioId: 42, lookbackDays: 180 });
    expect(getLastCall()).toEqual({
      endpoint: '/api/analytics/portfolio-factor-regression/42?lookbackDays=180',
      options: undefined,
    });
  });

  it('AuthClient calls auth status endpoint', async () => {
    const client = new AuthClient('http://localhost:3002');
    bindRequest(client);

    await client.getAuthStatus();
    expect(getLastCall()).toEqual({
      endpoint: '/api/jquants/auth/status',
      options: undefined,
    });
  });

  it('DatabaseClient covers validate/stats/refresh/sync endpoints', async () => {
    const client = new DatabaseClient('http://localhost:3002');
    bindRequest(client);

    await client.validateMarketDatabase();
    expect(getLastCall()).toEqual({
      endpoint: '/api/db/validate',
      options: undefined,
    });

    await client.getMarketStats();
    expect(getLastCall()).toEqual({
      endpoint: '/api/db/stats',
      options: undefined,
    });

    await client.refreshStocks(['7203', '9984']);
    expect(getLastCall()).toEqual({
      endpoint: '/api/db/stocks/refresh',
      options: {
        method: 'POST',
        body: JSON.stringify({ codes: ['7203', '9984'] }),
      },
    });

    await client.startSync('indices-only');
    expect(getLastCall()).toEqual({
      endpoint: '/api/db/sync',
      options: {
        method: 'POST',
        body: JSON.stringify({ mode: 'indices-only' }),
      },
    });

    await client.startSync();
    expect(getLastCall()).toEqual({
      endpoint: '/api/db/sync',
      options: {
        method: 'POST',
        body: JSON.stringify({ mode: 'auto' }),
      },
    });

    await client.getSyncJobStatus('sync-job-1');
    expect(getLastCall()).toEqual({
      endpoint: '/api/db/sync/jobs/sync-job-1',
      options: undefined,
    });

    await client.cancelSyncJob('sync-job-1');
    expect(getLastCall()).toEqual({
      endpoint: '/api/db/sync/jobs/sync-job-1',
      options: { method: 'DELETE' },
    });
  });

  it('DatasetClient builds create/resume/status/cancel/info/sample/search endpoints', async () => {
    const client = new DatasetClient('http://localhost:3002');
    bindRequest(client);

    await client.startDatasetCreate('dataset-v2', 'primeMarket', true);
    expect(getLastCall()).toEqual({
      endpoint: '/api/dataset',
      options: {
        method: 'POST',
        body: JSON.stringify({
          name: 'dataset-v2',
          preset: 'primeMarket',
          overwrite: true,
        }),
      },
    });

    await client.startDatasetResume('dataset-v2', 'primeMarket');
    expect(getLastCall()).toEqual({
      endpoint: '/api/dataset/resume',
      options: {
        method: 'POST',
        body: JSON.stringify({
          name: 'dataset-v2',
          preset: 'primeMarket',
        }),
      },
    });

    await client.getDatasetJobStatus('dataset-job-1');
    expect(getLastCall()).toEqual({
      endpoint: '/api/dataset/jobs/dataset-job-1',
      options: undefined,
    });

    await client.cancelDatasetJob('dataset-job-1');
    expect(getLastCall()).toEqual({
      endpoint: '/api/dataset/jobs/dataset-job-1',
      options: { method: 'DELETE' },
    });

    await client.getDatasetInfo('my dataset/v1');
    expect(getLastCall()).toEqual({
      endpoint: '/api/dataset/my%20dataset%2Fv1/info',
      options: undefined,
    });

    await client.sampleDataset('my dataset/v1', {
      size: 5,
      byMarket: true,
      bySector: false,
      seed: 7,
    });
    expect(getLastCall()).toEqual({
      endpoint: '/api/dataset/my%20dataset%2Fv1/sample?size=5&byMarket=true&bySector=false&seed=7',
      options: undefined,
    });

    await client.sampleDataset('my dataset/v1');
    expect(getLastCall()).toEqual({
      endpoint: '/api/dataset/my%20dataset%2Fv1/sample',
      options: undefined,
    });

    await client.searchDataset('my dataset/v1', 'toyota', { limit: 20, exact: true });
    expect(getLastCall()).toEqual({
      endpoint: '/api/dataset/my%20dataset%2Fv1/search?term=toyota&limit=20&exact=true',
      options: undefined,
    });
  });

  it('JQuantsClient builds chart/listed/margin/indices/topix endpoints', async () => {
    const client = new JQuantsClient('http://localhost:3002');
    bindRequest(client);

    await client.getDailyQuotes('7203', { from: '2026-01-01', to: '2026-01-31' });
    expect(getLastCall()).toEqual({
      endpoint: '/api/chart/stocks/7203?from=2026-01-01&to=2026-01-31',
      options: undefined,
    });

    await client.getListedInfo({ code: '7203', date: '2026-01-31' });
    expect(getLastCall()).toEqual({
      endpoint: '/api/jquants/listed-info?code=7203&date=2026-01-31',
      options: undefined,
    });

    await client.getMarginInterest('7203', { date: '2026-01-31' });
    expect(getLastCall()).toEqual({
      endpoint: '/api/jquants/stocks/7203/margin-interest?date=2026-01-31',
      options: undefined,
    });

    await client.getIndices({ code: 'topix', from: '2026-01-01', to: '2026-01-31' });
    expect(getLastCall()).toEqual({
      endpoint: '/api/jquants/indices?code=topix&from=2026-01-01&to=2026-01-31',
      options: undefined,
    });

    await client.getTOPIX();
    expect(getLastCall()).toEqual({
      endpoint: '/api/chart/indices/topix',
      options: undefined,
    });
  });

  it('PortfolioClient builds CRUD and stock item endpoints', async () => {
    const client = new PortfolioClient('http://localhost:3002');
    bindRequest(client);

    await client.listPortfolios();
    expect(getLastCall()).toEqual({
      endpoint: '/api/portfolio',
      options: undefined,
    });

    await client.createPortfolio({ name: 'Long-term', description: 'Core holdings' });
    expect(getLastCall()).toEqual({
      endpoint: '/api/portfolio',
      options: {
        method: 'POST',
        body: JSON.stringify({ name: 'Long-term', description: 'Core holdings' }),
      },
    });

    await client.getPortfolio(3);
    expect(getLastCall()).toEqual({
      endpoint: '/api/portfolio/3',
      options: undefined,
    });

    await client.updatePortfolio(3, { name: 'Long-term Updated' });
    expect(getLastCall()).toEqual({
      endpoint: '/api/portfolio/3',
      options: {
        method: 'PUT',
        body: JSON.stringify({ name: 'Long-term Updated' }),
      },
    });

    await client.deletePortfolio(3);
    expect(getLastCall()).toEqual({
      endpoint: '/api/portfolio/3',
      options: { method: 'DELETE' },
    });

    await client.addPortfolioItem(3, {
      code: '7203',
      quantity: 100,
      purchasePrice: 2500,
      purchaseDate: '2026-01-01',
      account: 'NISA',
      notes: 'core',
    });
    expect(getLastCall()).toEqual({
      endpoint: '/api/portfolio/3/items',
      options: {
        method: 'POST',
        body: JSON.stringify({
          code: '7203',
          quantity: 100,
          purchasePrice: 2500,
          purchaseDate: '2026-01-01',
          account: 'NISA',
          notes: 'core',
        }),
      },
    });

    await client.updatePortfolioItem(3, 8, { quantity: 120 });
    expect(getLastCall()).toEqual({
      endpoint: '/api/portfolio/3/items/8',
      options: {
        method: 'PUT',
        body: JSON.stringify({ quantity: 120 }),
      },
    });

    await client.deletePortfolioItem(3, 8);
    expect(getLastCall()).toEqual({
      endpoint: '/api/portfolio/3/items/8',
      options: { method: 'DELETE' },
    });

    await client.updatePortfolioStock('Growth NISA', '6758', { notes: 'rebalance' });
    expect(getLastCall()).toEqual({
      endpoint: '/api/portfolio/Growth%20NISA/stocks/6758',
      options: {
        method: 'PUT',
        body: JSON.stringify({ notes: 'rebalance' }),
      },
    });

    await client.deletePortfolioStock('Growth NISA', '6758');
    expect(getLastCall()).toEqual({
      endpoint: '/api/portfolio/Growth%20NISA/stocks/6758',
      options: { method: 'DELETE' },
    });
  });

  it('WatchlistClient builds CRUD and item endpoints', async () => {
    const client = new WatchlistClient('http://localhost:3002');
    bindRequest(client);

    await client.listWatchlists();
    expect(getLastCall()).toEqual({
      endpoint: '/api/watchlist',
      options: undefined,
    });

    await client.createWatchlist({ name: 'Breakout', description: 'watch momentum stocks' });
    expect(getLastCall()).toEqual({
      endpoint: '/api/watchlist',
      options: {
        method: 'POST',
        body: JSON.stringify({ name: 'Breakout', description: 'watch momentum stocks' }),
      },
    });

    await client.getWatchlist(9);
    expect(getLastCall()).toEqual({
      endpoint: '/api/watchlist/9',
      options: undefined,
    });

    await client.deleteWatchlist(9);
    expect(getLastCall()).toEqual({
      endpoint: '/api/watchlist/9',
      options: { method: 'DELETE' },
    });

    await client.addWatchlistItem(9, { code: '9984', memo: 'earnings watch' });
    expect(getLastCall()).toEqual({
      endpoint: '/api/watchlist/9/items',
      options: {
        method: 'POST',
        body: JSON.stringify({ code: '9984', memo: 'earnings watch' }),
      },
    });

    await client.deleteWatchlistItem(9, 2);
    expect(getLastCall()).toEqual({
      endpoint: '/api/watchlist/9/items/2',
      options: { method: 'DELETE' },
    });
  });
});
