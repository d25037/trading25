import type { MarketRankingSymbolResponse } from '@trading25/contracts/types/api-response-types';
import type { ApiDailyValuationDataPoint } from '@trading25/contracts/types/api-types';
import type { ShikihoQuoteV1 } from '@trading25/shikiho-extension/contract';
import { describe, expect, it } from 'vitest';
import type { StockDataPoint } from '@/types/chart';
import { composeShikihoDailyOverlay } from './shikihoDailyOverlay';

const quote: ShikihoQuoteV1 = {
  tradingDate: '2026-07-13',
  observedAt: '2026-07-13T01:30:00Z',
  delayMinutes: 15,
  currentPrice: 120,
  open: 112,
  high: 125,
  low: 110,
  previousClose: 108,
  volume: 123_000,
  openTime: '09:00',
  highTime: '10:00',
  lowTime: '09:10',
  sourceLabel: '会社四季報オンライン',
};

const bars: StockDataPoint[] = Array.from({ length: 9 }, (_, index) => ({
  time: `2026-07-${String(index + 2).padStart(2, '0')}`,
  open: 99 + index,
  high: 101 + index,
  low: 98 + index,
  close: 100 + index,
  volume: 10_000 + index,
}));

const ranking: MarketRankingSymbolResponse = {
  date: '2026-07-10',
  lastUpdated: '2026-07-10T08:00:00Z',
  item: {
    rank: 1,
    code: '7203',
    companyName: 'Toyota',
    marketCode: '0111',
    sector33Name: 'Transport',
    currentPrice: 108,
    previousPrice: 107,
    basePrice: 100,
    changeAmount: 1,
    changePercentage: 0.934,
    volume: 99_000,
    per: 10,
    forwardPer: 8,
    pbr: 1.2,
    psr: 2,
    forwardPsr: 1.5,
    marketCap: 1_080,
    sma5AboveCount5d: 4,
    sma5BelowStreak: 0,
    valueCompositeScore: 88,
    perPercentile: 12,
    sectorStrengthScore: 0.7,
    tradingValue: 999,
    forecastOperatingProfitGrowthRatio: 1.4,
  },
};

const valuation: ApiDailyValuationDataPoint = {
  date: '2026-07-10',
  close: 108,
  eps: 12,
  forwardEps: 15,
  bps: 100,
  sales: 540,
  forwardSales: 720,
  per: 9,
  forwardPer: 7.2,
  pbr: 1.08,
  psr: 2,
  forwardPsr: 1.5,
  marketCap: 1_080,
  freeFloatMarketCap: 864,
};

function compose(overrides: Partial<Parameters<typeof composeShikihoDailyOverlay>[0]> = {}) {
  return composeShikihoDailyOverlay({
    selectedSymbol: '7203',
    quoteCode: '7203',
    quote,
    dailyBars: bars,
    rankingResponse: ranking,
    latestValuation: valuation,
    marketCaps: { issuedShares: 1_080, freeFloat: 864 },
    relativeMode: false,
    chartSmaPeriod: 5,
    now: new Date('2026-07-13T01:44:59.999Z'),
    ...overrides,
  });
}

describe('composeShikihoDailyOverlay', () => {
  it('appends one validated current-JST provisional bar without mutating inputs', () => {
    const originalBars = structuredClone(bars);
    const originalRanking = structuredClone(ranking);
    const result = compose();

    expect(result.dailyBars).toHaveLength(10);
    expect(result.dailyBars.at(-1)).toEqual({
      time: '2026-07-13',
      open: 112,
      high: 125,
      low: 110,
      close: 120,
      volume: 123_000,
    });
    expect(result.provenance).toEqual({
      provisional: true,
      tradingDate: '2026-07-13',
      observedAt: quote.observedAt,
      delayMinutes: 15,
      sourceLabel: '会社四季報オンライン',
    });
    expect(bars).toEqual(originalBars);
    expect(ranking).toEqual(originalRanking);
    expect(result.dailyBars).not.toBe(bars);
    expect(result.rankingResponse).not.toBe(ranking);
  });

  it.each([
    ['relative mode', { relativeMode: true }],
    ['symbol mismatch', { quoteCode: '6758' }],
    ['different trading date', { quote: { ...quote, tradingDate: '2026-07-12' } }],
    ['invalid OHLC', { quote: { ...quote, high: 115 } }],
  ])('does not overlay for %s', (_label, overrides) => {
    const result = compose(overrides);
    expect(result.provenance).toBeNull();
    expect(result.dailyBars).toBe(bars);
    expect(result.rankingResponse).toBe(ranking);
  });

  it.each([
    ['exactly 15 minutes old', '2026-07-13T01:45:00.000Z', '2026-07-13T02:00:00.000Z'],
    ['future observation', '2026-07-13T02:00:00.001Z', '2026-07-13T02:00:00.000Z'],
    ['malformed observation', 'not-a-date', '2026-07-13T02:00:00.000Z'],
  ])('rejects a %s quote', (_label, observedAt, now) => {
    const result = compose({ quote: { ...quote, observedAt }, now: new Date(now) });
    expect(result.provenance).toBeNull();
  });

  it('accepts a quote observed 14:59.999 ago', () => {
    const result = compose({
      quote: { ...quote, observedAt: '2026-07-13T01:45:00.001Z' },
      now: new Date('2026-07-13T02:00:00.000Z'),
    });
    expect(result.provenance?.provisional).toBe(true);
  });

  it('keeps an official same-date row instead of replacing it', () => {
    const latestBar = bars[bars.length - 1];
    if (latestBar === undefined) throw new Error('fixture requires a latest bar');
    const sameDate = [...bars, { ...latestBar, time: quote.tradingDate }];
    const result = compose({ dailyBars: sameDate });
    expect(result.provenance).toBeNull();
    expect(result.dailyBars).toBe(sameDate);
  });

  it('keeps an official same-date row when the quote observation is stale', () => {
    const latestBar = bars[bars.length - 1];
    if (latestBar === undefined) throw new Error('fixture requires a latest bar');
    const sameDate = [...bars, { ...latestBar, time: quote.tradingDate }];
    const result = compose({
      dailyBars: sameDate,
      quote: { ...quote, observedAt: '2026-07-13T01:45:00.000Z' },
      now: new Date('2026-07-13T02:00:00.000Z'),
    });
    expect(result.provenance).toBeNull();
    expect(result.dailyBars).toBe(sameDate);
  });

  it('omits unknown volume from the provisional bar and preserves ranking volume and trading value', () => {
    const result = compose({ quote: { ...quote, volume: null } });
    expect(result.dailyBars.at(-1)).not.toHaveProperty('volume');
    expect(result.rankingResponse?.item).toMatchObject({ volume: 99_000, tradingValue: 999 });
  });

  it('recomputes day change and current SMA5 count/streak while preserving cross-sectional fields', () => {
    const result = compose();
    expect(result.rankingResponse?.item).toMatchObject({
      currentPrice: 120,
      previousPrice: 108,
      changeAmount: 12,
      changePercentage: 100 / 9,
      volume: 123_000,
      sma5AboveCount5d: 5,
      sma5BelowStreak: 0,
      valueCompositeScore: 88,
      perPercentile: 12,
      sectorStrengthScore: 0.7,
      tradingValue: 999,
      forecastOperatingProfitGrowthRatio: 1.4,
    });
    expect(result.chartSmaPoint).toEqual({ time: '2026-07-13', value: 109.2 });
  });

  it('returns null SMA5-derived ranking metrics when history is insufficient', () => {
    const result = compose({ dailyBars: bars.slice(-3) });
    expect(result.chartSmaPoint).toBeNull();
    expect(result.rankingResponse?.item).toMatchObject({ sma5AboveCount5d: null, sma5BelowStreak: null });
  });

  it('computes the configured chart SMA period instead of appending SMA5', () => {
    const result = compose({ chartSmaPeriod: 3 });
    expect(result.chartSmaPoint?.value).toBeCloseTo((107 + 108 + 120) / 3);
    expect(result.rankingResponse?.item?.sma5AboveCount5d).toBe(5);
  });

  it('omits a provisional chart SMA point when the configured period is unavailable', () => {
    expect(compose({ chartSmaPeriod: undefined }).chartSmaPoint).toBeNull();
    expect(compose({ chartSmaPeriod: 20 }).chartSmaPoint).toBeNull();
  });

  it('uses stable denominators for valuations and scales market caps', () => {
    const result = compose();
    expect(result.rankingResponse?.item).toMatchObject({
      per: 10,
      forwardPer: 8,
      pbr: 1.2,
      psr: 20 / 9,
      forwardPsr: 5 / 3,
      marketCap: 1_200,
    });
    expect(result.marketCaps).toEqual({ issuedShares: 1_200, freeFloat: 960 });
  });

  it('falls back to price scaling and preserves null price-linear values', () => {
    const response = structuredClone(ranking);
    if (response.item) {
      response.item.forwardPer = null;
      response.item.pbr = 1.08;
    }
    const result = compose({ latestValuation: null, rankingResponse: response });
    expect(result.rankingResponse?.item).toMatchObject({
      per: 100 / 9,
      forwardPer: null,
      psr: 20 / 9,
      forwardPsr: 5 / 3,
      marketCap: 1_200,
    });
    expect(result.rankingResponse?.item?.pbr).toBeCloseTo(1.2);
  });
});
