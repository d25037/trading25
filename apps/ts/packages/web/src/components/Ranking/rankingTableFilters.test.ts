import type { RankingItem } from '@trading25/contracts/types/api-response-types';
import { describe, expect, it } from 'vitest';
import { countActiveDailyRankingTableFilters, filterDailyRankingItems } from './rankingTableFilters';

const baseItem: RankingItem = {
  rank: 1,
  code: '6758',
  companyName: 'Sony Group',
  marketCode: 'prime',
  sector33Name: 'Electric Appliances',
  currentPrice: 3000,
  volume: 1_000_000,
  tradingValue: 3_000_000_000,
  changePercentage: 2.4,
  sma5AboveCount5d: 3,
  per: 18,
  forwardPer: 14,
  pbr: 1.3,
  marketCap: 10_000_000_000_000,
  liquidityRegime: 'neutral_rerating',
  liquidityResidualZ: -0.8,
  sectorStrengthScore: 0.75,
};

function item(overrides: Partial<RankingItem>): RankingItem {
  return { ...baseItem, ...overrides };
}

describe('rankingTableFilters', () => {
  const items = [
    baseItem,
    item({
      rank: 2,
      code: '7203',
      companyName: 'Toyota Motor',
      sector33Name: 'Transportation Equipment',
      marketCode: 'prime',
      tradingValue: 5_000_000_000,
      changePercentage: -1.2,
      sma5AboveCount5d: 5,
      per: 12,
      forwardPer: 9,
      pbr: 0.8,
      liquidityRegime: 'crowded_rerating',
      riskFlags: ['overheat'],
      technicalFlags: ['momentum_20_60_top20'],
    }),
    item({
      rank: 3,
      code: '4478',
      companyName: 'Freee',
      sector33Name: 'Information & Communication',
      marketCode: 'growth',
      tradingValue: 300_000_000,
      changePercentage: 4.8,
      sma5AboveCount5d: 1,
      per: null,
      forwardPer: null,
      pbr: 7.2,
      liquidityRegime: 'stale_liquidity',
      riskFlags: ['stale_rally_fade'],
      technicalFlags: ['atr20_acceleration'],
    }),
  ];

  it('counts only non-empty filters', () => {
    expect(
      countActiveDailyRankingTableFilters({
        text: 'sony',
        market: undefined,
        watchlistId: 2,
        minForwardPer: 0,
        minForecastOperatingProfitGrowthRatio: 1.2,
        minPer: 10,
        maxForwardPer: undefined,
        sector33Name: '',
      })
    ).toBe(5);
  });

  it('matches text against code and company but leaves sector to the sector filter', () => {
    expect(filterDailyRankingItems(items, { text: 'toyota' }).map((row) => row.code)).toEqual(['7203']);
    expect(filterDailyRankingItems(items, { text: '4478' }).map((row) => row.code)).toEqual(['4478']);
    expect(filterDailyRankingItems(items, { text: 'electric' }).map((row) => row.code)).toEqual([]);
  });

  it('filters category and signal fields together', () => {
    expect(
      filterDailyRankingItems(items, {
        market: 'prime',
        regimeState: 'crowded_rerating',
        riskState: 'overheat',
        technicalState: 'momentum_20_60_top20',
      }).map((row) => row.code)
    ).toEqual(['7203']);
  });

  it('applies inclusive numeric ranges and excludes null values when bounded', () => {
    expect(
      filterDailyRankingItems(items, {
        minChangePct: -2,
        maxChangePct: 3,
        minForwardPer: 8,
        maxForwardPer: 15,
      }).map((row) => row.code)
    ).toEqual(['6758', '7203']);
  });

  it('filters actual PER separately from forward PER', () => {
    expect(filterDailyRankingItems(items, { minPer: 10 }).map((row) => row.code)).toEqual(['6758', '7203']);
    expect(filterDailyRankingItems(items, { minForwardPer: 10 }).map((row) => row.code)).toEqual(['6758']);
  });

  it('filters SMA5 count with inclusive numeric ranges', () => {
    expect(
      filterDailyRankingItems(items, { minSma5AboveCount5d: 2, maxSma5AboveCount5d: 4 }).map((row) => row.code)
    ).toEqual(['6758']);
  });

  it('filters to stocks included in a selected watchlist code set', () => {
    const watchlistCodes = new Set(['7203', '9984']);

    expect(filterDailyRankingItems(items, { watchlistId: 12 }, watchlistCodes).map((row) => row.code)).toEqual([
      '7203',
    ]);
    expect(filterDailyRankingItems(items, { watchlistId: 12 }, new Set()).map((row) => row.code)).toEqual([]);
  });

  it('filters PSR separately from forward PSR', () => {
    const psrItems = [
      item({ code: '1001', psr: 1.1, forwardPsr: 0.9 }),
      item({ code: '1002', psr: 3.4, forwardPsr: 1.8 }),
      item({ code: '1003', psr: 5.2, forwardPsr: 4.1 }),
      item({ code: '1004', psr: null, forwardPsr: null }),
    ];

    expect(filterDailyRankingItems(psrItems, { minPsr: 2, maxPsr: 4 }).map((row) => row.code)).toEqual(['1002']);
    expect(filterDailyRankingItems(psrItems, { minForwardPsr: 1, maxForwardPsr: 2 }).map((row) => row.code)).toEqual([
      '1002',
    ]);
  });

  it('filters Fwd OP/OP with inclusive numeric ranges', () => {
    const growthItems = [
      item({ code: '1001', forecastOperatingProfitGrowthRatio: 0.7 }),
      item({ code: '1002', forecastOperatingProfitGrowthRatio: 1.2 }),
      item({ code: '1003', forecastOperatingProfitGrowthRatio: 1.6 }),
      item({ code: '1004', forecastOperatingProfitGrowthRatio: null }),
    ];

    expect(
      filterDailyRankingItems(growthItems, {
        minForecastOperatingProfitGrowthRatio: 1.0,
        maxForecastOperatingProfitGrowthRatio: 1.5,
      }).map((row) => row.code)
    ).toEqual(['1002']);
  });

  it('filters valuation signals derived from table item metrics', () => {
    const signalItems = [
      item({ code: '1001', pbrPercentile: 0.15, forwardPerPercentile: 0.5 }),
      item({ code: '1002', pbrPercentile: 0.95 }),
      item({ code: '1003', perPercentile: null, forwardPerPercentile: null }),
    ];

    expect(filterDailyRankingItems(signalItems, { valuationSignal: 'undervalued' }).map((row) => row.code)).toEqual([
      '1001',
    ]);
    expect(filterDailyRankingItems(signalItems, { valuationSignal: 'very_overvalued' }).map((row) => row.code)).toEqual(
      ['1002']
    );
    expect(filterDailyRankingItems(signalItems, { valuationSignal: 'no_earnings' }).map((row) => row.code)).toEqual([
      '1003',
    ]);
  });
});
