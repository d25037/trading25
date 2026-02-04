import { describe, expect, it } from 'bun:test';
import {
  buildMarketCodeFilter,
  buildOrderLimitClause,
  mapToRankingItem,
  RANKING_BASE_COLUMNS,
  VALID_MARKET_CODES,
} from './query-builder-helpers';

describe('buildMarketCodeFilter', () => {
  it('returns empty clause for undefined', () => {
    const result = buildMarketCodeFilter(undefined);
    expect(result.clause).toBe('');
    expect(result.params).toEqual([]);
  });

  it('returns empty clause for empty array', () => {
    const result = buildMarketCodeFilter([]);
    expect(result.clause).toBe('');
  });

  it('builds clause for valid codes', () => {
    const result = buildMarketCodeFilter(['prime', 'growth']);
    expect(result.clause).toContain('IN');
    expect(result.params).toEqual(['prime', 'growth']);
  });

  it('throws for invalid codes', () => {
    expect(() => buildMarketCodeFilter(['invalid'])).toThrow('Invalid market codes');
  });
});

describe('buildOrderLimitClause', () => {
  it('builds DESC clause by default', () => {
    expect(buildOrderLimitClause('price')).toContain('DESC');
  });

  it('builds ASC clause', () => {
    expect(buildOrderLimitClause('price', 'ASC')).toContain('ASC');
  });
});

describe('mapToRankingItem', () => {
  it('maps row to RankingItem', () => {
    const row = {
      code: '7203',
      company_name: 'Toyota',
      market_code: 'prime',
      sector33_name: 'Transport',
      current_price: 2500,
      volume: 1000000,
      trading_value: 2500000000,
      previous_price: 2400,
      change_amount: 100,
      change_percentage: 4.17,
    };
    const item = mapToRankingItem(row, 0, 5);
    expect(item.rank).toBe(1);
    expect(item.code).toBe('7203');
    expect(item.companyName).toBe('Toyota');
    expect(item.lookbackDays).toBe(5);
    expect(item.tradingValue).toBe(2500000000);
  });
});

describe('constants', () => {
  it('VALID_MARKET_CODES has 3 entries', () => {
    expect(VALID_MARKET_CODES).toHaveLength(3);
  });

  it('RANKING_BASE_COLUMNS contains expected columns', () => {
    expect(RANKING_BASE_COLUMNS).toContain('s.code');
    expect(RANKING_BASE_COLUMNS).toContain('s.company_name');
  });
});
