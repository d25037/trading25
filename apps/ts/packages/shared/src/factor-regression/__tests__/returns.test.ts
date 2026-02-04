import { describe, expect, it } from 'bun:test';
import { alignReturns, calculateDailyReturns, calculateSimpleReturns, type PriceData } from '../returns';

describe('calculateDailyReturns', () => {
  it('calculates log returns for two prices', () => {
    const data: PriceData[] = [
      { date: '2024-01-01', close: 100 },
      { date: '2024-01-02', close: 110 },
    ];
    const returns = calculateDailyReturns(data);
    expect(returns).toHaveLength(1);
    expect(returns[0]?.return).toBeCloseTo(Math.log(110 / 100), 10);
    expect(returns[0]?.date).toBe('2024-01-02');
  });

  it('returns empty for insufficient data', () => {
    expect(calculateDailyReturns([])).toEqual([]);
    expect(calculateDailyReturns([{ date: '2024-01-01', close: 100 }])).toEqual([]);
  });

  it('skips zero and negative prices', () => {
    const data: PriceData[] = [
      { date: '2024-01-01', close: 100 },
      { date: '2024-01-02', close: 0 },
      { date: '2024-01-03', close: 110 },
    ];
    const returns = calculateDailyReturns(data);
    // (100→0) skipped because close<=0, (0→110) skipped because previous<=0
    expect(returns).toHaveLength(0);
  });
});

describe('calculateSimpleReturns', () => {
  it('calculates arithmetic returns', () => {
    const data: PriceData[] = [
      { date: '2024-01-01', close: 100 },
      { date: '2024-01-02', close: 110 },
    ];
    const returns = calculateSimpleReturns(data);
    expect(returns).toHaveLength(1);
    expect(returns[0]?.return).toBeCloseTo(0.1, 10);
  });

  it('returns empty for insufficient data', () => {
    expect(calculateSimpleReturns([])).toEqual([]);
    expect(calculateSimpleReturns([{ date: '2024-01-01', close: 100 }])).toEqual([]);
  });

  it('skips zero previous price', () => {
    const data: PriceData[] = [
      { date: '2024-01-01', close: 0 },
      { date: '2024-01-02', close: 110 },
    ];
    const returns = calculateSimpleReturns(data);
    expect(returns).toHaveLength(0);
  });
});

describe('alignReturns', () => {
  it('aligns only overlapping dates', () => {
    const stock = [
      { date: '2024-01-01', return: 0.01 },
      { date: '2024-01-02', return: 0.02 },
      { date: '2024-01-03', return: 0.03 },
    ];
    const index = [
      { date: '2024-01-02', return: 0.05 },
      { date: '2024-01-03', return: 0.06 },
      { date: '2024-01-04', return: 0.07 },
    ];
    const aligned = alignReturns(stock, index);
    expect(aligned.dates).toEqual(['2024-01-02', '2024-01-03']);
    expect(aligned.stockReturns).toEqual([0.02, 0.03]);
    expect(aligned.indexReturns).toEqual([0.05, 0.06]);
  });

  it('returns empty when no overlapping dates', () => {
    const stock = [{ date: '2024-01-01', return: 0.01 }];
    const index = [{ date: '2024-01-02', return: 0.05 }];
    const aligned = alignReturns(stock, index);
    expect(aligned.dates).toHaveLength(0);
    expect(aligned.stockReturns).toHaveLength(0);
    expect(aligned.indexReturns).toHaveLength(0);
  });

  it('handles partial overlap', () => {
    const stock = [
      { date: '2024-01-01', return: 0.01 },
      { date: '2024-01-02', return: 0.02 },
    ];
    const index = [{ date: '2024-01-02', return: 0.05 }];
    const aligned = alignReturns(stock, index);
    expect(aligned.dates).toEqual(['2024-01-02']);
  });
});
