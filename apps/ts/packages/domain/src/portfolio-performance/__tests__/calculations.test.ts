import { describe, expect, it } from 'bun:test';
import {
  calculateAllHoldingsPerformance,
  calculateHoldingPerformance,
  calculatePortfolioSummary,
  calculatePortfolioTimeSeries,
  formatCurrency,
  formatReturnRate,
  logToSimpleReturn,
  type PortfolioItemWithPrice,
  type PriceTimeSeries,
} from '../calculations';
import type { HoldingPerformance } from '../types';

describe('calculateHoldingPerformance', () => {
  const baseItem: PortfolioItemWithPrice = {
    code: '7203',
    companyName: 'Toyota',
    quantity: 100,
    purchasePrice: 2000,
    purchaseDate: '2024-01-01',
    currentPrice: 2500,
  };

  it('calculates profit correctly', () => {
    const result = calculateHoldingPerformance(baseItem, 250000);
    expect(result.cost).toBe(200000);
    expect(result.marketValue).toBe(250000);
    expect(result.pnl).toBe(50000);
    expect(result.returnRate).toBeCloseTo(0.25, 10);
    expect(result.weight).toBeCloseTo(1.0, 10);
  });

  it('calculates loss correctly', () => {
    const item = { ...baseItem, currentPrice: 1500 };
    const result = calculateHoldingPerformance(item, 300000);
    expect(result.pnl).toBe(-50000);
    expect(result.returnRate).toBeCloseTo(-0.25, 10);
  });

  it('handles zero cost', () => {
    const item = { ...baseItem, purchasePrice: 0 };
    const result = calculateHoldingPerformance(item, 100000);
    expect(result.returnRate).toBe(0);
  });

  it('handles zero portfolio value', () => {
    const result = calculateHoldingPerformance(baseItem, 0);
    expect(result.weight).toBe(0);
  });

  it('preserves account field', () => {
    const item = { ...baseItem, account: 'NISA' };
    const result = calculateHoldingPerformance(item, 250000);
    expect(result.account).toBe('NISA');
  });

  it('returns undefined account when not set', () => {
    const result = calculateHoldingPerformance(baseItem, 250000);
    expect(result.account).toBeUndefined();
  });
});

describe('calculatePortfolioSummary', () => {
  it('calculates summary for multiple holdings', () => {
    const holdings: HoldingPerformance[] = [
      {
        code: '7203',
        companyName: 'Toyota',
        quantity: 100,
        purchasePrice: 2000,
        currentPrice: 2500,
        cost: 200000,
        marketValue: 250000,
        pnl: 50000,
        returnRate: 0.25,
        weight: 0.5,
        purchaseDate: '2024-01-01',
      },
      {
        code: '6758',
        companyName: 'Sony',
        quantity: 50,
        purchasePrice: 3000,
        currentPrice: 5000,
        cost: 150000,
        marketValue: 250000,
        pnl: 100000,
        returnRate: 0.667,
        weight: 0.5,
        purchaseDate: '2024-01-01',
      },
    ];
    const summary = calculatePortfolioSummary(holdings);
    expect(summary.totalCost).toBe(350000);
    expect(summary.currentValue).toBe(500000);
    expect(summary.totalPnL).toBe(150000);
    expect(summary.returnRate).toBeCloseTo(150000 / 350000, 10);
  });

  it('returns zeros for empty array', () => {
    const summary = calculatePortfolioSummary([]);
    expect(summary.totalCost).toBe(0);
    expect(summary.currentValue).toBe(0);
    expect(summary.totalPnL).toBe(0);
    expect(summary.returnRate).toBe(0);
  });

  it('handles single holding', () => {
    const holdings: HoldingPerformance[] = [
      {
        code: '7203',
        companyName: 'Toyota',
        quantity: 100,
        purchasePrice: 2000,
        currentPrice: 2500,
        cost: 200000,
        marketValue: 250000,
        pnl: 50000,
        returnRate: 0.25,
        weight: 1.0,
        purchaseDate: '2024-01-01',
      },
    ];
    const summary = calculatePortfolioSummary(holdings);
    expect(summary.returnRate).toBeCloseTo(0.25, 10);
  });
});

describe('calculateAllHoldingsPerformance', () => {
  it('calculates weights summing to ~1.0 for multiple stocks', () => {
    const items: PortfolioItemWithPrice[] = [
      {
        code: '7203',
        companyName: 'Toyota',
        quantity: 100,
        purchasePrice: 2000,
        purchaseDate: '2024-01-01',
        currentPrice: 2500,
      },
      {
        code: '6758',
        companyName: 'Sony',
        quantity: 50,
        purchasePrice: 3000,
        purchaseDate: '2024-01-01',
        currentPrice: 5000,
      },
    ];
    const { holdings, summary } = calculateAllHoldingsPerformance(items);
    expect(holdings).toHaveLength(2);
    const totalWeight = holdings.reduce((sum, h) => sum + h.weight, 0);
    expect(totalWeight).toBeCloseTo(1.0, 10);
    expect(summary.totalCost).toBe(350000);
  });

  it('returns empty holdings and zero summary for empty array', () => {
    const { holdings, summary } = calculateAllHoldingsPerformance([]);
    expect(holdings).toHaveLength(0);
    expect(summary.totalCost).toBe(0);
  });
});

describe('calculatePortfolioTimeSeries', () => {
  it('calculates time series for two stocks with overlapping dates', () => {
    const stockPrices = new Map<string, PriceTimeSeries[]>([
      [
        '7203',
        [
          { date: '2024-01-01', close: 100 },
          { date: '2024-01-02', close: 110 },
          { date: '2024-01-03', close: 105 },
        ],
      ],
      [
        '6758',
        [
          { date: '2024-01-01', close: 200 },
          { date: '2024-01-02', close: 210 },
          { date: '2024-01-03', close: 220 },
        ],
      ],
    ]);
    const weights = new Map([
      ['7203', 0.5],
      ['6758', 0.5],
    ]);
    const result = calculatePortfolioTimeSeries(stockPrices, weights);
    expect(result).toHaveLength(2);
    expect(result[0]?.date).toBe('2024-01-02');
    expect(result[1]?.date).toBe('2024-01-03');
    // Cumulative return should be sum of daily returns
    expect(result[1]?.cumulativeReturn).toBeCloseTo(
      (result[0]?.dailyReturn as number) + (result[1]?.dailyReturn as number),
      10
    );
  });

  it('returns empty for insufficient dates', () => {
    const stockPrices = new Map<string, PriceTimeSeries[]>([['7203', [{ date: '2024-01-01', close: 100 }]]]);
    const weights = new Map([['7203', 1.0]]);
    const result = calculatePortfolioTimeSeries(stockPrices, weights);
    expect(result).toHaveLength(0);
  });

  it('handles partial data (stock missing some dates)', () => {
    const stockPrices = new Map<string, PriceTimeSeries[]>([
      [
        '7203',
        [
          { date: '2024-01-01', close: 100 },
          { date: '2024-01-02', close: 110 },
        ],
      ],
      ['6758', [{ date: '2024-01-01', close: 200 }]],
    ]);
    const weights = new Map([
      ['7203', 0.5],
      ['6758', 0.5],
    ]);
    const result = calculatePortfolioTimeSeries(stockPrices, weights);
    // 7203 has data for both dates, 6758 only for first
    // The total weight for 7203 on day 2 is 0.5 < 0.99, so normalizes
    expect(result).toHaveLength(1);
  });
});

describe('logToSimpleReturn', () => {
  it('converts 0 to 0', () => {
    expect(logToSimpleReturn(0)).toBeCloseTo(0, 10);
  });

  it('converts log(1.1) to ~0.1', () => {
    expect(logToSimpleReturn(Math.log(1.1))).toBeCloseTo(0.1, 10);
  });

  it('converts negative log return', () => {
    expect(logToSimpleReturn(Math.log(0.9))).toBeCloseTo(-0.1, 10);
  });
});

describe('formatReturnRate', () => {
  it('formats positive rate with + sign', () => {
    expect(formatReturnRate(0.25)).toBe('+25.00%');
  });

  it('formats negative rate with - sign', () => {
    expect(formatReturnRate(-0.1)).toBe('-10.00%');
  });

  it('formats zero as +0.00%', () => {
    expect(formatReturnRate(0)).toBe('+0.00%');
  });

  it('accepts custom decimal places', () => {
    expect(formatReturnRate(0.12345, 1)).toBe('+12.3%');
  });
});

describe('formatCurrency', () => {
  it('formats with ja-JP locale by default', () => {
    const result = formatCurrency(1234567);
    expect(result).toBe('1,234,567');
  });
});
