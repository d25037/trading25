import { describe, expect, it } from 'bun:test';
import {
  calculateBenchmarkMetrics,
  formatBenchmarkMetrics,
  generateBenchmarkTimeSeries,
  logReturnToPercent,
} from '../benchmark';
import type { PriceData } from '../regression-utils';
import type { BenchmarkMetrics, PerformanceDataPoint } from '../types';

function makePriceData(startPrice: number, changes: number[]): PriceData[] {
  const data: PriceData[] = [{ date: '2024-01-01', close: startPrice }];
  let price = startPrice;
  for (let i = 0; i < changes.length; i++) {
    const change = changes[i] as number;
    price = price * (1 + change);
    const day = String(i + 2).padStart(2, '0');
    data.push({ date: `2024-01-${day}`, close: price });
  }
  return data;
}

function makePortfolioReturns(count: number): PerformanceDataPoint[] {
  const points: PerformanceDataPoint[] = [];
  let cum = 0;
  for (let i = 0; i < count; i++) {
    const daily = 0.001 * (i % 3 === 0 ? -1 : 1);
    cum += daily;
    const month = i < 28 ? '01' : '02';
    const dayNum = i < 28 ? i + 2 : i - 26;
    points.push({
      date: `2024-${month}-${String(dayNum).padStart(2, '0')}`,
      dailyReturn: daily,
      cumulativeReturn: cum,
    });
  }
  return points;
}

describe('calculateBenchmarkMetrics', () => {
  it('returns null when portfolio returns are insufficient', () => {
    const result = calculateBenchmarkMetrics(
      makePortfolioReturns(5),
      makePriceData(
        100,
        Array.from({ length: 50 }, () => 0.01)
      ),
      '0000',
      'TOPIX',
      30
    );
    expect(result).toBeNull();
  });

  it('returns null when benchmark prices are insufficient', () => {
    const result = calculateBenchmarkMetrics(
      makePortfolioReturns(40),
      makePriceData(
        100,
        Array.from({ length: 5 }, () => 0.01)
      ),
      '0000',
      'TOPIX',
      30
    );
    expect(result).toBeNull();
  });

  it('calculates metrics for sufficient data', () => {
    const n = 60;
    const changes = Array.from({ length: n }, (_, i) => 0.005 * (i % 2 === 0 ? 1 : -1));
    const benchmarkPrices = makePriceData(100, changes);
    const portfolioReturns = makePortfolioReturns(n);
    const result = calculateBenchmarkMetrics(portfolioReturns, benchmarkPrices, '0000', 'TOPIX', 5);
    if (result) {
      expect(result.code).toBe('0000');
      expect(result.name).toBe('TOPIX');
      expect(typeof result.beta).toBe('number');
      expect(typeof result.alpha).toBe('number');
      expect(result.correlation).toBeGreaterThanOrEqual(-1);
      expect(result.correlation).toBeLessThanOrEqual(1);
      expect(result.rSquared).toBeGreaterThanOrEqual(0);
      expect(result.rSquared).toBeLessThanOrEqual(1);
    }
  });
});

describe('generateBenchmarkTimeSeries', () => {
  it('generates aligned time series', () => {
    const portfolioReturns: PerformanceDataPoint[] = [
      { date: '2024-01-02', dailyReturn: 0.01, cumulativeReturn: 0.01 },
      { date: '2024-01-03', dailyReturn: 0.02, cumulativeReturn: 0.03 },
    ];
    const benchmarkPrices: PriceData[] = [
      { date: '2024-01-01', close: 100 },
      { date: '2024-01-02', close: 105 },
      { date: '2024-01-03', close: 110 },
    ];
    const result = generateBenchmarkTimeSeries(portfolioReturns, benchmarkPrices);
    expect(result).toHaveLength(2);
    expect(result[0]?.date).toBe('2024-01-02');
    expect(result[0]?.portfolioReturn).toBe(0.01);
    expect(result[1]?.date).toBe('2024-01-03');
  });

  it('returns empty when no overlapping dates', () => {
    const portfolioReturns: PerformanceDataPoint[] = [
      { date: '2024-02-01', dailyReturn: 0.01, cumulativeReturn: 0.01 },
    ];
    const benchmarkPrices: PriceData[] = [
      { date: '2024-01-01', close: 100 },
      { date: '2024-01-02', close: 105 },
    ];
    const result = generateBenchmarkTimeSeries(portfolioReturns, benchmarkPrices);
    expect(result).toHaveLength(0);
  });
});

describe('logReturnToPercent', () => {
  it('converts log return to percentage', () => {
    expect(logReturnToPercent(0)).toBeCloseTo(0, 10);
    expect(logReturnToPercent(Math.log(1.1))).toBeCloseTo(10, 5);
  });
});

describe('formatBenchmarkMetrics', () => {
  const metrics: BenchmarkMetrics = {
    code: '0000',
    name: 'TOPIX',
    beta: 1.15,
    alpha: 0.05,
    correlation: 0.85,
    rSquared: 0.72,
    benchmarkReturn: 0.1,
    relativeReturn: 0.03,
  };

  it('formats positive alpha with + sign', () => {
    const formatted = formatBenchmarkMetrics(metrics);
    expect(formatted.beta).toBe('1.15');
    expect(formatted.alpha).toContain('+');
    expect(formatted.correlation).toBe('0.85');
    expect(formatted.rSquared).toBe('72.0%');
  });

  it('formats negative alpha with - sign', () => {
    const negMetrics = { ...metrics, alpha: -0.03 };
    const formatted = formatBenchmarkMetrics(negMetrics);
    expect(formatted.alpha).toContain('-');
  });
});
