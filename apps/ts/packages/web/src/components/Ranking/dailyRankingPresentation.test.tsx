import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import {
  DAILY_RANKING_VALUE_METRICS,
  type DailyRankingMetric,
  type DailyRankingMetricKey,
  DailyRankingMetricValue,
  DailyRankingRegimeChip,
  DailyRankingSignalChips,
  SectorStrengthScoreChip,
} from './dailyRankingPresentation';
import type { EquityRankingItem } from './EquityRankingTable';

const representativeItem: EquityRankingItem = {
  rank: 1,
  code: '7203',
  companyName: 'Toyota Motor',
  marketCode: 'prime',
  sector33Name: 'Transport Equipment',
  sectorStrengthScore: 0.9,
  currentPrice: 3000,
  volume: 1_000_000,
  changePercentage: 2.345,
  sma5AboveCount5d: 4,
  per: 8,
  perPercentile: 0.15,
  forwardPer: 6,
  forwardPerPercentile: 0.15,
  forecastOperatingProfitGrowthRatio: 1.25,
  psr: 2,
  psrPercentile: 0.95,
  forwardPsr: 1.5,
  forwardPsrPercentile: 0.85,
  pbr: 0.5,
  pbrPercentile: 0.05,
  valueCompositeScore: 0.92,
  liquidityResidualZ: -1.4,
  liquidityRegime: 'neutral_rerating',
  tradingValue: 1_500_000_000,
  riskFlags: ['overheat'],
  technicalFlags: ['momentum_20_60_top20'],
};

function getMetric(key: DailyRankingMetricKey): DailyRankingMetric {
  const metric = DAILY_RANKING_VALUE_METRICS.find((candidate) => candidate.key === key);
  if (!metric) throw new Error(`Missing Daily Ranking metric: ${key}`);
  return metric;
}

describe('daily ranking metric presentation', () => {
  it('publishes metrics in the canonical order with canonical labels', () => {
    expect(DAILY_RANKING_VALUE_METRICS.map(({ key, label }) => [key, label])).toEqual([
      ['sectorStrengthScore', 'Sector Strength'],
      ['currentPrice', '現在値'],
      ['changePercentage', '騰落率'],
      ['sma5AboveCount5d', 'SMA5 5D'],
      ['per', 'PER'],
      ['forwardPer', 'Fwd PER'],
      ['forecastOperatingProfitGrowthRatio', 'Fwd OP/OP'],
      ['psr', 'PSR'],
      ['forwardPsr', 'Fwd PSR'],
      ['pbr', 'PBR'],
      ['valueCompositeScore', 'Value Score'],
      ['liquidityResidualZ', '流動性Z'],
      ['tradingValue', '売買代金'],
    ]);
  });

  it('formats representative values and preserves evidence text classes', () => {
    render(
      DAILY_RANKING_VALUE_METRICS.map((metric) => (
        <div key={metric.key} data-testid={metric.key}>
          <DailyRankingMetricValue item={representativeItem} metric={metric} />
        </div>
      ))
    );

    const expectedText: Record<string, string> = {
      sectorStrengthScore: '0.90',
      currentPrice: '￥3,000',
      changePercentage: '+2.35%',
      sma5AboveCount5d: '4',
      per: '8.00x',
      forwardPer: '6.00x',
      forecastOperatingProfitGrowthRatio: '1.25x',
      psr: '2.00x',
      forwardPsr: '1.50x',
      pbr: '0.50x',
      valueCompositeScore: '0.92',
      liquidityResidualZ: '-1.40',
      tradingValue: '1.50B',
    };
    for (const [key, text] of Object.entries(expectedText)) {
      expect(screen.getByTestId(key)).toHaveTextContent(text);
    }

    expect(screen.getByTestId('changePercentage').firstChild).toHaveClass('text-green-600');
    expect(screen.getByTestId('per').firstChild).toHaveClass('text-sky-600');
    expect(screen.getByTestId('forwardPer').firstChild).toHaveClass('text-green-600');
    expect(screen.getByTestId('forecastOperatingProfitGrowthRatio').firstChild).toHaveClass('text-sky-600');
    expect(screen.getByTestId('psr').firstChild).toHaveClass('text-purple-700');
    expect(screen.getByTestId('forwardPsr').firstChild).toHaveClass('text-red-600');
    expect(screen.getByTestId('pbr').firstChild).toHaveClass('text-green-600');
    expect(screen.getByTestId('valueCompositeScore').firstChild).toHaveClass('text-green-600');
    expect(screen.getByTestId('liquidityResidualZ').firstChild).toHaveClass('text-green-600');
  });

  it('formats missing and negative values without changing sign semantics', () => {
    const missingItem = {
      ...representativeItem,
      changePercentage: null,
      per: null,
      tradingValue: null,
    };
    const negativeItem = { ...representativeItem, changePercentage: -2.345 };
    const changeMetric = getMetric('changePercentage');
    const perMetric = getMetric('per');
    const tradingValueMetric = getMetric('tradingValue');

    render(
      <>
        <div data-testid="missing-change">
          <DailyRankingMetricValue item={missingItem} metric={changeMetric} />
        </div>
        <div data-testid="missing-per">
          <DailyRankingMetricValue item={missingItem} metric={perMetric} />
        </div>
        <div data-testid="missing-trading-value">
          <DailyRankingMetricValue item={missingItem} metric={tradingValueMetric} />
        </div>
        <div data-testid="negative-change">
          <DailyRankingMetricValue item={negativeItem} metric={changeMetric} />
        </div>
      </>
    );

    expect(screen.getByTestId('missing-change')).toHaveTextContent('-');
    expect(screen.getByTestId('missing-per')).toHaveTextContent('-');
    expect(screen.getByTestId('missing-trading-value')).toHaveTextContent('-');
    expect(screen.getByTestId('negative-change')).toHaveTextContent('-2.35%');
    expect(screen.getByTestId('negative-change').firstChild).toHaveClass('text-red-600');
  });
});

describe('daily ranking semantic chips', () => {
  it('preserves sector, regime, valuation, risk, and technical badge semantics', () => {
    render(
      <>
        <SectorStrengthScoreChip value={representativeItem.sectorStrengthScore} />
        <DailyRankingRegimeChip item={representativeItem} />
        <DailyRankingSignalChips item={representativeItem} />
      </>
    );

    expect(screen.getByText('0.90')).toHaveClass('text-green-700');
    expect(screen.getByText('Neutral Rerating')).toHaveClass('text-green-700');
    expect(screen.getByText('Deep Value')).toHaveClass('text-green-700');
    expect(screen.getByText('Overheat')).toHaveClass('text-purple-700');
    expect(screen.getByText('20/60D Mom')).toHaveClass('text-sky-700');
  });

  it('preserves warning and missing chip semantics', () => {
    const warningItem = {
      ...representativeItem,
      perPercentile: 0.95,
      forwardPerPercentile: 0.5,
      pbrPercentile: 0.5,
      liquidityRegime: 'distribution_stress' as const,
      riskFlags: ['stale_rally_fade' as const],
      technicalFlags: ['atr20_acceleration' as const],
    };
    render(
      <>
        <SectorStrengthScoreChip value={null} />
        <DailyRankingRegimeChip item={warningItem} />
        <DailyRankingSignalChips item={warningItem} />
      </>
    );

    expect(screen.getByTitle('Selected sector strength family score')).toHaveTextContent('-');
    expect(screen.getByText('Stress')).toHaveClass('text-yellow-800');
    expect(screen.getByText('Very Overvalued')).toHaveClass('text-red-700');
    expect(screen.getByText('Rally Fade')).toHaveClass('text-red-700');
    expect(screen.getByText('ATR Accel')).toHaveClass('text-emerald-700');
  });
});
