import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import { describe, expect, it, vi } from 'vitest';
import type { SortOrder, Topix100RankingResponse, Topix100RankingSortKey } from '@/types/ranking';
import { Topix100RankingTable } from './Topix100RankingTable';

function createResponse(metric: Topix100RankingResponse['rankingMetric']): Topix100RankingResponse {
  return {
    date: '2026-03-30',
    studyMode: 'intraday',
    rankingMetric: metric,
    smaWindow: 50,
    shortWindowStreaks: 3,
    longWindowStreaks: 53,
    longScoreHorizonDays: 5,
    shortScoreHorizonDays: 1,
    scoreTarget: 'next_session_open_close',
    intradayScoreTarget: 'next_session_open_close',
    scoreModelType: 'daily_refit',
    scoreTrainWindowDays: 756,
    scoreTestWindowDays: 1,
    scoreStepDays: 1,
    scoreSplitTrainStart: '2023-01-04',
    scoreSplitTrainEnd: '2025-12-30',
    scoreSplitTestStart: null,
    scoreSplitTestEnd: null,
    scoreSplitPartialTail: false,
    scoreSourceRunId: '20260406_180623_c0eb7f87',
    primaryBenchmark: null,
    secondaryBenchmark: null,
    primaryBenchmarkReturn: null,
    secondaryBenchmarkReturn: null,
    benchmarkEntryDate: null,
    benchmarkExitDate: null,
    itemCount: 4,
    lastUpdated: '2026-03-30T00:00:00Z',
    items: [
      {
        rank: 1,
        code: '7203',
        companyName: 'Toyota',
        marketCode: 'prime',
        sector33Name: 'Transport',
        scaleCategory: 'TOPIX Core30',
        currentPrice: 3000,
        volume: 1_000_000,
        priceVsSmaGap: 0.12,
        priceSma20_80: 1.23,
        volumeSma5_20: 1.11,
        priceDecile: 1,
        priceBucket: 'q1',
        volumeBucket: 'high',
        streakShortMode: 'bullish',
        streakLongMode: 'bullish',
        streakStateKey: 'long_bullish__short_bullish',
        streakStateLabel: 'Long Bullish / Short Bullish',
        intradayScore: -0.0032,
        intradayLongRank: 3,
        intradayShortRank: 2,
        nextSessionDate: '2026-03-31',
        nextSessionIntradayReturn: 0.011,
      },
      {
        rank: 2,
        code: '6758',
        companyName: 'Sony',
        marketCode: 'prime',
        sector33Name: 'Electronics',
        scaleCategory: 'TOPIX Large70',
        currentPrice: 12000,
        volume: 800_000,
        priceVsSmaGap: -0.08,
        priceSma20_80: 0.95,
        volumeSma5_20: 0.88,
        priceDecile: 10,
        priceBucket: 'q10',
        volumeBucket: 'low',
        streakShortMode: 'bearish',
        streakLongMode: 'bearish',
        streakStateKey: 'long_bearish__short_bearish',
        streakStateLabel: 'Long Bearish / Short Bearish',
        intradayScore: 0.0125,
        intradayLongRank: 1,
        intradayShortRank: 4,
        nextSessionDate: '2026-03-31',
        nextSessionIntradayReturn: -0.004,
      },
      {
        rank: 3,
        code: '9432',
        companyName: 'NTT',
        marketCode: 'prime',
        sector33Name: 'Telecom',
        scaleCategory: 'TOPIX Mid400',
        currentPrice: 150,
        volume: 2_000_000,
        priceVsSmaGap: 0.01,
        priceSma20_80: 1.01,
        volumeSma5_20: 1.05,
        priceDecile: 3,
        priceBucket: 'q234',
        volumeBucket: 'high',
        streakShortMode: 'bullish',
        streakLongMode: 'bearish',
        streakStateKey: 'long_bearish__short_bullish',
        streakStateLabel: 'Long Bearish / Short Bullish',
        intradayScore: 0.0041,
        intradayLongRank: 2,
        intradayShortRank: 3,
        nextSessionDate: '2026-03-31',
        nextSessionIntradayReturn: 0.006,
      },
      {
        rank: 4,
        code: '8306',
        companyName: 'MUFG',
        marketCode: 'prime',
        sector33Name: 'Banks',
        scaleCategory: 'TOPIX Mid400',
        currentPrice: 1800,
        volume: 1_500_000,
        priceVsSmaGap: -0.01,
        priceSma20_80: 0.99,
        volumeSma5_20: 1.02,
        priceDecile: 7,
        priceBucket: 'other',
        volumeBucket: null,
        streakShortMode: null,
        streakLongMode: null,
        streakStateKey: null,
        streakStateLabel: null,
        intradayScore: null,
        intradayLongRank: null,
        intradayShortRank: null,
        nextSessionDate: null,
        nextSessionIntradayReturn: null,
      },
    ],
  };
}

function createPortfolioSummaryResponse(): Topix100RankingResponse {
  return {
    date: '2026-03-30',
    studyMode: 'intraday',
    rankingMetric: 'price_vs_sma_gap',
    smaWindow: 50,
    shortWindowStreaks: 3,
    longWindowStreaks: 53,
    longScoreHorizonDays: 5,
    shortScoreHorizonDays: 1,
    scoreTarget: 'next_session_open_close',
    intradayScoreTarget: 'next_session_open_close',
    scoreModelType: 'daily_refit',
    scoreTrainWindowDays: 756,
    scoreTestWindowDays: 1,
    scoreStepDays: 1,
    scoreSplitTrainStart: '2023-01-04',
    scoreSplitTrainEnd: '2025-12-30',
    scoreSplitTestStart: null,
    scoreSplitTestEnd: null,
    scoreSplitPartialTail: false,
    scoreSourceRunId: '20260406_180623_c0eb7f87',
    primaryBenchmark: null,
    secondaryBenchmark: null,
    primaryBenchmarkReturn: null,
    secondaryBenchmarkReturn: null,
    benchmarkEntryDate: null,
    benchmarkExitDate: null,
    itemCount: 8,
    lastUpdated: '2026-03-30T00:00:00Z',
    items: [
      {
        rank: 1,
        code: '6758',
        companyName: 'Sony',
        marketCode: 'prime',
        sector33Name: 'Electronics',
        scaleCategory: 'TOPIX Large70',
        currentPrice: 12000,
        volume: 800_000,
        priceVsSmaGap: -0.08,
        priceSma20_80: 0.95,
        volumeSma5_20: 0.88,
        priceDecile: 10,
        priceBucket: 'q10',
        volumeBucket: 'low',
        streakShortMode: 'bearish',
        streakLongMode: 'bearish',
        streakStateKey: 'long_bearish__short_bearish',
        streakStateLabel: 'Long Bearish / Short Bearish',
        intradayScore: 0.0125,
        intradayLongRank: 1,
        intradayShortRank: 8,
        nextSessionDate: '2026-03-31',
        nextSessionIntradayReturn: -0.004,
      },
      {
        rank: 2,
        code: '9432',
        companyName: 'NTT',
        marketCode: 'prime',
        sector33Name: 'Telecom',
        scaleCategory: 'TOPIX Large70',
        currentPrice: 150,
        volume: 2_000_000,
        priceVsSmaGap: 0.01,
        priceSma20_80: 1.01,
        volumeSma5_20: 1.05,
        priceDecile: 3,
        priceBucket: 'q234',
        volumeBucket: 'high',
        streakShortMode: 'bullish',
        streakLongMode: 'bearish',
        streakStateKey: 'long_bearish__short_bullish',
        streakStateLabel: 'Long Bearish / Short Bullish',
        intradayScore: 0.0041,
        intradayLongRank: 2,
        intradayShortRank: 7,
        nextSessionDate: '2026-03-31',
        nextSessionIntradayReturn: 0.006,
      },
      {
        rank: 3,
        code: '6501',
        companyName: 'Hitachi',
        marketCode: 'prime',
        sector33Name: 'Electric Machinery',
        scaleCategory: 'TOPIX Large70',
        currentPrice: 4200,
        volume: 700_000,
        priceVsSmaGap: 0.04,
        priceSma20_80: 1.04,
        volumeSma5_20: 0.93,
        priceDecile: 2,
        priceBucket: 'q234',
        volumeBucket: 'high',
        streakShortMode: 'bullish',
        streakLongMode: 'bullish',
        streakStateKey: 'long_bullish__short_bullish',
        streakStateLabel: 'Long Bullish / Short Bullish',
        intradayScore: 0.0021,
        intradayLongRank: 3,
        intradayShortRank: 6,
        nextSessionDate: '2026-03-31',
        nextSessionIntradayReturn: -0.007,
      },
      {
        rank: 4,
        code: '7203',
        companyName: 'Toyota',
        marketCode: 'prime',
        sector33Name: 'Transport',
        scaleCategory: 'TOPIX Core30',
        currentPrice: 3000,
        volume: 1_000_000,
        priceVsSmaGap: 0.12,
        priceSma20_80: 1.23,
        volumeSma5_20: 1.11,
        priceDecile: 1,
        priceBucket: 'q1',
        volumeBucket: 'high',
        streakShortMode: 'bullish',
        streakLongMode: 'bullish',
        streakStateKey: 'long_bullish__short_bullish',
        streakStateLabel: 'Long Bullish / Short Bullish',
        intradayScore: -0.0032,
        intradayLongRank: 4,
        intradayShortRank: 5,
        nextSessionDate: '2026-03-31',
        nextSessionIntradayReturn: 0.011,
      },
      {
        rank: 5,
        code: '6861',
        companyName: 'Keyence',
        marketCode: 'prime',
        sector33Name: 'Electric Machinery',
        scaleCategory: 'TOPIX Core30',
        currentPrice: 64000,
        volume: 120_000,
        priceVsSmaGap: -0.02,
        priceSma20_80: 0.98,
        volumeSma5_20: 1.08,
        priceDecile: 8,
        priceBucket: 'other',
        volumeBucket: 'low',
        streakShortMode: 'bearish',
        streakLongMode: 'bullish',
        streakStateKey: 'long_bullish__short_bearish',
        streakStateLabel: 'Long Bullish / Short Bearish',
        intradayScore: -0.0065,
        intradayLongRank: 5,
        intradayShortRank: 4,
        nextSessionDate: '2026-03-31',
        nextSessionIntradayReturn: -0.003,
      },
      {
        rank: 6,
        code: '8306',
        companyName: 'MUFG',
        marketCode: 'prime',
        sector33Name: 'Banks',
        scaleCategory: 'TOPIX Core30',
        currentPrice: 1800,
        volume: 1_500_000,
        priceVsSmaGap: -0.01,
        priceSma20_80: 0.99,
        volumeSma5_20: 1.02,
        priceDecile: 7,
        priceBucket: 'other',
        volumeBucket: 'low',
        streakShortMode: 'bearish',
        streakLongMode: 'bearish',
        streakStateKey: 'long_bearish__short_bearish',
        streakStateLabel: 'Long Bearish / Short Bearish',
        intradayScore: -0.008,
        intradayLongRank: 6,
        intradayShortRank: 3,
        nextSessionDate: '2026-03-31',
        nextSessionIntradayReturn: 0.002,
      },
      {
        rank: 7,
        code: '8035',
        companyName: 'Tokyo Electron',
        marketCode: 'prime',
        sector33Name: 'Electric Machinery',
        scaleCategory: 'TOPIX Core30',
        currentPrice: 28000,
        volume: 300_000,
        priceVsSmaGap: -0.09,
        priceSma20_80: 0.91,
        volumeSma5_20: 0.86,
        priceDecile: 9,
        priceBucket: 'other',
        volumeBucket: 'low',
        streakShortMode: 'bearish',
        streakLongMode: 'bearish',
        streakStateKey: 'long_bearish__short_bearish',
        streakStateLabel: 'Long Bearish / Short Bearish',
        intradayScore: -0.011,
        intradayLongRank: 7,
        intradayShortRank: 2,
        nextSessionDate: '2026-03-31',
        nextSessionIntradayReturn: 0.009,
      },
      {
        rank: 8,
        code: '9984',
        companyName: 'SoftBank Group',
        marketCode: 'prime',
        sector33Name: 'Telecom',
        scaleCategory: 'TOPIX Large70',
        currentPrice: 9000,
        volume: 600_000,
        priceVsSmaGap: -0.12,
        priceSma20_80: 0.89,
        volumeSma5_20: 0.82,
        priceDecile: 10,
        priceBucket: 'q10',
        volumeBucket: 'low',
        streakShortMode: 'bearish',
        streakLongMode: 'bearish',
        streakStateKey: 'long_bearish__short_bearish',
        streakStateLabel: 'Long Bearish / Short Bearish',
        intradayScore: -0.015,
        intradayLongRank: 8,
        intradayShortRank: 1,
        nextSessionDate: '2026-03-31',
        nextSessionIntradayReturn: -0.012,
      },
    ],
  };
}

function createSwingResponse(): Topix100RankingResponse {
  const base = createResponse('price_vs_sma_gap');
  return {
    ...base,
    studyMode: 'swing_5d',
    scoreTarget: 'next_session_open_to_close_5d',
    intradayScoreTarget: 'next_session_open_to_close_5d',
    primaryBenchmark: 'topix',
    secondaryBenchmark: 'topix100_universe',
    primaryBenchmarkReturn: 0.006,
    secondaryBenchmarkReturn: 0.004,
    benchmarkEntryDate: '2026-03-31',
    benchmarkExitDate: '2026-04-04',
    items: base.items.map((item, index) => ({
      ...item,
      longScore5d: 0.012 - index * 0.002,
      longScore5dRank: index + 1,
      swingEntryDate: '2026-03-31',
      swingExitDate: '2026-04-04',
      openToClose5dReturn: [0.018, 0.009, 0.007, null][index] ?? null,
    })),
  };
}

describe('Topix100RankingTable', () => {
  it('renders the price / SMA50 gap mode and row clicks', async () => {
    const user = userEvent.setup();
    const onStockClick = vi.fn();

    render(
      <Topix100RankingTable
        data={createResponse('price_vs_sma_gap')}
        isLoading={false}
        error={null}
        onStockClick={onStockClick}
        studyMode="intraday"
        rankingMetric="price_vs_sma_gap"
        rankingSmaWindow={50}
        priceBucketFilter="all"
        volumeBucketFilter="all"
        shortModeFilter="all"
        longModeFilter="all"
        sortBy="intradayScore"
        sortOrder="asc"
        onSortChange={vi.fn()}
      />
    );

    expect(screen.getAllByText('Price / SMA50 Gap')).toHaveLength(2);
    expect(screen.getByText('Q10 = below SMA')).toBeInTheDocument();
    expect(screen.getByText('Q2-4 = trough')).toBeInTheDocument();
    expect(screen.getByText('Decile-only score')).toBeInTheDocument();
    expect(screen.getByText('Volume/state = context')).toBeInTheDocument();
    expect(screen.getByText('State X = 3/53')).toBeInTheDocument();
    expect(
      screen.getByText('Score = daily fresh-fit LightGBM (trailing 756 signal days, cadence 1)')
    ).toBeInTheDocument();
    expect(screen.getByText('Train = 2023-01-04 -> 2025-12-30')).toBeInTheDocument();
    expect(screen.getByText('Realized = next available open -> close when present')).toBeInTheDocument();
    expect(screen.getByText('+12.00%')).toBeInTheDocument();
    expect(screen.getByText('+1.25%')).toBeInTheDocument();
    expect(screen.getByText('+1.10%')).toBeInTheDocument();
    expect(screen.getAllByText('2026-03-31').length).toBeGreaterThan(0);
    expect(screen.getByText('Toyota')).toBeInTheDocument();
    expect(screen.queryByText('Q2-4 Trough')).not.toBeInTheDocument();
    expect(screen.getAllByText('Bullish').length).toBeGreaterThan(0);

    await user.click(screen.getByText('7203'));
    expect(onStockClick).toHaveBeenCalledWith('7203');
  });

  it('renders the legacy SMA ratio mode and applies bucket filters', () => {
    render(
      <Topix100RankingTable
        data={createResponse('price_sma_20_80')}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        studyMode="intraday"
        rankingMetric="price_vs_sma_gap"
        rankingSmaWindow={50}
        priceBucketFilter="q10"
        volumeBucketFilter="low"
        shortModeFilter="bearish"
        longModeFilter="bearish"
        sortBy="intradayScore"
        sortOrder="asc"
        onSortChange={vi.fn()}
      />
    );

    expect(screen.getAllByText('Price SMA 20/80')).toHaveLength(2);
    expect(screen.getByText('Legacy comparison')).toBeInTheDocument();
    expect(screen.getByText('Decile-only intraday score')).toBeInTheDocument();
    expect(screen.getByText('0.95x')).toBeInTheDocument();
    expect(screen.getByText('Sony')).toBeInTheDocument();
    expect(screen.queryByText('Toyota')).not.toBeInTheDocument();
    expect(screen.queryByText('NTT')).not.toBeInTheDocument();
  });

  it('filters by the streak short and long state', () => {
    render(
      <Topix100RankingTable
        data={createResponse('price_vs_sma_gap')}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        studyMode="intraday"
        rankingMetric="price_vs_sma_gap"
        rankingSmaWindow={50}
        priceBucketFilter="all"
        volumeBucketFilter="all"
        shortModeFilter="bearish"
        longModeFilter="bearish"
        sortBy="rank"
        sortOrder="asc"
        onSortChange={vi.fn()}
      />
    );

    expect(screen.getByText('Sony')).toBeInTheDocument();
    expect(screen.queryByText('Toyota')).not.toBeInTheDocument();
    expect(screen.queryByText('NTT')).not.toBeInTheDocument();
  });

  it('shows the empty state when filters remove every row', () => {
    render(
      <Topix100RankingTable
        data={createResponse('price_vs_sma_gap')}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        studyMode="intraday"
        rankingMetric="price_vs_sma_gap"
        rankingSmaWindow={50}
        priceBucketFilter="q1"
        volumeBucketFilter="low"
        shortModeFilter="all"
        longModeFilter="all"
        sortBy="intradayScore"
        sortOrder="asc"
        onSortChange={vi.fn()}
      />
    );

    expect(screen.getByText('No TOPIX100 ranking data available')).toBeInTheDocument();
  });

  it('sorts rows when a column header is clicked', async () => {
    const user = userEvent.setup();

    function Harness() {
      const [sortBy, setSortBy] = useState<Topix100RankingSortKey>('intradayScore');
      const [sortOrder, setSortOrder] = useState<SortOrder>('desc');

      return (
        <Topix100RankingTable
          data={createResponse('price_vs_sma_gap')}
          isLoading={false}
          error={null}
          onStockClick={vi.fn()}
          studyMode="intraday"
          rankingMetric="price_vs_sma_gap"
          rankingSmaWindow={50}
          priceBucketFilter="all"
          volumeBucketFilter="all"
          shortModeFilter="all"
          longModeFilter="all"
          sortBy={sortBy}
          sortOrder={sortOrder}
          onSortChange={(nextSortBy, nextSortOrder) => {
            setSortBy(nextSortBy);
            setSortOrder(nextSortOrder);
          }}
        />
      );
    }

    render(<Harness />);

    const rowsBefore = screen.getAllByRole('row').slice(1);
    expect(rowsBefore[0]).toHaveTextContent('6758');

    await user.click(screen.getByRole('button', { name: /ID Score/i }));

    const rowsAfter = screen.getAllByRole('row').slice(1);
    expect(rowsAfter[0]).toHaveTextContent('7203');
    expect(rowsAfter[1]).toHaveTextContent('9432');
    expect(rowsAfter[2]).toHaveTextContent('6758');
  });

  it('renders snapshot portfolio summaries and top3/bottom3 book edges', () => {
    render(
      <Topix100RankingTable
        data={createPortfolioSummaryResponse()}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        studyMode="intraday"
        rankingMetric="price_vs_sma_gap"
        rankingSmaWindow={50}
        priceBucketFilter="all"
        volumeBucketFilter="all"
        shortModeFilter="all"
        longModeFilter="all"
        sortBy="intradayScore"
        sortOrder="desc"
        onSortChange={vi.fn()}
      />
    );

    expect(screen.getByText('Snapshot Books')).toBeInTheDocument();
    expect(screen.getByText('Research comparison uses `Pair 50/50`.')).toBeInTheDocument();
    expect(screen.getByText('Top 1 / Bottom 1')).toBeInTheDocument();
    expect(screen.getByText('Top 3 / Bottom 3')).toBeInTheDocument();
    expect(screen.getByText('Book3')).toBeInTheDocument();
    expect(screen.getByText('Edge3')).toBeInTheDocument();
    expect(screen.getByText('L1')).toBeInTheDocument();
    expect(screen.getByText('S1')).toBeInTheDocument();
    expect(screen.getByText('-0.07%')).toBeInTheDocument();
    expect(screen.getByText('long -0.17% | short edge +0.03% | gross -0.13% | 2026-03-31')).toBeInTheDocument();
  });

  it('renders swing summary versus TOPIX and TOPIX100 universe', () => {
    render(
      <Topix100RankingTable
        data={createSwingResponse()}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        studyMode="swing_5d"
        rankingMetric="price_vs_sma_gap"
        rankingSmaWindow={50}
        priceBucketFilter="all"
        volumeBucketFilter="all"
        shortModeFilter="all"
        longModeFilter="all"
        sortBy="longScore5d"
        sortOrder="desc"
        onSortChange={vi.fn()}
      />
    );

    expect(screen.getByText('KPI = vs TOPIX')).toBeInTheDocument();
    expect(screen.getByText('Check = vs TOPIX100 EW')).toBeInTheDocument();
    expect(screen.getByText('Realized = next available open -> 5th close when present')).toBeInTheDocument();
    expect(screen.getByText('Swing Summary')).toBeInTheDocument();
    expect(screen.getByText('Top 1')).toBeInTheDocument();
    expect(screen.getAllByText('+1.20%').length).toBeGreaterThan(0);
    expect(screen.getByText('raw +1.80% | vs TOPIX100 EW +1.40% | 2026-03-31 -> 2026-04-04')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /5D Score/i })).toBeInTheDocument();
    expect(screen.getAllByText('2026-03-31 -> 2026-04-04').length).toBeGreaterThan(0);
  });
});
