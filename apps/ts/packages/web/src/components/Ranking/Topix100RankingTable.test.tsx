import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import { describe, expect, it, vi } from 'vitest';
import type { SortOrder, Topix100RankingResponse, Topix100RankingSortKey } from '@/types/ranking';
import { Topix100RankingTable } from './Topix100RankingTable';

function createResponse(metric: Topix100RankingResponse['rankingMetric']): Topix100RankingResponse {
  return {
    date: '2026-03-30',
    rankingMetric: metric,
    smaWindow: 50,
    shortWindowStreaks: 3,
    longWindowStreaks: 53,
    longScoreHorizonDays: 5,
    shortScoreHorizonDays: 1,
    intradayScoreTarget: 'next_session_open_close',
    scoreSourceRunId: '20260406_180623_c0eb7f87',
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
      },
    ],
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
    expect(screen.getByText('Volume split by decile')).toBeInTheDocument();
    expect(screen.getByText('Next-session intraday score')).toBeInTheDocument();
    expect(screen.getByText('State X = 3/53')).toBeInTheDocument();
    expect(screen.getByText('Score = Next-session open → close LightGBM')).toBeInTheDocument();
    expect(screen.getByText('+12.00%')).toBeInTheDocument();
    expect(screen.getByText('+1.25%')).toBeInTheDocument();
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
    expect(screen.getByText('Intraday score = SMA50 / Vol 5/20')).toBeInTheDocument();
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
});
