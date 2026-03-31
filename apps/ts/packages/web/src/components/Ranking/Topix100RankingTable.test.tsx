import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import type { Topix100RankingResponse } from '@/types/ranking';
import { Topix100RankingTable } from './Topix100RankingTable';

function createResponse(metric: Topix100RankingResponse['rankingMetric']): Topix100RankingResponse {
  return {
    date: '2026-03-30',
    rankingMetric: metric,
    itemCount: 3,
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
        priceVsSma20Gap: 0.12,
        priceSma20_80: 1.23,
        volumeSma20_80: 1.11,
        priceDecile: 1,
        priceBucket: 'q1',
        volumeBucket: 'high',
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
        priceVsSma20Gap: -0.08,
        priceSma20_80: 0.95,
        volumeSma20_80: 0.88,
        priceDecile: 10,
        priceBucket: 'q10',
        volumeBucket: 'low',
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
        priceVsSma20Gap: 0.01,
        priceSma20_80: 1.01,
        volumeSma20_80: 1.05,
        priceDecile: 5,
        priceBucket: 'q456',
        volumeBucket: 'high',
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
        priceVsSma20Gap: -0.01,
        priceSma20_80: 0.99,
        volumeSma20_80: 1.02,
        priceDecile: 7,
        priceBucket: 'other',
        volumeBucket: null,
      },
    ],
  };
}

describe('Topix100RankingTable', () => {
  it('renders the price / SMA20 gap mode and row clicks', async () => {
    const user = userEvent.setup();
    const onStockClick = vi.fn();

    render(
      <Topix100RankingTable
        data={createResponse('price_vs_sma20_gap')}
        isLoading={false}
        error={null}
        onStockClick={onStockClick}
        rankingMetric="price_vs_sma20_gap"
        priceBucketFilter="all"
        volumeBucketFilter="all"
      />
    );

    expect(screen.getByText('Metric: Price / SMA20 Gap')).toBeInTheDocument();
    expect(screen.getByText('+12.00%')).toBeInTheDocument();
    expect(screen.getByText('Toyota')).toBeInTheDocument();

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
        rankingMetric="price_vs_sma20_gap"
        priceBucketFilter="q10"
        volumeBucketFilter="low"
      />
    );

    expect(screen.getByText('Metric: Price SMA 20/80')).toBeInTheDocument();
    expect(screen.getByText('0.95x')).toBeInTheDocument();
    expect(screen.getByText('Sony')).toBeInTheDocument();
    expect(screen.queryByText('Toyota')).not.toBeInTheDocument();
    expect(screen.queryByText('NTT')).not.toBeInTheDocument();
  });

  it('shows the empty state when filters remove every row', () => {
    render(
      <Topix100RankingTable
        data={createResponse('price_vs_sma20_gap')}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        rankingMetric="price_vs_sma20_gap"
        priceBucketFilter="q1"
        volumeBucketFilter="low"
      />
    );

    expect(screen.getByText('No TOPIX100 ranking data available')).toBeInTheDocument();
  });
});
