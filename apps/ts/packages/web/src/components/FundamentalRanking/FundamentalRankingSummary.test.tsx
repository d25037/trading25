import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import type { MarketFundamentalRankingResponse } from '@/types/fundamentalRanking';
import { FundamentalRankingSummary } from './FundamentalRankingSummary';

const baseItem = {
  rank: 1,
  marketCode: 'prime',
  sector33Name: 'Transport Equipment',
  currentPrice: 3000,
  volume: 1000000,
  disclosedDate: '2025-01-30',
  periodType: 'FY',
  source: 'fy' as const,
};

const mockData: MarketFundamentalRankingResponse = {
  date: '2025-01-30',
  markets: ['prime', 'standard'],
  metricKey: 'eps_forecast_to_actual',
  lastUpdated: '2025-01-30T12:00:00Z',
  rankings: {
    ratioHigh: [{ ...baseItem, code: '7203', companyName: 'Toyota', epsValue: 1.48 }],
    ratioLow: [{ ...baseItem, code: '6502', companyName: 'Toshiba', epsValue: 0.72 }],
  },
};

describe('FundamentalRankingSummary', () => {
  it('renders null when data is undefined', () => {
    const { container } = render(<FundamentalRankingSummary data={undefined} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders date and markets', () => {
    render(<FundamentalRankingSummary data={mockData} />);
    expect(screen.getByText('2025-01-30')).toBeInTheDocument();
    expect(screen.getByText('prime, standard')).toBeInTheDocument();
    expect(screen.getByText('Metric: eps_forecast_to_actual')).toBeInTheDocument();
  });

  it('renders high/low ratio codes', () => {
    render(<FundamentalRankingSummary data={mockData} />);
    expect(screen.getByText('7203')).toBeInTheDocument();
    expect(screen.getByText('6502')).toBeInTheDocument();
  });

  it('renders fallback values when ranking items are missing', () => {
    const emptyData: MarketFundamentalRankingResponse = {
      ...mockData,
      rankings: {
        ratioHigh: [],
        ratioLow: [],
      },
    };

    render(<FundamentalRankingSummary data={emptyData} />);
    expect(screen.getAllByText('-').length).toBeGreaterThanOrEqual(2);
  });
});
