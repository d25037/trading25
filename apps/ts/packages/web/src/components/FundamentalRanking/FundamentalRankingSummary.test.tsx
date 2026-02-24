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
  lastUpdated: '2025-01-30T12:00:00Z',
  rankings: {
    forecastHigh: [{ ...baseItem, code: '7203', companyName: 'Toyota', epsValue: 520 }],
    forecastLow: [{ ...baseItem, code: '6502', companyName: 'Toshiba', epsValue: -12 }],
    actualHigh: [{ ...baseItem, code: '6758', companyName: 'Sony', epsValue: 480 }],
    actualLow: [{ ...baseItem, code: '8306', companyName: 'MUFG', epsValue: -18 }],
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
  });

  it('renders forecast and actual codes', () => {
    render(<FundamentalRankingSummary data={mockData} />);
    expect(screen.getByText('7203')).toBeInTheDocument();
    expect(screen.getByText('6502')).toBeInTheDocument();
    expect(screen.getByText('6758')).toBeInTheDocument();
    expect(screen.getByText('8306')).toBeInTheDocument();
  });
});
