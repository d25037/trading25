import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import type { MarketRankingResponse } from '@/types/ranking';
import { RankingSummary } from './RankingSummary';

const baseItem = {
  rank: 1,
  marketCode: 'prime',
  sector33Name: 'Transport Equipment',
  currentPrice: 3000,
  volume: 1000000,
};

const mockData: MarketRankingResponse = {
  date: '2025-01-30',
  markets: ['prime', 'standard'],
  lookbackDays: 1,
  periodDays: 250,
  lastUpdated: '2025-01-30T12:00:00Z',
  rankings: {
    tradingValue: [
      { ...baseItem, code: '7203', companyName: 'Toyota', changePercentage: 1.5, tradingValue: 50000000000 },
    ],
    gainers: [
      { ...baseItem, code: '9984', companyName: 'SoftBank', changePercentage: 8.42, tradingValue: 10000000000 },
    ],
    losers: [{ ...baseItem, code: '6758', companyName: 'Sony', changePercentage: -3.21, tradingValue: 8000000000 }],
    periodHigh: [],
    periodLow: [],
  },
};

describe('RankingSummary', () => {
  it('renders null when data is undefined', () => {
    const { container } = render(<RankingSummary data={undefined} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders date and market info', () => {
    render(<RankingSummary data={mockData} />);

    expect(screen.getByText('2025-01-30')).toBeInTheDocument();
    expect(screen.getByText('prime, standard')).toBeInTheDocument();
  });

  it('renders top volume stock', () => {
    render(<RankingSummary data={mockData} />);

    expect(screen.getByText('Toyota')).toBeInTheDocument();
    expect(screen.getByText('7203')).toBeInTheDocument();
  });

  it('renders top gainer with percentage', () => {
    render(<RankingSummary data={mockData} />);

    expect(screen.getByText('+8.42%')).toBeInTheDocument();
    expect(screen.getByText('9984')).toBeInTheDocument();
  });

  it('renders top loser with percentage', () => {
    render(<RankingSummary data={mockData} />);

    expect(screen.getByText('-3.21%')).toBeInTheDocument();
    expect(screen.getByText('6758')).toBeInTheDocument();
  });
});
