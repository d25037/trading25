import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';
import { FundamentalRankingFilters } from './FundamentalRankingFilters';

describe('FundamentalRankingFilters', () => {
  const defaultParams: FundamentalRankingParams = {
    markets: 'prime',
    limit: 20,
    forecastAboveRecentFyActuals: false,
    forecastLookbackFyCount: 3,
  };

  it('renders filter card with title', () => {
    render(<FundamentalRankingFilters params={defaultParams} onChange={vi.fn()} />);
    expect(screen.getByText('Fundamental Ranking Filters')).toBeInTheDocument();
  });

  it('renders limit control', () => {
    render(<FundamentalRankingFilters params={defaultParams} onChange={vi.fn()} />);
    expect(screen.getByText('Results per ranking')).toBeInTheDocument();
  });

  it('renders eps condition control', () => {
    render(<FundamentalRankingFilters params={defaultParams} onChange={vi.fn()} />);
    expect(screen.getByText('EPS Condition')).toBeInTheDocument();
  });

  it('renders lookback control', () => {
    render(<FundamentalRankingFilters params={defaultParams} onChange={vi.fn()} />);
    expect(screen.getByText('Recent FY lookback')).toBeInTheDocument();
  });
});
