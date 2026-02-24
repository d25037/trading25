import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';
import { FundamentalRankingFilters } from './FundamentalRankingFilters';

describe('FundamentalRankingFilters', () => {
  const defaultParams: FundamentalRankingParams = {
    markets: 'prime',
    limit: 20,
  };

  it('renders filter card with title', () => {
    render(<FundamentalRankingFilters params={defaultParams} onChange={vi.fn()} />);
    expect(screen.getByText('Fundamental Ranking Filters')).toBeInTheDocument();
  });

  it('renders limit control', () => {
    render(<FundamentalRankingFilters params={defaultParams} onChange={vi.fn()} />);
    expect(screen.getByText('Results per ranking')).toBeInTheDocument();
  });
});
