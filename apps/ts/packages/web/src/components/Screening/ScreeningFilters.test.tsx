import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { ScreeningParams } from '@/types/screening';
import { ScreeningFilters } from './ScreeningFilters';

describe('ScreeningFilters', () => {
  const defaultParams: ScreeningParams = {
    markets: 'prime',
    rangeBreakFast: true,
    rangeBreakSlow: true,
    recentDays: 10,
    sortBy: 'date',
    order: 'desc',
    limit: 50,
  };

  it('renders filter card with title', () => {
    render(<ScreeningFilters params={defaultParams} onChange={vi.fn()} />);

    expect(screen.getByText('Filters')).toBeInTheDocument();
  });

  it('renders screening type toggles', () => {
    render(<ScreeningFilters params={defaultParams} onChange={vi.fn()} />);

    expect(screen.getByText('Range Break Fast')).toBeInTheDocument();
    expect(screen.getByText('Range Break Slow')).toBeInTheDocument();
  });

  it('renders number inputs', () => {
    render(<ScreeningFilters params={defaultParams} onChange={vi.fn()} />);

    expect(screen.getByText('Min Break %')).toBeInTheDocument();
    expect(screen.getByText('Min Volume Ratio')).toBeInTheDocument();
  });

  it('renders sort and order selects', () => {
    render(<ScreeningFilters params={defaultParams} onChange={vi.fn()} />);

    expect(screen.getByText('Sort By')).toBeInTheDocument();
    expect(screen.getByText('Order')).toBeInTheDocument();
  });

  it('renders recent days and limit selects', () => {
    render(<ScreeningFilters params={defaultParams} onChange={vi.fn()} />);

    expect(screen.getByText('Recent Days')).toBeInTheDocument();
    expect(screen.getByText('Limit')).toBeInTheDocument();
  });
});
