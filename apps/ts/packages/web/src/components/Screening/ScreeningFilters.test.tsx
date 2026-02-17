import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { ScreeningParams } from '@/types/screening';
import { ScreeningFilters } from './ScreeningFilters';

describe('ScreeningFilters', () => {
  const defaultParams: ScreeningParams = {
    markets: 'prime',
    recentDays: 10,
    backtestMetric: 'sharpe_ratio',
    sortBy: 'bestStrategyScore',
    order: 'desc',
    limit: 50,
  };

  const strategyOptions = ['range_break_v15', 'forward_eps_driven'];

  it('renders filter card with title', () => {
    render(
      <ScreeningFilters
        params={defaultParams}
        onChange={vi.fn()}
        strategyOptions={strategyOptions}
        strategiesLoading={false}
      />
    );

    expect(screen.getByText('Filters')).toBeInTheDocument();
  });

  it('renders dynamic strategy options', () => {
    render(
      <ScreeningFilters
        params={defaultParams}
        onChange={vi.fn()}
        strategyOptions={strategyOptions}
        strategiesLoading={false}
      />
    );

    expect(screen.getByText('Strategies')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'range_break_v15' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'forward_eps_driven' })).toBeInTheDocument();
  });

  it('renders backtest metric and sort controls', () => {
    render(
      <ScreeningFilters
        params={defaultParams}
        onChange={vi.fn()}
        strategyOptions={strategyOptions}
        strategiesLoading={false}
      />
    );

    expect(screen.getByText('Backtest Metric')).toBeInTheDocument();
    expect(screen.getByText('Sort By')).toBeInTheDocument();
    expect(screen.getByText('Order')).toBeInTheDocument();
  });

  it('renders recent days and limit selects', () => {
    render(
      <ScreeningFilters
        params={defaultParams}
        onChange={vi.fn()}
        strategyOptions={strategyOptions}
        strategiesLoading={false}
      />
    );

    expect(screen.getByText('Recent Days')).toBeInTheDocument();
    expect(screen.getByText('Limit')).toBeInTheDocument();
  });
});
