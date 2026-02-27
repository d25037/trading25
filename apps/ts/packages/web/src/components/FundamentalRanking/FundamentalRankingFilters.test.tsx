import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
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

  it('disables lookback control when EPS condition is all stocks', () => {
    render(<FundamentalRankingFilters params={defaultParams} onChange={vi.fn()} />);
    expect(screen.getByRole('combobox', { name: 'Recent FY lookback' })).toBeDisabled();
    expect(
      screen.getByText('Enabled only when EPS Condition is "Latest Forecast EPS > Recent FY Actual EPS".')
    ).toBeInTheDocument();
  });

  it('enables lookback control when forecast filter is enabled', () => {
    render(
      <FundamentalRankingFilters params={{ ...defaultParams, forecastAboveRecentFyActuals: true }} onChange={vi.fn()} />
    );
    expect(screen.getByRole('combobox', { name: 'Recent FY lookback' })).toBeEnabled();
    expect(
      screen.queryByText('Enabled only when EPS Condition is "Latest Forecast EPS > Recent FY Actual EPS".')
    ).not.toBeInTheDocument();
  });

  it('calls onChange when market and EPS condition are updated', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(<FundamentalRankingFilters params={defaultParams} onChange={onChange} />);

    await user.click(screen.getByRole('combobox', { name: 'Markets' }));
    await user.click(screen.getByText('Standard'));
    expect(onChange).toHaveBeenCalledWith({ ...defaultParams, markets: 'standard' });

    await user.click(screen.getByRole('combobox', { name: 'EPS Condition' }));
    await user.click(screen.getByText('Latest Forecast EPS > Recent FY Actual EPS'));
    expect(onChange).toHaveBeenCalledWith({
      ...defaultParams,
      forecastAboveRecentFyActuals: true,
    });
  });

  it('calls onChange when lookback and limit are updated', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    const enabledParams = { ...defaultParams, forecastAboveRecentFyActuals: true };

    render(<FundamentalRankingFilters params={enabledParams} onChange={onChange} />);

    await user.click(screen.getByRole('combobox', { name: 'Recent FY lookback' }));
    await user.click(screen.getByText('5 FY'));
    expect(onChange).toHaveBeenCalledWith({
      ...enabledParams,
      forecastLookbackFyCount: 5,
    });

    await user.click(screen.getByRole('combobox', { name: 'Results per ranking' }));
    await user.click(screen.getByText('50'));
    expect(onChange).toHaveBeenCalledWith({
      ...enabledParams,
      limit: 50,
    });
  });

  it('uses fallback defaults when markets, lookback, and limit are omitted', () => {
    render(<FundamentalRankingFilters params={{}} onChange={vi.fn()} />);

    expect(screen.getByRole('combobox', { name: 'Markets' })).toHaveTextContent('Prime');
    expect(screen.getByRole('combobox', { name: 'Recent FY lookback' })).toHaveTextContent('3 FY');
    expect(screen.getByRole('combobox', { name: 'Results per ranking' })).toHaveTextContent('20');
  });

  it('enables lookback when legacy forecastAboveAllActuals is true', () => {
    render(
      <FundamentalRankingFilters
        params={{ ...defaultParams, forecastAboveRecentFyActuals: undefined, forecastAboveAllActuals: true }}
        onChange={vi.fn()}
      />
    );
    expect(screen.getByRole('combobox', { name: 'Recent FY lookback' })).toBeEnabled();
  });
});
