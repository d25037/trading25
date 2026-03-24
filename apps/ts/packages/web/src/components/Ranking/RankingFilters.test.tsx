import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { RankingParams } from '@/types/ranking';
import { RankingFilters } from './RankingFilters';

vi.mock('@/components/shared/filters', () => ({
  DateInput: ({
    value,
    onChange,
    id,
  }: {
    value: string | undefined;
    onChange: (value: string | undefined) => void;
    id?: string;
  }) => (
    <button type="button" data-testid={id} onClick={() => onChange(value ? undefined : '2024-01-19')}>
      Date Input
    </button>
  ),
  MarketsSelect: ({ onChange, id }: { onChange: (value: string) => void; id?: string }) => (
    <button type="button" data-testid={id} onClick={() => onChange('prime,standard')}>
      Markets Select
    </button>
  ),
  NumberSelect: ({ label, onChange, id }: { label: string; onChange: (value: number) => void; id: string }) => (
    <button
      type="button"
      data-testid={id}
      onClick={() => onChange(label === 'Lookback Days' ? 5 : label === 'Results per ranking' ? 50 : 120)}
    >
      {label}
    </button>
  ),
}));

describe('RankingFilters', () => {
  const defaultParams: RankingParams = {
    markets: 'prime',
    lookbackDays: 1,
    limit: 20,
    periodDays: 250,
  };

  it('renders filter card with title', () => {
    render(<RankingFilters params={defaultParams} onChange={vi.fn()} />);

    expect(screen.getByText('Ranking Filters')).toBeInTheDocument();
  });

  it('renders all filter controls', () => {
    render(<RankingFilters params={defaultParams} onChange={vi.fn()} />);

    expect(screen.getByText('Lookback Days')).toBeInTheDocument();
    expect(screen.getByText('Results per ranking')).toBeInTheDocument();
    expect(screen.getByText('Period Days (High/Low)')).toBeInTheDocument();
  });

  it('wires filter callbacks into ranking params updates', () => {
    const onChange = vi.fn();
    render(<RankingFilters params={defaultParams} onChange={onChange} />);

    fireEvent.click(screen.getByTestId('ranking-markets'));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      markets: 'prime,standard',
    });

    fireEvent.click(screen.getByTestId('ranking-lookbackDays'));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      lookbackDays: 5,
    });

    fireEvent.click(screen.getByTestId('ranking-limit'));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      limit: 50,
    });

    fireEvent.click(screen.getByTestId('ranking-periodDays'));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      periodDays: 120,
    });

    fireEvent.click(screen.getByTestId('ranking-date'));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      date: '2024-01-19',
    });
  });

  it('uses fallback defaults when ranking params are omitted', () => {
    render(<RankingFilters params={{}} onChange={vi.fn()} />);

    expect(screen.getByTestId('ranking-markets')).toBeInTheDocument();
    expect(screen.getByTestId('ranking-lookbackDays')).toBeInTheDocument();
    expect(screen.getByTestId('ranking-limit')).toBeInTheDocument();
    expect(screen.getByTestId('ranking-periodDays')).toBeInTheDocument();
  });
});
