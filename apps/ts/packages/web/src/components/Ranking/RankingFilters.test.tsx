import { fireEvent, render, screen } from '@testing-library/react';
import type { ReactNode } from 'react';
import { describe, expect, it, vi } from 'vitest';
import type { RankingParams } from '@/types/ranking';
import { RankingFilters, TechnicalEventFilters } from './RankingFilters';

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
    <button type="button" data-testid={id} onClick={() => onChange(label === 'Lookback Days' ? 5 : 120)}>
      {label}
    </button>
  ),
}));

vi.mock('@/components/ui/select', () => ({
  Select: ({ children, onValueChange }: { children: ReactNode; onValueChange: (value: string) => void }) => (
    <button type="button" data-testid="technical-event-type" onClick={() => onValueChange('periodLow')}>
      {children}
    </button>
  ),
  SelectContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  SelectItem: ({ children }: { children: ReactNode }) => <span>{children}</span>,
  SelectTrigger: ({ children }: { children: ReactNode }) => <span>{children}</span>,
  SelectValue: () => <span>Event Type Value</span>,
}));

describe('RankingFilters', () => {
  const defaultParams: RankingParams = {
    markets: 'prime',
    lookbackDays: 1,
    limit: 0,
    periodDays: 250,
    technicalEventType: 'periodHigh',
  };

  it('renders filter card with title', () => {
    render(<RankingFilters params={defaultParams} onChange={vi.fn()} />);

    expect(screen.getByText('Ranking Filters')).toBeInTheDocument();
  });

  it('renders all filter controls', () => {
    render(<RankingFilters params={defaultParams} onChange={vi.fn()} />);

    expect(screen.getByText('Lookback Days')).toBeInTheDocument();
    expect(screen.queryByText('Results per ranking')).not.toBeInTheDocument();
    expect(screen.queryByText('Period Days (High/Low)')).not.toBeInTheDocument();
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
    expect(screen.queryByTestId('ranking-limit')).not.toBeInTheDocument();
    expect(screen.queryByTestId('ranking-periodDays')).not.toBeInTheDocument();
  });

  it('renders technical event filters and wires event params', () => {
    const onChange = vi.fn();
    render(<TechnicalEventFilters params={defaultParams} onChange={onChange} />);

    expect(screen.getByText('Technical Events')).toBeInTheDocument();
    expect(screen.getByText('Period Days')).toBeInTheDocument();

    fireEvent.click(screen.getByTestId('technical-event-type'));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      technicalEventType: 'periodLow',
    });

    fireEvent.click(screen.getByTestId('ranking-technical-periodDays'));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      periodDays: 120,
    });
  });
});
