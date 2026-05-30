import { fireEvent, render, screen } from '@testing-library/react';
import { createContext, type ReactNode, useContext } from 'react';
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
  NumberSelect: ({ label, onChange, id }: { label: string; onChange: (value: number) => void; id: string }) => {
    const nextValue = label === 'Lookback Days' ? 5 : label === 'Fwd EPS Disclosure' ? 126 : 120;
    return (
      <button type="button" data-testid={id} onClick={() => onChange(nextValue)}>
        {label}
      </button>
    );
  },
}));

const SelectContext = createContext<{ onValueChange: (value: string) => void } | null>(null);

vi.mock('@/components/ui/select', () => ({
  Select: ({ children, onValueChange }: { children: ReactNode; onValueChange: (value: string) => void }) => (
    <SelectContext.Provider value={{ onValueChange }}>{children}</SelectContext.Provider>
  ),
  SelectContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  SelectItem: ({ children }: { children: ReactNode }) => <span>{children}</span>,
  SelectTrigger: ({ children, id }: { children: ReactNode; id?: string }) => {
    const context = useContext(SelectContext);
    const nextValue =
      id === 'ranking-regime-state'
        ? 'neutral_rerating_good'
        : id === 'ranking-risk-state'
          ? 'overheat'
          : id === 'ranking-confirmation-state'
            ? 'momentum_20_60_top20'
            : 'periodLow';
    return (
      <button type="button" data-testid={id} onClick={() => context?.onValueChange(nextValue)}>
        {children}
      </button>
    );
  },
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
    expect(screen.getByText('Fwd EPS Disclosure')).toBeInTheDocument();
    expect(screen.getByText('Regime')).toBeInTheDocument();
    expect(screen.getByText('Neutral Rerating - Good')).toBeInTheDocument();
    expect(screen.getByText('Warning')).toBeInTheDocument();
    expect(screen.getByText('Rally Fade')).toBeInTheDocument();
    expect(screen.getByText('Confirmation')).toBeInTheDocument();
    expect(screen.getByText('ATR20 Accel')).toBeInTheDocument();
    expect(screen.getByText('20/60D Momentum')).toBeInTheDocument();
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

    fireEvent.click(screen.getByTestId('ranking-forward-eps-disclosed-within-days'));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      forwardEpsDisclosedWithinDays: 126,
    });

    fireEvent.click(screen.getByTestId('ranking-regime-state'));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      regimeState: 'neutral_rerating_good',
    });

    fireEvent.click(screen.getByTestId('ranking-risk-state'));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      riskState: 'overheat',
    });

    fireEvent.click(screen.getByTestId('ranking-confirmation-state'));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      technicalState: 'momentum_20_60_top20',
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
    expect(screen.getByTestId('ranking-forward-eps-disclosed-within-days')).toBeInTheDocument();
    expect(screen.getByTestId('ranking-regime-state')).toBeInTheDocument();
    expect(screen.getByTestId('ranking-risk-state')).toBeInTheDocument();
    expect(screen.getByTestId('ranking-confirmation-state')).toBeInTheDocument();
    expect(screen.queryByTestId('ranking-limit')).not.toBeInTheDocument();
    expect(screen.queryByTestId('ranking-periodDays')).not.toBeInTheDocument();
  });

  it('renders technical event filters and wires event params', () => {
    const onChange = vi.fn();
    render(<TechnicalEventFilters params={defaultParams} onChange={onChange} />);

    expect(screen.getByText('Technical Events')).toBeInTheDocument();
    expect(screen.getByText('Period Days')).toBeInTheDocument();

    fireEvent.click(screen.getByTestId('ranking-technical-event-type'));
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
