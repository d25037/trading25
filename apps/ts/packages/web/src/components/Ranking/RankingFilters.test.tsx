import { fireEvent, render, screen } from '@testing-library/react';
import { createContext, type ReactNode, useContext } from 'react';
import { describe, expect, it, vi } from 'vitest';
import type { DailyRankingTableFilters, RankingParams } from '@/types/ranking';
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
const SELECT_TRIGGER_NEXT_VALUE: Record<string, string> = {
  'ranking-preset': 'momentum_value',
  'ranking-sector-strength-family': 'long_hybrid_leadership',
};

vi.mock('@/components/ui/select', () => ({
  Select: ({ children, onValueChange }: { children: ReactNode; onValueChange: (value: string) => void }) => (
    <SelectContext.Provider value={{ onValueChange }}>{children}</SelectContext.Provider>
  ),
  SelectContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  SelectItem: ({ children }: { children: ReactNode }) => <span>{children}</span>,
  SelectTrigger: ({ children, id }: { children: ReactNode; id?: string }) => {
    const context = useContext(SelectContext);
    const nextValue = (id && SELECT_TRIGGER_NEXT_VALUE[id]) || 'periodLow';
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
    expect(screen.getByText('Sector Selector')).toBeInTheDocument();
    expect(screen.getByText('Balanced Sector Strength')).toBeInTheDocument();
    expect(screen.getByText('Long Hybrid Leadership')).toBeInTheDocument();
    expect(screen.getByText('Preset')).toBeInTheDocument();
    expect(screen.getByText('Core Long')).toBeInTheDocument();
    expect(screen.getByText('Earnings Priority')).toBeInTheDocument();
    expect(screen.getByText('Aggressive Rerating')).toBeInTheDocument();
    expect(screen.getByText('Overvalued Breakdown')).toBeInTheDocument();
    expect(screen.getByText('Momentum Value')).toBeInTheDocument();
    expect(screen.getByText('Crowded Momentum')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Show preset conditions' })).toBeInTheDocument();
    expect(screen.queryByText(/Core Long:/)).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Show preset conditions' }));
    expect(screen.getByText(/Core Long:/)).toBeInTheDocument();
    fireEvent.pointerDown(document.body);
    expect(screen.queryByText(/Core Long:/)).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Show preset conditions' }));
    expect(screen.getByText(/Core Long:/)).toBeInTheDocument();
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(screen.queryByText(/Core Long:/)).not.toBeInTheDocument();
    expect(screen.queryByText('Advanced')).not.toBeInTheDocument();
    expect(screen.queryByText('Neutral Rerating - Good')).not.toBeInTheDocument();
    expect(screen.queryByText('Stale')).not.toBeInTheDocument();
    expect(screen.queryByText('Warning')).not.toBeInTheDocument();
    expect(screen.queryByText('Rally Fade')).not.toBeInTheDocument();
    expect(screen.queryByText('ATR')).not.toBeInTheDocument();
    expect(screen.queryByText('ATR20 Accel')).not.toBeInTheDocument();
    expect(screen.queryByText('20/60D Momentum')).not.toBeInTheDocument();
    expect(screen.queryByText('Results per ranking')).not.toBeInTheDocument();
    expect(screen.queryByText('Period Days (High/Low)')).not.toBeInTheDocument();
  });

  it('wires filter callbacks into ranking params updates', () => {
    const onChange = vi.fn();
    const onTableFiltersChange = vi.fn();
    const tableFilters: DailyRankingTableFilters = { text: 'sony', maxForwardPer: 18 };
    render(
      <RankingFilters
        params={defaultParams}
        tableFilters={tableFilters}
        onChange={onChange}
        onTableFiltersChange={onTableFiltersChange}
      />
    );

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

    fireEvent.click(screen.getByTestId('ranking-sector-strength-family'));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      sectorStrengthFamily: 'long_hybrid_leadership',
    });

    fireEvent.click(screen.getByTestId('ranking-preset'));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      regimeState: undefined,
      fundamentalState: undefined,
      riskState: undefined,
      technicalState: undefined,
    });
    expect(onTableFiltersChange).toHaveBeenLastCalledWith({
      regimeState: 'neutral_rerating',
      technicalState: 'momentum_20_60_top20',
      valuationSignal: 'deep_value',
      minLiquidityZ: -1,
      maxLiquidityZ: 2,
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
    expect(screen.getByTestId('ranking-sector-strength-family')).toBeInTheDocument();
    expect(screen.getByTestId('ranking-preset')).toBeInTheDocument();
    expect(screen.queryByTestId('ranking-regime-state')).not.toBeInTheDocument();
    expect(screen.queryByTestId('ranking-risk-state')).not.toBeInTheDocument();
    expect(screen.queryByTestId('ranking-confirmation-state')).not.toBeInTheDocument();
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
