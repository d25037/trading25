import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ChangeEvent, ReactNode } from 'react';
import * as React from 'react';
import { describe, expect, it, vi } from 'vitest';
import type { ScreeningParams } from '@/types/screening';
import { ScreeningFilters } from './ScreeningFilters';

vi.mock('@/components/shared/filters', () => ({
  MarketsSelect: ({
    value,
    onChange,
    options,
    id,
  }: {
    value: string;
    onChange: (value: string) => void;
    options?: Array<{ value: string; label: string }>;
    id: string;
  }) => (
    <label htmlFor={id}>
      Markets
      <select id={id} value={value} onChange={(event) => onChange(event.target.value)}>
        {(options ?? []).map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  ),
  NumberSelect: ({
    value,
    onChange,
    options,
    id,
    label,
  }: {
    value: number;
    onChange: (value: number) => void;
    options: Array<{ value: number; label: string }>;
    id: string;
    label: string;
  }) => (
    <label htmlFor={id}>
      {label}
      <select id={id} value={String(value)} onChange={(event) => onChange(Number(event.target.value))}>
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  ),
  DateInput: ({
    value,
    onChange,
    id,
    label,
  }: {
    value?: string;
    onChange: (value: string) => void;
    id: string;
    label: string;
  }) => (
    <label htmlFor={id}>
      {label}
      <input
        id={id}
        type="date"
        value={value ?? ''}
        onChange={(event: ChangeEvent<HTMLInputElement>) => onChange(event.target.value)}
      />
    </label>
  ),
}));

vi.mock('@/components/ui/select', () => {
  const SelectContext = React.createContext<(value: string) => void>(() => {});

  return {
    Select: ({ children, onValueChange }: { children: ReactNode; onValueChange?: (value: string) => void }) => (
      <SelectContext.Provider value={onValueChange ?? (() => {})}>{children}</SelectContext.Provider>
    ),
    SelectTrigger: ({ children, id }: { children: ReactNode; id?: string }) => (
      <div data-testid={id ?? 'select-trigger'}>{children}</div>
    ),
    SelectValue: () => <span>Select value</span>,
    SelectContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
    SelectItem: ({ children, value }: { children: ReactNode; value: string }) => {
      const setValue = React.useContext(SelectContext);
      return (
        <button type="button" onClick={() => setValue(value)}>
          {children}
        </button>
      );
    },
  };
});

describe('ScreeningFilters', () => {
  const defaultParams: ScreeningParams = {
    entry_decidability: 'pre_open_decidable',
    recentDays: 10,
    sortBy: 'matchedDate',
    order: 'desc',
    limit: 50,
  };

  const strategyOptions = ['production/range_break_v15', 'production/forward_eps_driven'];

  it('renders standard strategy controls and default summary', () => {
    render(
      <ScreeningFilters
        entryDecidability="pre_open_decidable"
        params={defaultParams}
        onChange={vi.fn()}
        strategyOptions={strategyOptions}
        autoMarkets={['prime', 'standard']}
        strategiesLoading={false}
      />
    );

    expect(screen.getByText('Filters')).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'Auto (Prime + Standard)' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'production/range_break_v15' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'All pre-open production' })).toBeInTheDocument();
    expect(
      screen.getByText('No explicit selection: all pre-open production strategies are evaluated.')
    ).toBeInTheDocument();
  });

  it('renders Auto fallback and dynamic market options for non-standard combinations', () => {
    render(
      <ScreeningFilters
        entryDecidability="pre_open_decidable"
        params={{ ...defaultParams, markets: 'custom,growth' }}
        onChange={vi.fn()}
        strategyOptions={strategyOptions}
        autoMarkets={[]}
        strategiesLoading={false}
      />
    );

    expect(screen.getByRole('option', { name: 'Auto' })).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'Growth + custom' })).toBeInTheDocument();
  });

  it('shows all-markets label when auto markets cover every built-in market', () => {
    render(
      <ScreeningFilters
        entryDecidability="pre_open_decidable"
        params={defaultParams}
        onChange={vi.fn()}
        strategyOptions={strategyOptions}
        autoMarkets={['growth', 'prime', 'standard']}
        strategiesLoading={false}
      />
    );

    expect(screen.getByRole('option', { name: 'Auto (All Markets)' })).toBeInTheDocument();
  });

  it('toggles strategies on and off and clears explicit selections', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    const { rerender } = render(
      <ScreeningFilters
        entryDecidability="pre_open_decidable"
        params={defaultParams}
        onChange={onChange}
        strategyOptions={strategyOptions}
        autoMarkets={['prime']}
        strategiesLoading={false}
      />
    );

    await user.click(screen.getByRole('button', { name: 'production/range_break_v15' }));
    expect(onChange).toHaveBeenCalledWith({
      ...defaultParams,
      strategies: 'production/range_break_v15',
    });

    rerender(
      <ScreeningFilters
        entryDecidability="pre_open_decidable"
        params={{ ...defaultParams, strategies: 'production/range_break_v15,production/forward_eps_driven' }}
        onChange={onChange}
        strategyOptions={strategyOptions}
        autoMarkets={['prime']}
        strategiesLoading={false}
      />
    );

    expect(screen.getByText('2 strategies selected')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'production/range_break_v15' }));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      strategies: 'production/forward_eps_driven',
    });

    rerender(
      <ScreeningFilters
        entryDecidability="pre_open_decidable"
        params={{ ...defaultParams, strategies: 'production/range_break_v15' }}
        onChange={onChange}
        strategyOptions={strategyOptions}
        autoMarkets={['prime']}
        strategiesLoading={false}
      />
    );

    await user.click(screen.getByRole('button', { name: 'production/range_break_v15' }));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      strategies: undefined,
    });

    await user.click(screen.getByRole('button', { name: 'All pre-open production' }));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      strategies: undefined,
    });
  });

  it('shows loading and empty states with mode-specific labels', () => {
    const { rerender } = render(
      <ScreeningFilters
        entryDecidability="pre_open_decidable"
        params={defaultParams}
        onChange={vi.fn()}
        strategyOptions={[]}
        autoMarkets={['prime']}
        strategiesLoading
      />
    );

    expect(screen.getByText('Loading pre-open production strategies...')).toBeInTheDocument();

    rerender(
      <ScreeningFilters
        entryDecidability="requires_same_session_observation"
        params={{ ...defaultParams, entry_decidability: 'requires_same_session_observation' }}
        onChange={vi.fn()}
        strategyOptions={[]}
        autoMarkets={['growth']}
        strategiesLoading={false}
      />
    );

    expect(screen.getByRole('button', { name: 'All in-session production' })).toBeInTheDocument();
    expect(screen.getByText('No in-session production strategies available')).toBeInTheDocument();
    expect(
      screen.getByText('No explicit selection: all in-session production strategies are evaluated.')
    ).toBeInTheDocument();
  });

  it('updates market, date, sort, order, recent days, and limit filters', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(
      <ScreeningFilters
        entryDecidability="pre_open_decidable"
        params={defaultParams}
        onChange={onChange}
        strategyOptions={strategyOptions}
        autoMarkets={['prime', 'standard']}
        strategiesLoading={false}
      />
    );

    await user.selectOptions(screen.getByLabelText('Markets'), 'growth');
    expect(onChange).toHaveBeenCalledWith({
      ...defaultParams,
      markets: 'growth',
    });

    await user.selectOptions(screen.getByLabelText('Markets'), '__auto__');
    expect(onChange).toHaveBeenCalledWith({
      ...defaultParams,
      markets: undefined,
    });

    await user.selectOptions(screen.getByLabelText('Recent Days'), '20');
    expect(onChange).toHaveBeenCalledWith({
      ...defaultParams,
      recentDays: 20,
    });

    await user.type(screen.getByLabelText('Reference Date (optional)'), '2026-03-06');
    expect(onChange).toHaveBeenCalledWith({
      ...defaultParams,
      date: '2026-03-06',
    });

    await user.click(screen.getByRole('button', { name: 'Stock Code' }));
    expect(onChange).toHaveBeenCalledWith({
      ...defaultParams,
      sortBy: 'stockCode',
    });

    await user.click(screen.getByRole('button', { name: 'Ascending' }));
    expect(onChange).toHaveBeenCalledWith({
      ...defaultParams,
      order: 'asc',
    });

    await user.selectOptions(screen.getByLabelText('Limit'), '100');
    expect(onChange).toHaveBeenCalledWith({
      ...defaultParams,
      limit: 100,
    });
  });
});
