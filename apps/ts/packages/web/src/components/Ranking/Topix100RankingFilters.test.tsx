import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import type { RankingParams } from '@/types/ranking';
import { Topix100RankingFilters } from './Topix100RankingFilters';

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
    <button type="button" data-testid={id} onClick={() => onChange(value ? undefined : '2026-03-30')}>
      Date Input
    </button>
  ),
}));

describe('Topix100RankingFilters', () => {
  const defaultParams: RankingParams = {
    topix100PriceBucket: 'all',
    topix100VolumeBucket: 'all',
  };

  it('renders the default metric mode when params are omitted', () => {
    render(<Topix100RankingFilters params={{}} onChange={vi.fn()} />);

    expect(screen.getByText('TOPIX100 Ranking')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Price / SMA20 Gap' })).toHaveAttribute('data-state', 'active');
    expect(screen.getByText(/Default short-term price \/ SMA20 gap ranking/i)).toBeInTheDocument();
  });

  it('updates metric, date, and bucket filters', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<Topix100RankingFilters params={defaultParams} onChange={onChange} />);

    await user.click(screen.getByRole('button', { name: 'Price SMA 20/80' }));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      topix100Metric: 'price_sma_20_80',
    });

    fireEvent.click(screen.getByTestId('topix100-ranking-date'));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      date: '2026-03-30',
    });

    await user.click(screen.getByRole('combobox', { name: 'Price Bucket' }));
    await user.click(screen.getByRole('option', { name: 'Q10' }));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      topix100PriceBucket: 'q10',
    });

    await user.click(screen.getByRole('combobox', { name: 'Volume Bucket' }));
    await user.click(screen.getByRole('option', { name: 'Volume Low' }));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      topix100VolumeBucket: 'low',
    });
  });
});
