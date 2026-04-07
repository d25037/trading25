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
  NumberSelect: ({
    label,
    onChange,
  }: {
    label: string;
    onChange: (value: number) => void;
  }) => (
    <button type="button" aria-label={label} onClick={() => onChange(100)}>
      {label}
    </button>
  ),
}));

describe('Topix100RankingFilters', () => {
  const defaultParams: RankingParams = {
    topix100Metric: 'price_vs_sma_gap',
    topix100SmaWindow: 50,
    topix100PriceBucket: 'all',
    topix100VolumeBucket: 'all',
    topix100ShortMode: 'all',
    topix100LongMode: 'all',
  };

  it('renders the default metric mode when params are omitted', () => {
    render(<Topix100RankingFilters params={{}} onChange={vi.fn()} />);

    expect(screen.getByText('TOPIX100 SMA Divergence')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Price / SMA Gap' })).toHaveAttribute('data-state', 'active');
    expect(
      screen.getByText(
        'Start at Price / SMA50 Gap. SMA50 baseline. Q10 = below SMA; Q2-4 = trough; Volume Low (5/20) first. Overlay the fixed streak 3/53 short and long states plus the next-session intraday LightGBM score for open-to-close selection.'
      )
    ).toBeInTheDocument();
    expect(screen.getByText('SMA Window')).toBeInTheDocument();
    expect(screen.getByText('Short State')).toBeInTheDocument();
    expect(screen.getByText('Long State')).toBeInTheDocument();
  });

  it('renders the legacy metric copy without the sma window control', () => {
    render(
      <Topix100RankingFilters
        params={{
          topix100Metric: 'price_sma_20_80',
          topix100PriceBucket: 'all',
          topix100VolumeBucket: 'all',
        }}
        onChange={vi.fn()}
      />
    );

    expect(
      screen.getByText(
        'Legacy SMA 20/80 comparison view. The score layer now uses the next-session intraday LightGBM read on the same streak 3/53 + SMA50 / volume 5/20 feature family.'
      )
    ).toBeInTheDocument();
    expect(screen.queryByText('SMA Window')).not.toBeInTheDocument();
  });

  it('updates metric, sma window, date, and bucket filters', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<Topix100RankingFilters params={defaultParams} onChange={onChange} />);

    await user.click(screen.getByRole('button', { name: 'SMA Window' }));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      topix100SmaWindow: 100,
    });

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
    await user.click(screen.getByRole('option', { name: 'Q10 Below SMA' }));
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

    await user.click(screen.getByRole('combobox', { name: 'Short State' }));
    await user.click(screen.getByRole('option', { name: 'Bearish' }));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      topix100ShortMode: 'bearish',
    });

    await user.click(screen.getByRole('combobox', { name: 'Long State' }));
    await user.click(screen.getByRole('option', { name: 'Bullish' }));
    expect(onChange).toHaveBeenLastCalledWith({
      ...defaultParams,
      topix100LongMode: 'bullish',
    });
  });
});
