import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import type { BacktestResultSummary } from '@/types/backtest';
import { ResultMetricsCard } from './ResultMetricsCard';

function createSummary(overrides: Partial<BacktestResultSummary> = {}): BacktestResultSummary {
  return {
    total_return: 12.34,
    sharpe_ratio: 1.23,
    max_drawdown: -8.5,
    win_rate: 64.2,
    calmar_ratio: 0.87,
    trade_count: 42,
    ...overrides,
  } as BacktestResultSummary;
}

describe('ResultMetricsCard', () => {
  it('renders nothing when summary is missing', () => {
    const { container } = render(<ResultMetricsCard summary={null} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders metrics with positive return and high win-rate styles', () => {
    render(<ResultMetricsCard summary={createSummary()} />);

    expect(screen.getByText('Return')).toBeInTheDocument();
    expect(screen.getByText('+12.34%')).toHaveClass('text-green-500');
    expect(screen.getByText('1.23')).toBeInTheDocument();
    expect(screen.getByText('-8.50%')).toHaveClass('text-red-500');
    expect(screen.getByText('+64.20%')).toHaveClass('text-green-500');
    expect(screen.getByText('0.87')).toBeInTheDocument();
    expect(screen.getByText('42')).toBeInTheDocument();
  });

  it('renders negative return and low win-rate styles', () => {
    render(
      <ResultMetricsCard
        summary={createSummary({
          total_return: -1.2,
          win_rate: 49.4,
        })}
      />
    );

    expect(screen.getByText('-1.20%')).toHaveClass('text-red-500');
    expect(screen.getByText('+49.40%')).toHaveClass('text-yellow-500');
  });
});
