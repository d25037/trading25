import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import type { ScreeningSummary as Summary } from '@/types/screening';
import { ScreeningSummary } from './ScreeningSummary';

const mockSummary: Summary = {
  totalStocksScreened: 1500,
  matchCount: 42,
  skippedCount: 0,
  byStrategy: {
    range_break_v15: 28,
    forward_eps_driven: 14,
  },
  strategiesEvaluated: ['range_break_v15', 'forward_eps_driven'],
  strategiesWithoutBacktestMetrics: ['forward_eps_driven'],
  warnings: ['benchmark load failed'],
};

describe('ScreeningSummary', () => {
  it('renders null when summary is undefined', () => {
    const { container } = render(<ScreeningSummary summary={undefined} markets={['prime']} recentDays={10} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders total screened count and match hit rate', () => {
    render(<ScreeningSummary summary={mockSummary} markets={['prime']} recentDays={10} />);

    expect(screen.getByText('1,500')).toBeInTheDocument();
    expect(screen.getByText('42')).toBeInTheDocument();
    expect(screen.getByText('2.8% hit rate')).toBeInTheDocument();
  });

  it('renders strategy and warning metrics', () => {
    render(<ScreeningSummary summary={mockSummary} markets={['prime']} recentDays={10} />);

    expect(screen.getByText('2')).toBeInTheDocument();
    expect(screen.getByText('1')).toBeInTheDocument();
    expect(screen.getByText('range_break_v15 (28)')).toBeInTheDocument();
    expect(screen.getByText('1 warnings')).toBeInTheDocument();
  });

  it('shows market info with reference date when provided', () => {
    render(
      <ScreeningSummary
        summary={mockSummary}
        markets={['prime', 'standard']}
        recentDays={20}
        referenceDate="2025-01-30"
      />
    );

    expect(screen.getByText('prime, standard / 20d / 2025-01-30')).toBeInTheDocument();
  });
});
