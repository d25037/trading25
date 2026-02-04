import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import type { ScreeningSummary as Summary } from '@/types/screening';
import { ScreeningSummary } from './ScreeningSummary';

const mockSummary: Summary = {
  totalStocksScreened: 1500,
  matchCount: 42,
  skippedCount: 0,
  byScreeningType: {
    rangeBreakFast: 28,
    rangeBreakSlow: 14,
  },
};

describe('ScreeningSummary', () => {
  it('renders null when summary is undefined', () => {
    const { container } = render(<ScreeningSummary summary={undefined} markets={['prime']} recentDays={10} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders total screened count', () => {
    render(<ScreeningSummary summary={mockSummary} markets={['prime']} recentDays={10} />);

    expect(screen.getByText('1,500')).toBeInTheDocument();
  });

  it('renders match count and hit rate', () => {
    render(<ScreeningSummary summary={mockSummary} markets={['prime']} recentDays={10} />);

    expect(screen.getByText('42')).toBeInTheDocument();
    expect(screen.getByText('2.8% hit rate')).toBeInTheDocument();
  });

  it('renders screening type breakdown', () => {
    render(<ScreeningSummary summary={mockSummary} markets={['prime']} recentDays={10} />);

    expect(screen.getByText('28')).toBeInTheDocument();
    expect(screen.getByText('14')).toBeInTheDocument();
    expect(screen.getByText('EMA 30/120')).toBeInTheDocument();
    expect(screen.getByText('SMA 50/150')).toBeInTheDocument();
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

  it('shows market info without reference date when not provided', () => {
    render(<ScreeningSummary summary={mockSummary} markets={['prime']} recentDays={10} />);

    expect(screen.getByText('prime / 10d')).toBeInTheDocument();
  });
});
