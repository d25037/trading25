import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { ScreeningResultItem } from '@/types/screening';
import { ScreeningTable } from './ScreeningTable';

const mockResults: ScreeningResultItem[] = [
  {
    stockCode: '7203',
    companyName: 'Toyota Motor',
    sector33Name: '輸送用機器',
    matchedDate: '2026-01-07',
    bestStrategyName: 'range_break_v15',
    bestStrategyScore: 1.2345,
    matchStrategyCount: 2,
    matchedStrategies: [
      { strategyName: 'range_break_v15', matchedDate: '2026-01-07', strategyScore: 1.2345 },
      { strategyName: 'forward_eps_driven', matchedDate: '2026-01-06', strategyScore: 0.9 },
    ],
  },
];

describe('ScreeningTable', () => {
  it('renders simplified columns without best strategy/score', () => {
    render(<ScreeningTable results={mockResults} isLoading={false} error={null} onStockClick={vi.fn()} />);

    expect(screen.queryByText('Best Strategy')).not.toBeInTheDocument();
    expect(screen.queryByText('Score')).not.toBeInTheDocument();
    expect(screen.getByText('Matches')).toBeInTheDocument();
  });

  it('renders matched strategy list and count', () => {
    render(<ScreeningTable results={mockResults} isLoading={false} error={null} onStockClick={vi.fn()} />);

    expect(screen.getByText('2')).toBeInTheDocument();
    expect(screen.getByText('range_break_v15, forward_eps_driven')).toBeInTheDocument();
  });

  it('shows updating indicator while refetching', () => {
    render(<ScreeningTable results={mockResults} isLoading={false} isFetching error={null} onStockClick={vi.fn()} />);
    expect(screen.getByText('Updating...')).toBeInTheDocument();
  });
});
