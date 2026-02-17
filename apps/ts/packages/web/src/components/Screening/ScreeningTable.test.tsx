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
  it('renders strategy-centric columns', () => {
    render(<ScreeningTable results={mockResults} isLoading={false} error={null} onStockClick={vi.fn()} />);

    expect(screen.getByText('Best Strategy')).toBeInTheDocument();
    expect(screen.getByText('Score')).toBeInTheDocument();
    expect(screen.getByText('Matches')).toBeInTheDocument();
  });

  it('renders best strategy and score values', () => {
    render(<ScreeningTable results={mockResults} isLoading={false} error={null} onStockClick={vi.fn()} />);

    expect(screen.getByText('range_break_v15')).toBeInTheDocument();
    expect(screen.getByText('1.234')).toBeInTheDocument();
    expect(screen.getByText('2')).toBeInTheDocument();
  });
});
