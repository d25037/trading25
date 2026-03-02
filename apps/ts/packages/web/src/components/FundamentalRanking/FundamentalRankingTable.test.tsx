import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import type { FundamentalRankings } from '@/types/fundamentalRanking';
import { FundamentalRankingTable } from './FundamentalRankingTable';

const baseItem = {
  rank: 1,
  marketCode: 'prime',
  sector33Name: 'Transport Equipment',
  currentPrice: 3000,
  volume: 1000000,
  disclosedDate: '2025-01-30',
  periodType: 'FY',
  source: 'fy' as const,
};

const rankings: FundamentalRankings = {
  ratioHigh: [{ ...baseItem, code: '7203', companyName: 'Toyota', epsValue: 1.5 }],
  ratioLow: [{ ...baseItem, code: '6502', companyName: 'Toshiba', epsValue: 0.72 }],
};

function createRankings(count: number): FundamentalRankings {
  const items = Array.from({ length: count }, (_, index) => ({
    ...baseItem,
    rank: index + 1,
    code: String(5000 + index),
    companyName: `Company ${index + 1}`,
    epsValue: 1 + index / 100,
  }));

  return {
    ratioHigh: items,
    ratioLow: items,
  };
}

describe('FundamentalRankingTable', () => {
  it('renders default tab rows', () => {
    render(<FundamentalRankingTable rankings={rankings} isLoading={false} error={null} onStockClick={vi.fn()} />);
    expect(screen.getByText('Toyota')).toBeInTheDocument();
  });

  it('switches tab and renders selected rows', async () => {
    const user = userEvent.setup();
    render(<FundamentalRankingTable rankings={rankings} isLoading={false} error={null} onStockClick={vi.fn()} />);
    await user.click(screen.getByRole('button', { name: 'Ratio Low' }));
    expect(screen.getByText('Toshiba')).toBeInTheDocument();
  });

  it('shows empty state when no data', () => {
    render(
      <FundamentalRankingTable
        rankings={{ ratioHigh: [], ratioLow: [] }}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
      />
    );
    expect(screen.getByText('No fundamental ranking data available')).toBeInTheDocument();
  });

  it('virtualizes rows when item count exceeds threshold', () => {
    render(
      <FundamentalRankingTable rankings={createRankings(130)} isLoading={false} error={null} onStockClick={vi.fn()} />
    );

    expect(screen.getByText('Company 1')).toBeInTheDocument();
    expect(screen.queryByText('Company 130')).not.toBeInTheDocument();
  });
});
