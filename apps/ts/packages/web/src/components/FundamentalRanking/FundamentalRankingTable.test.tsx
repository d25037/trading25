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
});
