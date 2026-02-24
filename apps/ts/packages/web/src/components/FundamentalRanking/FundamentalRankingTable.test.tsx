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
  forecastHigh: [{ ...baseItem, code: '7203', companyName: 'Toyota', epsValue: 500 }],
  forecastLow: [{ ...baseItem, code: '6502', companyName: 'Toshiba', epsValue: -20 }],
  actualHigh: [{ ...baseItem, code: '6758', companyName: 'Sony', epsValue: 450 }],
  actualLow: [{ ...baseItem, code: '8306', companyName: 'MUFG', epsValue: -15 }],
};

describe('FundamentalRankingTable', () => {
  it('renders default tab rows', () => {
    render(<FundamentalRankingTable rankings={rankings} isLoading={false} error={null} onStockClick={vi.fn()} />);
    expect(screen.getByText('Toyota')).toBeInTheDocument();
  });

  it('switches tab and renders selected rows', async () => {
    const user = userEvent.setup();
    render(<FundamentalRankingTable rankings={rankings} isLoading={false} error={null} onStockClick={vi.fn()} />);
    await user.click(screen.getByRole('button', { name: 'Actual Low' }));
    expect(screen.getByText('MUFG')).toBeInTheDocument();
  });

  it('shows empty state when no data', () => {
    render(
      <FundamentalRankingTable
        rankings={{ forecastHigh: [], forecastLow: [], actualHigh: [], actualLow: [] }}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
      />
    );
    expect(screen.getByText('No fundamental ranking data available')).toBeInTheDocument();
  });
});
