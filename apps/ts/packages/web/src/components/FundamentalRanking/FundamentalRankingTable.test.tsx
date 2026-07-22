import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { FundamentalRankingTable } from './FundamentalRankingTable';

vi.mock('@/hooks/useVirtualizedRows', () => ({
  useVirtualizedRows: (items: unknown[]) => ({
    visibleItems: items,
    paddingTop: 0,
    paddingBottom: 0,
    onScroll: vi.fn(),
  }),
}));

const item = (code: string, actualEps: number, forecastEps: number) => ({
  rank: 1,
  code,
  companyName: `Company ${code}`,
  marketCode: '0111',
  sector33Name: 'Sector',
  currentPrice: 1000,
  volume: 100_000,
  epsValue: forecastEps / actualEps,
  actualEps,
  forecastEps,
  forecastToActualRatio: forecastEps / actualEps,
  forecastEpsChangeRate: ((forecastEps - actualEps) / actualEps) * 100,
  disclosedDate: '2024-05-15',
  actualDisclosedDate: '2024-05-10',
  forecastDisclosedDate: '2024-05-15',
  periodType: 'FY',
  source: 'revised',
});

describe('FundamentalRankingTable', () => {
  it('shows the four EPS ranking tabs and switches to actual low', async () => {
    const user = userEvent.setup();
    render(
      <FundamentalRankingTable
        rankings={
          {
            ratioHigh: [],
            ratioLow: [],
            forecastHigh: [item('1111', 10, 30)],
            forecastLow: [item('2222', 10, 12)],
            actualHigh: [item('3333', 40, 50)],
            actualLow: [item('4444', 2, 8)],
          } as never
        }
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
      />,
    );

    expect(screen.getByText('Company 1111')).toBeInTheDocument();
    expect(screen.getByText('30')).toBeInTheDocument();

    await user.click(screen.getByRole('combobox', { name: 'Fundamental ranking type' }));
    expect(screen.getByRole('option', { name: 'Forecast High' })).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'Forecast Low' })).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'Actual High' })).toBeInTheDocument();
    await user.click(screen.getByRole('option', { name: 'Actual Low' }));

    expect(screen.getByText('Company 4444')).toBeInTheDocument();
    expect(screen.getByText('2')).toBeInTheDocument();
  });
});
