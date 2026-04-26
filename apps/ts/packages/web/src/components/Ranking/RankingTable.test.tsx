import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Rankings } from '@/types/ranking';
import { RankingTable } from './RankingTable';

const baseItem = {
  rank: 1,
  marketCode: 'prime',
  sector33Name: 'Transport Equipment',
  currentPrice: 3000,
  volume: 1000000,
};

function createItem(index: number) {
  return {
    ...baseItem,
    rank: index + 1,
    code: String(7000 + index),
    companyName: `Company ${index + 1}`,
    tradingValue: 1_000_000_000 + index,
    changePercentage: (index % 5) - 2,
  };
}

function createRankings(count: number): Rankings {
  const items = Array.from({ length: count }, (_, index) => createItem(index));
  return {
    tradingValue: items,
    gainers: items,
    losers: items,
    periodHigh: items,
    periodLow: items,
  };
}

function mockRankingMediaQuery(matches: boolean) {
  vi.stubGlobal(
    'matchMedia',
    vi.fn().mockImplementation((query: string) => ({
      matches,
      media: query,
      onchange: null,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      addListener: vi.fn(),
      removeListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }))
  );
}

describe('RankingTable', () => {
  beforeEach(() => {
    vi.unstubAllGlobals();
  });
  it('renders trading value rows by default', () => {
    render(<RankingTable rankings={createRankings(5)} isLoading={false} error={null} onStockClick={vi.fn()} />);
    expect(screen.getByText('Company 1')).toBeInTheDocument();
    expect(screen.queryByText('Change')).not.toBeInTheDocument();
  });

  it('switches to period tab and updates change header', async () => {
    const user = userEvent.setup();
    render(
      <RankingTable
        rankings={createRankings(5)}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        periodDays={30}
      />
    );

    await user.click(screen.getByRole('combobox'));
    await user.click(screen.getByRole('option', { name: '30D High' }));
    expect(screen.getByText('Break %')).toBeInTheDocument();
  });

  it('renders mobile ranking cards and keeps stock navigation', async () => {
    const user = userEvent.setup();
    const onStockClick = vi.fn();
    mockRankingMediaQuery(true);

    render(<RankingTable rankings={createRankings(5)} isLoading={false} error={null} onStockClick={onStockClick} />);

    expect(screen.queryByRole('columnheader', { name: 'Code' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: /7000/ })).toHaveTextContent('Company 1');

    await user.click(screen.getByRole('button', { name: /7000/ }));
    expect(onStockClick).toHaveBeenCalledWith('7000');
  });

  it('calls onStockClick when row is clicked', async () => {
    const user = userEvent.setup();
    const onStockClick = vi.fn();
    render(<RankingTable rankings={createRankings(5)} isLoading={false} error={null} onStockClick={onStockClick} />);

    await user.click(screen.getByText('7000'));
    expect(onStockClick).toHaveBeenCalledWith('7000');
  });

  it('virtualizes rows when item count exceeds threshold', () => {
    render(<RankingTable rankings={createRankings(130)} isLoading={false} error={null} onStockClick={vi.fn()} />);

    expect(screen.getByText('Company 1')).toBeInTheDocument();
    expect(screen.queryByText('Company 130')).not.toBeInTheDocument();
  });
});
