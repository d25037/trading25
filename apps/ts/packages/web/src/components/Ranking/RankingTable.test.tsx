import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { RankingItem } from '@/types/ranking';
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
    tradingValue: 1_000_000_000 + (10 - index) * 1_000,
    changePercentage: (index % 5) - 2,
  };
}

function createItems(count: number): RankingItem[] {
  return Array.from({ length: count }, (_, index) => createItem(index));
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
    render(<RankingTable items={createItems(5)} isLoading={false} error={null} onStockClick={vi.fn()} />);
    expect(screen.getByText('Company 1')).toBeInTheDocument();
    expect(screen.queryByRole('combobox')).not.toBeInTheDocument();
  });

  it('sorts the full provided item set with table headers', async () => {
    const user = userEvent.setup();
    render(
      <RankingTable
        items={createItems(5)}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        showChangeForTradingValue
        enableColumnSort
      />
    );

    expect(screen.getAllByRole('row')[1]).toHaveTextContent('7000');

    await user.click(screen.getByRole('button', { name: /騰落率/ }));
    expect(screen.getAllByRole('row')[1]).toHaveTextContent('7004');

    await user.click(screen.getByRole('button', { name: /売買代金/ }));
    expect(screen.getAllByRole('row')[1]).toHaveTextContent('7000');
  });

  it('renders mobile ranking cards and keeps stock navigation', async () => {
    const user = userEvent.setup();
    const onStockClick = vi.fn();
    mockRankingMediaQuery(true);

    render(<RankingTable items={createItems(5)} isLoading={false} error={null} onStockClick={onStockClick} />);

    expect(screen.queryByRole('columnheader', { name: 'コード' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: /7000/ })).toHaveTextContent('Company 1');

    await user.click(screen.getByRole('button', { name: /7000/ }));
    expect(onStockClick).toHaveBeenCalledWith('7000');
  });

  it('keeps mobile virtualized ranking cards scrollable for long lists', () => {
    mockRankingMediaQuery(true);
    const { container } = render(
      <RankingTable items={createItems(130)} isLoading={false} error={null} onStockClick={vi.fn()} />
    );
    const scrollArea = container.querySelector('.overflow-auto');

    expect(scrollArea).not.toBeNull();
    expect(screen.getByText('Company 1')).toBeInTheDocument();
    expect(screen.queryByText('Company 130')).not.toBeInTheDocument();
    expect(container.querySelector('[aria-hidden="true"][style*="height"]')).not.toBeNull();

    fireEvent.scroll(scrollArea as Element, { target: { scrollTop: 128 * 125 } });

    expect(screen.getByText('Company 130')).toBeInTheDocument();
  });

  it('calls onStockClick when row is clicked', async () => {
    const user = userEvent.setup();
    const onStockClick = vi.fn();
    render(<RankingTable items={createItems(5)} isLoading={false} error={null} onStockClick={onStockClick} />);

    await user.click(screen.getByText('7000'));
    expect(onStockClick).toHaveBeenCalledWith('7000');
  });

  it('virtualizes rows when item count exceeds threshold', () => {
    render(<RankingTable items={createItems(130)} isLoading={false} error={null} onStockClick={vi.fn()} />);

    expect(screen.getByText('Company 1')).toBeInTheDocument();
    expect(screen.queryByText('Company 130')).not.toBeInTheDocument();
  });
});
