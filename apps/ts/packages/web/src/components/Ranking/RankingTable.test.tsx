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

  it('colors valuation percentiles and liquidity evidence tiers', () => {
    render(
      <RankingTable
        items={[
          {
            ...createItem(0),
            per: 8,
            perPercentile: 0.15,
            forwardPer: 11,
            forwardPerPercentile: 0.85,
            forwardPOp: 9,
            forwardPOpPercentile: 0.95,
            pbr: 3,
            pbrPercentile: 0.95,
            liquidityRegime: 'crowded_rerating',
            liquidityResidualZ: 1.2,
            adv60ToFreeFloatPct: 8,
            riskFlags: ['overheat'],
          },
          {
            ...createItem(1),
            per: 13,
            perPercentile: 0.15,
            forwardPer: 7,
            forwardPerPercentile: 0.5,
            forwardPOp: 6,
            forwardPOpPercentile: 0.5,
            pbr: 0.5,
            pbrPercentile: 0.05,
            liquidityRegime: 'neutral_rerating',
            liquidityResidualZ: -1.4,
            adv60ToFreeFloatPct: 2,
          },
          {
            ...createItem(2),
            per: 10,
            perPercentile: 0.15,
            forwardPer: 8.5,
            forwardPerPercentile: 0.15,
            pbr: 0.6,
            pbrPercentile: 0.1,
            liquidityRegime: 'neutral_rerating',
            liquidityResidualZ: -0.2,
            adv60ToFreeFloatPct: 3,
          },
          {
            ...createItem(3),
            per: 10,
            perPercentile: 0.15,
            forwardPOp: 14,
            forwardPOpPercentile: 0.5,
            liquidityRegime: 'distribution_stress',
            liquidityResidualZ: 1.4,
            adv60ToFreeFloatPct: 10,
          },
          {
            ...createItem(4),
            per: 10,
            perPercentile: 0.15,
            forwardPer: 5,
            forwardPerPercentile: 0.5,
            pbr: 0.7,
            pbrPercentile: 0.4,
            liquidityRegime: 'crowded_rerating',
            liquidityResidualZ: 2,
            adv60ToFreeFloatPct: 6,
          },
          {
            ...createItem(5),
            per: 72,
            perPercentile: 0.98,
            forwardPer: 315,
            forwardPerPercentile: 0.99,
            pbr: 0.75,
            pbrPercentile: 0.16,
            liquidityRegime: 'crowded_rerating',
            liquidityResidualZ: 1.6,
            adv60ToFreeFloatPct: 7,
          },
          {
            ...createItem(6),
            per: null,
            perPercentile: null,
            forwardPer: null,
            forwardPerPercentile: null,
            pbr: 0.46,
            pbrPercentile: 0.03,
            liquidityRegime: 'crowded_rerating',
            liquidityResidualZ: 3.05,
            adv60ToFreeFloatPct: 12,
          },
        ]}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        showValuation
        showLiquidity
      />
    );

    expect(screen.getByText('Stress')).toHaveClass('text-yellow-800');
    expect(screen.getAllByText('Neutral Rerating')[0]).toHaveClass('text-green-700');
    expect(screen.getAllByText('Neutral Rerating')[1]).toHaveClass('text-sky-700');
    expect(screen.getAllByText('Crowded Rerating')[0]).toHaveClass('text-yellow-800');
    expect(screen.getAllByText('Crowded Rerating')[1]).toHaveClass('text-green-700');
    expect(screen.getAllByText('Crowded Rerating')[2]).toHaveClass('text-yellow-800');
    expect(screen.getAllByText('Crowded Rerating')[3]).toHaveClass('text-yellow-800');
    expect(screen.getByText('Prime 20d excess evidence')).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Fwd P/OP' })).toBeInTheDocument();
    expect(screen.getByText('Overheat')).toHaveClass('text-purple-700');
    expect(screen.getByText('8.00x')).toHaveClass('text-sky-600');
    expect(screen.getByText('11.00x')).toHaveClass('text-yellow-600');
    expect(screen.getByText('9.00x')).toHaveClass('text-red-600');
    expect(screen.getByText('3.00x')).toHaveClass('text-red-600');
    expect(screen.getByText('7.00x')).toHaveClass('text-green-600');
    expect(screen.getByText('6.00x')).not.toHaveClass('text-sky-600');
    expect(screen.getByText('14.00x')).toHaveClass('text-yellow-600');
    expect(screen.getByText('0.50x')).toHaveClass('text-green-600');
    expect(screen.getByText('+1.20')).toHaveClass('text-yellow-600');
    expect(screen.getByText('-1.40')).toHaveClass('text-green-600');
    expect(screen.getByText('-0.20')).toHaveClass('text-sky-600');
    expect(screen.getByText('+2.00')).toHaveClass('text-green-600');
    expect(screen.getByText('+1.60')).toHaveClass('text-yellow-600');
    expect(screen.getByText('+3.05')).toHaveClass('text-yellow-600');
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

    fireEvent.scroll(scrollArea as Element, { target: { scrollTop: 160 * 125 } });

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
