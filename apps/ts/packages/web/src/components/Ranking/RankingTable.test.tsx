import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { RankingItem } from '@trading25/contracts/types/api-response-types';
import { beforeEach, describe, expect, it, vi } from 'vitest';
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

  it('places market cap immediately to the right of trading value when valuation columns are shown', () => {
    render(<RankingTable items={createItems(5)} isLoading={false} error={null} onStockClick={vi.fn()} showValuation />);

    const headerRow = screen.getAllByRole('row').at(0);
    expect(headerRow).toBeDefined();
    const headerText = headerRow?.textContent ?? '';
    expect(headerText.indexOf('売買代金')).toBeLessThan(headerText.indexOf('時価総額'));
  });

  it('shows momentum technical flags in the liquidity state chips', () => {
    render(
      <RankingTable
        items={[
          {
            ...createItem(0),
            liquidityRegime: 'neutral_rerating',
            technicalFlags: ['momentum_20_60_top20'],
          },
        ]}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        showLiquidity
      />
    );

    expect(screen.getByText('20/60D Mom')).toBeInTheDocument();
  });

  it('shows sector score next to sector when provided and supports score sorting', async () => {
    const user = userEvent.setup();
    render(
      <RankingTable
        items={[
          {
            ...createItem(0),
            sectorStrengthScore: 0.9,
            sectorStrengthBucket: 'sector_strong',
          },
          {
            ...createItem(1),
            sectorStrengthScore: 0.1,
            sectorStrengthBucket: 'sector_weak',
          },
        ]}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        enableColumnSort
      />
    );

    expect(screen.getByRole('button', { name: /Sector Score/ })).toBeInTheDocument();
    expect(screen.getByText('0.90')).toBeInTheDocument();
    expect(screen.getByText('0.10')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: /Sector Score/ }));
    expect(screen.getAllByRole('row')[1]).toHaveTextContent('7000');

    await user.click(screen.getByRole('button', { name: /Sector Score/ }));
    expect(screen.getAllByRole('row')[1]).toHaveTextContent('7001');
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
            ...createItem(9),
            per: 12,
            perPercentile: 0.5,
            forwardPer: 13,
            forwardPerPercentile: 0.5,
            pbr: 0.6,
            pbrPercentile: 0.1,
            liquidityRegime: 'neutral_rerating',
            liquidityResidualZ: -0.35,
            adv60ToFreeFloatPct: 3,
          },
          {
            ...createItem(10),
            per: 16,
            perPercentile: 0.5,
            forwardPer: 17,
            forwardPerPercentile: 0.5,
            pbr: 1.2,
            pbrPercentile: 0.5,
            liquidityRegime: 'neutral_rerating',
            liquidityResidualZ: -0.45,
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
          {
            ...createItem(7),
            per: 80,
            perPercentile: 0.85,
            forwardPer: 90,
            forwardPerPercentile: 0.7,
            pbr: 5,
            pbrPercentile: 0.9,
            liquidityRegime: 'stale_liquidity',
            liquidityResidualZ: -1.8,
            adv60ToFreeFloatPct: 1,
            riskFlags: ['stale_rally_fade'],
          },
          {
            ...createItem(8),
            per: 10,
            perPercentile: 0.5,
            forwardPer: 12,
            forwardPerPercentile: 0.5,
            pbr: 0.6,
            pbrPercentile: 0.1,
            liquidityRegime: 'stale_liquidity',
            liquidityResidualZ: -1.7,
            adv60ToFreeFloatPct: 1,
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
    expect(screen.getAllByText('Neutral Rerating')[2]).toHaveClass('text-cyan-700');
    expect(screen.getAllByText('Neutral Rerating')[3]).toHaveClass('text-muted-foreground');
    expect(screen.getAllByText('Crowded Rerating')[0]).toHaveClass('text-yellow-800');
    expect(screen.getAllByText('Crowded Rerating')[1]).toHaveClass('text-green-700');
    expect(screen.getAllByText('Crowded Rerating')[2]).toHaveClass('text-yellow-800');
    expect(screen.getAllByText('Crowded Rerating')[3]).toHaveClass('text-yellow-800');
    expect(screen.getAllByText('Stale')[0]).toHaveClass('text-red-700');
    expect(screen.getAllByText('Stale')[1]).toHaveClass('text-yellow-800');
    expect(screen.getByText('Prime 20d excess evidence')).toBeInTheDocument();
    expect(screen.getByText('light')).toHaveClass('text-cyan-600');
    expect(screen.getByRole('columnheader', { name: 'Fwd P/OP' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: '流動性Z' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Regime' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Signals' })).toBeInTheDocument();
    expect(screen.queryByRole('columnheader', { name: 'Med ADV60/FF' })).not.toBeInTheDocument();
    expect(screen.queryByText('8.00%')).not.toBeInTheDocument();
    expect(screen.getByText('Overheat')).toHaveClass('text-purple-700');
    expect(screen.getByText('Rally Fade')).toHaveClass('text-red-700');
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
    expect(screen.getByText('-0.35')).toHaveClass('text-cyan-600');
    expect(screen.getByText('-0.45')).not.toHaveClass('text-sky-600');
    expect(screen.getByText('-0.45')).not.toHaveClass('text-cyan-600');
    expect(screen.getByText('+2.00')).toHaveClass('text-green-600');
    expect(screen.getByText('+1.60')).toHaveClass('text-yellow-600');
    expect(screen.getByText('+3.05')).toHaveClass('text-yellow-600');
    expect(screen.getByText('-1.80')).toHaveClass('text-red-600');
    expect(screen.getByText('-1.70')).toHaveClass('text-yellow-600');
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
