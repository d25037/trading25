import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { RankingItem } from '@trading25/contracts/types/api-response-types';
import { useState } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { DailyRankingTableFilters } from '@/types/ranking';
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

const mockUseStockSearch = vi.fn();

const SEARCH_RESULT = {
  code: '7203',
  companyName: 'Toyota Motor',
  companyNameEnglish: null,
  marketCode: '0111',
  marketName: 'Prime',
  sector33Name: '輸送用機器',
};

vi.mock('@/hooks/useStockSearch', () => ({
  useStockSearch: (...args: unknown[]) => mockUseStockSearch(...args),
}));

function createItems(count: number): RankingItem[] {
  return Array.from({ length: count }, (_, index) => createItem(index));
}

function ControlledRankingTableFilters({
  onFilterChange,
}: {
  onFilterChange: (filters: DailyRankingTableFilters) => void;
}) {
  const [filterState, setFilterState] = useState<DailyRankingTableFilters>({});
  const handleFilterChange = (filters: DailyRankingTableFilters) => {
    setFilterState(filters);
    onFilterChange(filters);
  };
  return (
    <RankingTable
      items={createItems(5)}
      isLoading={false}
      error={null}
      onStockClick={vi.fn()}
      enableTableFilters
      filterState={filterState}
      onFilterChange={handleFilterChange}
    />
  );
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
    window.sessionStorage.clear();
    mockUseStockSearch.mockReset();
    mockUseStockSearch.mockImplementation((query: string) => ({
      data: query ? { results: [SEARCH_RESULT] } : { results: [] },
      isLoading: false,
    }));
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

  it('uses an initial sort state without making the table controlled', async () => {
    const user = userEvent.setup();
    render(
      <RankingTable
        items={createItems(5)}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        showChangeForTradingValue
        enableColumnSort
        initialSortState={{ field: 'changePercentage', order: 'desc' }}
      />
    );

    expect(screen.getAllByRole('row')[1]).toHaveTextContent('7004');

    await user.click(screen.getByRole('button', { name: /騰落率/ }));
    expect(screen.getAllByRole('row')[1]).toHaveTextContent('7000');
  });

  it('filters displayed items before applying table sort', async () => {
    const user = userEvent.setup();
    render(
      <RankingTable
        items={[
          { ...createItem(0), code: '7000', companyName: 'Alpha', sector33Name: 'Electric', forwardPer: 22 },
          { ...createItem(1), code: '7001', companyName: 'Beta', sector33Name: 'Electric', forwardPer: 10 },
          { ...createItem(2), code: '7002', companyName: 'Gamma', sector33Name: 'Retail', forwardPer: 8 },
        ]}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        enableColumnSort
        showValuation
        enableTableFilters
        filterState={{ sector33Name: 'Electric', maxForwardPer: 15 }}
      />
    );

    expect(screen.getByRole('heading', { name: /Market Rankings/ })).toHaveTextContent('(1 / 3)');
    expect(screen.queryByText('Alpha')).not.toBeInTheDocument();
    expect(screen.getByText('Beta')).toBeInTheDocument();
    expect(screen.queryByText('Gamma')).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: /Fwd PER/ }));
    expect(screen.getAllByRole('row')[1]).toHaveTextContent('7001');
  });

  it('closes the table filter dialog with the Close button and Escape', async () => {
    const user = userEvent.setup();

    render(
      <RankingTable
        items={createItems(5)}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        enableTableFilters
        filterState={{}}
        onFilterChange={vi.fn()}
      />
    );

    await user.click(screen.getByRole('button', { name: 'Filter' }));
    expect(screen.getByRole('dialog', { name: 'Table Filters' })).toBeInTheDocument();
    expect(screen.getAllByRole('button', { name: 'Close' })).toHaveLength(2);

    await user.keyboard('{Escape}');
    expect(screen.queryByRole('dialog', { name: 'Table Filters' })).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Filter' }));
    const closeButtons = screen.getAllByRole('button', { name: 'Close' });
    await user.click(closeButtons.at(-1) as HTMLElement);
    expect(screen.queryByRole('dialog', { name: 'Table Filters' })).not.toBeInTheDocument();
  });

  it('highlights configured table filters and removes them from active chips', async () => {
    const user = userEvent.setup();
    const onFilterChange = vi.fn();

    render(
      <RankingTable
        items={createItems(5)}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        enableTableFilters
        filterState={{
          market: 'prime',
          text: 'Company',
          valuationSignal: 'undervalued',
          warningSignal: 'sma5_below_streak_3',
          minForwardPer: 20,
          minForecastOperatingProfitGrowthRatio: 1.2,
          minSma5AboveCount5d: 4,
        }}
        onFilterChange={onFilterChange}
      />
    );

    await user.click(screen.getByRole('button', { name: /Filter/ }));

    expect(screen.getByRole('region', { name: 'Active table filters' })).toHaveTextContent('Search: Company');
    expect(screen.getByRole('region', { name: 'Active table filters' })).toHaveTextContent('Market: prime');
    expect(screen.getByRole('region', { name: 'Active table filters' })).toHaveTextContent('Fundamental: Undervalued');
    expect(screen.getByRole('region', { name: 'Active table filters' })).toHaveTextContent(
      'Warning: SMA5 Bear Streak 3'
    );
    expect(screen.getByRole('region', { name: 'Active table filters' })).toHaveTextContent('Fwd PER >= 20');
    expect(screen.getByRole('region', { name: 'Active table filters' })).toHaveTextContent('Fwd OP/OP >= 1.2');
    expect(screen.getByRole('region', { name: 'Active table filters' })).toHaveTextContent('SMA5 >= 4');
    expect(screen.getByLabelText('Fwd PER Min')).toHaveClass('border-primary/70');
    expect(screen.getByLabelText('Fwd PER Min')).toHaveClass('bg-primary/5');
    expect(screen.getAllByPlaceholderText('Min').length).toBeGreaterThan(0);
    expect(screen.getAllByPlaceholderText('Max').length).toBeGreaterThan(0);
    expect(screen.queryByPlaceholderText('Fwd PER Min')).not.toBeInTheDocument();
    expect(screen.getByLabelText('Fwd OP/OP Min')).toHaveClass('border-primary/70');
    expect(screen.getByLabelText('SMA5 Min')).toHaveClass('border-primary/70');
    expect(screen.getByText('Fundamental')).toBeInTheDocument();
    expect(screen.queryByText('Signal')).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Show Regime filter details' }));
    expect(screen.getByRole('dialog', { name: 'Show Regime filter details' })).toHaveTextContent('Liquidity regime');
    await user.keyboard('{Escape}');
    expect(screen.queryByRole('dialog', { name: 'Show Regime filter details' })).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Show Fundamental filter details' }));
    expect(screen.getByRole('dialog', { name: 'Show Fundamental filter details' })).toHaveTextContent('Deep Value');
    expect(screen.getByRole('dialog', { name: 'Show Fundamental filter details' })).toHaveTextContent('Undervalued');
    expect(screen.getByRole('dialog', { name: 'Show Fundamental filter details' })).toHaveTextContent(
      'Very Overvalued'
    );
    expect(screen.getByRole('dialog', { name: 'Show Fundamental filter details' })).toHaveTextContent('No Earnings');
    await user.keyboard('{Escape}');

    await user.click(screen.getByRole('button', { name: 'Show ATR filter details' }));
    expect(screen.getByRole('dialog', { name: 'Show ATR filter details' })).toHaveTextContent('ATR20 Accel');
    await user.keyboard('{Escape}');

    await user.click(screen.getByLabelText('Warning'));
    expect(screen.getByText('SMA5 Weak 0/1')).toBeInTheDocument();
    expect(screen.getAllByText('SMA5 Bear Streak 3').length).toBeGreaterThan(0);
    await user.keyboard('{Escape}');

    await user.click(screen.getByRole('button', { name: 'Show Warning filter details' }));
    expect(screen.getByRole('dialog', { name: 'Show Warning filter details' })).toHaveTextContent('SMA5 Weak 0/1');
    fireEvent.pointerDown(screen.getByRole('dialog', { name: 'Table Filters' }));
    expect(screen.queryByRole('dialog', { name: 'Show Warning filter details' })).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Remove Fwd PER >= 20' }));
    expect(onFilterChange).toHaveBeenCalledWith({
      market: 'prime',
      text: 'Company',
      valuationSignal: 'undervalued',
      warningSignal: 'sma5_below_streak_3',
      minForwardPer: undefined,
      minForecastOperatingProfitGrowthRatio: 1.2,
      minSma5AboveCount5d: 4,
    });
  });

  it('uses stock search suggestions for the table text filter', async () => {
    const user = userEvent.setup();
    const onFilterChange = vi.fn();

    render(<ControlledRankingTableFilters onFilterChange={onFilterChange} />);

    await user.click(screen.getByRole('button', { name: 'Filter' }));

    const searchInput = screen.getByRole('searchbox', { name: 'Search' });
    expect(searchInput).toHaveAttribute('placeholder', 'Code or company name');

    await user.type(searchInput, 'トヨタ');
    await user.click(await screen.findByRole('button', { name: /7203 Toyota Motor/i }));

    expect(onFilterChange).toHaveBeenLastCalledWith({ text: '7203' });
  });

  it('preserves decimal drafts in numeric table filters', async () => {
    const user = userEvent.setup();
    const onFilterChange = vi.fn();

    render(<ControlledRankingTableFilters onFilterChange={onFilterChange} />);

    await user.click(screen.getByRole('button', { name: 'Filter' }));

    const sectorStrengthMin = screen.getByLabelText('Sector Strength Min');
    await user.type(sectorStrengthMin, '0.8');

    expect(sectorStrengthMin).toHaveValue('0.8');
    expect(onFilterChange).toHaveBeenLastCalledWith({ minSectorScore: 0.8 });
  });

  it('places market cap immediately to the right of trading value when valuation columns are shown', () => {
    render(<RankingTable items={createItems(5)} isLoading={false} error={null} onStockClick={vi.fn()} showValuation />);

    const headerRow = screen.getAllByRole('row').at(0);
    expect(headerRow).toBeDefined();
    const headerText = headerRow?.textContent ?? '';
    expect(headerText.indexOf('売買代金')).toBeLessThan(headerText.indexOf('時価総額'));
  });

  it('renders Fwd OP/OP instead of Fwd P/OP between Fwd PER and PSR when valuation columns are shown', () => {
    render(
      <RankingTable
        items={[
          {
            ...createItem(0),
            forwardPOp: 7.3,
            forecastOperatingProfitGrowthRatio: 1.42,
            psr: 1.4,
            forwardPsr: 1.1,
            pbr: 0.8,
            valueCompositeScore: 0.82,
          } as RankingItem,
        ]}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        showValuation
      />
    );

    const headers = screen.getAllByRole('columnheader').map((header) => header.textContent ?? '');
    expect(headers).not.toContain('Fwd P/OP');
    expect(headers.indexOf('Fwd PER')).toBeLessThan(headers.indexOf('Fwd OP/OP'));
    expect(headers.indexOf('Fwd OP/OP')).toBeLessThan(headers.indexOf('PSR'));
    expect(headers.indexOf('PSR')).toBeLessThan(headers.indexOf('Fwd PSR'));
    expect(headers.indexOf('Fwd PSR')).toBeLessThan(headers.indexOf('PBR'));
    expect(headers.indexOf('PBR')).toBeLessThan(headers.indexOf('F/PBR Score'));
    expect(screen.getByText('1.42x')).toBeInTheDocument();
    expect(screen.getByText('1.40x')).toBeInTheDocument();
    expect(screen.getByText('1.10x')).toBeInTheDocument();
    expect(screen.getByText('0.82')).toBeInTheDocument();
  });

  it('sorts by Fwd OP/OP with missing values last', async () => {
    const user = userEvent.setup();
    render(
      <RankingTable
        items={[
          { ...createItem(0), code: '7000', forecastOperatingProfitGrowthRatio: 0.7 } as RankingItem,
          { ...createItem(1), code: '7001', forecastOperatingProfitGrowthRatio: 1.6 } as RankingItem,
          { ...createItem(2), code: '7002', forecastOperatingProfitGrowthRatio: null } as RankingItem,
        ]}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        showValuation
        enableColumnSort
      />
    );

    await user.click(screen.getByRole('button', { name: /Fwd OP\/OP/ }));
    expect(screen.getAllByRole('row')[1]).toHaveTextContent('7001');

    await user.click(screen.getByRole('button', { name: /Fwd OP\/OP/ }));
    expect(screen.getAllByRole('row')[1]).toHaveTextContent('7000');
    expect(screen.getAllByRole('row')[3]).toHaveTextContent('7002');
  });

  it('renders SMA5 5D count between current price and PER when valuation columns are shown', () => {
    render(
      <RankingTable
        items={[
          {
            ...createItem(0),
            sma5AboveCount5d: 4,
            per: 12.3,
          } as RankingItem,
        ]}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        showValuation
      />
    );

    const headers = screen.getAllByRole('columnheader').map((header) => header.textContent ?? '');
    expect(headers.indexOf('現在値')).toBeLessThan(headers.indexOf('SMA5 5D'));
    expect(headers.indexOf('SMA5 5D')).toBeLessThan(headers.indexOf('PER'));
    expect(screen.getByText('4')).toBeInTheDocument();
  });

  it('colors PSR and Fwd PSR by bad-side PIT percentile thresholds', () => {
    render(
      <RankingTable
        items={[
          {
            ...createItem(0),
            psr: 2.22,
            psrPercentile: 0.85,
            forwardPsr: 3.33,
            forwardPsrPercentile: 0.95,
          } as RankingItem,
        ]}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        showValuation
      />
    );

    expect(screen.getByText('2.22x')).toHaveClass('text-red-600');
    expect(screen.getByText('3.33x')).toHaveClass('text-purple-700');
  });

  it('renders and sorts F/PBR value composite score', async () => {
    const user = userEvent.setup();
    render(
      <RankingTable
        items={[
          { ...createItem(0), code: '7000', valueCompositeScore: 0.15 } as RankingItem,
          { ...createItem(1), code: '7001', valueCompositeScore: 0.92 } as RankingItem,
          { ...createItem(2), code: '7002', valueCompositeScore: null } as RankingItem,
        ]}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        showValuation
        enableColumnSort
      />
    );

    expect(screen.getByText('0.92')).toHaveClass('text-green-600');
    expect(screen.getByText('0.15')).toHaveClass('text-yellow-600');

    await user.click(screen.getByRole('button', { name: /F\/PBR Score/ }));
    expect(screen.getAllByRole('row')[1]).toHaveTextContent('7001');

    await user.click(screen.getByRole('button', { name: /F\/PBR Score/ }));
    expect(screen.getAllByRole('row')[1]).toHaveTextContent('7000');
    expect(screen.getAllByRole('row')[3]).toHaveTextContent('7002');
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

  it('shows valuation signals without collapsing extra chips behind a count', () => {
    render(
      <RankingTable
        items={[
          {
            ...createItem(0),
            pbrPercentile: 0.18,
            forwardPerPercentile: 0.18,
            liquidityRegime: 'crowded_rerating',
            riskFlags: ['overheat', 'stale_rally_fade'],
            technicalFlags: ['momentum_20_60_top20'],
          },
          {
            ...createItem(1),
            perPercentile: 0.85,
            forwardPerPercentile: 0.5,
            liquidityRegime: 'neutral_rerating',
          },
          {
            ...createItem(2),
            pbrPercentile: 0.95,
            liquidityRegime: 'neutral_rerating',
          },
          {
            ...createItem(3),
            perPercentile: null,
            forwardPerPercentile: null,
            liquidityRegime: 'crowded_rerating',
          },
        ]}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        showLiquidity
      />
    );

    expect(screen.getByText('Deep Value')).toBeInTheDocument();
    expect(screen.getByText('Overvalued')).toHaveClass('text-yellow-800');
    expect(screen.getByText('Very Overvalued')).toHaveClass('text-red-700');
    expect(screen.getByText('No Earnings')).toHaveClass('text-yellow-800');
    expect(screen.getByText('Overheat')).toBeInTheDocument();
    expect(screen.getByText('Rally Fade')).toBeInTheDocument();
    expect(screen.getByText('20/60D Mom')).toBeInTheDocument();
    expect(screen.queryByText('+2')).not.toBeInTheDocument();
    expect(screen.queryByText('+1')).not.toBeInTheDocument();
  });

  it('shows sector strength next to sector when provided and supports score sorting', async () => {
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

    expect(screen.getByRole('button', { name: /Sector Strength/ })).toBeInTheDocument();
    expect(screen.getByText('0.90')).toBeInTheDocument();
    expect(screen.getByText('0.10')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: /Sector Strength/ }));
    expect(screen.getAllByRole('row')[1]).toHaveTextContent('7000');

    await user.click(screen.getByRole('button', { name: /Sector Strength/ }));
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
            forecastOperatingProfitGrowthRatio: 0.73,
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
            forecastOperatingProfitGrowthRatio: 1.27,
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
            forecastOperatingProfitGrowthRatio: 0.9,
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
    expect(screen.getByRole('columnheader', { name: 'Fwd OP/OP' })).toBeInTheDocument();
    expect(screen.queryByRole('columnheader', { name: 'Fwd P/OP' })).not.toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: '流動性Z' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Regime' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Signals' })).toBeInTheDocument();
    expect(screen.queryByRole('columnheader', { name: 'Med ADV60/FF' })).not.toBeInTheDocument();
    expect(screen.queryByText('8.00%')).not.toBeInTheDocument();
    expect(screen.getByText('Overheat')).toHaveClass('text-purple-700');
    expect(screen.getByText('Rally Fade')).toHaveClass('text-red-700');
    expect(screen.getByText('8.00x')).toHaveClass('text-sky-600');
    expect(screen.getByText('11.00x')).toHaveClass('text-yellow-600');
    expect(screen.getByText('0.73x')).toHaveClass('text-red-600');
    expect(screen.getByText('3.00x')).toHaveClass('text-red-600');
    expect(screen.getByText('7.00x')).toHaveClass('text-green-600');
    expect(screen.getByText('1.27x')).toHaveClass('text-sky-600');
    expect(screen.getByText('0.90x')).toHaveClass('text-yellow-600');
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

  it('saves and restores the table scroll position for the same restoration key', () => {
    const { container, unmount } = render(
      <RankingTable
        items={createItems(130)}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        scrollRestorationKey="ranking-scroll:test"
      />
    );
    const scrollArea = container.querySelector('.overflow-auto') as HTMLDivElement;

    fireEvent.scroll(scrollArea, { target: { scrollTop: 480 } });

    expect(window.sessionStorage.getItem('ranking-scroll:test')).toBe('480');

    unmount();

    const restored = render(
      <RankingTable
        items={createItems(130)}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        scrollRestorationKey="ranking-scroll:test"
      />
    );
    const restoredScrollArea = restored.container.querySelector('.overflow-auto') as HTMLDivElement;

    expect(restoredScrollArea.scrollTop).toBe(480);
  });

  it('resets table scroll when the restoration key changes without a saved position', () => {
    const view = render(
      <RankingTable
        items={createItems(130)}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        scrollRestorationKey="ranking-scroll:first"
      />
    );
    const scrollArea = view.container.querySelector('.overflow-auto') as HTMLDivElement;

    fireEvent.scroll(scrollArea, { target: { scrollTop: 480 } });

    expect(scrollArea.scrollTop).toBe(480);

    view.rerender(
      <RankingTable
        items={createItems(130)}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        scrollRestorationKey="ranking-scroll:second"
      />
    );

    expect(scrollArea.scrollTop).toBe(0);
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
