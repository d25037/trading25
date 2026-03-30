import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { IndicesPage } from './IndicesPage';

type IndexItem = {
  code: string;
  name: string;
  category: string;
};

type SectorStockItem = {
  code: string;
  rank: number;
  marketCode: string;
  companyName: string;
  currentPrice: number;
  tradingValue: number;
  changePercentage: number;
};

let selectedIndexCode: string | null = null;

const mockSetSelectedIndexCode = vi.fn((code: string | null) => {
  selectedIndexCode = code;
});
const mockNavigate = vi.fn();

const mockUseIndicesList = vi.fn();
const mockUseIndexData = vi.fn();
const mockUseSectorStocks = vi.fn();

vi.mock('@/hooks/usePageRouteState', () => ({
  useIndicesRouteState: () => ({
    selectedIndexCode,
    setSelectedIndexCode: mockSetSelectedIndexCode,
  }),
  useMigrateIndicesRouteState: () => {},
}));

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => mockNavigate,
}));

vi.mock('@/hooks/useIndices', () => ({
  useIndicesList: () => mockUseIndicesList(),
  useIndexData: (code: string | null) => mockUseIndexData(code),
}));

vi.mock('@/hooks/useSectorStocks', () => ({
  useSectorStocks: (...args: unknown[]) => mockUseSectorStocks(...args),
}));

vi.mock('@/components/Chart/StockChart', () => ({
  StockChart: () => <div>StockChart</div>,
}));

vi.mock('@/components/Chart/LinePriceChart', () => ({
  LinePriceChart: () => <div>LinePriceChart</div>,
}));

const makeIndicesList = () => ({
  indices: [
    { code: 'N225_UNDERPX', name: '日経平均', category: 'synthetic' },
    { code: 'N225_VI', name: '日経VI', category: 'synthetic' },
    { code: 'NT_RATIO', name: 'NT倍率', category: 'synthetic' },
    { code: '1321', name: 'TOPIX', category: 'topix' },
    { code: '1305', name: 'TOPIX-33 Energy', category: 'sector33' },
  ] satisfies IndexItem[],
});

const makeSectorIndexData = () => ({
  data: [
    {
      date: '2026-02-13',
      open: 1000,
      high: 1200,
      low: 900,
      close: 1100,
    },
  ],
  code: '1305',
  name: 'TOPIX-33 Energy',
  category: 'sector33',
});

const makeTopixIndexData = () => ({
  data: [
    {
      date: '2026-02-13',
      open: 2500,
      high: 2600,
      low: 2400,
      close: 2550,
    },
  ],
  code: '1321',
  name: 'TOPIX',
});

const makeSyntheticIndexData = () => ({
  data: [
    {
      date: '2026-02-13',
      open: 39000,
      high: 39000,
      low: 39000,
      close: 39000,
    },
  ],
  code: 'N225_UNDERPX',
  name: '日経平均',
});

const makeNtRatioIndexData = () => ({
  data: [
    {
      date: '2026-02-13',
      open: 14.12,
      high: 14.12,
      low: 14.12,
      close: 14.12,
    },
  ],
  code: 'NT_RATIO',
  name: 'NT倍率',
});

const makeViIndexData = () => ({
  data: [
    {
      date: '2026-02-13',
      open: 22.34,
      high: 22.34,
      low: 22.34,
      close: 22.34,
    },
  ],
  code: 'N225_VI',
  name: '日経VI',
});

beforeEach(() => {
  vi.clearAllMocks();
  selectedIndexCode = null;

  mockUseIndicesList.mockReturnValue({
    data: { indices: [] },
    isLoading: false,
    error: null,
  });

  mockUseIndexData.mockImplementation(() => ({
    data: null,
    isLoading: false,
    error: null,
  }));

  mockUseSectorStocks.mockReturnValue({
    data: { stocks: [] },
    isLoading: false,
    error: null,
  });
});

describe('IndicesPage', () => {
  it('renders sector index chart and updates symbol on stock click', async () => {
    const user = userEvent.setup();
    selectedIndexCode = '1305';

    mockUseIndicesList.mockReturnValue({
      data: makeIndicesList(),
      isLoading: false,
      error: null,
    });
    mockUseIndexData.mockImplementation((code: string | null) => {
      if (code === '1305') {
        return {
          data: makeSectorIndexData(),
          isLoading: false,
          error: null,
        };
      }
      return {
        data: null,
        isLoading: false,
        error: null,
      };
    });
    mockUseSectorStocks.mockReturnValue({
      data: {
        stocks: [
          {
            code: '1301',
            rank: 1,
            marketCode: 'prime',
            companyName: 'Sample Energy',
            currentPrice: 2400,
            tradingValue: 1000,
            changePercentage: 2.5,
          } satisfies SectorStockItem,
        ],
      } as { stocks: SectorStockItem[]; totalCount?: number },
      isLoading: false,
      error: null,
    });

    render(<IndicesPage />);

    expect(screen.getByRole('button', { name: 'Select TOPIX-33 Energy' })).toBeInTheDocument();
    await user.click(screen.getByText('Sample Energy'));

    expect(mockNavigate).toHaveBeenCalledWith({ to: '/charts', search: { symbol: '1301' } });
  });

  it('renders empty state and chart placeholder when no index is selected', () => {
    selectedIndexCode = null;

    render(<IndicesPage />);

    expect(screen.getByText('No indices found')).toBeInTheDocument();
    expect(screen.getByText('Select an index to view chart')).toBeInTheDocument();
  });

  it('updates selected index when an index is clicked', async () => {
    const user = userEvent.setup();
    const indexList = makeIndicesList();

    mockUseIndicesList.mockReturnValue({
      data: indexList,
      isLoading: false,
      error: null,
    });
    mockUseIndexData.mockImplementation((code: string | null) => {
      if (code === '1321') {
        return {
          data: makeTopixIndexData(),
          isLoading: false,
          error: null,
        };
      }
      return {
        data: null,
        isLoading: false,
        error: null,
      };
    });

    const { rerender } = render(<IndicesPage />);

    await user.click(screen.getByRole('button', { name: 'Select TOPIX' }));
    expect(mockSetSelectedIndexCode).toHaveBeenCalledWith('1321');

    selectedIndexCode = '1321';
    rerender(<IndicesPage />);
    expect(await screen.findByText('Price Chart (1 data points)')).toBeInTheDocument();
  });

  it('handles keyboard navigation and wraps selection', () => {
    const scrollIntoViewMock = vi.fn();
    Object.defineProperty(HTMLElement.prototype, 'scrollIntoView', {
      configurable: true,
      value: scrollIntoViewMock,
    });

    selectedIndexCode = '1321';
    mockUseIndicesList.mockReturnValue({
      data: makeIndicesList(),
      isLoading: false,
      error: null,
    });

    render(<IndicesPage />);

    fireEvent.keyDown(window, { key: 'ArrowUp' });
    expect(mockSetSelectedIndexCode).toHaveBeenCalledWith('NT_RATIO');
    expect(scrollIntoViewMock).toHaveBeenCalled();

    fireEvent.keyDown(window, { key: 'Enter' });
    expect(mockSetSelectedIndexCode).toHaveBeenCalledTimes(1);
  });

  it('renders synthetic Nikkei in benchmarks section with line chart', () => {
    selectedIndexCode = 'N225_UNDERPX';
    mockUseIndicesList.mockReturnValue({
      data: makeIndicesList(),
      isLoading: false,
      error: null,
    });
    mockUseIndexData.mockImplementation((code: string | null) => {
      if (code === 'N225_UNDERPX') {
        return {
          data: makeSyntheticIndexData(),
          isLoading: false,
          error: null,
        };
      }
      if (code === 'N225_VI') {
        return {
          data: makeViIndexData(),
          isLoading: false,
          error: null,
        };
      }
      return {
        data: null,
        isLoading: false,
        error: null,
      };
    });

    render(<IndicesPage />);

    expect(screen.getByText('Benchmarks')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Select 日経平均' })).toBeInTheDocument();
    expect(screen.getByText('UnderPx derived daily reference series')).toBeInTheDocument();
    expect(screen.getByText('日経VI (1 data points)')).toBeInTheDocument();
    expect(screen.getByText('Latest VI')).toBeInTheDocument();
    expect(screen.getByText('22.34')).toBeInTheDocument();
    expect(screen.getAllByText('LinePriceChart')).toHaveLength(2);
    expect(screen.queryByText('StockChart')).not.toBeInTheDocument();
  });

  it('renders NT ratio under Nikkei in benchmarks section with line chart', () => {
    selectedIndexCode = 'NT_RATIO';
    mockUseIndicesList.mockReturnValue({
      data: makeIndicesList(),
      isLoading: false,
      error: null,
    });
    mockUseIndexData.mockImplementation((code: string | null) => {
      if (code === 'NT_RATIO') {
        return {
          data: makeNtRatioIndexData(),
          isLoading: false,
          error: null,
        };
      }
      return {
        data: null,
        isLoading: false,
        error: null,
      };
    });

    render(<IndicesPage />);

    const indexButtons = screen.getAllByRole('button', { name: /^Select / });
    expect(indexButtons[0]).toHaveAttribute('aria-label', 'Select 日経平均');
    expect(indexButtons[1]).toHaveAttribute('aria-label', 'Select 日経VI');
    expect(indexButtons[2]).toHaveAttribute('aria-label', 'Select NT倍率');
    expect(screen.getByText('Nikkei 225 close / TOPIX close from local market snapshot')).toBeInTheDocument();
    expect(screen.getByText('14.12')).toBeInTheDocument();
    expect(screen.getByText('LinePriceChart')).toBeInTheDocument();
  });

  it('renders Nikkei VI in benchmarks section with line chart and 2-decimal latest value', () => {
    selectedIndexCode = 'N225_VI';
    mockUseIndicesList.mockReturnValue({
      data: makeIndicesList(),
      isLoading: false,
      error: null,
    });
    mockUseIndexData.mockImplementation((code: string | null) => {
      if (code === 'N225_VI') {
        return {
          data: makeViIndexData(),
          isLoading: false,
          error: null,
        };
      }
      return {
        data: null,
        isLoading: false,
        error: null,
      };
    });

    render(<IndicesPage />);

    const indexButtons = screen.getAllByRole('button', { name: /^Select / });
    expect(indexButtons[0]).toHaveAttribute('aria-label', 'Select 日経平均');
    expect(indexButtons[1]).toHaveAttribute('aria-label', 'Select 日経VI');
    expect(indexButtons[2]).toHaveAttribute('aria-label', 'Select NT倍率');
    expect(screen.getByText('Daily BaseVol reference series derived from local N225 options snapshot')).toBeInTheDocument();
    expect(screen.getByText('22.34')).toBeInTheDocument();
    expect(screen.getByText('LinePriceChart')).toBeInTheDocument();
  });

  it('renders indices loading and sidebar error states', () => {
    mockUseIndicesList.mockReturnValue({
      data: { indices: [] },
      isLoading: true,
      error: new Error('failed to load indices'),
    });

    render(<IndicesPage />);

    expect(screen.getByText('Failed to load indices: failed to load indices')).toBeInTheDocument();
  });

  it('renders chart loading and chart error states', () => {
    selectedIndexCode = '1321';
    mockUseIndicesList.mockReturnValue({
      data: makeIndicesList(),
      isLoading: false,
      error: null,
    });
    mockUseIndexData.mockReturnValue({
      data: null,
      isLoading: true,
      error: null,
    });

    const { rerender } = render(<IndicesPage />);
    expect(document.querySelector('.animate-spin')).not.toBeNull();

    mockUseIndexData.mockReturnValue({
      data: null,
      isLoading: false,
      error: new Error('chart fetch failed'),
    });
    rerender(<IndicesPage />);
    expect(screen.getByText('Failed to load index data: chart fetch failed')).toBeInTheDocument();
  });

  it('covers sector stocks loading, error, empty, and sorting branches', async () => {
    const user = userEvent.setup();
    selectedIndexCode = '1305';
    mockUseIndicesList.mockReturnValue({
      data: makeIndicesList(),
      isLoading: false,
      error: null,
    });
    mockUseIndexData.mockReturnValue({
      data: makeSectorIndexData(),
      isLoading: false,
      error: null,
    });

    mockUseSectorStocks.mockReturnValue({
      data: null,
      isLoading: true,
      error: null,
    });
    const { rerender } = render(<IndicesPage />);
    expect(document.querySelector('.animate-spin')).not.toBeNull();

    mockUseSectorStocks.mockReturnValue({
      data: null,
      isLoading: false,
      error: new Error('sector fetch failed'),
    });
    rerender(<IndicesPage />);
    expect(screen.getByText('sector fetch failed')).toBeInTheDocument();

    mockUseSectorStocks.mockReturnValue({
      data: { stocks: [] },
      isLoading: false,
      error: null,
    });
    rerender(<IndicesPage />);
    expect(screen.getByText('銘柄が見つかりません')).toBeInTheDocument();

    mockUseSectorStocks.mockReturnValue({
      data: {
        stocks: [
          {
            code: '1301',
            rank: 1,
            marketCode: 'prime',
            companyName: 'Positive Stock',
            currentPrice: 3000,
            tradingValue: 1_000_000_000_000,
            changePercentage: 2.5,
          },
          {
            code: '1302',
            rank: 2,
            marketCode: 'standard',
            companyName: 'Negative Stock',
            currentPrice: 1500,
            tradingValue: 100_000_000,
            changePercentage: -1.25,
          },
          {
            code: '1303',
            rank: 3,
            marketCode: 'other',
            companyName: 'Flat Stock',
            currentPrice: 800,
            tradingValue: 10_000,
            changePercentage: 0,
          },
        ] satisfies SectorStockItem[],
      },
      isLoading: false,
      error: null,
    });
    rerender(<IndicesPage />);

    expect(screen.getByText('1.00兆')).toBeInTheDocument();
    expect(screen.getByText('1億')).toBeInTheDocument();
    expect(screen.getByText('1万')).toBeInTheDocument();
    expect(screen.getByText('+2.50%')).toBeInTheDocument();
    expect(screen.getByText('-1.25%')).toBeInTheDocument();
    expect(screen.getByText('0.00%')).toBeInTheDocument();
    expect(screen.getByText('other')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'コード' }));
    let latestCall = mockUseSectorStocks.mock.calls.at(-1);
    expect(latestCall?.[0]).toMatchObject({ sortBy: 'code', sortOrder: 'desc' });

    await user.click(screen.getByRole('button', { name: 'コード' }));
    latestCall = mockUseSectorStocks.mock.calls.at(-1);
    expect(latestCall?.[0]).toMatchObject({ sortBy: 'code', sortOrder: 'asc' });
  });
});
