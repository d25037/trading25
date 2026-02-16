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

const mockSetSelectedSymbol = vi.fn();

const mockUseIndicesList = vi.fn();
const mockUseIndexData = vi.fn();
const mockUseSectorStocks = vi.fn();

vi.mock('@/stores/uiStore', () => ({
  useUiStore: () => ({
    selectedIndexCode,
    setSelectedIndexCode: mockSetSelectedIndexCode,
  }),
}));

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => mockNavigate,
}));

vi.mock('@/stores/chartStore', () => ({
  useChartStore: () => ({
    setSelectedSymbol: mockSetSelectedSymbol,
  }),
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

const makeIndicesList = () => ({
  indices: [
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

    expect(mockSetSelectedSymbol).toHaveBeenCalledWith('1301');
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/charts' });
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
    expect(mockSetSelectedIndexCode).toHaveBeenCalledWith('1305');
    expect(scrollIntoViewMock).toHaveBeenCalled();

    fireEvent.keyDown(window, { key: 'Enter' });
    expect(mockSetSelectedIndexCode).toHaveBeenCalledTimes(1);
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
