import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { WatchlistWithItemsResponse } from '@trading25/contracts/types/api-response-types';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { WatchlistDetail } from './WatchlistDetail';

const mockNavigate = vi.fn();

const mockUseWatchlistPrices = vi.fn();
const mockUseWatchlists = vi.fn();
const mockUseAddWatchlistItem = vi.fn();
const mockUseDeleteWatchlist = vi.fn();
const mockUseRemoveWatchlistItem = vi.fn();
const mockUseUpdateWatchlist = vi.fn();
const mockUseUpdateWatchlistItem = vi.fn();
const mockUseStockSearch = vi.fn();
const mockUseRanking = vi.fn();
const mockRankingTable = vi.fn();
const mockAddItemMutate = vi.fn();
const mockDeleteWatchlistMutate = vi.fn();
const mockRemoveItemMutate = vi.fn();
const mockUpdateWatchlistMutate = vi.fn();
const mockUpdateWatchlistItemMutate = vi.fn();

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => mockNavigate,
}));

vi.mock('@/hooks/useWatchlist', () => ({
  useWatchlistPrices: (...args: unknown[]) => mockUseWatchlistPrices(...args),
  useWatchlists: (...args: unknown[]) => mockUseWatchlists(...args),
  useAddWatchlistItem: (...args: unknown[]) => mockUseAddWatchlistItem(...args),
  useDeleteWatchlist: (...args: unknown[]) => mockUseDeleteWatchlist(...args),
  useRemoveWatchlistItem: (...args: unknown[]) => mockUseRemoveWatchlistItem(...args),
  useUpdateWatchlist: (...args: unknown[]) => mockUseUpdateWatchlist(...args),
  useUpdateWatchlistItem: (...args: unknown[]) => mockUseUpdateWatchlistItem(...args),
}));

vi.mock('@/hooks/useStockSearch', () => ({
  useStockSearch: (...args: unknown[]) => mockUseStockSearch(...args),
}));

vi.mock('@/hooks/useRanking', () => ({
  useRanking: (...args: unknown[]) => mockUseRanking(...args),
}));

vi.mock('@/components/Ranking', () => ({
  RankingTable: (props: { headerActions?: ReactNode }) => {
    mockRankingTable(props);
    return (
      <section aria-label="Daily Ranking mock">
        <div>Daily Ranking Table</div>
        {props.headerActions}
      </section>
    );
  },
  SECTOR_STRENGTH_FAMILY_OPTIONS: [
    { value: 'balanced_sector_strength', label: 'Balanced Sector Strength' },
    { value: 'long_hybrid_leadership', label: 'Long Hybrid Leadership' },
  ],
}));

const sampleWatchlist: WatchlistWithItemsResponse = {
  id: 1,
  name: 'Tech Watchlist',
  description: 'Major names',
  createdAt: '2026-02-16T00:00:00Z',
  updatedAt: '2026-02-16T00:00:00Z',
  items: [
    {
      id: 11,
      watchlistId: 1,
      code: '7203',
      companyName: 'Toyota Motor',
      memo: 'Monitor breakout',
      createdAt: '2026-01-10T00:00:00Z',
    },
  ],
};

describe('WatchlistDetail', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockUseWatchlistPrices.mockReturnValue({
      data: { prices: [{ code: '7203', close: 2600, changePercent: 1.2, volume: 1000000 }] },
    });
    mockUseWatchlists.mockReturnValue({
      data: {
        watchlists: [
          {
            id: 1,
            name: 'Tech Watchlist',
            description: 'Major names',
            stockCount: 1,
            createdAt: '2026-02-16T00:00:00Z',
            updatedAt: '2026-02-16T00:00:00Z',
          },
          {
            id: 2,
            name: 'Long',
            description: null,
            stockCount: 3,
            createdAt: '2026-02-17T00:00:00Z',
            updatedAt: '2026-02-17T00:00:00Z',
          },
        ],
      },
      isLoading: false,
      error: null,
    });
    mockUseAddWatchlistItem.mockReturnValue({
      mutate: mockAddItemMutate,
      isPending: false,
      error: null,
    });
    mockUseDeleteWatchlist.mockReturnValue({
      mutate: mockDeleteWatchlistMutate,
      isPending: false,
    });
    mockUseRemoveWatchlistItem.mockReturnValue({
      mutate: mockRemoveItemMutate,
      isPending: false,
    });
    mockUseUpdateWatchlist.mockReturnValue({
      mutate: mockUpdateWatchlistMutate,
      isPending: false,
      error: null,
    });
    mockUseUpdateWatchlistItem.mockReturnValue({
      mutate: mockUpdateWatchlistItemMutate,
      isPending: false,
      error: null,
    });
    mockUseStockSearch.mockReturnValue({
      data: { results: [] },
      isLoading: false,
    });
    mockUseRanking.mockReturnValue({
      data: { rankings: { tradingValue: [{ code: '7203', companyName: 'Toyota Motor' }] } },
      isLoading: false,
      error: null,
    });
  });

  it('shows empty selection state when watchlist is not selected', () => {
    render(<WatchlistDetail watchlist={undefined} isLoading={false} error={null} />);

    expect(screen.getByText('Select a watchlist to view details')).toBeInTheDocument();
  });

  it('renders the stock list with the Daily Ranking watchlist filter state', () => {
    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    expect(screen.getByText('Daily Ranking Table')).toBeInTheDocument();
    expect(mockUseRanking).toHaveBeenCalledWith(
      expect.objectContaining({
        includeValuation: true,
        includeSectorStrength: true,
        limit: 0,
        markets: 'prime,standard,growth',
      }),
      true
    );
    expect(mockRankingTable).toHaveBeenCalledWith(
      expect.objectContaining({
        enableTableFilters: true,
        filterState: { watchlistId: 1 },
        filterWatchlistCodes: new Set(['7203']),
      })
    );
  });

  it('keeps watchlist controls in the Daily Ranking header instead of a separate selected watchlist panel', () => {
    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    expect(screen.queryByText('Selected Watchlist')).not.toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: 'Tech Watchlist' })).not.toBeInTheDocument();
    expect(screen.getByRole('combobox', { name: 'Index Strength' })).toBeInTheDocument();
    expect(screen.getByText('1 names')).toBeInTheDocument();
    expect(screen.getByText('1 memos')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Manage Watchlist' })).toBeInTheDocument();
  });

  it('updates Daily Ranking index strength family from the watchlist header', async () => {
    const user = userEvent.setup();

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    await user.click(screen.getByRole('combobox', { name: 'Index Strength' }));
    await user.click(screen.getByRole('option', { name: 'Long Hybrid Leadership' }));

    expect(mockUseRanking).toHaveBeenLastCalledWith(
      expect.objectContaining({
        sectorStrengthFamily: 'long_hybrid_leadership',
      }),
      true
    );
  });

  it('opens a single management dialog for stock, detail, and danger actions', async () => {
    const user = userEvent.setup();

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    expect(screen.getByRole('button', { name: 'Manage Watchlist' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Add Stock' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Manage Stocks' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Edit' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Delete' })).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Manage Watchlist' }));

    expect(screen.getByRole('heading', { name: 'Manage Watchlist' })).toBeInTheDocument();
    expect(screen.getByLabelText('Stock Code')).toBeInTheDocument();
    expect(screen.getByLabelText('Name')).toHaveValue('Tech Watchlist');
    expect(screen.getByRole('button', { name: 'Remove 7203 from watchlist' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Delete Watchlist' })).toBeInTheDocument();
  });

  it('focuses the add stock input when the management dialog opens', async () => {
    const user = userEvent.setup();

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    await user.click(screen.getByRole('button', { name: 'Manage Watchlist' }));

    expect(document.activeElement).toBe(screen.getByLabelText('Stock Code'));
  });

  it('keeps stock code and memo in the same add stock row', async () => {
    const user = userEvent.setup();

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    await user.click(screen.getByRole('button', { name: 'Manage Watchlist' }));

    expect(screen.getByTestId('watchlist-add-stock-fields')).toHaveClass('sm:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]');
  });

  it('submits add stock from the management dialog with trimmed values', async () => {
    const user = userEvent.setup();

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    await user.click(screen.getByRole('button', { name: 'Manage Watchlist' }));
    await user.type(screen.getByLabelText('Stock Code'), '6501');
    await user.type(screen.getByLabelText('Memo (optional)'), ' watch ');
    await user.click(screen.getByRole('button', { name: 'Add' }));

    expect(mockAddItemMutate).toHaveBeenCalledWith(
      { watchlistId: 1, data: { code: '6501', companyName: '6501', memo: 'watch' } },
      expect.any(Object)
    );
  });

  it('supports company name search and selects stock code from suggestion', async () => {
    const user = userEvent.setup();
    mockUseStockSearch.mockImplementation((query: string) => ({
      data: query
        ? {
            results: [
              {
                code: '6501',
                companyName: 'Hitachi',
                marketName: 'Prime',
                sector33Name: '電気機器',
              },
            ],
          }
        : { results: [] },
      isLoading: false,
    }));

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    await user.click(screen.getByRole('button', { name: 'Manage Watchlist' }));
    await user.type(screen.getByLabelText('Stock Code'), 'hita');
    await user.click(await screen.findByRole('button', { name: /6501 Hitachi/i }));
    await user.click(screen.getByRole('button', { name: 'Add' }));

    expect(mockAddItemMutate).toHaveBeenCalledWith(
      { watchlistId: 1, data: { code: '6501', companyName: 'Hitachi', memo: undefined } },
      expect.any(Object)
    );
  });

  it('does not submit when stock code is not 4 digits', async () => {
    const user = userEvent.setup();

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    await user.click(screen.getByRole('button', { name: 'Manage Watchlist' }));
    await user.type(screen.getByLabelText('Stock Code'), 'hitachi');

    const addButton = screen.getByRole('button', { name: 'Add' });
    expect(addButton).toBeDisabled();
    await user.click(addButton);

    expect(mockAddItemMutate).not.toHaveBeenCalled();
  });

  it('removes stock from the management dialog', async () => {
    const user = userEvent.setup();

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    await user.click(screen.getByRole('button', { name: 'Manage Watchlist' }));
    await user.click(screen.getByRole('button', { name: 'Remove 7203 from watchlist' }));

    expect(mockRemoveItemMutate).toHaveBeenCalledWith({ watchlistId: 1, itemId: 11 });
  });

  it('moves stock to another watchlist by adding it first and then removing it from the current watchlist', async () => {
    const user = userEvent.setup();
    mockAddItemMutate.mockImplementationOnce((_variables, options) => {
      options?.onSuccess?.({
        id: 21,
        watchlistId: 2,
        code: '7203',
        companyName: 'Toyota Motor',
        memo: 'Monitor breakout',
        createdAt: '2026-02-17T00:00:00Z',
      });
    });

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    await user.click(screen.getByRole('button', { name: 'Manage Watchlist' }));
    await user.click(screen.getByRole('button', { name: 'Move 7203 to another watchlist' }));
    await user.selectOptions(screen.getByLabelText('Move destination for 7203'), '2');
    await user.click(screen.getByRole('button', { name: 'Move' }));

    expect(mockAddItemMutate).toHaveBeenCalledWith(
      {
        watchlistId: 2,
        data: {
          code: '7203',
          companyName: 'Toyota Motor',
          memo: 'Monitor breakout',
        },
      },
      expect.objectContaining({
        onSuccess: expect.any(Function),
      })
    );
    expect(mockRemoveItemMutate).toHaveBeenCalledWith({ watchlistId: 1, itemId: 11 });
  });

  it('shows memo inline with company name and saves memo edits', async () => {
    const user = userEvent.setup();

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    await user.click(screen.getByRole('button', { name: 'Manage Watchlist' }));

    const stockRow = screen.getByTestId('watchlist-item-row-11');
    expect(stockRow.textContent).toContain('Toyota Motor');
    expect(screen.getByLabelText('Memo for 7203')).toHaveValue('Monitor breakout');

    await user.clear(screen.getByLabelText('Memo for 7203'));
    await user.type(screen.getByLabelText('Memo for 7203'), ' updated memo ');
    await user.click(screen.getByRole('button', { name: 'Save memo for 7203' }));

    expect(mockUpdateWatchlistItemMutate).toHaveBeenCalledWith({
      watchlistId: 1,
      itemId: 11,
      data: { memo: 'updated memo' },
    });
  });

  it('clears memo edits as null', async () => {
    const user = userEvent.setup();

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    await user.click(screen.getByRole('button', { name: 'Manage Watchlist' }));
    await user.clear(screen.getByLabelText('Memo for 7203'));
    await user.click(screen.getByRole('button', { name: 'Save memo for 7203' }));

    expect(mockUpdateWatchlistItemMutate).toHaveBeenCalledWith({
      watchlistId: 1,
      itemId: 11,
      data: { memo: null },
    });
  });

  it('renames watchlist with trimmed name and description', async () => {
    const user = userEvent.setup();

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    await user.click(screen.getByRole('button', { name: 'Manage Watchlist' }));
    await user.clear(screen.getByLabelText('Name'));
    await user.type(screen.getByLabelText('Name'), '  Breakout Watch  ');
    await user.clear(screen.getByLabelText('Description (optional)'));
    await user.type(screen.getByLabelText('Description (optional)'), '  Candidates  ');
    await user.click(screen.getByRole('button', { name: 'Save Details' }));

    expect(mockUpdateWatchlistMutate).toHaveBeenCalledWith({
      id: 1,
      data: { name: 'Breakout Watch', description: 'Candidates' },
    });
  });

  it('confirms watchlist deletion inside the management dialog', async () => {
    const user = userEvent.setup();

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    await user.click(screen.getByRole('button', { name: 'Manage Watchlist' }));
    await user.click(screen.getByRole('button', { name: 'Delete Watchlist' }));
    await user.click(screen.getByRole('button', { name: 'Confirm Delete' }));

    expect(mockDeleteWatchlistMutate).toHaveBeenCalledWith(1, expect.any(Object));
  });

  it('shows empty table state when watchlist has no items', () => {
    const emptyWatchlist: WatchlistWithItemsResponse = {
      ...sampleWatchlist,
      id: 2,
      name: 'Empty Watchlist',
      items: [],
    };

    render(<WatchlistDetail watchlist={emptyWatchlist} isLoading={false} error={null} />);

    expect(mockUseRanking).toHaveBeenCalledWith(expect.any(Object), false);
    expect(mockRankingTable).toHaveBeenCalledWith(
      expect.objectContaining({
        emptyMessage: 'No stocks in this watchlist',
        filterWatchlistCodes: new Set(),
      })
    );
  });
});
