import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { WatchlistWithItems } from '@/types/watchlist';
import { WatchlistDetail } from './WatchlistDetail';

const mockNavigate = vi.fn();
const mockSetSelectedSymbol = vi.fn();

const mockUseWatchlistPrices = vi.fn();
const mockUseAddWatchlistItem = vi.fn();
const mockUseDeleteWatchlist = vi.fn();
const mockUseRemoveWatchlistItem = vi.fn();
const mockAddItemMutate = vi.fn();
const mockDeleteWatchlistMutate = vi.fn();
const mockRemoveItemMutate = vi.fn();

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => mockNavigate,
}));

vi.mock('@/stores/chartStore', () => ({
  useChartStore: () => ({
    setSelectedSymbol: mockSetSelectedSymbol,
  }),
}));

vi.mock('@/hooks/useWatchlist', () => ({
  useWatchlistPrices: (...args: unknown[]) => mockUseWatchlistPrices(...args),
  useAddWatchlistItem: (...args: unknown[]) => mockUseAddWatchlistItem(...args),
  useDeleteWatchlist: (...args: unknown[]) => mockUseDeleteWatchlist(...args),
  useRemoveWatchlistItem: (...args: unknown[]) => mockUseRemoveWatchlistItem(...args),
}));

const sampleWatchlist: WatchlistWithItems = {
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
  });

  it('shows empty selection state when watchlist is not selected', () => {
    render(<WatchlistDetail watchlist={undefined} isLoading={false} error={null} />);

    expect(screen.getByText('Select a watchlist to view details')).toBeInTheDocument();
  });

  it('navigates to chart page with stock code when company name is clicked', async () => {
    const user = userEvent.setup();

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    await user.click(screen.getByRole('button', { name: 'View chart for Toyota Motor' }));

    expect(mockSetSelectedSymbol).toHaveBeenCalledWith('7203');
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/charts' });
  });

  it('submits add stock dialog with trimmed values', async () => {
    const user = userEvent.setup();

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    await user.click(screen.getByRole('button', { name: 'Add Stock' }));
    await user.type(screen.getByLabelText('Stock Code'), '6501');
    await user.type(screen.getByLabelText('Memo (optional)'), ' watch ');
    await user.click(screen.getByRole('button', { name: 'Add' }));

    expect(mockAddItemMutate).toHaveBeenCalledWith(
      { watchlistId: 1, data: { code: '6501', memo: 'watch' } },
      expect.any(Object)
    );
  });

  it('removes stock from watchlist row action', async () => {
    const user = userEvent.setup();

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    await user.click(screen.getByRole('button', { name: 'Remove 7203 from watchlist' }));

    expect(mockRemoveItemMutate).toHaveBeenCalledWith({ watchlistId: 1, itemId: 11 });
  });

  it('opens delete dialog and triggers watchlist deletion', async () => {
    const user = userEvent.setup();

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    const openDeleteButtons = screen.getAllByRole('button', { name: 'Delete' });
    const openDeleteButton = openDeleteButtons[0];
    if (!openDeleteButton) {
      throw new Error('Delete button was not found');
    }
    await user.click(openDeleteButton);

    const confirmDeleteButtons = screen.getAllByRole('button', { name: 'Delete' });
    const confirmDeleteButton = confirmDeleteButtons[confirmDeleteButtons.length - 1];
    if (!confirmDeleteButton) {
      throw new Error('Delete confirmation button was not found');
    }
    await user.click(confirmDeleteButton);

    expect(mockDeleteWatchlistMutate).toHaveBeenCalledWith(1, expect.any(Object));
  });

  it('shows empty table state when watchlist has no items', () => {
    const emptyWatchlist: WatchlistWithItems = {
      ...sampleWatchlist,
      id: 2,
      name: 'Empty Watchlist',
      items: [],
    };

    render(<WatchlistDetail watchlist={emptyWatchlist} isLoading={false} error={null} />);

    expect(screen.getByText('No stocks in this watchlist')).toBeInTheDocument();
  });

  it('renders fallback cells when price is unavailable', () => {
    mockUseWatchlistPrices.mockReturnValue({ data: { prices: [] } });

    render(<WatchlistDetail watchlist={sampleWatchlist} isLoading={false} error={null} />);

    expect(screen.getAllByText('-').length).toBeGreaterThan(0);
  });
});
