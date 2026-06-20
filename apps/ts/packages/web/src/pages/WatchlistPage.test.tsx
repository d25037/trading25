import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { WatchlistPage } from './WatchlistPage';

const mockRouteState = {
  selectedWatchlistId: 2 as number | null,
  setSelectedWatchlistId: vi.fn((id: number | null) => {
    mockRouteState.selectedWatchlistId = id;
  }),
};

let mockWatchlistsResult: {
  data?: { watchlists: Array<{ id: number; name: string }> };
  isLoading: boolean;
  error: Error | null;
};
let mockWatchlistWithItemsResult: {
  data?: { id: number; name: string; items: unknown[] };
  isLoading: boolean;
  error: Error | null;
};

vi.mock('@/hooks/usePageRouteState', () => ({
  useWatchlistRouteState: () => mockRouteState,
}));

vi.mock('@/hooks/useWatchlist', () => ({
  useWatchlists: () => mockWatchlistsResult,
  useWatchlistWithItems: (id: number | null) => {
    mockUseWatchlistWithItemsId = id;
    return mockWatchlistWithItemsResult;
  },
}));

let mockUseWatchlistWithItemsId: number | null = null;

vi.mock('@/components/Watchlist', () => ({
  WatchlistList: ({
    watchlists,
    selectedId,
  }: {
    watchlists: Array<{ id: number; name: string }>;
    selectedId: number | null;
  }) => (
    <div>
      Watchlist List ({watchlists.length}) selected:{selectedId ?? 'none'}
    </div>
  ),
  WatchlistDetail: ({ onWatchlistDeleted }: { onWatchlistDeleted: () => void }) => (
    <button type="button" onClick={onWatchlistDeleted}>
      Delete Watchlist
    </button>
  ),
}));

describe('WatchlistPage', () => {
  beforeEach(() => {
    mockRouteState.selectedWatchlistId = 2;
    mockRouteState.setSelectedWatchlistId.mockClear();
    mockUseWatchlistWithItemsId = null;
    mockWatchlistsResult = { data: { watchlists: [{ id: 2, name: 'Tech' }] }, isLoading: false, error: null };
    mockWatchlistWithItemsResult = { data: { id: 2, name: 'Tech', items: [] }, isLoading: false, error: null };
  });

  it('renders the watchlist workspace without legacy list tabs', () => {
    render(<WatchlistPage />);

    expect(screen.getByRole('heading', { name: 'Watchlist' })).toBeInTheDocument();
    expect(screen.getByText('Watchlist List (1) selected:2')).toBeInTheDocument();
    expect(screen.getByText('Delete Watchlist')).toBeInTheDocument();
    expect(mockUseWatchlistWithItemsId).toBe(2);
  });

  it('uses the first watchlist as the effective selection when none is selected', () => {
    mockRouteState.selectedWatchlistId = null;
    mockWatchlistsResult = {
      data: { watchlists: [{ id: 7, name: 'First' }, { id: 8, name: 'Second' }] },
      isLoading: false,
      error: null,
    };

    render(<WatchlistPage />);

    expect(screen.getByText('Watchlist List (2) selected:7')).toBeInTheDocument();
    expect(mockUseWatchlistWithItemsId).toBe(7);
  });

  it('renders watchlist fallback data and error state', () => {
    mockWatchlistsResult = { data: undefined, isLoading: false, error: new Error('watchlist boom') };

    render(<WatchlistPage />);

    expect(screen.getByText('Watchlist List (0) selected:none')).toBeInTheDocument();
    expect(screen.getByText('Failed to load watchlists: watchlist boom')).toBeInTheDocument();
  });
});
