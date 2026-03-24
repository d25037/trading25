import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { PortfolioPage } from './PortfolioPage';

const mockRouteState = {
  portfolioSubTab: 'portfolios' as string,
  setPortfolioSubTab: vi.fn((tab: string) => {
    mockRouteState.portfolioSubTab = tab;
  }),
  selectedPortfolioId: 1 as number | null,
  setSelectedPortfolioId: vi.fn((id: number | null) => {
    mockRouteState.selectedPortfolioId = id;
  }),
  selectedWatchlistId: 2 as number | null,
  setSelectedWatchlistId: vi.fn((id: number | null) => {
    mockRouteState.selectedWatchlistId = id;
  }),
};

let mockPortfoliosResult: {
  data?: { portfolios: Array<{ id: number; name: string }> };
  isLoading: boolean;
  error: Error | null;
};
let mockPortfolioWithItemsResult: {
  data?: { id: number; name: string; items: unknown[] };
  isLoading: boolean;
  error: Error | null;
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
  usePortfolioRouteState: () => mockRouteState,
  useMigratePortfolioRouteState: () => {},
}));

vi.mock('@/hooks/usePortfolio', () => ({
  usePortfolios: () => mockPortfoliosResult,
  usePortfolioWithItems: () => mockPortfolioWithItemsResult,
}));

vi.mock('@/hooks/useWatchlist', () => ({
  useWatchlists: () => mockWatchlistsResult,
  useWatchlistWithItems: () => mockWatchlistWithItemsResult,
}));

vi.mock('@/components/Portfolio', () => ({
  PortfolioList: ({ portfolios }: { portfolios: Array<{ id: number; name: string }> }) => (
    <div>Portfolio List ({portfolios.length})</div>
  ),
  PortfolioDetail: ({ onPortfolioDeleted }: { onPortfolioDeleted: () => void }) => (
    <button type="button" onClick={onPortfolioDeleted}>
      Delete Portfolio
    </button>
  ),
}));

vi.mock('@/components/Watchlist', () => ({
  WatchlistList: ({ watchlists }: { watchlists: Array<{ id: number; name: string }> }) => (
    <div>Watchlist List ({watchlists.length})</div>
  ),
  WatchlistDetail: ({ onWatchlistDeleted }: { onWatchlistDeleted: () => void }) => (
    <button type="button" onClick={onWatchlistDeleted}>
      Delete Watchlist
    </button>
  ),
}));

describe('PortfolioPage', () => {
  beforeEach(() => {
    mockRouteState.portfolioSubTab = 'portfolios';
    mockRouteState.selectedPortfolioId = 1;
    mockRouteState.selectedWatchlistId = 2;
    mockRouteState.setPortfolioSubTab.mockClear();
    mockRouteState.setSelectedPortfolioId.mockClear();
    mockRouteState.setSelectedWatchlistId.mockClear();
    mockPortfoliosResult = { data: { portfolios: [] }, isLoading: false, error: null };
    mockPortfolioWithItemsResult = { data: { id: 1, name: 'Main', items: [] }, isLoading: false, error: null };
    mockWatchlistsResult = { data: { watchlists: [] }, isLoading: false, error: null };
    mockWatchlistWithItemsResult = { data: { id: 2, name: 'Tech', items: [] }, isLoading: false, error: null };
  });

  it('renders portfolio tab and handles delete', async () => {
    const user = userEvent.setup();

    render(<PortfolioPage />);
    expect(screen.getByText('Portfolio List (0)')).toBeInTheDocument();

    await user.click(screen.getByText('Delete Portfolio'));
    expect(mockRouteState.setSelectedPortfolioId).toHaveBeenCalledWith(null);
  });

  it('switches to watchlist tab and handles delete', async () => {
    const user = userEvent.setup();
    const { rerender } = render(<PortfolioPage />);

    await user.click(screen.getByRole('button', { name: /Watchlists/i }));
    expect(mockRouteState.setPortfolioSubTab).toHaveBeenCalledWith('watchlists');

    mockRouteState.portfolioSubTab = 'watchlists';
    rerender(<PortfolioPage />);

    expect(screen.getByText('Watchlist List (0)')).toBeInTheDocument();

    await user.click(screen.getByText('Delete Watchlist'));
    expect(mockRouteState.setSelectedWatchlistId).toHaveBeenCalledWith(null);
  });

  it('renders portfolio fallback data and error state', () => {
    mockPortfoliosResult = { data: undefined, isLoading: false, error: new Error('portfolio boom') };

    render(<PortfolioPage />);

    expect(screen.getByText('Portfolio List (0)')).toBeInTheDocument();
    expect(screen.getByText('Failed to load portfolios: portfolio boom')).toBeInTheDocument();
  });

  it('renders watchlist fallback data and error state', () => {
    mockRouteState.portfolioSubTab = 'watchlists';
    mockWatchlistsResult = { data: undefined, isLoading: false, error: new Error('watchlist boom') };

    render(<PortfolioPage />);

    expect(screen.getByText('Watchlist List (0)')).toBeInTheDocument();
    expect(screen.getByText('Failed to load watchlists: watchlist boom')).toBeInTheDocument();
  });
});
