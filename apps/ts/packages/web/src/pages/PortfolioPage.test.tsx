import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
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

vi.mock('@/hooks/usePageRouteState', () => ({
  usePortfolioRouteState: () => mockRouteState,
  useMigratePortfolioRouteState: () => {},
}));

vi.mock('@/hooks/usePortfolio', () => ({
  usePortfolios: () => ({ data: { portfolios: [] }, isLoading: false, error: null }),
  usePortfolioWithItems: () => ({ data: { id: 1, name: 'Main', items: [] }, isLoading: false, error: null }),
}));

vi.mock('@/hooks/useWatchlist', () => ({
  useWatchlists: () => ({ data: { watchlists: [] }, isLoading: false, error: null }),
  useWatchlistWithItems: () => ({ data: { id: 2, name: 'Tech', items: [] }, isLoading: false, error: null }),
}));

vi.mock('@/components/Portfolio', () => ({
  PortfolioList: () => <div>Portfolio List</div>,
  PortfolioDetail: ({ onPortfolioDeleted }: { onPortfolioDeleted: () => void }) => (
    <button type="button" onClick={onPortfolioDeleted}>
      Delete Portfolio
    </button>
  ),
}));

vi.mock('@/components/Watchlist', () => ({
  WatchlistList: () => <div>Watchlist List</div>,
  WatchlistDetail: ({ onWatchlistDeleted }: { onWatchlistDeleted: () => void }) => (
    <button type="button" onClick={onWatchlistDeleted}>
      Delete Watchlist
    </button>
  ),
}));

describe('PortfolioPage', () => {
  it('renders portfolio tab and handles delete', async () => {
    const user = userEvent.setup();
    mockRouteState.portfolioSubTab = 'portfolios';

    render(<PortfolioPage />);
    expect(screen.getByText('Portfolio List')).toBeInTheDocument();

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

    expect(screen.getByText('Watchlist List')).toBeInTheDocument();

    await user.click(screen.getByText('Delete Watchlist'));
    expect(mockRouteState.setSelectedWatchlistId).toHaveBeenCalledWith(null);
  });
});
