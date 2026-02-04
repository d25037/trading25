import { Briefcase, Eye } from 'lucide-react';
import { useCallback } from 'react';
import { PortfolioDetail, PortfolioList } from '@/components/Portfolio';
import { WatchlistDetail, WatchlistList } from '@/components/Watchlist';
import { usePortfolios, usePortfolioWithItems } from '@/hooks/usePortfolio';
import { useWatchlists, useWatchlistWithItems } from '@/hooks/useWatchlist';
import { cn } from '@/lib/utils';
import { useUiStore } from '@/stores/uiStore';

function PortfolioContent() {
  const { selectedPortfolioId, setSelectedPortfolioId } = useUiStore();

  const { data: portfoliosData, isLoading: portfoliosLoading, error: portfoliosError } = usePortfolios();
  const {
    data: selectedPortfolio,
    isLoading: detailLoading,
    error: detailError,
  } = usePortfolioWithItems(selectedPortfolioId);

  const handlePortfolioDeleted = useCallback(() => {
    setSelectedPortfolioId(null);
  }, [setSelectedPortfolioId]);

  return (
    <div className="flex">
      <div className="w-80 border-r border-border/30 p-4 glass-panel shrink-0">
        <PortfolioList
          portfolios={portfoliosData?.portfolios || []}
          selectedId={selectedPortfolioId}
          onSelect={setSelectedPortfolioId}
          isLoading={portfoliosLoading}
        />
        {portfoliosError && (
          <div className="mt-4 p-4 rounded-lg bg-destructive/10 text-destructive text-sm">
            Failed to load portfolios: {portfoliosError.message}
          </div>
        )}
      </div>
      <div className="flex-1 p-6">
        <PortfolioDetail
          portfolio={selectedPortfolio}
          isLoading={detailLoading}
          error={detailError}
          onPortfolioDeleted={handlePortfolioDeleted}
        />
      </div>
    </div>
  );
}

function WatchlistContent() {
  const { selectedWatchlistId, setSelectedWatchlistId } = useUiStore();

  const { data: watchlistsData, isLoading: watchlistsLoading, error: watchlistsError } = useWatchlists();
  const {
    data: selectedWatchlist,
    isLoading: detailLoading,
    error: detailError,
  } = useWatchlistWithItems(selectedWatchlistId);

  const handleWatchlistDeleted = useCallback(() => {
    setSelectedWatchlistId(null);
  }, [setSelectedWatchlistId]);

  return (
    <div className="flex">
      <div className="w-80 border-r border-border/30 p-4 glass-panel shrink-0">
        <WatchlistList
          watchlists={watchlistsData?.watchlists || []}
          selectedId={selectedWatchlistId}
          onSelect={setSelectedWatchlistId}
          isLoading={watchlistsLoading}
        />
        {watchlistsError && (
          <div className="mt-4 p-4 rounded-lg bg-destructive/10 text-destructive text-sm">
            Failed to load watchlists: {watchlistsError.message}
          </div>
        )}
      </div>
      <div className="flex-1 p-6">
        <WatchlistDetail
          watchlist={selectedWatchlist}
          isLoading={detailLoading}
          error={detailError}
          onWatchlistDeleted={handleWatchlistDeleted}
        />
      </div>
    </div>
  );
}

export function PortfolioPage() {
  const { portfolioSubTab, setPortfolioSubTab } = useUiStore();

  const tabs = [
    { key: 'portfolios' as const, label: 'Portfolios', icon: Briefcase },
    { key: 'watchlists' as const, label: 'Watchlists', icon: Eye },
  ];

  return (
    <div className="flex flex-col h-full">
      {/* Sub-tab navigation */}
      <div className="border-b border-border/30 px-4">
        <div className="flex gap-1">
          {tabs.map(({ key, label, icon: Icon }) => (
            <button
              key={key}
              type="button"
              onClick={() => setPortfolioSubTab(key)}
              className={cn(
                'flex items-center gap-2 px-4 py-3 text-sm font-medium transition-colors border-b-2',
                portfolioSubTab === key
                  ? 'border-primary text-primary'
                  : 'border-transparent text-muted-foreground hover:text-foreground'
              )}
            >
              <Icon className="h-4 w-4" />
              {label}
            </button>
          ))}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto">
        {portfolioSubTab === 'portfolios' ? <PortfolioContent /> : <WatchlistContent />}
      </div>
    </div>
  );
}
