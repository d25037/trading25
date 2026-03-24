import { Briefcase, Eye } from 'lucide-react';
import { useCallback } from 'react';
import { SegmentedTabs, SplitLayout, SplitMain, SplitSidebar } from '@/components/Layout/Workspace';
import { PortfolioDetail, PortfolioList } from '@/components/Portfolio';
import { WatchlistDetail, WatchlistList } from '@/components/Watchlist';
import { useMigratePortfolioRouteState, usePortfolioRouteState } from '@/hooks/usePageRouteState';
import { usePortfolios, usePortfolioWithItems } from '@/hooks/usePortfolio';
import { useWatchlists, useWatchlistWithItems } from '@/hooks/useWatchlist';

function PortfolioContent() {
  const { selectedPortfolioId, setSelectedPortfolioId } = usePortfolioRouteState();

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
    <SplitLayout className="gap-0 overflow-hidden">
      <SplitSidebar className="w-80 border-r border-border/30 p-4 glass-panel">
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
      </SplitSidebar>
      <SplitMain className="overflow-auto p-6">
        <PortfolioDetail
          portfolio={selectedPortfolio}
          isLoading={detailLoading}
          error={detailError}
          onPortfolioDeleted={handlePortfolioDeleted}
        />
      </SplitMain>
    </SplitLayout>
  );
}

function WatchlistContent() {
  const { selectedWatchlistId, setSelectedWatchlistId } = usePortfolioRouteState();

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
    <SplitLayout className="gap-0 overflow-hidden">
      <SplitSidebar className="w-80 border-r border-border/30 p-4 glass-panel">
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
      </SplitSidebar>
      <SplitMain className="overflow-auto p-6">
        <WatchlistDetail
          watchlist={selectedWatchlist}
          isLoading={detailLoading}
          error={detailError}
          onWatchlistDeleted={handleWatchlistDeleted}
        />
      </SplitMain>
    </SplitLayout>
  );
}

export function PortfolioPage() {
  useMigratePortfolioRouteState();
  const { portfolioSubTab, setPortfolioSubTab } = usePortfolioRouteState();

  const tabs = [
    { value: 'portfolios' as const, label: 'Portfolios', icon: Briefcase },
    { value: 'watchlists' as const, label: 'Watchlists', icon: Eye },
  ];

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="border-b border-border/30 px-4 py-3">
        <SegmentedTabs items={tabs} value={portfolioSubTab} onChange={setPortfolioSubTab} />
      </div>

      <div className="flex-1 overflow-auto">
        {portfolioSubTab === 'portfolios' ? <PortfolioContent /> : <WatchlistContent />}
      </div>
    </div>
  );
}
