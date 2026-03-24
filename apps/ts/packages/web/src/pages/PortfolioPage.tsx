import { Briefcase, Eye } from 'lucide-react';
import { useCallback } from 'react';
import {
  SectionEyebrow,
  SegmentedTabs,
  SplitLayout,
  SplitMain,
  SplitSidebar,
  Surface,
} from '@/components/Layout/Workspace';
import { PortfolioDetail, PortfolioList } from '@/components/Portfolio';
import { WatchlistDetail, WatchlistList } from '@/components/Watchlist';
import { useMigratePortfolioRouteState, usePortfolioRouteState } from '@/hooks/usePageRouteState';
import { usePortfolios, usePortfolioWithItems } from '@/hooks/usePortfolio';
import { useWatchlists, useWatchlistWithItems } from '@/hooks/useWatchlist';
import type { PortfolioSummary } from '@/types/portfolio';
import type { WatchlistSummary } from '@/types/watchlist';

function PortfolioContent({
  portfolios,
  portfoliosLoading,
  portfoliosError,
}: {
  portfolios: PortfolioSummary[];
  portfoliosLoading: boolean;
  portfoliosError: Error | null;
}) {
  const { selectedPortfolioId, setSelectedPortfolioId } = usePortfolioRouteState();
  const {
    data: selectedPortfolio,
    isLoading: detailLoading,
    error: detailError,
  } = usePortfolioWithItems(selectedPortfolioId);

  const handlePortfolioDeleted = useCallback(() => {
    setSelectedPortfolioId(null);
  }, [setSelectedPortfolioId]);

  return (
    <SplitLayout className="min-h-0 flex-1 flex-col gap-3 lg:flex-row lg:items-stretch">
      <SplitSidebar className="w-full lg:w-[18.5rem]">
        <Surface className="flex h-full min-h-0 flex-col p-4">
          <div className="space-y-1">
            <SectionEyebrow>Workspace</SectionEyebrow>
            <div>
              <h2 className="text-lg font-semibold tracking-tight text-foreground">Portfolios</h2>
              <p className="text-sm text-muted-foreground">Select a list, then inspect holdings and performance.</p>
            </div>
          </div>

          <div className="mt-4 min-h-0 flex-1 overflow-y-auto">
            <PortfolioList
              portfolios={portfolios}
              selectedId={selectedPortfolioId}
              onSelect={setSelectedPortfolioId}
              isLoading={portfoliosLoading}
            />
          </div>

          {portfoliosError && (
            <div className="mt-4 rounded-2xl border border-destructive/25 bg-destructive/10 px-3 py-3 text-sm text-destructive">
              Failed to load portfolios: {portfoliosError.message}
            </div>
          )}
        </Surface>
      </SplitSidebar>

      <SplitMain className="min-h-0 lg:overflow-y-auto lg:pr-1">
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

function WatchlistContent({
  watchlists,
  watchlistsLoading,
  watchlistsError,
}: {
  watchlists: WatchlistSummary[];
  watchlistsLoading: boolean;
  watchlistsError: Error | null;
}) {
  const { selectedWatchlistId, setSelectedWatchlistId } = usePortfolioRouteState();
  const {
    data: selectedWatchlist,
    isLoading: detailLoading,
    error: detailError,
  } = useWatchlistWithItems(selectedWatchlistId);

  const handleWatchlistDeleted = useCallback(() => {
    setSelectedWatchlistId(null);
  }, [setSelectedWatchlistId]);

  return (
    <SplitLayout className="min-h-0 flex-1 flex-col gap-3 lg:flex-row lg:items-stretch">
      <SplitSidebar className="w-full lg:w-[18.5rem]">
        <Surface className="flex h-full min-h-0 flex-col p-4">
          <div className="space-y-1">
            <SectionEyebrow>Workspace</SectionEyebrow>
            <div>
              <h2 className="text-lg font-semibold tracking-tight text-foreground">Watchlists</h2>
              <p className="text-sm text-muted-foreground">Keep names close at hand, then review prices and notes.</p>
            </div>
          </div>

          <div className="mt-4 min-h-0 flex-1 overflow-y-auto">
            <WatchlistList
              watchlists={watchlists}
              selectedId={selectedWatchlistId}
              onSelect={setSelectedWatchlistId}
              isLoading={watchlistsLoading}
            />
          </div>

          {watchlistsError && (
            <div className="mt-4 rounded-2xl border border-destructive/25 bg-destructive/10 px-3 py-3 text-sm text-destructive">
              Failed to load watchlists: {watchlistsError.message}
            </div>
          )}
        </Surface>
      </SplitSidebar>

      <SplitMain className="min-h-0 lg:overflow-y-auto lg:pr-1">
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
  const { portfolioSubTab, setPortfolioSubTab, selectedPortfolioId, selectedWatchlistId } = usePortfolioRouteState();
  const { data: portfoliosData, isLoading: portfoliosLoading, error: portfoliosError } = usePortfolios();
  const { data: watchlistsData, isLoading: watchlistsLoading, error: watchlistsError } = useWatchlists();

  const tabs = [
    { value: 'portfolios' as const, label: 'Portfolios', icon: Briefcase },
    { value: 'watchlists' as const, label: 'Watchlists', icon: Eye },
  ];
  const portfolios = portfoliosData?.portfolios ?? [];
  const watchlists = watchlistsData?.watchlists ?? [];
  const selectedPortfolioName = portfolios.find((portfolio) => portfolio.id === selectedPortfolioId)?.name;
  const selectedWatchlistName = watchlists.find((watchlist) => watchlist.id === selectedWatchlistId)?.name;
  const isPortfolioView = portfolioSubTab === 'portfolios';

  const introMeta = isPortfolioView
    ? [
        { label: 'Selected', value: selectedPortfolioName ?? 'None selected' },
        { label: 'Lists', value: `${portfolios.length} portfolios` },
        { label: 'Priority', value: 'Holdings first' },
      ]
    : [
        { label: 'Selected', value: selectedWatchlistName ?? 'None selected' },
        { label: 'Lists', value: `${watchlists.length} watchlists` },
        { label: 'Priority', value: 'Symbols first' },
      ];

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto p-4 lg:overflow-hidden">
      <Surface className="px-4 py-4 sm:px-5">
        <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
          <div className="min-w-0 space-y-2">
            <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
              <h1 className="text-xl font-semibold tracking-tight text-foreground">
                {isPortfolioView ? 'Portfolio Workspace' : 'Watchlist Workspace'}
              </h1>
              <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
                {introMeta.map((item) => (
                  <span key={item.label} className="inline-flex items-center gap-2">
                    <span className="uppercase tracking-[0.14em]">{item.label}</span>
                    <span className="font-medium text-foreground">{item.value}</span>
                  </span>
                ))}
              </div>
            </div>
            <p className="max-w-3xl text-sm text-muted-foreground">
              {isPortfolioView
                ? 'Review holdings, benchmark context, and factor exposure with the position table kept in view.'
                : 'Track names you care about, then move directly into prices and chart navigation without extra chrome.'}
            </p>
          </div>
          <SegmentedTabs items={tabs} value={portfolioSubTab} onChange={setPortfolioSubTab} />
        </div>
      </Surface>

      {isPortfolioView ? (
        <PortfolioContent
          portfolios={portfolios}
          portfoliosLoading={portfoliosLoading}
          portfoliosError={portfoliosError}
        />
      ) : (
        <WatchlistContent
          watchlists={watchlists}
          watchlistsLoading={watchlistsLoading}
          watchlistsError={watchlistsError}
        />
      )}
    </div>
  );
}
