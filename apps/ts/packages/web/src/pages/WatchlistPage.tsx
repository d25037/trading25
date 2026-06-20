import type { WatchlistSummaryResponse } from '@trading25/contracts/types/api-response-types';
import { useCallback } from 'react';
import { SectionEyebrow, SplitLayout, SplitMain, SplitSidebar, Surface } from '@/components/Layout/Workspace';
import { WatchlistDetail, WatchlistList } from '@/components/Watchlist';
import { useWatchlistRouteState } from '@/hooks/usePageRouteState';
import { useWatchlists, useWatchlistWithItems } from '@/hooks/useWatchlist';

function WatchlistContent({
  watchlists,
  watchlistsLoading,
  watchlistsError,
}: {
  watchlists: WatchlistSummaryResponse[];
  watchlistsLoading: boolean;
  watchlistsError: Error | null;
}) {
  const { selectedWatchlistId, setSelectedWatchlistId } = useWatchlistRouteState();
  const effectiveSelectedWatchlistId =
    watchlists.find((watchlist) => watchlist.id === selectedWatchlistId)?.id ?? watchlists[0]?.id ?? null;
  const {
    data: selectedWatchlist,
    isLoading: detailLoading,
    error: detailError,
  } = useWatchlistWithItems(effectiveSelectedWatchlistId);

  const handleWatchlistDeleted = useCallback(() => {
    setSelectedWatchlistId(null);
  }, [setSelectedWatchlistId]);

  return (
    <SplitLayout className="min-h-0 flex-1 flex-col gap-2 lg:flex-row lg:items-stretch">
      <SplitSidebar className="w-full lg:w-[16rem] xl:w-[17rem]">
        <Surface className="flex h-full min-h-0 flex-col p-3">
          <div className="space-y-1 pb-2">
            <SectionEyebrow>Workspace</SectionEyebrow>
            <div>
              <h2 className="text-base font-semibold tracking-tight text-foreground">Watchlists</h2>
              <p className="text-xs text-muted-foreground">{watchlists.length} lists</p>
            </div>
          </div>

          <div className="min-h-0 flex-1 overflow-y-auto">
            <WatchlistList
              watchlists={watchlists}
              selectedId={effectiveSelectedWatchlistId}
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

      <SplitMain className="min-h-0 lg:overflow-hidden">
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

export function WatchlistPage() {
  const { selectedWatchlistId } = useWatchlistRouteState();
  const { data: watchlistsData, isLoading: watchlistsLoading, error: watchlistsError } = useWatchlists();

  const watchlists = watchlistsData?.watchlists ?? [];
  const effectiveSelectedWatchlistId =
    watchlists.find((watchlist) => watchlist.id === selectedWatchlistId)?.id ?? watchlists[0]?.id ?? null;
  const selectedWatchlistName = watchlists.find((watchlist) => watchlist.id === effectiveSelectedWatchlistId)?.name;
  const introMeta = [
    { label: 'Selected', value: selectedWatchlistName ?? 'None selected' },
    { label: 'Lists', value: `${watchlists.length} watchlists` },
  ];

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-2 overflow-y-auto p-3 lg:overflow-hidden">
      <Surface className="px-4 py-2">
        <div className="flex flex-col gap-2 xl:flex-row xl:items-center xl:justify-between">
          <div className="min-w-0 space-y-2">
            <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
              <h1 className="text-xl font-semibold tracking-tight text-foreground">Watchlist</h1>
              <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
                {introMeta.map((item) => (
                  <span key={item.label} className="inline-flex items-center gap-2">
                    <span className="uppercase tracking-[0.14em]">{item.label}</span>
                    <span className="font-medium text-foreground">{item.value}</span>
                  </span>
                ))}
              </div>
            </div>
          </div>
        </div>
      </Surface>

      <WatchlistContent
        watchlists={watchlists}
        watchlistsLoading={watchlistsLoading}
        watchlistsError={watchlistsError}
      />
    </div>
  );
}
