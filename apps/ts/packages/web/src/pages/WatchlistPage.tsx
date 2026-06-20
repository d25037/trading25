import type { WatchlistSummaryResponse } from '@trading25/contracts/types/api-response-types';
import { useCallback } from 'react';
import { PageIntroMetaList, SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { CreateWatchlistDialog, WatchlistDetail } from '@/components/Watchlist';
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
    <div className="flex min-h-0 flex-1 flex-col gap-2 lg:overflow-hidden">
      {watchlistsError && (
        <div className="rounded-lg border border-destructive/25 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          Failed to load watchlists: {watchlistsError.message}
        </div>
      )}
      <WatchlistDetail
        watchlist={selectedWatchlist}
        isLoading={watchlistsLoading || detailLoading}
        error={detailError}
        onWatchlistDeleted={handleWatchlistDeleted}
      />
    </div>
  );
}

export function WatchlistPage() {
  const { selectedWatchlistId, setSelectedWatchlistId } = useWatchlistRouteState();
  const { data: watchlistsData, isLoading: watchlistsLoading, error: watchlistsError } = useWatchlists();

  const watchlists = watchlistsData?.watchlists ?? [];
  const effectiveSelectedWatchlistId =
    watchlists.find((watchlist) => watchlist.id === selectedWatchlistId)?.id ?? watchlists[0]?.id ?? null;
  const selectedWatchlist = watchlists.find((watchlist) => watchlist.id === effectiveSelectedWatchlistId);
  const introMeta = [
    { label: 'Selected', value: selectedWatchlist?.name ?? 'None selected' },
    { label: 'Lists', value: `${watchlists.length} watchlists` },
  ];

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto p-3 lg:overflow-hidden">
      <Surface className="px-4 py-2">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div className="min-w-0 shrink-0 space-y-1">
            <SectionEyebrow>Portfolio Workspace</SectionEyebrow>
            <div className="space-y-0.5">
              <h1 className="text-2xl font-semibold tracking-tight text-foreground">Watchlist</h1>
              <p className="max-w-2xl text-xs text-muted-foreground sm:text-sm">
                Monitor selected names through the Daily Ranking table.
              </p>
            </div>
          </div>
          <div className="flex min-w-0 flex-col gap-2 md:flex-row md:items-end md:justify-end">
            <PageIntroMetaList
              items={introMeta}
              className="shrink-0 gap-x-2.5 gap-y-1 [&>div]:min-w-[6.5rem] [&>div]:pl-2"
            />
            <div className="flex flex-wrap items-end gap-2">
              <div className="w-full min-w-[15rem] space-y-1.5 sm:w-[18rem]">
                <Label htmlFor="watchlist-select" className="text-xs">
                  Watchlist
                </Label>
                <Select
                  value={effectiveSelectedWatchlistId != null ? String(effectiveSelectedWatchlistId) : undefined}
                  onValueChange={(value) => setSelectedWatchlistId(Number(value))}
                  disabled={watchlistsLoading || watchlists.length === 0}
                >
                  <SelectTrigger id="watchlist-select" className="h-9 text-sm">
                    <SelectValue placeholder={watchlistsLoading ? 'Loading watchlists' : 'No watchlist'} />
                  </SelectTrigger>
                  <SelectContent>
                    {watchlists.map((watchlist) => (
                      <SelectItem key={watchlist.id} value={String(watchlist.id)}>
                        {watchlist.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <CreateWatchlistDialog onSuccess={setSelectedWatchlistId} />
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
