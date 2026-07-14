import type { MarketRankingSymbolResponse } from '@trading25/contracts/types/api-response-types';
import type { DataProvenance, ResponseDiagnostics } from '@trading25/contracts/types/api-types';
import { BookOpen, Loader2, Plus, RotateCcw, SettingsIcon, TrendingUp } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { TimeframeSelector } from '@/components/Chart/TimeframeSelector';
import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { DailyRankingSnapshot } from '@/components/Ranking/DailyRankingSnapshot';
import { ShikihoPanel } from '@/components/SymbolWorkbench/ShikihoPanel';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import type { ShikihoSnapshotResult } from '@/hooks/useShikihoSnapshot';
import type { StockInfoResponse } from '@/hooks/useStockInfo';
import { useAddWatchlistItem, useWatchlists } from '@/hooks/useWatchlist';
import type { ShikihoDailyOverlayProvenance } from '@/lib/shikihoDailyOverlay';
import { cn } from '@/lib/utils';
import type { useChartStore } from '@/stores/chartStore';

type ChartSettings = ReturnType<typeof useChartStore.getState>['settings'];

export interface ChartRefreshFeedback {
  tone: 'success' | 'error';
  message: string;
}

interface ChartWatchlistFeedback {
  tone: 'success' | 'error';
  message: string;
}

export interface ChartHeaderMarketCaps {
  freeFloat: number | null;
  issuedShares: number | null;
}

export function resolveLatestMarketCaps(
  dailyValuation:
    | Array<{
        freeFloatMarketCap?: number | null;
        marketCap?: number | null;
      }>
    | null
    | undefined
): ChartHeaderMarketCaps {
  if (!dailyValuation || dailyValuation.length === 0) {
    return {
      freeFloat: null,
      issuedShares: null,
    };
  }

  const latest = dailyValuation[dailyValuation.length - 1];
  return {
    freeFloat: latest?.freeFloatMarketCap ?? null,
    issuedShares: latest?.marketCap ?? null,
  };
}

function ChartHeaderMetaChip({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex min-w-0 items-center gap-2 text-xs">
      <span className="shrink-0 uppercase tracking-[0.14em] text-muted-foreground">{label}</span>
      <span className="min-w-0 truncate font-medium text-foreground">{value}</span>
    </div>
  );
}

function formatOptionalDate(value: string | null | undefined): string {
  if (!value) return '-';
  return value;
}

function formatList(values: string[] | null | undefined): string {
  if (!values || values.length === 0) return '-';
  return values.join(', ');
}

function mergeUniqueStrings(...groups: Array<string[] | null | undefined>): string[] {
  const seen = new Set<string>();
  for (const group of groups) {
    for (const value of group ?? []) {
      if (value) {
        seen.add(value);
      }
    }
  }
  return [...seen];
}

function mergeWarnings(...groups: Array<ResponseDiagnostics | DataProvenance | null | undefined>): string[] {
  return mergeUniqueStrings(...groups.map((group) => group?.warnings));
}

function openCompanyPage(baseUrl: string, selectedSymbol: string | null, suffix = '') {
  if (!selectedSymbol) return;
  window.open(`${baseUrl}${selectedSymbol}${suffix}`, '_blank', 'noopener,noreferrer');
}

function ChartRefreshFeedbackBanner({ feedback }: { feedback: ChartRefreshFeedback }) {
  const toneClassName =
    feedback.tone === 'success'
      ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-700'
      : 'border-red-500/20 bg-red-500/10 text-red-700';

  return <div className={cn('rounded-xl border px-4 py-3 text-sm', toneClassName)}>{feedback.message}</div>;
}

function ChartWatchlistFeedbackBanner({ feedback }: { feedback: ChartWatchlistFeedback }) {
  const toneClassName =
    feedback.tone === 'success'
      ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-700'
      : 'border-red-500/20 bg-red-500/10 text-red-700';

  return <div className={cn('rounded-xl border px-4 py-3 text-sm', toneClassName)}>{feedback.message}</div>;
}

function AddToWatchlistDialog({
  selectedSymbol,
  stockInfo,
  onFeedback,
}: {
  selectedSymbol: string;
  stockInfo: StockInfoResponse | undefined;
  onFeedback: (feedback: ChartWatchlistFeedback | null) => void;
}) {
  const [open, setOpen] = useState(false);
  const [selectedWatchlistId, setSelectedWatchlistId] = useState<number | null>(null);
  const [memo, setMemo] = useState('');
  const watchlistsQuery = useWatchlists();
  const addWatchlistItem = useAddWatchlistItem();
  const watchlists = useMemo(() => watchlistsQuery.data?.watchlists ?? [], [watchlistsQuery.data?.watchlists]);
  const selectedWatchlist = useMemo(
    () => watchlists.find((watchlist) => watchlist.id === selectedWatchlistId) ?? null,
    [selectedWatchlistId, watchlists]
  );
  const companyName = stockInfo?.companyName?.trim() || selectedSymbol;

  useEffect(() => {
    if (!open) return;

    if (watchlists.length === 0) {
      setSelectedWatchlistId(null);
      return;
    }

    const firstWatchlist = watchlists[0];
    if (
      firstWatchlist &&
      (!selectedWatchlist || !watchlists.some((watchlist) => watchlist.id === selectedWatchlist.id))
    ) {
      setSelectedWatchlistId(firstWatchlist.id);
    }
  }, [open, selectedWatchlist, watchlists]);

  const handleOpenChange = (nextOpen: boolean) => {
    setOpen(nextOpen);
    if (!nextOpen) {
      setMemo('');
    }
  };

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!selectedWatchlist) return;

    onFeedback(null);
    addWatchlistItem.mutate(
      {
        watchlistId: selectedWatchlist.id,
        data: {
          code: selectedSymbol,
          companyName,
          memo: memo.trim() || undefined,
        },
      },
      {
        onSuccess: () => {
          onFeedback({ tone: 'success', message: `Added ${selectedSymbol} to ${selectedWatchlist.name}.` });
          setMemo('');
          setOpen(false);
        },
      }
    );
  };

  const hasWatchlists = watchlists.length > 0;

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button type="button" variant="outline" size="sm" className="flex-1 sm:flex-none">
          <Plus className="mr-1 h-4 w-4" />
          Add to Watchlist
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Add to Watchlist</DialogTitle>
          <DialogDescription>
            Add {selectedSymbol} {companyName !== selectedSymbol ? companyName : ''} to a selected watchlist.
          </DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="grid gap-2">
            <Label htmlFor="symbol-watchlist-select">Watchlist</Label>
            <select
              id="symbol-watchlist-select"
              value={selectedWatchlistId ?? ''}
              onChange={(event) => setSelectedWatchlistId(Number(event.target.value))}
              disabled={watchlistsQuery.isLoading || !hasWatchlists}
              className="h-9 rounded-md border border-input bg-background px-3 text-sm text-foreground shadow-sm outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {watchlistsQuery.isLoading ? (
                <option value="">Loading watchlists...</option>
              ) : hasWatchlists ? (
                watchlists.map((watchlist) => (
                  <option key={watchlist.id} value={watchlist.id}>
                    {watchlist.name} ({watchlist.stockCount})
                  </option>
                ))
              ) : (
                <option value="">No watchlists</option>
              )}
            </select>
          </div>

          <div className="grid gap-2">
            <Label htmlFor="symbol-watchlist-memo">Memo (optional)</Label>
            <Input
              id="symbol-watchlist-memo"
              value={memo}
              onChange={(event) => setMemo(event.target.value)}
              placeholder="Watching for breakout"
            />
          </div>

          {watchlistsQuery.error && <p className="text-sm text-destructive">{watchlistsQuery.error.message}</p>}
          {addWatchlistItem.error && <p className="text-sm text-destructive">{addWatchlistItem.error.message}</p>}
          {!watchlistsQuery.isLoading && !hasWatchlists && (
            <p className="text-sm text-muted-foreground">Create a watchlist from the Watchlist page first.</p>
          )}

          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => handleOpenChange(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={!selectedWatchlist || addWatchlistItem.isPending}>
              {addWatchlistItem.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Add
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

export function ChartHeader({
  settings,
  selectedSymbol,
  stockInfo,
  latestMarketCaps,
  rankingSnapshot,
  rankingSnapshotLoading,
  rankingSnapshotError,
  onRetryRankingSnapshot,
  shikihoSnapshot,
  shikihoCanonicalSnapshot,
  shikihoCandidate,
  shikihoTrace,
  shikihoDiagnostic,
  shikihoCaptureState,
  shikihoProvenance = null,
  isShikihoRefreshing,
  onRefreshShikiho,
  onSelectSymbol,
  strategyName,
  matchedDate,
  signalProvenance,
  signalDiagnostics,
  fundamentalsProvenance,
  refreshFeedback,
  isRefreshing,
  onRefresh,
  onOpenMobileSettings,
}: {
  settings: ChartSettings;
  selectedSymbol: string;
  stockInfo: StockInfoResponse | undefined;
  latestMarketCaps: ChartHeaderMarketCaps;
  rankingSnapshot: MarketRankingSymbolResponse | undefined;
  rankingSnapshotLoading: boolean;
  rankingSnapshotError: Error | null;
  onRetryRankingSnapshot: () => void;
  shikihoSnapshot: ShikihoSnapshotResult['snapshot'];
  shikihoCanonicalSnapshot: ShikihoSnapshotResult['snapshot'];
  shikihoCandidate?: ShikihoSnapshotResult['candidate'];
  shikihoTrace?: ShikihoSnapshotResult['trace'];
  shikihoDiagnostic: ShikihoSnapshotResult['diagnostic'];
  shikihoCaptureState: ShikihoSnapshotResult['captureState'];
  shikihoProvenance?: ShikihoDailyOverlayProvenance | null;
  isShikihoRefreshing: ShikihoSnapshotResult['isRefreshing'];
  onRefreshShikiho: ShikihoSnapshotResult['refresh'];
  onSelectSymbol: (symbol: string) => void;
  strategyName: string | null;
  matchedDate: string | null;
  signalProvenance: DataProvenance | null | undefined;
  signalDiagnostics: ResponseDiagnostics | null | undefined;
  fundamentalsProvenance: DataProvenance | null | undefined;
  refreshFeedback: ChartRefreshFeedback | null;
  isRefreshing: boolean;
  onRefresh: () => void;
  onOpenMobileSettings: () => void;
}) {
  const [watchlistFeedback, setWatchlistFeedback] = useState<ChartWatchlistFeedback | null>(null);
  const mergedLoadedDomains = mergeUniqueStrings(
    signalProvenance?.loaded_domains,
    fundamentalsProvenance?.loaded_domains
  );
  const warnings = mergeWarnings(signalProvenance, fundamentalsProvenance, signalDiagnostics);
  const marketSnapshotId = signalProvenance?.market_snapshot_id ?? fundamentalsProvenance?.market_snapshot_id ?? '-';
  let overlayLabel = '-';
  if (strategyName) {
    overlayLabel = `${strategyName} (strategy)`;
  } else if (settings.signalOverlay?.enabled) {
    overlayLabel = 'ad hoc signal overlay';
  }

  return (
    <div className="space-y-3">
      <Surface className="px-3 py-3 sm:px-5 sm:py-4">
        <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
          <div className="min-w-0 space-y-3">
            <div className="flex items-center gap-2 sm:gap-3">
              <div className="hidden h-10 w-10 items-center justify-center rounded-2xl bg-[var(--app-surface-muted)] text-primary sm:flex">
                <TrendingUp className="h-5 w-5" />
              </div>
              <div className="min-w-0 flex-1">
                <SectionEyebrow>Symbol Workbench</SectionEyebrow>
                <h2 className="truncate text-lg font-semibold tracking-tight text-foreground sm:text-2xl">
                  {selectedSymbol}
                  {stockInfo?.companyName && (
                    <span className="ml-2 font-medium text-foreground">{stockInfo.companyName}</span>
                  )}
                  {settings.relativeMode && <span className="font-medium text-muted-foreground"> / TOPIX</span>}
                </h2>
              </div>
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="shrink-0 lg:hidden"
                onClick={onOpenMobileSettings}
              >
                <SettingsIcon className="mr-1 h-4 w-4" />
                設定
              </Button>
            </div>

            <div className="hidden flex-wrap gap-x-5 gap-y-2 sm:flex">
              <ChartHeaderMetaChip label="Overlay" value={overlayLabel} />
              <ChartHeaderMetaChip label="Matched Date" value={formatOptionalDate(matchedDate)} />
              <ChartHeaderMetaChip label="Market Snapshot" value={marketSnapshotId} />
              <ChartHeaderMetaChip label="Signal Domains" value={formatList(mergedLoadedDomains)} />
            </div>
            <div className="grid grid-cols-2 gap-2 text-xs sm:hidden">
              <ChartHeaderMetaChip label="Overlay" value={overlayLabel} />
              <ChartHeaderMetaChip label="Date" value={formatOptionalDate(matchedDate)} />
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2 xl:justify-end">
            <AddToWatchlistDialog
              selectedSymbol={selectedSymbol}
              stockInfo={stockInfo}
              onFeedback={setWatchlistFeedback}
            />
            <Button
              variant="outline"
              size="sm"
              className="hidden sm:inline-flex"
              onClick={() => openCompanyPage('https://shikiho.toyokeizai.net/stocks/', selectedSymbol)}
              title="四季報を開く"
            >
              <BookOpen className="mr-1 h-4 w-4" />
              四季報
            </Button>
            <Button
              variant="outline"
              size="sm"
              className="flex-1 sm:flex-none"
              onClick={onRefresh}
              disabled={isRefreshing}
            >
              {isRefreshing ? (
                <>
                  <Loader2 className="mr-1 h-4 w-4 animate-spin" />
                  Refreshing...
                </>
              ) : (
                <>
                  <RotateCcw className="mr-1 h-4 w-4" />
                  Stock Refresh
                </>
              )}
            </Button>
            <TimeframeSelector />
          </div>
        </div>

        <DailyRankingSnapshot
          response={rankingSnapshot}
          isLoading={rankingSnapshotLoading}
          error={rankingSnapshotError}
          onRetry={onRetryRankingSnapshot}
          stockInfo={stockInfo}
          latestMarketCaps={latestMarketCaps}
          provisionalProvenance={shikihoProvenance}
        />

        <ShikihoPanel
          symbol={selectedSymbol}
          snapshot={shikihoSnapshot}
          canonicalSnapshot={shikihoCanonicalSnapshot}
          candidate={shikihoCandidate ?? null}
          trace={shikihoTrace ?? null}
          diagnostic={shikihoDiagnostic}
          captureState={shikihoCaptureState}
          isRefreshing={isShikihoRefreshing}
          onRefresh={onRefreshShikiho}
          onSelectSymbol={onSelectSymbol}
          provisionalProvenance={shikihoProvenance}
        />

        {(signalProvenance?.reference_date || fundamentalsProvenance?.reference_date || warnings.length > 0) && (
          <div className="mt-4 border-t border-border/60 pt-3 text-xs text-muted-foreground">
            <div>
              Reference Date:{' '}
              <span className="font-medium text-foreground">
                {signalProvenance?.reference_date ?? fundamentalsProvenance?.reference_date ?? '-'}
              </span>
            </div>
            {warnings.length > 0 && (
              <div className="mt-1">
                Warnings: <span className="font-medium text-foreground">{warnings.join(' | ')}</span>
              </div>
            )}
          </div>
        )}
      </Surface>

      {refreshFeedback && <ChartRefreshFeedbackBanner feedback={refreshFeedback} />}
      {watchlistFeedback && <ChartWatchlistFeedbackBanner feedback={watchlistFeedback} />}
    </div>
  );
}
