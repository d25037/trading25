import { useNavigate } from '@tanstack/react-router';
import type {
  WatchlistSummaryResponse,
  WatchlistWithItemsResponse,
} from '@trading25/contracts/types/api-response-types';
import { ArrowRightLeft, Check, Eye, ListChecks, Loader2, Plus, Trash2 } from 'lucide-react';
import type { ReactNode } from 'react';
import { useEffect, useMemo, useState } from 'react';
import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { RankingTable, type RankingTableSortState, SECTOR_STRENGTH_FAMILY_OPTIONS } from '@/components/Ranking';
import { StockSearchInput } from '@/components/Stock/StockSearchInput';
import { Button } from '@/components/ui/button';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
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
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useRanking } from '@/hooks/useRanking';
import type { StockSearchResultItem } from '@/hooks/useStockSearch';
import {
  useAddWatchlistItem,
  useDeleteWatchlist,
  useRemoveWatchlistItem,
  useUpdateWatchlist,
  useUpdateWatchlistItem,
  useWatchlists,
} from '@/hooks/useWatchlist';
import { DEFAULT_RANKING_PARAMS } from '@/stores/screeningStore';
import type { RankingParams } from '@/types/ranking';
import { formatCount } from '@/utils/formatters';

const WATCHLIST_RANKING_PARAMS: RankingParams = {
  ...DEFAULT_RANKING_PARAMS,
  markets: 'prime,standard,growth',
  limit: 0,
  includeValuation: true,
  includeSectorStrength: true,
  sectorStrengthFamily: 'balanced_sector_strength',
  forwardEpsDisclosedWithinDays: 0,
};

const WATCHLIST_RANKING_SORT: RankingTableSortState = {
  field: 'tradingValue',
  order: 'desc',
};

function normalizeStockCode(value: string): string {
  return value.trim();
}

function resolveCompanyName(code: string, selectedStock: StockSearchResultItem | null): string {
  const selectedCode = selectedStock ? normalizeStockCode(selectedStock.code) : '';
  return selectedStock && selectedCode === code ? selectedStock.companyName : code;
}

function ManageWatchlistDialog({
  watchlist,
  onWatchlistDeleted,
}: {
  watchlist: WatchlistWithItemsResponse;
  onWatchlistDeleted?: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [code, setCode] = useState('');
  const [memo, setMemo] = useState('');
  const [name, setName] = useState(watchlist.name);
  const [description, setDescription] = useState(watchlist.description || '');
  const [itemMemos, setItemMemos] = useState<Record<number, string>>({});
  const [moveItemId, setMoveItemId] = useState<number | null>(null);
  const [moveTargetIds, setMoveTargetIds] = useState<Record<number, number>>({});
  const [isDeleteConfirming, setIsDeleteConfirming] = useState(false);
  const [selectedStock, setSelectedStock] = useState<StockSearchResultItem | null>(null);
  const watchlistsQuery = useWatchlists();
  const addItem = useAddWatchlistItem();
  const removeItem = useRemoveWatchlistItem();
  const updateWatchlist = useUpdateWatchlist();
  const updateItem = useUpdateWatchlistItem();
  const deleteWatchlist = useDeleteWatchlist();
  const normalizedCode = normalizeStockCode(code);
  const isValidCode = /^\d{4}$/.test(normalizedCode);
  const moveTargetWatchlists = useMemo(
    () => (watchlistsQuery.data?.watchlists ?? []).filter((candidate) => candidate.id !== watchlist.id),
    [watchlist.id, watchlistsQuery.data?.watchlists]
  );

  useEffect(() => {
    if (open) {
      setName(watchlist.name);
      setDescription(watchlist.description || '');
      setItemMemos(Object.fromEntries(watchlist.items.map((item) => [item.id, item.memo ?? ''])));
      setMoveItemId(null);
      setMoveTargetIds({});
      setIsDeleteConfirming(false);
    }
  }, [open, watchlist.name, watchlist.description, watchlist.items]);

  const handleAddStock = (e: React.FormEvent) => {
    e.preventDefault();
    if (!isValidCode) return;

    const resolvedCompanyName = resolveCompanyName(normalizedCode, selectedStock);

    addItem.mutate(
      {
        watchlistId: watchlist.id,
        data: {
          code: normalizedCode,
          companyName: resolvedCompanyName,
          memo: memo.trim() || undefined,
        },
      },
      {
        onSuccess: () => {
          setCode('');
          setMemo('');
          setSelectedStock(null);
        },
      }
    );
  };

  const handleSaveDetails = (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;

    updateWatchlist.mutate({
      id: watchlist.id,
      data: { name: name.trim(), description: description.trim() || undefined },
    });
  };

  const handleDelete = () => {
    deleteWatchlist.mutate(watchlist.id, {
      onSuccess: () => {
        setOpen(false);
        onWatchlistDeleted?.();
      },
    });
  };

  const handleSaveItemMemo = (itemId: number) => {
    const draft = itemMemos[itemId] ?? '';
    updateItem.mutate({
      watchlistId: watchlist.id,
      itemId,
      data: { memo: draft.trim() || null },
    });
  };

  const handleStartMove = (itemId: number) => {
    const firstTarget = moveTargetWatchlists[0];
    if (!firstTarget) return;
    setMoveItemId((current) => (current === itemId ? null : itemId));
    setMoveTargetIds((current) => ({
      ...current,
      [itemId]: current[itemId] ?? firstTarget.id,
    }));
  };

  const handleMoveItem = (item: WatchlistWithItemsResponse['items'][number]) => {
    const targetWatchlistId = moveTargetIds[item.id];
    if (!targetWatchlistId || targetWatchlistId === watchlist.id) return;

    addItem.mutate(
      {
        watchlistId: targetWatchlistId,
        data: {
          code: item.code,
          companyName: item.companyName,
          memo: item.memo ?? undefined,
        },
      },
      {
        onSuccess: () => {
          removeItem.mutate({ watchlistId: watchlist.id, itemId: item.id });
          setMoveItemId(null);
        },
      }
    );
  };

  const handleOpenChange = (open: boolean) => {
    setOpen(open);
    if (!open) {
      setCode('');
      setMemo('');
      setSelectedStock(null);
      setMoveItemId(null);
      setMoveTargetIds({});
      setIsDeleteConfirming(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button size="sm" variant="secondary" className="gap-2">
          <ListChecks className="h-4 w-4" />
          Manage Watchlist
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-4xl">
        <DialogHeader>
          <DialogTitle>Manage Watchlist</DialogTitle>
          <DialogDescription>
            {watchlist.name} · {formatCount(watchlist.items.length)} names
          </DialogDescription>
        </DialogHeader>

        <div className="grid max-h-[72vh] gap-5 overflow-y-auto py-4 pr-1">
          <form onSubmit={handleAddStock} className="rounded-lg border border-border/70 p-3">
            <div className="mb-3 flex items-center gap-2">
              <Plus className="h-4 w-4 text-muted-foreground" />
              <h3 className="text-sm font-semibold text-foreground">Add Stock</h3>
            </div>
            <div
              data-testid="watchlist-add-stock-fields"
              className="grid gap-3 sm:grid-cols-[minmax(0,2fr)_minmax(0,1fr)] sm:items-end"
            >
              <div className="grid gap-2">
                <Label htmlFor="stock-code">Stock Code</Label>
                <StockSearchInput
                  id="stock-code"
                  value={code}
                  onValueChange={(value) => {
                    setCode(value);
                    const selectedCode = selectedStock ? normalizeStockCode(selectedStock.code) : '';
                    if (selectedStock && value.trim() !== selectedCode) {
                      setSelectedStock(null);
                    }
                  }}
                  onSelect={(stock) => {
                    setCode(stock.code);
                    setSelectedStock(stock);
                  }}
                  placeholder="銘柄コードまたは会社名で検索..."
                  required
                  autoFocus
                  className="border-input bg-transparent"
                  searchLimit={50}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="stock-memo">Memo (optional)</Label>
                <Input
                  id="stock-memo"
                  value={memo}
                  onChange={(e) => setMemo(e.target.value)}
                  placeholder="Watching for breakout"
                />
              </div>
            </div>
            <p className="mt-2 text-xs text-muted-foreground">Search by code or company name, then select a symbol.</p>
            {addItem.error && <p className="mt-3 text-sm text-destructive">{addItem.error.message}</p>}
            <div className="mt-3 flex justify-end">
              <Button type="submit" disabled={!isValidCode || addItem.isPending}>
                {addItem.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                Add
              </Button>
            </div>
          </form>

          <section className="rounded-lg border border-border/70 p-3">
            <h3 className="mb-3 text-sm font-semibold text-foreground">Stocks</h3>
            {watchlist.items.length === 0 ? (
              <div className="rounded-lg border border-dashed border-border/70 px-4 py-8 text-center text-sm text-muted-foreground">
                No stocks in this watchlist
              </div>
            ) : (
              <div className="max-h-[18rem] overflow-auto rounded-lg border border-border/70">
                {watchlist.items.map((item) => (
                  <div
                    key={item.id}
                    data-testid={`watchlist-item-row-${item.id}`}
                    className="grid gap-2 border-b border-border/50 px-3 py-2 last:border-b-0 sm:grid-cols-[minmax(0,1.2fr)_minmax(0,1fr)_auto] sm:items-center"
                  >
                    <div className="flex min-w-0 items-center gap-2">
                      <span className="font-medium tabular-nums text-foreground">{item.code}</span>
                      <span className="truncate text-sm text-foreground">{item.companyName}</span>
                    </div>
                    <div className="min-w-0">
                      <Label htmlFor={`watchlist-item-memo-${item.id}`} className="sr-only">
                        Memo for {item.code}
                      </Label>
                      <Input
                        id={`watchlist-item-memo-${item.id}`}
                        value={itemMemos[item.id] ?? ''}
                        onChange={(e) =>
                          setItemMemos((current) => ({
                            ...current,
                            [item.id]: e.target.value,
                          }))
                        }
                        placeholder="Memo"
                        className="h-8 text-xs"
                      />
                    </div>
                    <div className="flex items-center justify-end gap-1">
                      <Button
                        size="icon"
                        variant="ghost"
                        className="h-8 w-8 shrink-0"
                        onClick={() => handleSaveItemMemo(item.id)}
                        disabled={updateItem.isPending || (itemMemos[item.id] ?? '') === (item.memo ?? '')}
                        aria-label={`Save memo for ${item.code}`}
                      >
                        {updateItem.isPending ? (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        ) : (
                          <Check className="h-4 w-4" />
                        )}
                      </Button>
                      <Button
                        size="icon"
                        variant="ghost"
                        className="h-8 w-8 shrink-0"
                        onClick={() => handleStartMove(item.id)}
                        disabled={addItem.isPending || removeItem.isPending || moveTargetWatchlists.length === 0}
                        aria-label={`Move ${item.code} to another watchlist`}
                      >
                        <ArrowRightLeft className="h-4 w-4" />
                      </Button>
                      <Button
                        size="icon"
                        variant="ghost"
                        className="h-8 w-8 shrink-0 text-destructive hover:text-destructive"
                        onClick={() => removeItem.mutate({ watchlistId: watchlist.id, itemId: item.id })}
                        disabled={removeItem.isPending}
                        aria-label={`Remove ${item.code} from watchlist`}
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                    {moveItemId === item.id && (
                      <div className="grid gap-2 rounded-md border border-border/60 bg-muted/30 p-2 sm:col-span-3 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-end">
                        <div className="grid gap-1">
                          <Label htmlFor={`watchlist-move-target-${item.id}`} className="text-xs">
                            Move destination for {item.code}
                          </Label>
                          <select
                            id={`watchlist-move-target-${item.id}`}
                            value={moveTargetIds[item.id] ?? ''}
                            onChange={(event) =>
                              setMoveTargetIds((current) => ({
                                ...current,
                                [item.id]: Number(event.target.value),
                              }))
                            }
                            className="h-8 rounded-md border border-input bg-background px-2 text-xs text-foreground shadow-sm outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
                          >
                            {moveTargetWatchlists.map((target) => (
                              <option key={target.id} value={target.id}>
                                {target.name} ({target.stockCount})
                              </option>
                            ))}
                          </select>
                        </div>
                        <Button
                          type="button"
                          size="sm"
                          onClick={() => handleMoveItem(item)}
                          disabled={!moveTargetIds[item.id] || addItem.isPending || removeItem.isPending}
                        >
                          {addItem.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                          Move
                        </Button>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
            {updateItem.error && <p className="mt-3 text-sm text-destructive">{updateItem.error.message}</p>}
            {watchlistsQuery.error && <p className="mt-3 text-sm text-destructive">{watchlistsQuery.error.message}</p>}
          </section>

          <form onSubmit={handleSaveDetails} className="rounded-lg border border-border/70 p-3">
            <h3 className="mb-3 text-sm font-semibold text-foreground">Details</h3>
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="grid gap-2">
                <Label htmlFor="edit-watchlist-name">Name</Label>
                <Input
                  id="edit-watchlist-name"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="My Watchlist"
                  required
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="edit-watchlist-description">Description (optional)</Label>
                <Input
                  id="edit-watchlist-description"
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                  placeholder="Breakout candidates"
                />
              </div>
            </div>
            {updateWatchlist.error && <p className="mt-3 text-sm text-destructive">{updateWatchlist.error.message}</p>}
            <div className="mt-3 flex justify-end">
              <Button type="submit" disabled={!name.trim() || updateWatchlist.isPending}>
                {updateWatchlist.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                Save Details
              </Button>
            </div>
          </form>

          <section className="rounded-lg border border-destructive/30 bg-destructive/5 p-3">
            <h3 className="text-sm font-semibold text-destructive">Danger Zone</h3>
            <div className="mt-3 flex flex-wrap items-center justify-between gap-3">
              <p className="text-xs text-muted-foreground">
                Delete this watchlist and remove {formatCount(watchlist.items.length)} names from it.
              </p>
              {isDeleteConfirming ? (
                <div className="flex items-center gap-2">
                  <Button type="button" variant="outline" onClick={() => setIsDeleteConfirming(false)}>
                    Cancel
                  </Button>
                  <Button
                    type="button"
                    variant="destructive"
                    onClick={handleDelete}
                    disabled={deleteWatchlist.isPending}
                  >
                    {deleteWatchlist.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                    Confirm Delete
                  </Button>
                </div>
              ) : (
                <Button type="button" variant="destructive" onClick={() => setIsDeleteConfirming(true)}>
                  <Trash2 className="mr-2 h-4 w-4" />
                  Delete Watchlist
                </Button>
              )}
            </div>
          </section>
        </div>

        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => handleOpenChange(false)}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

interface WatchlistDetailProps {
  watchlist: WatchlistWithItemsResponse | undefined;
  isLoading: boolean;
  error: Error | null;
  onWatchlistDeleted?: () => void;
}

function EmptySelectionState(): ReactNode {
  return (
    <Surface className="flex min-h-[24rem] items-center justify-center px-6 py-16">
      <div className="flex flex-col items-center justify-center">
        <Eye className="mb-4 h-16 w-16 text-muted-foreground" />
        <p className="text-lg text-muted-foreground">Select a watchlist to view details</p>
      </div>
    </Surface>
  );
}

function WatchlistDetailContent({
  watchlist,
  onWatchlistDeleted,
}: {
  watchlist: WatchlistWithItemsResponse;
  onWatchlistDeleted?: () => void;
}) {
  const navigate = useNavigate();
  const [sectorStrengthFamily, setSectorStrengthFamily] =
    useState<RankingParams['sectorStrengthFamily']>('balanced_sector_strength');
  const rankingParams = useMemo<RankingParams>(
    () => ({
      ...WATCHLIST_RANKING_PARAMS,
      sectorStrengthFamily,
    }),
    [sectorStrengthFamily]
  );
  const rankingQuery = useRanking(rankingParams, watchlist.items.length > 0);
  const watchlistCodes = useMemo(() => new Set(watchlist.items.map((item) => item.code)), [watchlist.items]);
  const watchlistSummary = useMemo<WatchlistSummaryResponse>(
    () => ({
      id: watchlist.id,
      name: watchlist.name,
      description: watchlist.description,
      stockCount: watchlist.items.length,
      createdAt: watchlist.createdAt,
      updatedAt: watchlist.updatedAt,
    }),
    [watchlist]
  );

  const handleNavigateToChart = (code: string) => {
    void navigate({ to: '/symbol-workbench', search: { symbol: code } });
  };
  const memoCount = watchlist.items.filter((item) => item.memo?.trim()).length;

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-2">
      <Surface className="px-4 py-3">
        <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
          <div className="min-w-0 space-y-1">
            <SectionEyebrow>Selected Watchlist</SectionEyebrow>
            <h2 className="truncate text-lg font-semibold tracking-tight text-foreground">{watchlist.name}</h2>
            {watchlist.description ? (
              <p className="max-w-2xl truncate text-xs text-muted-foreground">{watchlist.description}</p>
            ) : null}
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <div className="w-52 space-y-1">
              <Label htmlFor="watchlist-sector-strength-family" className="text-xs">
                Index Strength
              </Label>
              <Select
                value={sectorStrengthFamily}
                onValueChange={(value) => setSectorStrengthFamily(value as RankingParams['sectorStrengthFamily'])}
              >
                <SelectTrigger id="watchlist-sector-strength-family" className="h-8 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {SECTOR_STRENGTH_FAMILY_OPTIONS.map((option) => (
                    <SelectItem key={option.value} value={option.value}>
                      {option.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="mr-1 flex items-center gap-2 text-xs text-muted-foreground">
              <span>{formatCount(watchlist.items.length)} names</span>
              {memoCount > 0 ? <span>{formatCount(memoCount)} memos</span> : null}
            </div>
            <ManageWatchlistDialog watchlist={watchlist} onWatchlistDeleted={onWatchlistDeleted} />
          </div>
        </div>
      </Surface>

      <RankingTable
        items={rankingQuery.data?.rankings.tradingValue}
        isLoading={watchlist.items.length > 0 ? rankingQuery.isLoading : false}
        error={rankingQuery.error}
        onStockClick={handleNavigateToChart}
        title="Daily Ranking"
        eyebrow="Watchlist Filter"
        showValuation
        showLiquidity
        showChangeForTradingValue
        enableColumnSort
        className="flex min-h-[24rem] flex-1 flex-col overflow-visible"
        initialSortState={WATCHLIST_RANKING_SORT}
        enableTableFilters
        filterState={{ watchlistId: watchlist.id }}
        filterWatchlists={[watchlistSummary]}
        filterWatchlistCodes={watchlistCodes}
        emptyMessage={watchlist.items.length === 0 ? 'No stocks in this watchlist' : 'No Daily Ranking rows'}
        emptySubMessage={
          watchlist.items.length === 0
            ? 'Add a stock to begin monitoring it through Daily Ranking.'
            : 'The selected names may be outside the current market universe or ranking snapshot.'
        }
        scrollRestorationKey={`watchlist:daily-ranking:${watchlist.id}`}
      />
    </div>
  );
}

export function WatchlistDetail({ watchlist, isLoading, error, onWatchlistDeleted }: WatchlistDetailProps) {
  if (!watchlist && !isLoading && !error) {
    return <EmptySelectionState />;
  }

  return (
    <DataStateWrapper isLoading={isLoading} error={error} height="h-64">
      {watchlist && <WatchlistDetailContent watchlist={watchlist} onWatchlistDeleted={onWatchlistDeleted} />}
    </DataStateWrapper>
  );
}
