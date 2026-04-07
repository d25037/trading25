import { useNavigate } from '@tanstack/react-router';
import { Eye, Loader2, Plus, Trash2, TrendingUp } from 'lucide-react';
import type { ReactNode } from 'react';
import { useMemo, useState } from 'react';
import { CompactMetric, SectionEyebrow, SectionHeading, Surface } from '@/components/Layout/Workspace';
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
import type { StockSearchResultItem } from '@/hooks/useStockSearch';
import {
  useAddWatchlistItem,
  useDeleteWatchlist,
  useRemoveWatchlistItem,
  useWatchlistPrices,
} from '@/hooks/useWatchlist';
import type { WatchlistItem, WatchlistStockPrice, WatchlistWithItems } from '@/types/watchlist';
import { getPositiveNegativeColor } from '@/utils/color-schemes';

function normalizeStockCode(value: string): string {
  return value.trim();
}

function resolveCompanyName(code: string, selectedStock: StockSearchResultItem | null): string {
  const selectedCode = selectedStock ? normalizeStockCode(selectedStock.code) : '';
  return selectedStock && selectedCode === code ? selectedStock.companyName : code;
}

function AddStockDialog({ watchlistId }: { watchlistId: number }) {
  const [open, setOpen] = useState(false);
  const [code, setCode] = useState('');
  const [memo, setMemo] = useState('');
  const [selectedStock, setSelectedStock] = useState<StockSearchResultItem | null>(null);
  const addItem = useAddWatchlistItem();
  const normalizedCode = normalizeStockCode(code);
  const isValidCode = /^\d{4}$/.test(normalizedCode);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!isValidCode) return;

    const resolvedCompanyName = resolveCompanyName(normalizedCode, selectedStock);

    addItem.mutate(
      {
        watchlistId,
        data: {
          code: normalizedCode,
          companyName: resolvedCompanyName,
          memo: memo.trim() || undefined,
        },
      },
      {
        onSuccess: () => {
          setOpen(false);
          setCode('');
          setMemo('');
          setSelectedStock(null);
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
    }
  };

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button size="sm" variant="secondary" className="gap-2">
          <Plus className="h-4 w-4" />
          Add Stock
        </Button>
      </DialogTrigger>
      <DialogContent>
        <form onSubmit={handleSubmit}>
          <DialogHeader>
            <DialogTitle>Add Stock to Watchlist</DialogTitle>
            <DialogDescription>Enter a stock code. Company name will be fetched automatically.</DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
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
              <p className="text-xs text-muted-foreground">Search by code or company name, then select a symbol.</p>
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
          {addItem.error && <p className="mb-4 text-sm text-destructive">{addItem.error.message}</p>}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => handleOpenChange(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={!isValidCode || addItem.isPending}>
              {addItem.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Add
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

function DeleteWatchlistDialog({ watchlist, onSuccess }: { watchlist: WatchlistWithItems; onSuccess?: () => void }) {
  const [open, setOpen] = useState(false);
  const deleteWatchlist = useDeleteWatchlist();

  const handleDelete = () => {
    deleteWatchlist.mutate(watchlist.id, {
      onSuccess: () => {
        setOpen(false);
        onSuccess?.();
      },
    });
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button size="sm" variant="destructive" className="gap-2">
          <Trash2 className="h-4 w-4" />
          Delete
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete Watchlist</DialogTitle>
          <DialogDescription>
            Are you sure you want to delete &quot;{watchlist.name}&quot;? This will remove {watchlist.items.length}{' '}
            stock{watchlist.items.length !== 1 ? 's' : ''} from the watchlist. This action cannot be undone.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button variant="destructive" onClick={handleDelete} disabled={deleteWatchlist.isPending}>
            {deleteWatchlist.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            Delete
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

interface StockRowProps {
  item: WatchlistItem;
  price: WatchlistStockPrice | undefined;
  watchlistId: number;
  onNavigateToChart: (code: string) => void;
}

function StockRow({ item, price, watchlistId, onNavigateToChart }: StockRowProps) {
  const removeItem = useRemoveWatchlistItem();

  return (
    <tr className="border-b border-border/50 transition-colors hover:bg-[var(--app-surface-muted)]">
      <td className="px-4 py-3">
        <button
          type="button"
          onClick={() => onNavigateToChart(item.code)}
          aria-label={`View chart for ${item.code} ${item.companyName}`}
          className="flex items-center gap-2 font-medium text-primary transition-colors hover:text-primary/80"
        >
          <TrendingUp className="h-4 w-4" />
          {item.code}
        </button>
      </td>
      <td className="px-4 py-3">
        <button
          type="button"
          onClick={() => onNavigateToChart(item.code)}
          aria-label={`View chart for ${item.companyName}`}
          className="text-left transition-colors hover:text-primary"
        >
          {item.companyName}
        </button>
      </td>
      <td className="px-4 py-3 text-right tabular-nums">{price ? price.close.toLocaleString() : '-'}</td>
      <td
        className={`px-4 py-3 text-right tabular-nums ${price?.changePercent != null ? getPositiveNegativeColor(price.changePercent) : ''}`}
      >
        {price?.changePercent != null
          ? `${price.changePercent >= 0 ? '+' : ''}${price.changePercent.toFixed(2)}%`
          : '-'}
      </td>
      <td className="px-4 py-3 text-right tabular-nums">{price ? price.volume.toLocaleString() : '-'}</td>
      <td className="px-4 py-3 text-sm text-muted-foreground">{item.memo ?? ''}</td>
      <td className="px-2 py-3">
        <Button
          size="icon"
          variant="ghost"
          className="h-8 w-8 text-destructive hover:text-destructive"
          onClick={() => removeItem.mutate({ watchlistId, itemId: item.id })}
          disabled={removeItem.isPending}
          aria-label={`Remove ${item.code} from watchlist`}
        >
          <Trash2 className="h-4 w-4" />
        </Button>
      </td>
    </tr>
  );
}

function WatchlistTable({
  items,
  priceMap,
  watchlistId,
  onNavigateToChart,
}: {
  items: WatchlistItem[];
  priceMap: Map<string, WatchlistStockPrice>;
  watchlistId: number;
  onNavigateToChart: (code: string) => void;
}) {
  return (
    <Surface className="flex min-h-[26rem] flex-col overflow-hidden">
      <div className="border-b border-border/60 px-5 py-4">
        <SectionHeading
          eyebrow="Results"
          title="Tracked Stocks"
          description="Monitor live prices, daily change, and memo context in one table."
          actions={<div className="text-sm text-muted-foreground">{items.length} names</div>}
        />
      </div>

      <div className="min-h-0 flex-1">
        {items.length === 0 ? (
          <div className="flex h-full flex-col items-center justify-center px-6 py-10 text-center text-muted-foreground">
            <Eye className="mx-auto mb-4 h-12 w-12 opacity-50" />
            <p>No stocks in this watchlist</p>
            <p className="mt-1 text-sm">Click &quot;Add Stock&quot; above to add your first stock.</p>
          </div>
        ) : (
          <div className="h-full overflow-auto">
            <table className="w-full">
              <thead className="sticky top-0 z-10">
                <tr>
                  <th className="bg-[var(--app-surface-muted)] px-4 py-3 text-left text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
                    Code
                  </th>
                  <th className="bg-[var(--app-surface-muted)] px-4 py-3 text-left text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
                    Company
                  </th>
                  <th className="bg-[var(--app-surface-muted)] px-4 py-3 text-right text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
                    Price
                  </th>
                  <th className="bg-[var(--app-surface-muted)] px-4 py-3 text-right text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
                    Change
                  </th>
                  <th className="bg-[var(--app-surface-muted)] px-4 py-3 text-right text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
                    Volume
                  </th>
                  <th className="bg-[var(--app-surface-muted)] px-4 py-3 text-left text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
                    Memo
                  </th>
                  <th className="bg-[var(--app-surface-muted)] px-2 py-3 text-center text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground" />
                </tr>
              </thead>
              <tbody>
                {items.map((item) => (
                  <StockRow
                    key={item.id}
                    item={item}
                    price={priceMap.get(item.code)}
                    watchlistId={watchlistId}
                    onNavigateToChart={onNavigateToChart}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </Surface>
  );
}

interface WatchlistDetailProps {
  watchlist: WatchlistWithItems | undefined;
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
  watchlist: WatchlistWithItems;
  onWatchlistDeleted?: () => void;
}) {
  const navigate = useNavigate();
  const { data: pricesData } = useWatchlistPrices(watchlist.id);

  const priceMap = useMemo(() => {
    const map = new Map<string, WatchlistStockPrice>();
    if (pricesData?.prices) {
      for (const price of pricesData.prices) {
        map.set(price.code, price);
      }
    }
    return map;
  }, [pricesData]);

  const handleNavigateToChart = (code: string) => {
    void navigate({ to: '/symbol-workbench', search: { symbol: code } });
  };
  const memoCount = watchlist.items.filter((item) => item.memo?.trim()).length;

  return (
    <div className="flex min-h-0 flex-col gap-3">
      <Surface className="p-5">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
          <div className="space-y-3">
            <SectionEyebrow>Selected Watchlist</SectionEyebrow>
            <div className="space-y-1">
              <h2 className="text-2xl font-semibold tracking-tight text-foreground">{watchlist.name}</h2>
              <p className="max-w-2xl text-sm text-muted-foreground">
                {watchlist.description ||
                  'Keep monitored names close, then jump straight into prices and the symbol workbench.'}
              </p>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <AddStockDialog watchlistId={watchlist.id} />
            <DeleteWatchlistDialog watchlist={watchlist} onSuccess={onWatchlistDeleted} />
          </div>
        </div>

        <div className="mt-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <CompactMetric label="Stocks" value={watchlist.items.length.toLocaleString()} detail="Tracked names" />
          <CompactMetric label="Live Prices" value={priceMap.size.toLocaleString()} detail="Symbols with current data" />
          <CompactMetric label="Memos" value={memoCount.toLocaleString()} detail="Names with notes" />
          <CompactMetric label="Created" value={watchlist.createdAt.slice(0, 10)} detail="Watchlist record" />
        </div>
      </Surface>

      <WatchlistTable
        items={watchlist.items}
        priceMap={priceMap}
        watchlistId={watchlist.id}
        onNavigateToChart={handleNavigateToChart}
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
