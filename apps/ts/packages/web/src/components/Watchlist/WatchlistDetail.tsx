import { Eye, Loader2, Plus, Trash2, TrendingUp } from 'lucide-react';
import { useNavigate } from '@tanstack/react-router';
import type { ReactNode } from 'react';
import { useMemo, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
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
import {
  useAddWatchlistItem,
  useDeleteWatchlist,
  useRemoveWatchlistItem,
  useWatchlistPrices,
} from '@/hooks/useWatchlist';
import { useChartStore } from '@/stores/chartStore';
import type { WatchlistItem, WatchlistStockPrice, WatchlistWithItems } from '@/types/watchlist';
import { getPositiveNegativeColor } from '@/utils/color-schemes';

function AddStockDialog({ watchlistId }: { watchlistId: number }) {
  const [open, setOpen] = useState(false);
  const [code, setCode] = useState('');
  const [memo, setMemo] = useState('');
  const addItem = useAddWatchlistItem();

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!code.trim()) return;

    addItem.mutate(
      { watchlistId, data: { code: code.trim(), memo: memo.trim() || undefined } },
      {
        onSuccess: () => {
          setOpen(false);
          setCode('');
          setMemo('');
        },
      }
    );
  };

  const handleOpenChange = (open: boolean) => {
    setOpen(open);
    if (!open) {
      setCode('');
      setMemo('');
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
              <Input
                id="stock-code"
                value={code}
                onChange={(e) => setCode(e.target.value)}
                placeholder="7203"
                required
                autoFocus
                maxLength={4}
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
          {addItem.error && <p className="text-sm text-destructive mb-4">{addItem.error.message}</p>}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={!code.trim() || addItem.isPending}>
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
    <tr className="border-b border-border/30 hover:bg-accent/30 transition-colors">
      <td className="py-3 px-4">
        <button
          type="button"
          onClick={() => onNavigateToChart(item.code)}
          aria-label={`View chart for ${item.code} ${item.companyName}`}
          className="flex items-center gap-2 text-primary hover:text-primary/80 font-medium transition-colors"
        >
          <TrendingUp className="h-4 w-4" />
          {item.code}
        </button>
      </td>
      <td className="py-3 px-4">
        <button
          type="button"
          onClick={() => onNavigateToChart(item.code)}
          aria-label={`View chart for ${item.companyName}`}
          className="text-left hover:text-primary transition-colors"
        >
          {item.companyName}
        </button>
      </td>
      <td className="py-3 px-4 text-right tabular-nums">{price ? price.close.toLocaleString() : '-'}</td>
      <td
        className={`py-3 px-4 text-right tabular-nums ${price?.changePercent != null ? getPositiveNegativeColor(price.changePercent) : ''}`}
      >
        {price?.changePercent != null
          ? `${price.changePercent >= 0 ? '+' : ''}${price.changePercent.toFixed(2)}%`
          : '-'}
      </td>
      <td className="py-3 px-4 text-right tabular-nums">{price ? price.volume.toLocaleString() : '-'}</td>
      <td className="py-3 px-4 text-muted-foreground text-sm">{item.memo ?? ''}</td>
      <td className="py-3 px-2">
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
    <Card className="glass-panel overflow-hidden">
      <CardHeader className="border-b border-border/30">
        <CardTitle>Stocks ({items.length})</CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        {items.length === 0 ? (
          <div className="py-8 text-center text-muted-foreground">
            <Eye className="h-12 w-12 mx-auto mb-4 opacity-50" />
            <p>No stocks in this watchlist</p>
            <p className="text-sm mt-1">Click &quot;Add Stock&quot; above to add your first stock.</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-muted/30">
                <tr>
                  <th className="py-3 px-4 text-left font-medium">Code</th>
                  <th className="py-3 px-4 text-left font-medium">Company</th>
                  <th className="py-3 px-4 text-right font-medium">Price</th>
                  <th className="py-3 px-4 text-right font-medium">Change</th>
                  <th className="py-3 px-4 text-right font-medium">Volume</th>
                  <th className="py-3 px-4 text-left font-medium">Memo</th>
                  <th className="py-3 px-2 text-center font-medium w-12" />
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
      </CardContent>
    </Card>
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
    <Card className="glass-panel">
      <CardContent className="flex flex-col items-center justify-center py-16">
        <Eye className="h-16 w-16 text-muted-foreground mb-4" />
        <p className="text-muted-foreground text-lg">Select a watchlist to view details</p>
      </CardContent>
    </Card>
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
  const { setSelectedSymbol } = useChartStore();
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
    setSelectedSymbol(code);
    void navigate({ to: '/charts' });
  };

  return (
    <div className="space-y-4">
      <div className="px-6 py-4 gradient-primary rounded-xl">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-2xl font-bold text-white">{watchlist.name}</h2>
            {watchlist.description && <p className="text-white/80">{watchlist.description}</p>}
          </div>
          <div className="text-right text-white">
            <p className="text-sm opacity-80">Stocks</p>
            <p className="text-2xl font-bold tabular-nums">{watchlist.items.length}</p>
          </div>
        </div>
        <div className="flex items-center gap-2 mt-4">
          <AddStockDialog watchlistId={watchlist.id} />
          <DeleteWatchlistDialog watchlist={watchlist} onSuccess={onWatchlistDeleted} />
        </div>
      </div>

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
