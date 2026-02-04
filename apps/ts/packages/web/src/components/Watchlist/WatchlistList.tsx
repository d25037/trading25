import { ChevronRight, Eye, Loader2, Plus } from 'lucide-react';
import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
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
import { useCreateWatchlist } from '@/hooks/useWatchlist';
import { cn } from '@/lib/utils';
import type { WatchlistSummary } from '@/types/watchlist';

interface CreateWatchlistDialogProps {
  onSuccess?: (id: number) => void;
}

function CreateWatchlistDialog({ onSuccess }: CreateWatchlistDialogProps) {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const createWatchlist = useCreateWatchlist();

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;

    createWatchlist.mutate(
      { name: name.trim(), description: description.trim() || undefined },
      {
        onSuccess: (data) => {
          setOpen(false);
          setName('');
          setDescription('');
          onSuccess?.(data.id);
        },
      }
    );
  };

  const handleOpenChange = (open: boolean) => {
    setOpen(open);
    if (!open) {
      setName('');
      setDescription('');
    }
  };

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button size="sm" className="gap-2">
          <Plus className="h-4 w-4" />
          New Watchlist
        </Button>
      </DialogTrigger>
      <DialogContent>
        <form onSubmit={handleSubmit}>
          <DialogHeader>
            <DialogTitle>Create Watchlist</DialogTitle>
            <DialogDescription>Create a new watchlist to monitor stocks.</DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="wl-name">Name</Label>
              <Input
                id="wl-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Tech Stocks"
                required
                autoFocus
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="wl-description">Description (optional)</Label>
              <Input
                id="wl-description"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="Technology sector stocks to monitor"
              />
            </div>
          </div>
          {createWatchlist.error && <p className="text-sm text-destructive mb-4">{createWatchlist.error.message}</p>}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={!name.trim() || createWatchlist.isPending}>
              {createWatchlist.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Create
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

interface WatchlistListProps {
  watchlists: WatchlistSummary[];
  selectedId: number | null;
  onSelect: (id: number) => void;
  isLoading: boolean;
}

function EmptyWatchlistState({ onSelect }: { onSelect: (id: number) => void }) {
  return (
    <Card className="glass-panel">
      <CardContent className="flex flex-col items-center justify-center py-8">
        <Eye className="h-12 w-12 text-muted-foreground mb-4" />
        <p className="text-muted-foreground text-center">No watchlists found</p>
        <p className="text-sm text-muted-foreground text-center mt-2 mb-4">
          Create your first watchlist to start monitoring stocks.
        </p>
        <CreateWatchlistDialog onSuccess={onSelect} />
      </CardContent>
    </Card>
  );
}

function WatchlistListContent({
  watchlists,
  selectedId,
  onSelect,
}: {
  watchlists: WatchlistSummary[];
  selectedId: number | null;
  onSelect: (id: number) => void;
}) {
  if (watchlists.length === 0) {
    return <EmptyWatchlistState onSelect={onSelect} />;
  }

  return (
    <div className="space-y-3">
      <div className="flex justify-end">
        <CreateWatchlistDialog onSuccess={onSelect} />
      </div>
      {watchlists.map((watchlist) => (
        <button
          key={watchlist.id}
          type="button"
          onClick={() => onSelect(watchlist.id)}
          aria-label={`Select ${watchlist.name} watchlist`}
          aria-pressed={selectedId === watchlist.id}
          className={cn(
            'w-full text-left p-4 rounded-xl transition-all duration-200',
            'hover:scale-[1.01] active:scale-[0.99]',
            selectedId === watchlist.id ? 'gradient-primary text-white shadow-lg' : 'glass-panel hover:bg-accent/50'
          )}
        >
          <div className="flex items-center justify-between">
            <div className="min-w-0 flex-1">
              <h3 className="font-semibold truncate">{watchlist.name}</h3>
              {watchlist.description && <p className="text-sm opacity-80 truncate">{watchlist.description}</p>}
              <div className="flex gap-4 mt-2 text-sm opacity-70">
                <span>{watchlist.stockCount} stocks</span>
              </div>
            </div>
            <ChevronRight className="h-5 w-5 flex-shrink-0 ml-2" />
          </div>
        </button>
      ))}
    </div>
  );
}

export function WatchlistList({ watchlists, selectedId, onSelect, isLoading }: WatchlistListProps) {
  return (
    <DataStateWrapper isLoading={isLoading}>
      <WatchlistListContent watchlists={watchlists} selectedId={selectedId} onSelect={onSelect} />
    </DataStateWrapper>
  );
}
