import { Loader2, Trash2 } from 'lucide-react';
import { useState } from 'react';
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
import { useDeletePortfolio } from '@/hooks/usePortfolio';
import type { PortfolioWithItems } from '@/types/portfolio';

interface DeletePortfolioDialogProps {
  portfolio: PortfolioWithItems;
  onSuccess?: () => void;
}

export function DeletePortfolioDialog({ portfolio, onSuccess }: DeletePortfolioDialogProps) {
  const [open, setOpen] = useState(false);
  const deletePortfolio = useDeletePortfolio();

  const handleDelete = () => {
    deletePortfolio.mutate(portfolio.id, {
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
          <DialogTitle>Delete Portfolio</DialogTitle>
          <DialogDescription>
            Are you sure you want to delete "{portfolio.name}"? This action cannot be undone.
            {portfolio.items.length > 0 && (
              <span className="block mt-2 text-destructive">
                This portfolio contains {portfolio.items.length} stock(s) that will also be deleted.
              </span>
            )}
          </DialogDescription>
        </DialogHeader>
        {deletePortfolio.error && <p className="text-sm text-destructive">{deletePortfolio.error.message}</p>}
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button variant="destructive" onClick={handleDelete} disabled={deletePortfolio.isPending}>
            {deletePortfolio.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            Delete Portfolio
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
