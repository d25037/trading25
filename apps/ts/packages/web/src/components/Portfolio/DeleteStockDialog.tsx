import type { PortfolioItemResponse } from '@trading25/contracts/types/api-response-types';
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
import { useDeletePortfolioItem } from '@/hooks/usePortfolio';
import { formatCount, formatCurrency } from '@/utils/formatters';

interface DeleteStockDialogProps {
  item: PortfolioItemResponse;
}

export function DeleteStockDialog({ item }: DeleteStockDialogProps) {
  const [open, setOpen] = useState(false);
  const deleteStock = useDeletePortfolioItem();

  const handleDelete = () => {
    deleteStock.mutate(
      { portfolioId: item.portfolioId, itemId: item.id },
      {
        onSuccess: () => {
          setOpen(false);
        },
      }
    );
  };

  const totalValue = item.quantity * item.purchasePrice;

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button size="icon" variant="ghost" className="h-8 w-8 text-destructive hover:text-destructive">
          <Trash2 className="h-4 w-4" />
          <span className="sr-only">Delete {item.code}</span>
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete Stock</DialogTitle>
          <DialogDescription>
            Are you sure you want to remove {item.code} ({item.companyName}) from your portfolio?
            <span className="block mt-2 text-muted-foreground">
              {formatCount(item.quantity)} shares at {formatCurrency(item.purchasePrice)} yen ={' '}
              {formatCurrency(totalValue)} yen
            </span>
          </DialogDescription>
        </DialogHeader>
        {deleteStock.error && <p className="text-sm text-destructive">{deleteStock.error.message}</p>}
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button variant="destructive" onClick={handleDelete} disabled={deleteStock.isPending}>
            {deleteStock.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            Delete Stock
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
