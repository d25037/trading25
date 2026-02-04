import { Loader2, Pencil } from 'lucide-react';
import { useEffect, useState } from 'react';
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
import { useUpdatePortfolioItem } from '@/hooks/usePortfolio';
import type { PortfolioItem } from '@/types/portfolio';
import { type StockFormData, StockFormFields, validateStockForm } from './StockFormFields';

const createFormDataFromItem = (item: PortfolioItem): StockFormData => ({
  quantity: item.quantity.toString(),
  purchasePrice: item.purchasePrice.toString(),
  purchaseDate: item.purchaseDate,
  account: item.account || '',
  notes: item.notes || '',
});

interface EditStockDialogProps {
  item: PortfolioItem;
}

export function EditStockDialog({ item }: EditStockDialogProps) {
  const [open, setOpen] = useState(false);
  const [formData, setFormData] = useState<StockFormData>(() => createFormDataFromItem(item));
  const updateStock = useUpdatePortfolioItem();

  useEffect(() => {
    if (open) {
      setFormData(createFormDataFromItem(item));
    }
  }, [open, item]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!validateStockForm(formData, false)) return;

    updateStock.mutate(
      {
        portfolioId: item.portfolioId,
        itemId: item.id,
        data: {
          quantity: Number.parseInt(formData.quantity, 10),
          purchasePrice: Number.parseFloat(formData.purchasePrice),
          purchaseDate: formData.purchaseDate,
          account: formData.account.trim() || undefined,
          notes: formData.notes.trim() || undefined,
        },
      },
      {
        onSuccess: () => {
          setOpen(false);
        },
      }
    );
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button size="icon" variant="ghost" className="h-8 w-8">
          <Pencil className="h-4 w-4" />
          <span className="sr-only">Edit {item.code}</span>
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-md">
        <form onSubmit={handleSubmit}>
          <DialogHeader>
            <DialogTitle>
              Edit {item.code} - {item.companyName}
            </DialogTitle>
            <DialogDescription>Update the stock details in your portfolio.</DialogDescription>
          </DialogHeader>
          <StockFormFields data={formData} onChange={setFormData} showCodeField={false} idPrefix="edit" />
          {updateStock.error && <p className="text-sm text-destructive mb-4">{updateStock.error.message}</p>}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={!validateStockForm(formData, false) || updateStock.isPending}>
              {updateStock.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Save Changes
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
