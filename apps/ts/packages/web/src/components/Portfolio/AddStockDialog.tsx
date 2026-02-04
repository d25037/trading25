import { Loader2, Plus } from 'lucide-react';
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
import { useAddPortfolioItem } from '@/hooks/usePortfolio';
import { type StockFormData, StockFormFields, validateStockForm } from './StockFormFields';

const getTodayDate = (): string => {
  const dateStr = new Date().toISOString().split('T')[0];
  return dateStr ?? '';
};

const createInitialFormData = (): StockFormData => ({
  code: '',
  quantity: '100',
  purchasePrice: '',
  purchaseDate: getTodayDate(),
  account: '',
  notes: '',
});

interface AddStockDialogProps {
  portfolioId: number;
}

export function AddStockDialog({ portfolioId }: AddStockDialogProps) {
  const [open, setOpen] = useState(false);
  const [formData, setFormData] = useState<StockFormData>(createInitialFormData);
  const addStock = useAddPortfolioItem();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!validateStockForm(formData)) return;

    addStock.mutate(
      {
        portfolioId,
        data: {
          code: (formData.code || '').trim().toUpperCase(),
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
          setFormData(createInitialFormData());
        },
      }
    );
  };

  const handleOpenChange = (newOpen: boolean) => {
    setOpen(newOpen);
    if (!newOpen) {
      setFormData(createInitialFormData());
    }
  };

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button size="sm" className="gap-2">
          <Plus className="h-4 w-4" />
          Add Stock
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-md">
        <form onSubmit={handleSubmit}>
          <DialogHeader>
            <DialogTitle>Add Stock</DialogTitle>
            <DialogDescription>Add a new stock to your portfolio.</DialogDescription>
          </DialogHeader>
          <StockFormFields data={formData} onChange={setFormData} showCodeField={true} idPrefix="add" />
          {addStock.error && <p className="text-sm text-destructive mb-4">{addStock.error.message}</p>}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={!validateStockForm(formData) || addStock.isPending}>
              {addStock.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Add Stock
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
