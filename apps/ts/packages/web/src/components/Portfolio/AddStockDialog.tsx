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
import type { StockSearchResultItem } from '@/hooks/useStockSearch';
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

function normalizeStockCode(value: string | undefined): string {
  return (value || '').trim().toUpperCase();
}

function resolveCompanyName(code: string, selectedStock: StockSearchResultItem | null): string {
  const selectedCode = selectedStock ? normalizeStockCode(selectedStock.code) : '';
  return selectedStock && selectedCode === code ? selectedStock.companyName : code;
}

export function AddStockDialog({ portfolioId }: AddStockDialogProps) {
  const [open, setOpen] = useState(false);
  const [formData, setFormData] = useState<StockFormData>(createInitialFormData);
  const [selectedStock, setSelectedStock] = useState<StockSearchResultItem | null>(null);
  const addStock = useAddPortfolioItem();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!validateStockForm(formData)) return;

    const normalizedCode = normalizeStockCode(formData.code);
    const resolvedCompanyName = resolveCompanyName(normalizedCode, selectedStock);

    addStock.mutate(
      {
        portfolioId,
        data: {
          code: normalizedCode,
          companyName: resolvedCompanyName,
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
          setSelectedStock(null);
        },
      }
    );
  };

  const handleOpenChange = (newOpen: boolean) => {
    setOpen(newOpen);
    if (!newOpen) {
      setFormData(createInitialFormData());
      setSelectedStock(null);
    }
  };

  const handleFormChange = (nextData: StockFormData) => {
    const normalizedInputCode = normalizeStockCode(nextData.code);
    const selectedCode = selectedStock ? normalizeStockCode(selectedStock.code) : '';
    if (selectedStock && normalizedInputCode !== selectedCode) {
      setSelectedStock(null);
    }
    setFormData(nextData);
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
          <StockFormFields
            data={formData}
            onChange={handleFormChange}
            showCodeField={true}
            idPrefix="add"
            onStockSelect={setSelectedStock}
          />
          {addStock.error && <p className="text-sm text-destructive mb-4">{addStock.error.message}</p>}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => handleOpenChange(false)}>
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
