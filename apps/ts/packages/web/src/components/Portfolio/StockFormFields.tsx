import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';

export interface StockFormData {
  code?: string;
  quantity: string;
  purchasePrice: string;
  purchaseDate: string;
  account: string;
  notes: string;
}

interface StockFormFieldsProps {
  data: StockFormData;
  onChange: (data: StockFormData) => void;
  showCodeField?: boolean;
  idPrefix?: string;
}

export function StockFormFields({ data, onChange, showCodeField = true, idPrefix = 'stock' }: StockFormFieldsProps) {
  const updateField = <K extends keyof StockFormData>(field: K, value: StockFormData[K]) => {
    onChange({ ...data, [field]: value });
  };

  return (
    <div className="grid gap-4 py-4">
      <div className="grid grid-cols-2 gap-4">
        {showCodeField && (
          <div className="grid gap-2">
            <Label htmlFor={`${idPrefix}-code`}>Stock Code</Label>
            <Input
              id={`${idPrefix}-code`}
              value={data.code || ''}
              onChange={(e) => updateField('code', e.target.value)}
              placeholder="7203"
              maxLength={4}
              required
              autoFocus
            />
            <p className="text-xs text-muted-foreground">4-digit code (e.g., 7203)</p>
          </div>
        )}
        <div className="grid gap-2">
          <Label htmlFor={`${idPrefix}-quantity`}>Quantity</Label>
          <Input
            id={`${idPrefix}-quantity`}
            type="number"
            value={data.quantity}
            onChange={(e) => updateField('quantity', e.target.value)}
            placeholder="100"
            min="100"
            step="100"
            required
            autoFocus={!showCodeField}
          />
        </div>
        {showCodeField ? null : (
          <div className="grid gap-2">
            <Label htmlFor={`${idPrefix}-price`}>Purchase Price</Label>
            <Input
              id={`${idPrefix}-price`}
              type="number"
              value={data.purchasePrice}
              onChange={(e) => updateField('purchasePrice', e.target.value)}
              placeholder="2500"
              min="0"
              step="0.01"
              required
            />
          </div>
        )}
      </div>
      {showCodeField && (
        <div className="grid grid-cols-2 gap-4">
          <div className="grid gap-2">
            <Label htmlFor={`${idPrefix}-price`}>Purchase Price</Label>
            <Input
              id={`${idPrefix}-price`}
              type="number"
              value={data.purchasePrice}
              onChange={(e) => updateField('purchasePrice', e.target.value)}
              placeholder="2500"
              min="0"
              step="0.01"
              required
            />
          </div>
          <div className="grid gap-2">
            <Label htmlFor={`${idPrefix}-date`}>Purchase Date</Label>
            <Input
              id={`${idPrefix}-date`}
              type="date"
              value={data.purchaseDate}
              onChange={(e) => updateField('purchaseDate', e.target.value)}
              required
            />
          </div>
        </div>
      )}
      {!showCodeField && (
        <div className="grid gap-2">
          <Label htmlFor={`${idPrefix}-date`}>Purchase Date</Label>
          <Input
            id={`${idPrefix}-date`}
            type="date"
            value={data.purchaseDate}
            onChange={(e) => updateField('purchaseDate', e.target.value)}
            required
          />
        </div>
      )}
      <div className="grid gap-2">
        <Label htmlFor={`${idPrefix}-account`}>Account (optional)</Label>
        <Input
          id={`${idPrefix}-account`}
          value={data.account}
          onChange={(e) => updateField('account', e.target.value)}
          placeholder="NISA, iDeCo, etc."
        />
      </div>
      <div className="grid gap-2">
        <Label htmlFor={`${idPrefix}-notes`}>Notes (optional)</Label>
        <Input
          id={`${idPrefix}-notes`}
          value={data.notes}
          onChange={(e) => updateField('notes', e.target.value)}
          placeholder="Additional notes"
        />
      </div>
    </div>
  );
}

export function validateStockForm(data: StockFormData, requireCode = true): boolean {
  const parsedQuantity = Number.parseInt(data.quantity, 10);
  const parsedPrice = Number.parseFloat(data.purchasePrice);
  const codeValid = requireCode ? /^\d{4}$/.test((data.code || '').trim()) : true;

  return (
    codeValid &&
    !Number.isNaN(parsedQuantity) &&
    parsedQuantity >= 100 &&
    !Number.isNaN(parsedPrice) &&
    parsedPrice > 0 &&
    data.purchaseDate.length > 0
  );
}
