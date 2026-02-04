import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';

export interface MarketOption {
  value: string;
  label: string;
}

const DEFAULT_MARKET_OPTIONS: MarketOption[] = [
  { value: 'prime', label: 'Prime' },
  { value: 'standard', label: 'Standard' },
  { value: 'growth', label: 'Growth' },
  { value: 'prime,standard', label: 'Prime + Standard' },
  { value: 'prime,standard,growth', label: 'All Markets' },
];

interface MarketsSelectProps {
  value: string;
  onChange: (value: string) => void;
  options?: MarketOption[];
  id?: string;
  label?: string;
}

export function MarketsSelect({
  value,
  onChange,
  options = DEFAULT_MARKET_OPTIONS,
  id = 'markets',
  label = 'Markets',
}: MarketsSelectProps) {
  return (
    <div className="space-y-2">
      <Label htmlFor={id} className="text-xs">
        {label}
      </Label>
      <Select value={value} onValueChange={onChange}>
        <SelectTrigger id={id} className="h-8 text-xs">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {options.map((opt) => (
            <SelectItem key={opt.value} value={opt.value}>
              {opt.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}
