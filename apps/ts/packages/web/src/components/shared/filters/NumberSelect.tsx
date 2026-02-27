import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';

export interface NumberOption {
  value: number;
  label: string;
}

interface NumberSelectProps {
  value: number;
  onChange: (value: number) => void;
  options: NumberOption[];
  id: string;
  label: string;
  disabled?: boolean;
  description?: string;
}

export function NumberSelect({
  value,
  onChange,
  options,
  id,
  label,
  disabled = false,
  description,
}: NumberSelectProps) {
  return (
    <div className="space-y-2">
      <Label htmlFor={id} className="text-xs">
        {label}
      </Label>
      <Select value={String(value)} onValueChange={(v) => onChange(Number(v))} disabled={disabled}>
        <SelectTrigger id={id} className="h-8 text-xs">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {options.map((opt) => (
            <SelectItem key={opt.value} value={String(opt.value)}>
              {opt.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      {description ? <p className="text-xs text-muted-foreground">{description}</p> : null}
    </div>
  );
}
