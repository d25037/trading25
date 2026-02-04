import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';

interface DateInputProps {
  value: string | undefined;
  onChange: (value: string | undefined) => void;
  id?: string;
  label?: string;
}

export function DateInput({ value, onChange, id = 'date', label = 'Date (optional)' }: DateInputProps) {
  return (
    <div className="space-y-2">
      <Label htmlFor={id} className="text-xs">
        {label}
      </Label>
      <Input
        id={id}
        type="date"
        className="h-8 text-xs"
        value={value ?? ''}
        onChange={(e) => onChange(e.target.value || undefined)}
      />
    </div>
  );
}
