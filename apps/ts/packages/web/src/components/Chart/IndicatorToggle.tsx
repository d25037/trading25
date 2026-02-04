import type { ReactNode } from 'react';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';

interface IndicatorToggleProps {
  label: string;
  enabled: boolean;
  onToggle: (enabled: boolean) => void;
  children?: ReactNode;
  disabled?: boolean;
}

/**
 * Reusable indicator toggle component with collapsible settings
 */
export function IndicatorToggle({ label, enabled, onToggle, children, disabled }: IndicatorToggleProps) {
  return (
    <div className="space-y-1.5 p-2 rounded glass-panel">
      <div className="flex items-center justify-between">
        <Label className="text-xs font-medium cursor-pointer">{label}</Label>
        <Switch checked={enabled} onCheckedChange={onToggle} disabled={disabled} />
      </div>
      {enabled && children}
    </div>
  );
}

interface NumberInputProps {
  label: string;
  value: number;
  onChange: (value: number) => void;
  step?: string;
  defaultValue?: number;
  disabled?: boolean;
}

/**
 * Compact number input for indicator settings
 */
export function NumberInput({ label, value, onChange, step, defaultValue = 0, disabled }: NumberInputProps) {
  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const parsed = step ? Number.parseFloat(e.target.value) : Number.parseInt(e.target.value, 10);
    onChange(Number.isNaN(parsed) ? defaultValue : parsed);
  };

  return (
    <div>
      <Label className="text-xs text-muted-foreground">{label}</Label>
      <Input type="number" step={step} value={value} onChange={handleChange} className="h-7 text-xs" disabled={disabled} />
    </div>
  );
}
