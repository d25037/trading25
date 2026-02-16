import { type ReactNode, useId } from 'react';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';

interface IndicatorToggleProps {
  label: string;
  enabled: boolean;
  onToggle: (enabled: boolean) => void;
  children?: ReactNode;
  disabled?: boolean;
  meta?: string;
}

/**
 * Reusable indicator toggle component with collapsible settings
 */
export function IndicatorToggle({ label, enabled, onToggle, children, disabled, meta }: IndicatorToggleProps) {
  const switchId = useId();

  return (
    <div className="space-y-1.5 p-2 rounded glass-panel">
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <Label htmlFor={switchId} className="text-xs font-medium cursor-pointer">
            {label}
          </Label>
          {meta && <p className="text-[10px] text-muted-foreground leading-tight">{meta}</p>}
        </div>
        <Switch id={switchId} checked={enabled} onCheckedChange={onToggle} disabled={disabled} />
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
  const inputId = useId();

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const parsed = step ? Number.parseFloat(e.target.value) : Number.parseInt(e.target.value, 10);
    onChange(Number.isNaN(parsed) ? defaultValue : parsed);
  };

  return (
    <div>
      <Label htmlFor={inputId} className="text-xs text-muted-foreground">
        {label}
      </Label>
      <Input id={inputId} type="number" step={step} value={value} onChange={handleChange} className="h-7 text-xs" disabled={disabled} />
    </div>
  );
}
