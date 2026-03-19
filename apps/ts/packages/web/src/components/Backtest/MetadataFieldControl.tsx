import { useId } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { Textarea } from '@/components/ui/textarea';
import { cn } from '@/lib/utils';
import type { AuthoringFieldSchema } from '@/types/backtest';
import { formatConstraints } from './signalConstraints';

interface MetadataFieldControlProps {
  field: AuthoringFieldSchema;
  value: unknown;
  effectiveValue?: unknown;
  overridden?: boolean;
  disabled?: boolean;
  optionValues?: string[];
  onChange: (value: unknown) => void;
  onReset?: () => void;
}

function formatValueLabel(value: unknown): string {
  if (value == null || value === '') return 'unset';
  if (typeof value === 'boolean') return value ? 'on' : 'off';
  if (Array.isArray(value)) return value.join(', ');
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

function normalizeStringList(value: string): string[] {
  return value
    .split(/[\n,]/)
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

function MetadataFieldHeader({
  field,
  overridden,
  effectiveValue,
}: {
  field: AuthoringFieldSchema;
  overridden?: boolean;
  effectiveValue?: unknown;
}) {
  const constraintParts = formatConstraints(field.constraints);

  return (
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div className="space-y-1">
        <div className="flex flex-wrap items-center gap-2">
          <div className="text-sm font-medium">{field.label}</div>
          {overridden === true ? (
            <span className="rounded bg-emerald-500/10 px-2 py-0.5 text-[11px] font-medium text-emerald-600">
              Overridden
            </span>
          ) : null}
          {overridden === false ? (
            <span className="rounded bg-slate-500/10 px-2 py-0.5 text-[11px] font-medium text-slate-600">
              Inherited
            </span>
          ) : null}
        </div>
        {field.summary ? <p className="text-xs text-foreground/90">{field.summary}</p> : null}
        {field.description ? <p className="text-xs text-muted-foreground">{field.description}</p> : null}
        <div className="flex flex-wrap gap-2 text-[11px] text-muted-foreground">
          {overridden === false ? <span>Effective: {formatValueLabel(effectiveValue)}</span> : null}
          {field.unit ? <span>Unit: {field.unit}</span> : null}
          {constraintParts.length > 0 ? <span>{constraintParts.join(', ')}</span> : null}
        </div>
      </div>
    </div>
  );
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: widget dispatch stays centralized so shared metadata fields render consistently across editors.
function MetadataFieldInput({
  field,
  value,
  disabled,
  optionValues,
  onChange,
}: Omit<MetadataFieldControlProps, 'effectiveValue' | 'overridden' | 'onReset'>) {
  const datalistId = useId();
  const inputId = useId();
  const options = optionValues ?? field.options ?? [];
  const stringValue = typeof value === 'string' ? value : value == null ? '' : String(value);

  switch (field.widget) {
    case 'switch':
      return (
        <div className="rounded-md border border-border/60 px-3 py-2">
          <div className="flex items-center justify-between gap-3">
            <Label htmlFor={inputId} className="text-sm">
              {field.label}
            </Label>
            <Switch id={inputId} checked={Boolean(value)} onCheckedChange={onChange} disabled={disabled} />
          </div>
        </div>
      );
    case 'textarea':
      return (
        <>
          <Label htmlFor={inputId} className="sr-only">
            {field.label}
          </Label>
          <Textarea
            id={inputId}
            value={stringValue}
            placeholder={field.placeholder ?? undefined}
            disabled={disabled}
            onChange={(event) => onChange(event.target.value)}
          />
        </>
      );
    case 'number':
      return (
        <>
          <Label htmlFor={inputId} className="sr-only">
            {field.label}
          </Label>
          <Input
            id={inputId}
            type="number"
            value={value == null ? '' : String(value)}
            placeholder={field.placeholder ?? undefined}
            disabled={disabled}
            onChange={(event) => {
              const nextValue = event.target.value.trim();
              onChange(nextValue === '' ? null : Number(nextValue));
            }}
          />
        </>
      );
    case 'select':
      return (
        <Select value={stringValue} onValueChange={onChange} disabled={disabled}>
          <SelectTrigger aria-label={field.label}>
            <SelectValue placeholder={field.placeholder ?? 'Select value'} />
          </SelectTrigger>
          <SelectContent>
            {options.map((option) => (
              <SelectItem key={option} value={option}>
                {option}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      );
    case 'combobox':
      return (
        <div className="space-y-1.5">
          <Label htmlFor={inputId} className="sr-only">
            {field.label}
          </Label>
          <Input
            id={inputId}
            list={datalistId}
            value={stringValue}
            placeholder={field.placeholder ?? undefined}
            disabled={disabled}
            onChange={(event) => onChange(event.target.value)}
          />
          <datalist id={datalistId}>
            {options.map((option) => (
              <option key={option} value={option} />
            ))}
          </datalist>
        </div>
      );
    case 'string_list':
      return (
        <>
          <Label htmlFor={inputId} className="sr-only">
            {field.label}
          </Label>
          <Textarea
            id={inputId}
            value={Array.isArray(value) ? value.join('\n') : typeof value === 'string' ? value : ''}
            placeholder={field.placeholder ?? 'One value per line'}
            disabled={disabled}
            onChange={(event) => onChange(normalizeStringList(event.target.value))}
          />
        </>
      );
    default:
      return (
        <>
          <Label htmlFor={inputId} className="sr-only">
            {field.label}
          </Label>
          <Input
            id={inputId}
            value={stringValue}
            placeholder={field.placeholder ?? undefined}
            disabled={disabled}
            onChange={(event) => onChange(event.target.value)}
          />
        </>
      );
  }
}

export function MetadataFieldControl({
  field,
  value,
  effectiveValue,
  overridden,
  disabled,
  optionValues,
  onChange,
  onReset,
}: MetadataFieldControlProps) {
  const inheritedValue = effectiveValue ?? value;

  return (
    <div className="rounded-lg border border-border/60 bg-background/70 p-3">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <MetadataFieldHeader field={field} overridden={overridden} effectiveValue={inheritedValue} />
        {onReset ? (
          <Button
            variant="outline"
            size="sm"
            className="h-8"
            onClick={onReset}
            disabled={disabled || overridden !== true}
          >
            Reset
          </Button>
        ) : null}
      </div>

      <div className="mt-3">
        <MetadataFieldInput
          field={field}
          value={value}
          disabled={disabled}
          optionValues={optionValues}
          onChange={onChange}
        />
      </div>

      {field.examples && field.examples.length > 0 ? (
        <div className={cn('mt-2 text-[11px] text-muted-foreground')}>Example: {field.examples.join(' | ')}</div>
      ) : null}
    </div>
  );
}
