import { useId } from 'react';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { cn } from '@/lib/utils';
import type { SignalDefinition, SignalFieldDefinition } from '@/types/backtest';
import { formatConstraints } from './signalConstraints';

type PrimitiveValue = boolean | number | string;
type EditableSignalValue = PrimitiveValue | null;

interface DefaultSignalParamsOptions {
  excludeFields?: string[];
}

interface SignalFieldInputsProps {
  fields: SignalFieldDefinition[];
  values: Record<string, unknown>;
  onFieldChange: (field: SignalFieldDefinition, value: EditableSignalValue) => void;
  disabled?: boolean;
  excludeFields?: string[];
  compact?: boolean;
  columns?: 1 | 2;
  showDescriptions?: boolean;
}

function toPrimitiveDefault(field: SignalFieldDefinition): PrimitiveValue {
  if (typeof field.default === 'boolean' || typeof field.default === 'number' || typeof field.default === 'string') {
    return field.default;
  }
  switch (field.type) {
    case 'boolean':
      return false;
    case 'number':
      return 0;
    default:
      return '';
  }
}

function toDisplayedValue(field: SignalFieldDefinition, value: unknown): PrimitiveValue {
  if (typeof value === 'boolean' || typeof value === 'number' || typeof value === 'string') {
    return value;
  }
  return toPrimitiveDefault(field);
}

function isFloatField(field: SignalFieldDefinition): boolean {
  return typeof field.default === 'number' && !Number.isInteger(field.default);
}

function buildHelperText(field: SignalFieldDefinition, showDescriptions?: boolean): string | null {
  const description = field.description.trim();
  const constraintParts = formatConstraints(field.constraints);
  const helperText = [
    showDescriptions && description ? description : null,
    field.unit ? `Unit: ${field.unit}` : null,
    constraintParts.length > 0 ? constraintParts.join(', ') : null,
  ]
    .filter((part): part is string => Boolean(part))
    .join(' | ');

  return helperText || null;
}

function SignalFieldText({
  label,
  helperText,
  compact,
}: {
  label: string;
  helperText: string | null;
  compact?: boolean;
}) {
  return (
    <>
      <div className={cn('font-medium', compact ? 'text-[11px]' : 'text-sm')}>{label}</div>
      {helperText ? <p className="text-[11px] text-muted-foreground">{helperText}</p> : null}
    </>
  );
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: signal field widget dispatch is shared between chart overlays and authoring cards.
function SignalFieldValueInput({
  field,
  value,
  onChange,
  disabled,
  compact,
}: {
  field: SignalFieldDefinition;
  value: unknown;
  onChange: (value: EditableSignalValue) => void;
  disabled?: boolean;
  compact?: boolean;
}) {
  const datalistId = useId();
  const inputId = useId();
  const className = compact ? 'h-8 text-xs' : 'h-9 text-sm';
  const displayedValue = value == null ? '' : String(toDisplayedValue(field, value));

  if (field.type === 'boolean') {
    return (
      <div className="flex items-center justify-between gap-3 rounded-md border border-border/60 px-3 py-2">
        <Label htmlFor={inputId} className={cn('font-medium', compact ? 'text-xs' : 'text-sm')}>
          {field.label ?? field.name}
        </Label>
        <Switch id={inputId} checked={Boolean(value)} onCheckedChange={onChange} disabled={disabled} />
      </div>
    );
  }

  if (field.type === 'select' && field.options && field.options.length > 0) {
    return (
      <Select value={displayedValue} onValueChange={onChange} disabled={disabled}>
        <SelectTrigger className={className} aria-label={field.label ?? field.name}>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {field.options.map((option) => (
            <SelectItem key={option} value={option} className={compact ? 'text-xs' : 'text-sm'}>
              {option}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    );
  }

  if (field.type === 'number') {
    return (
      <>
        <Label htmlFor={inputId} className="sr-only">
          {field.label ?? field.name}
        </Label>
        <Input
          id={inputId}
          type="number"
          value={displayedValue}
          step={isFloatField(field) ? '0.1' : '1'}
          placeholder={field.placeholder ?? undefined}
          className={className}
          disabled={disabled}
          onChange={(event) => {
            const nextValue = event.target.value;
            if (nextValue.trim() === '') {
              onChange(null);
              return;
            }
            const parsed = Number(nextValue);
            if (Number.isFinite(parsed)) {
              onChange(parsed);
            }
          }}
        />
      </>
    );
  }

  if (field.options && field.options.length > 0) {
    return (
      <>
        <Label htmlFor={inputId} className="sr-only">
          {field.label ?? field.name}
        </Label>
        <Input
          id={inputId}
          list={datalistId}
          value={displayedValue}
          placeholder={field.placeholder ?? undefined}
          className={className}
          disabled={disabled}
          onChange={(event) => onChange(event.target.value)}
        />
        <datalist id={datalistId}>
          {field.options.map((option) => (
            <option key={option} value={option} />
          ))}
        </datalist>
      </>
    );
  }

  return (
    <>
      <Label htmlFor={inputId} className="sr-only">
        {field.label ?? field.name}
      </Label>
      <Input
        id={inputId}
        value={displayedValue}
        placeholder={field.placeholder ?? undefined}
        className={className}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
      />
    </>
  );
}

export function buildDefaultSignalParams(
  definition: SignalDefinition,
  options: DefaultSignalParamsOptions = {}
): Record<string, PrimitiveValue> {
  const excluded = new Set(options.excludeFields ?? []);
  return Object.fromEntries(
    definition.fields
      .filter((field) => !excluded.has(field.name))
      .map((field) => [field.name, toPrimitiveDefault(field)])
  );
}

function SignalFieldControl({
  field,
  value,
  onChange,
  disabled,
  compact,
  showDescriptions,
}: {
  field: SignalFieldDefinition;
  value: unknown;
  onChange: (value: EditableSignalValue) => void;
  disabled?: boolean;
  compact?: boolean;
  showDescriptions?: boolean;
}) {
  const label = field.label ?? field.name;
  const helperText = buildHelperText(field, showDescriptions);

  return (
    <div className="space-y-1.5">
      {field.type === 'boolean' ? null : <SignalFieldText label={label} helperText={helperText} compact={compact} />}
      <SignalFieldValueInput field={field} value={value} onChange={onChange} disabled={disabled} compact={compact} />
      {field.type === 'boolean' && helperText ? (
        <p className="text-[11px] text-muted-foreground">{helperText}</p>
      ) : null}
    </div>
  );
}

export function SignalFieldInputs({
  fields,
  values,
  onFieldChange,
  disabled,
  excludeFields,
  compact,
  columns = 2,
  showDescriptions = true,
}: SignalFieldInputsProps) {
  const excluded = new Set(excludeFields ?? []);
  const visibleFields = fields.filter((field) => !excluded.has(field.name));

  if (visibleFields.length === 0) {
    return <div className="text-xs text-muted-foreground">No configurable parameters.</div>;
  }

  return (
    <div className={cn('grid gap-3', columns === 1 ? 'grid-cols-1' : 'grid-cols-1 lg:grid-cols-2')}>
      {visibleFields.map((field) => (
        <SignalFieldControl
          key={field.name}
          field={field}
          value={values[field.name] ?? field.default}
          disabled={disabled}
          compact={compact}
          showDescriptions={showDescriptions}
          onChange={(value) => onFieldChange(field, value)}
        />
      ))}
    </div>
  );
}
