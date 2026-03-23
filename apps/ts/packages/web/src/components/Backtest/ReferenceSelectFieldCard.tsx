import { useId } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Label } from '@/components/ui/label';
import type { AuthoringFieldSchema } from '@/types/backtest';

interface ReferenceSelectFieldCardProps {
  field: AuthoringFieldSchema;
  value: unknown;
  effectiveValue: unknown;
  overridden: boolean;
  optionValues: string[];
  chooserLabel: string;
  placeholderLabel: string;
  onChange: (value: string) => void;
  onReset: () => void;
}

export function ReferenceSelectFieldCard({
  field,
  value,
  effectiveValue,
  overridden,
  optionValues,
  chooserLabel,
  placeholderLabel,
  onChange,
  onReset,
}: ReferenceSelectFieldCardProps) {
  const selectId = useId();
  const currentValue = typeof value === 'string' ? value : '';
  const options = Array.from(new Set([...optionValues, currentValue].filter((item) => item.length > 0)));

  return (
    <Card key={field.path} className="border-border/60">
      <CardHeader className="pb-3">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="space-y-1">
            <div className="flex flex-wrap items-center gap-2">
              <CardTitle className="text-base">{field.label}</CardTitle>
              {overridden ? (
                <span className="rounded bg-emerald-500/10 px-2 py-0.5 text-[11px] font-medium text-emerald-600">
                  Overridden
                </span>
              ) : (
                <span className="rounded bg-slate-500/10 px-2 py-0.5 text-[11px] font-medium text-slate-600">
                  Inherited
                </span>
              )}
            </div>
            {field.summary ? <p className="text-xs text-foreground/90">{field.summary}</p> : null}
            {field.description ? <p className="text-xs text-muted-foreground">{field.description}</p> : null}
            {!overridden ? (
              <div className="text-[11px] text-muted-foreground">Effective: {String(effectiveValue ?? 'unset')}</div>
            ) : null}
          </div>
          <Button variant="outline" size="sm" className="h-8" onClick={onReset} disabled={!overridden}>
            Reset
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-2">
        <Label htmlFor={selectId} className="text-xs font-medium text-muted-foreground">
          {chooserLabel}
        </Label>
        <select
          id={selectId}
          aria-label={field.label}
          className="h-10 w-full rounded-md border border-input bg-background px-3 text-sm"
          value={currentValue}
          onChange={(event) => onChange(event.target.value)}
        >
          {currentValue.length === 0 ? <option value="">{field.placeholder ?? placeholderLabel}</option> : null}
          {options.map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
        <div className="text-[11px] text-muted-foreground">{options.length} option(s) available.</div>
      </CardContent>
    </Card>
  );
}
