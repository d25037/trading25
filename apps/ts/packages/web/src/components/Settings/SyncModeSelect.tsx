import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import type { SyncMode } from '@/types/sync';

interface SyncModeSelectProps {
  value: SyncMode;
  onChange: (value: SyncMode) => void;
  disabled?: boolean;
}

const SYNC_MODE_OPTIONS: { value: SyncMode; label: string; description: string }[] = [
  { value: 'auto', label: 'Auto', description: 'Detect initial/incremental from DuckDB SoT state' },
  {
    value: 'initial',
    label: 'Initial',
    description: 'Full bootstrap of the local DuckDB snapshot; optional reset is configured below',
  },
  { value: 'incremental', label: 'Incremental', description: 'Backfill missing dates and append latest market data' },
  {
    value: 'indices-only',
    label: 'Indices Only',
    description: 'Resync index series plus synthetic Nikkei UnderPx data',
  },
  {
    value: 'repair',
    label: 'Repair Warnings',
    description: 'Backfill listed-market fundamentals and other non-price warnings',
  },
];

export function SyncModeSelect({ value, onChange, disabled }: SyncModeSelectProps) {
  return (
    <div className="space-y-2">
      <Label htmlFor="sync-mode">Sync Mode</Label>
      <Select value={value} onValueChange={(v) => onChange(v as SyncMode)} disabled={disabled}>
        <SelectTrigger id="sync-mode" className="w-full">
          <SelectValue placeholder="Select sync mode" />
        </SelectTrigger>
        <SelectContent>
          {SYNC_MODE_OPTIONS.map((option) => (
            <SelectItem key={option.value} value={option.value}>
              <div className="flex flex-col">
                <span>{option.label}</span>
                <span className="text-xs text-muted-foreground">{option.description}</span>
              </div>
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}
