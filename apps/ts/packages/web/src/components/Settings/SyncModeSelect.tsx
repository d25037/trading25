import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import type { SyncMode } from '@/types/sync';

interface SyncModeSelectProps {
  value: SyncMode;
  onChange: (value: SyncMode) => void;
  disabled?: boolean;
}

const SYNC_MODE_OPTIONS: { value: SyncMode; label: string; description: string }[] = [
  { value: 'auto', label: 'Auto', description: 'Detect based on database state' },
  { value: 'initial', label: 'Initial', description: 'Full sync (2 years, ~552 API calls)' },
  { value: 'incremental', label: 'Incremental', description: 'Update only new data' },
  { value: 'indices-only', label: 'Indices Only', description: 'Sync indices only (~52 API calls)' },
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
