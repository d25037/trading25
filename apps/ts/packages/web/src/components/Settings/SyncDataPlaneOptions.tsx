import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import type { SyncDataBackend } from '@/types/sync';

interface SyncDataPlaneOptionsProps {
  backend: SyncDataBackend;
  sqliteMirror: boolean;
  onBackendChange: (value: SyncDataBackend) => void;
  onSqliteMirrorChange: (value: boolean) => void;
  disabled?: boolean;
}

const DATA_BACKEND_OPTIONS: { value: SyncDataBackend; label: string; description: string }[] = [
  { value: 'default', label: 'Server Default', description: 'Use backend from backend env settings' },
  { value: 'duckdb-parquet', label: 'DuckDB + Parquet', description: 'Phase 2 data plane backend' },
  { value: 'sqlite', label: 'SQLite (Legacy)', description: 'Write to sqlite market.db only' },
];

export function SyncDataPlaneOptions({
  backend,
  sqliteMirror,
  onBackendChange,
  onSqliteMirrorChange,
  disabled,
}: SyncDataPlaneOptionsProps) {
  const showSqliteMirror = backend !== 'sqlite';
  const sqliteMirrorDisabled = disabled || backend === 'default';

  return (
    <div className="space-y-4 rounded-md border p-4">
      <div className="space-y-2">
        <Label htmlFor="sync-data-backend">Data Backend</Label>
        <Select value={backend} onValueChange={(value) => onBackendChange(value as SyncDataBackend)} disabled={disabled}>
          <SelectTrigger id="sync-data-backend" aria-label="Data Backend" className="w-full">
            <SelectValue placeholder="Select data backend" />
          </SelectTrigger>
          <SelectContent>
            {DATA_BACKEND_OPTIONS.map((option) => (
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

      {showSqliteMirror && (
        <div className="flex items-start justify-between gap-3">
          <div className="space-y-1">
            <Label htmlFor="sync-sqlite-mirror">SQLite Mirror</Label>
            <p className="text-xs text-muted-foreground">
              {backend === 'default'
                ? 'Server default is used unless backend is explicitly overridden.'
                : 'Also write synced rows to sqlite market.db for compatibility.'}
            </p>
          </div>
          <Switch
            id="sync-sqlite-mirror"
            checked={sqliteMirror}
            onCheckedChange={onSqliteMirrorChange}
            disabled={sqliteMirrorDisabled}
            aria-label="SQLite Mirror"
          />
        </div>
      )}
    </div>
  );
}
