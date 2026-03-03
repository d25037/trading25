import { Database, Loader2 } from 'lucide-react';
import { useState } from 'react';
import { SyncModeSelect } from '@/components/Settings/SyncModeSelect';
import { SyncStatusCard } from '@/components/Settings/SyncStatusCard';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { useCancelSync, useDbStats, useDbValidation, useStartSync, useSyncJobStatus } from '@/hooks/useDbSync';
import type { StartSyncRequest, SyncMode } from '@/types/sync';

function formatTimestamp(value?: string | null): string {
  if (!value) return 'n/a';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

export function SettingsPage() {
  const [syncMode, setSyncMode] = useState<SyncMode>('auto');
  const [activeJobId, setActiveJobId] = useState<string | null>(null);

  const startSync = useStartSync();
  const { data: jobStatus, isLoading: isPolling } = useSyncJobStatus(activeJobId);
  const cancelSync = useCancelSync();
  const { data: dbStats, isLoading: isStatsLoading, error: statsError } = useDbStats();
  const { data: dbValidation, isLoading: isValidationLoading, error: validationError } = useDbValidation();

  const isRunning = jobStatus?.status === 'pending' || jobStatus?.status === 'running';

  const handleStartSync = () => {
    const request: StartSyncRequest = { mode: syncMode };
    startSync.mutate(request, {
      onSuccess: (data) => setActiveJobId(data.jobId),
    });
  };

  const handleCancel = () => {
    if (activeJobId) {
      cancelSync.mutate(activeJobId);
    }
  };

  return (
    <div className="p-6 max-w-2xl mx-auto">
      <h2 className="text-2xl font-bold mb-6">Settings</h2>

      {/* Database Sync Section */}
      <Card>
        <CardHeader>
          <div className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            <CardTitle>Database Sync</CardTitle>
          </div>
          <CardDescription>
            Synchronize J-Quants market data into DuckDB SoT. Use incremental to resume interrupted syncs.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <SyncModeSelect value={syncMode} onChange={setSyncMode} disabled={isRunning || startSync.isPending} />

          <Button onClick={handleStartSync} disabled={isRunning || startSync.isPending} className="w-full">
            {startSync.isPending ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Starting...
              </>
            ) : isRunning ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Sync in Progress...
              </>
            ) : (
              'Start Sync'
            )}
          </Button>

          {startSync.error && (
            <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{startSync.error.message}</div>
          )}
        </CardContent>
      </Card>

      <Card className="mt-4">
        <CardHeader>
          <CardTitle>DuckDB Snapshot</CardTitle>
          <CardDescription>Current DuckDB SoT status from FastAPI (`/api/db/stats`, `/api/db/validate`).</CardDescription>
        </CardHeader>
        <CardContent className="space-y-3 text-sm">
          {(isStatsLoading || isValidationLoading) && (
            <div className="flex items-center gap-2 text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              Loading market DB status...
            </div>
          )}

          {!isStatsLoading && !isValidationLoading && (
            <>
              {(statsError || validationError) && (
                <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">
                  {(statsError || validationError)?.message ?? 'Failed to load market DB status'}
                </div>
              )}

              {dbStats && dbValidation && (
                <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                  <div>
                    <span className="text-muted-foreground">Validation Status:</span>
                    <span className="ml-2 font-medium uppercase">{dbValidation.status}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Time-Series Source:</span>
                    <span className="ml-2 font-medium">{dbStats.timeSeriesSource}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Initialized:</span>
                    <span className="ml-2 font-medium">{dbStats.initialized ? 'Yes' : 'No'}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Last Sync:</span>
                    <span className="ml-2 font-medium">{formatTimestamp(dbStats.lastSync)}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Stock Data Latest:</span>
                    <span className="ml-2 font-medium">{dbStats.stockData.dateRange?.max ?? 'n/a'}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground">TOPIX Latest:</span>
                    <span className="ml-2 font-medium">{dbStats.topix.dateRange?.max ?? 'n/a'}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Indices Latest:</span>
                    <span className="ml-2 font-medium">{dbStats.indices.dateRange?.max ?? 'n/a'}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Missing Stock Dates:</span>
                    <span className="ml-2 font-medium">{dbValidation.stockData.missingDatesCount}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Failed Sync Dates:</span>
                    <span className="ml-2 font-medium">{dbValidation.failedDatesCount}</span>
                  </div>
                </div>
              )}
            </>
          )}
        </CardContent>
      </Card>

      {/* Sync Status */}
      <SyncStatusCard
        job={jobStatus}
        isLoading={isPolling}
        onCancel={handleCancel}
        isCancelling={cancelSync.isPending}
      />
    </div>
  );
}
