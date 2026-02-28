import { Database, Loader2 } from 'lucide-react';
import { useState } from 'react';
import { SyncDataPlaneOptions } from '@/components/Settings/SyncDataPlaneOptions';
import { SyncModeSelect } from '@/components/Settings/SyncModeSelect';
import { SyncStatusCard } from '@/components/Settings/SyncStatusCard';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { useCancelSync, useStartSync, useSyncJobStatus } from '@/hooks/useDbSync';
import type { StartSyncRequest, SyncDataBackend, SyncMode } from '@/types/sync';

export function SettingsPage() {
  const [syncMode, setSyncMode] = useState<SyncMode>('auto');
  const [dataBackend, setDataBackend] = useState<SyncDataBackend>('duckdb-parquet');
  const [sqliteMirror, setSqliteMirror] = useState(false);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);

  const startSync = useStartSync();
  const { data: jobStatus, isLoading: isPolling } = useSyncJobStatus(activeJobId);
  const cancelSync = useCancelSync();

  const isRunning = jobStatus?.status === 'pending' || jobStatus?.status === 'running';

  const handleStartSync = () => {
    const request: StartSyncRequest = { mode: syncMode };
    if (dataBackend === 'duckdb-parquet') {
      request.dataPlane = { backend: dataBackend, sqliteMirror };
    } else if (dataBackend === 'sqlite') {
      request.dataPlane = { backend: dataBackend };
    }

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
          <SyncDataPlaneOptions
            backend={dataBackend}
            sqliteMirror={sqliteMirror}
            onBackendChange={setDataBackend}
            onSqliteMirrorChange={setSqliteMirror}
            disabled={isRunning || startSync.isPending}
          />

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
