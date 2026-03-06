import { Database, Loader2, RotateCcw } from 'lucide-react';
import { useEffect, useState } from 'react';
import { SyncModeSelect } from '@/components/Settings/SyncModeSelect';
import { SyncStatusCard } from '@/components/Settings/SyncStatusCard';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import {
  useActiveSyncJob,
  useCancelSync,
  useDbStats,
  useDbValidation,
  useRefreshStocks,
  useStartSync,
  useSyncFetchDetails,
  useSyncJobStatus,
} from '@/hooks/useDbSync';
import { ApiError } from '@/lib/api-client';
import type {
  MarketRefreshResponse,
  MarketStatsResponse,
  MarketValidationResponse,
  StartSyncRequest,
  SyncMode,
} from '@/types/sync';

function formatTimestamp(value?: string | null): string {
  if (!value) return 'n/a';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

const ACTIVE_SYNC_JOB_STORAGE_KEY = 'trading25.settings.sync.activeJobId';

function readPersistedActiveSyncJobId(): string | null {
  if (typeof window === 'undefined') {
    return null;
  }
  try {
    const value = window.localStorage.getItem(ACTIVE_SYNC_JOB_STORAGE_KEY);
    if (typeof value !== 'string') {
      return null;
    }
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  } catch {
    return null;
  }
}

function persistActiveSyncJobId(jobId: string | null): void {
  if (typeof window === 'undefined') {
    return;
  }
  try {
    if (jobId) {
      window.localStorage.setItem(ACTIVE_SYNC_JOB_STORAGE_KEY, jobId);
      return;
    }
    window.localStorage.removeItem(ACTIVE_SYNC_JOB_STORAGE_KEY);
  } catch {
    // localStorage can fail in restricted environments.
  }
}

interface SyncActionButtonProps {
  isRunning: boolean;
  isStarting: boolean;
  onClick: () => void;
}

function SyncActionButton({ isRunning, isStarting, onClick }: SyncActionButtonProps) {
  return (
    <Button onClick={onClick} disabled={isRunning || isStarting} className="w-full">
      {isStarting ? (
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
  );
}

interface SnapshotStatusProps {
  isStatsLoading: boolean;
  isValidationLoading: boolean;
  statsError: Error | null;
  validationError: Error | null;
  dbStats: MarketStatsResponse | undefined;
  dbValidation: MarketValidationResponse | undefined;
}

interface SnapshotItem {
  label: string;
  value: string | number;
}

interface RepairTargets {
  stocksNeedingRefresh: number;
  missingPrimeFundamentals: number;
  failedFundamentalsDates: number;
  failedFundamentalsCodes: number;
}

function getValidationDetailsTitle(status: MarketValidationResponse['status']): string {
  switch (status) {
    case 'warning':
      return 'Warning Details';
    case 'error':
      return 'Error Details';
    default:
      return 'Validation Notes';
  }
}

function getValidationDetailsClassName(status: MarketValidationResponse['status']): string {
  if (status === 'healthy') {
    return 'rounded-md border border-border bg-muted/30 p-3';
  }
  return 'rounded-md border border-yellow-500/30 bg-yellow-500/10 p-3';
}

function buildSnapshotItems(
  dbStats: MarketStatsResponse,
  dbValidation: MarketValidationResponse
): SnapshotItem[] {
  return [
    { label: 'Validation Status', value: dbValidation.status.toUpperCase() },
    { label: 'Time-Series Source', value: dbStats.timeSeriesSource },
    { label: 'Initialized', value: dbStats.initialized ? 'Yes' : 'No' },
    { label: 'Last Sync', value: formatTimestamp(dbStats.lastSync) },
    { label: 'Stock Data Latest', value: dbStats.stockData.dateRange?.max ?? 'n/a' },
    { label: 'TOPIX Latest', value: dbStats.topix.dateRange?.max ?? 'n/a' },
    { label: 'Indices Latest', value: dbStats.indices.dateRange?.max ?? 'n/a' },
    { label: 'Missing Stock Dates', value: dbValidation.stockData.missingDatesCount },
    { label: 'Failed Sync Dates', value: dbValidation.failedDatesCount },
    { label: 'Stocks Needing Refresh', value: dbValidation.stocksNeedingRefreshCount ?? 0 },
    { label: 'Missing Prime Fundamentals', value: dbValidation.fundamentals.missingPrimeStocksCount ?? 0 },
    { label: 'Readiness Issues', value: dbValidation.integrityIssuesCount ?? 0 },
  ];
}

function resolveRepairTargets(dbValidation: MarketValidationResponse | undefined): RepairTargets {
  return {
    stocksNeedingRefresh: dbValidation?.stocksNeedingRefreshCount ?? 0,
    missingPrimeFundamentals: dbValidation?.fundamentals.missingPrimeStocksCount ?? 0,
    failedFundamentalsDates: dbValidation?.fundamentals.failedDatesCount ?? 0,
    failedFundamentalsCodes: dbValidation?.fundamentals.failedCodesCount ?? 0,
  };
}

function hasRepairTargets(targets: RepairTargets): boolean {
  return (
    targets.stocksNeedingRefresh > 0 ||
    targets.missingPrimeFundamentals > 0 ||
    targets.failedFundamentalsDates > 0 ||
    targets.failedFundamentalsCodes > 0
  );
}

function SnapshotDetails({
  dbStats,
  dbValidation,
}: {
  dbStats: MarketStatsResponse;
  dbValidation: MarketValidationResponse;
}) {
  const recommendations = dbValidation.recommendations ?? [];
  const snapshotItems = buildSnapshotItems(dbStats, dbValidation);

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
        {snapshotItems.map((item) => (
          <div key={item.label}>
            <span className="text-muted-foreground">{item.label}:</span>
            <span className="ml-2 font-medium">{item.value}</span>
          </div>
        ))}
      </div>

      {recommendations.length > 0 ? (
        <div className={getValidationDetailsClassName(dbValidation.status)}>
          <p className="font-medium">{getValidationDetailsTitle(dbValidation.status)}</p>
          <ul className="mt-2 list-disc space-y-1 pl-5 text-sm text-muted-foreground">
            {recommendations.map((recommendation) => (
              <li key={recommendation}>{recommendation}</li>
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  );
}

function SnapshotStatus({
  isStatsLoading,
  isValidationLoading,
  statsError,
  validationError,
  dbStats,
  dbValidation,
}: SnapshotStatusProps) {
  if (isStatsLoading || isValidationLoading) {
    return (
      <div className="flex items-center gap-2 text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading market DB status...
      </div>
    );
  }

  const errorMessage = statsError?.message ?? validationError?.message ?? null;

  return (
    <>
      {errorMessage ? (
        <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">
          {errorMessage ?? 'Failed to load market DB status'}
        </div>
      ) : null}

      {dbStats && dbValidation ? <SnapshotDetails dbStats={dbStats} dbValidation={dbValidation} /> : null}
    </>
  );
}

function parseStockCodes(value: string): string[] {
  const tokens = value
    .split(/[,\s]+/)
    .map((token) => token.trim())
    .filter((token) => token.length > 0);
  const unique = new Set<string>();
  for (const token of tokens) {
    if (/^\d{4}$/.test(token)) {
      unique.add(token);
    }
  }
  return [...unique];
}

function RefreshResultTable({ result }: { result: MarketRefreshResponse }) {
  return (
    <div className="space-y-2 text-sm">
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-3">
        <div>
          <span className="text-muted-foreground">Total Stocks:</span>
          <span className="ml-2 font-medium">{result.totalStocks}</span>
        </div>
        <div>
          <span className="text-muted-foreground">Success:</span>
          <span className="ml-2 font-medium text-green-500">{result.successCount}</span>
        </div>
        <div>
          <span className="text-muted-foreground">Failed:</span>
          <span className="ml-2 font-medium text-red-500">{result.failedCount}</span>
        </div>
        <div>
          <span className="text-muted-foreground">API Calls:</span>
          <span className="ml-2 font-medium">{result.totalApiCalls}</span>
        </div>
        <div>
          <span className="text-muted-foreground">Records Stored:</span>
          <span className="ml-2 font-medium">{result.totalRecordsStored}</span>
        </div>
        <div>
          <span className="text-muted-foreground">Updated:</span>
          <span className="ml-2 font-medium">{formatTimestamp(result.lastUpdated)}</span>
        </div>
      </div>

      <div className="max-h-56 overflow-auto rounded-md border">
        <table className="w-full text-xs">
          <thead className="bg-muted/40 sticky top-0">
            <tr>
              <th className="px-2 py-1 text-left">Code</th>
              <th className="px-2 py-1 text-left">Status</th>
              <th className="px-2 py-1 text-right">Fetched</th>
              <th className="px-2 py-1 text-right">Stored</th>
              <th className="px-2 py-1 text-left">Error</th>
            </tr>
          </thead>
          <tbody>
            {result.results.map((item) => (
              <tr key={item.code} className="border-t">
                <td className="px-2 py-1 font-medium">{item.code}</td>
                <td className={`px-2 py-1 ${item.success ? 'text-green-500' : 'text-red-500'}`}>
                  {item.success ? 'ok' : 'failed'}
                </td>
                <td className="px-2 py-1 text-right">{item.recordsFetched}</td>
                <td className="px-2 py-1 text-right">{item.recordsStored}</td>
                <td className="px-2 py-1">{item.error ?? '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export function SettingsPage() {
  const [syncMode, setSyncMode] = useState<SyncMode>('auto');
  const [enforceBulkForStockData, setEnforceBulkForStockData] = useState(false);
  const [activeJobId, setActiveJobId] = useState<string | null>(readPersistedActiveSyncJobId);
  const [refreshCodesInput, setRefreshCodesInput] = useState('');
  const [refreshInputError, setRefreshInputError] = useState<string | null>(null);
  const [refreshResult, setRefreshResult] = useState<MarketRefreshResponse | null>(null);

  const startSync = useStartSync();
  const { data: activeSyncJob } = useActiveSyncJob();
  const { data: jobStatus, isLoading: isPolling, error: syncJobError } = useSyncJobStatus(activeJobId);
  const { data: syncFetchDetails } = useSyncFetchDetails(activeJobId);
  const cancelSync = useCancelSync();
  const refreshStocks = useRefreshStocks();

  useEffect(() => {
    if (!activeSyncJob?.jobId) {
      return;
    }
    setActiveJobId(activeSyncJob.jobId);
  }, [activeSyncJob?.jobId]);

  useEffect(() => {
    persistActiveSyncJobId(activeJobId);
  }, [activeJobId]);

  useEffect(() => {
    if (!(syncJobError instanceof ApiError) || syncJobError.status !== 404) {
      return;
    }
    setActiveJobId(null);
  }, [syncJobError]);

  const isJobStatusRunning = jobStatus?.status === 'pending' || jobStatus?.status === 'running';
  const isActiveJobRunning = activeSyncJob?.status === 'pending' || activeSyncJob?.status === 'running';
  const isRunning = startSync.isPending || isJobStatusRunning || (!jobStatus && isActiveJobRunning);
  const {
    data: dbStats,
    isLoading: isStatsLoading,
    error: statsError,
    refetch: refetchDbStats,
  } = useDbStats({ isSyncRunning: isRunning });
  const {
    data: dbValidation,
    isLoading: isValidationLoading,
    error: validationError,
    refetch: refetchDbValidation,
  } = useDbValidation({ isSyncRunning: isRunning });
  const repairTargets = resolveRepairTargets(dbValidation);
  const hasBulkRepairTargets = hasRepairTargets(repairTargets);

  useEffect(() => {
    if (!isRunning) {
      return;
    }
    void refetchDbStats();
    void refetchDbValidation();
  }, [isRunning, refetchDbStats, refetchDbValidation]);

  const handleStartSync = () => {
    const request: StartSyncRequest = { mode: syncMode, enforceBulkForStockData };
    startSync.mutate(request, {
      onSuccess: (data) => setActiveJobId(data.jobId),
    });
  };

  const handleRepairWarnings = () => {
    setRefreshResult(null);
    startSync.mutate(
      { mode: 'repair', enforceBulkForStockData: false },
      {
        onSuccess: (data) => setActiveJobId(data.jobId),
      }
    );
  };

  const handleCancel = () => {
    if (activeJobId) {
      cancelSync.mutate(activeJobId);
    }
  };

  const handleRefreshStocks = () => {
    setRefreshResult(null);
    const codes = parseStockCodes(refreshCodesInput);
    if (codes.length === 0) {
      setRefreshInputError('Enter at least one 4-digit stock code (comma/space/newline separated).');
      return;
    }
    if (codes.length > 50) {
      setRefreshInputError('Maximum 50 stock codes are allowed.');
      return;
    }

    setRefreshInputError(null);
    refreshStocks.mutate(
      { codes },
      {
        onSuccess: (data) => {
          setRefreshResult(data);
        },
      }
    );
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
          <div className="flex items-center justify-between rounded-md border p-3">
            <div className="space-y-1">
              <Label htmlFor="enforce-stock-bulk">Enforce BULK for stock_data</Label>
              <p className="text-xs text-muted-foreground">
                When enabled, stock_data sync fails if BULK is unavailable (no REST fallback).
              </p>
            </div>
            <Switch
              id="enforce-stock-bulk"
              checked={enforceBulkForStockData}
              onCheckedChange={setEnforceBulkForStockData}
              disabled={isRunning || startSync.isPending}
            />
          </div>

          <SyncActionButton isRunning={isRunning} isStarting={startSync.isPending} onClick={handleStartSync} />

          {startSync.error && (
            <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{startSync.error.message}</div>
          )}
        </CardContent>
      </Card>

      <Card className="mt-4">
        <CardHeader>
          <div className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            <CardTitle>Warning Recovery</CardTitle>
          </div>
          <CardDescription>
            Resolve DuckDB snapshot warnings in one async job without entering individual stock codes.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
            <div>
              <span className="text-muted-foreground">Stocks needing refresh:</span>
              <span className="ml-2 font-medium">{repairTargets.stocksNeedingRefresh}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Missing Prime fundamentals:</span>
              <span className="ml-2 font-medium">{repairTargets.missingPrimeFundamentals}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Failed fundamentals dates:</span>
              <span className="ml-2 font-medium">{repairTargets.failedFundamentalsDates}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Failed fundamentals codes:</span>
              <span className="ml-2 font-medium">{repairTargets.failedFundamentalsCodes}</span>
            </div>
          </div>
          <p className="text-xs text-muted-foreground">
            Runs `repair` sync mode to bulk-refresh adjustment-affected stock series and backfill Prime fundamentals.
          </p>
          <Button onClick={handleRepairWarnings} disabled={isRunning || startSync.isPending || !hasBulkRepairTargets} className="w-full">
            {startSync.isPending || isRunning ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Repair Job in Progress...
              </>
            ) : hasBulkRepairTargets ? (
              'Repair Warnings'
            ) : (
              'No Repairs Needed'
            )}
          </Button>
          {!hasBulkRepairTargets && !isValidationLoading ? (
            <p className="text-xs text-muted-foreground">No bulk warning-repair targets were found in the latest snapshot.</p>
          ) : null}
        </CardContent>
      </Card>

      <Card className="mt-4">
        <CardHeader>
          <div className="flex items-center gap-2">
            <RotateCcw className="h-5 w-5" />
            <CardTitle>Stock Refresh (Manual)</CardTitle>
          </div>
          <CardDescription>
            Re-fetch specific DuckDB stock series when you need a one-off repair outside the bulk warning flow.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <Input
            placeholder="e.g. 7203, 6758, 9984"
            value={refreshCodesInput}
            onChange={(e) => {
              setRefreshCodesInput(e.target.value);
              setRefreshInputError(null);
            }}
            disabled={refreshStocks.isPending}
          />
          <p className="text-xs text-muted-foreground">
            Accepts comma/space/newline separated 4-digit codes, up to 50 at once.
          </p>
          <Button onClick={handleRefreshStocks} disabled={refreshStocks.isPending} className="w-full">
            {refreshStocks.isPending ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Refreshing...
              </>
            ) : (
              'Refresh Stocks'
            )}
          </Button>

          {refreshInputError && <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{refreshInputError}</div>}
          {refreshStocks.error && (
            <div className="rounded-md bg-red-500/10 p-3 text-sm text-red-500">{refreshStocks.error.message}</div>
          )}
          {refreshResult && <RefreshResultTable result={refreshResult} />}
        </CardContent>
      </Card>

      <Card className="mt-4">
        <CardHeader>
          <CardTitle>DuckDB Snapshot</CardTitle>
          <CardDescription>Current DuckDB SoT status from FastAPI (`/api/db/stats`, `/api/db/validate`).</CardDescription>
        </CardHeader>
        <CardContent className="space-y-3 text-sm">
          <SnapshotStatus
            isStatsLoading={isStatsLoading}
            isValidationLoading={isValidationLoading}
            statsError={statsError}
            validationError={validationError}
            dbStats={dbStats}
            dbValidation={dbValidation}
          />
        </CardContent>
      </Card>

      {/* Sync Status */}
      <SyncStatusCard
        job={jobStatus}
        fetchDetails={syncFetchDetails}
        isLoading={isPolling}
        onCancel={handleCancel}
        isCancelling={cancelSync.isPending}
      />
    </div>
  );
}
