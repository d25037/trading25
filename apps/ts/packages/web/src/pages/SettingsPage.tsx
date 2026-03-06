import { Activity, Database, Loader2, RotateCcw, Wrench } from 'lucide-react';
import { useEffect, useState } from 'react';
import { SyncModeSelect } from '@/components/Settings/SyncModeSelect';
import { SyncStatusCard } from '@/components/Settings/SyncStatusCard';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
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
import { cn } from '@/lib/utils';
import type {
  MarketRefreshResponse,
  MarketStatsResponse,
  MarketValidationResponse,
  StartSyncRequest,
  SyncJobResponse,
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

type StatusTone = 'neutral' | 'accent' | 'success' | 'warning' | 'danger';
type SyncJobStatusShape = Pick<SyncJobResponse, 'status'> | null | undefined;

function getToneClasses(tone: StatusTone): string {
  switch (tone) {
    case 'accent':
      return 'border-primary/20 bg-primary/10 text-primary';
    case 'success':
      return 'border-emerald-500/20 bg-emerald-500/10 text-emerald-700';
    case 'warning':
      return 'border-amber-500/20 bg-amber-500/10 text-amber-700';
    case 'danger':
      return 'border-red-500/20 bg-red-500/10 text-red-700';
    default:
      return 'border-border/70 bg-background/70 text-foreground';
  }
}

function getValidationTone(status: MarketValidationResponse['status'] | undefined): StatusTone {
  switch (status) {
    case 'healthy':
      return 'success';
    case 'warning':
      return 'warning';
    case 'error':
      return 'danger';
    default:
      return 'neutral';
  }
}

function getJobTone(status: SyncJobResponse['status'] | null | undefined): StatusTone {
  switch (status) {
    case 'pending':
    case 'running':
      return 'accent';
    case 'completed':
      return 'success';
    case 'failed':
      return 'danger';
    case 'cancelled':
      return 'warning';
    default:
      return 'neutral';
  }
}

function formatSyncJobLabel(job: SyncJobResponse | null): string {
  if (!job) {
    return 'IDLE';
  }
  return job.status.toUpperCase();
}

function isSyncJobRunning(job: SyncJobStatusShape): boolean {
  return job?.status === 'pending' || job?.status === 'running';
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

interface ValidationDiagnostic {
  label: string;
  value: number;
  helpText: string;
  sampleCodes?: string[];
}

interface RepairTargets {
  stocksNeedingRefresh: number;
  missingListedMarketFundamentals: number;
  failedFundamentalsDates: number;
  failedFundamentalsCodes: number;
}

const EMPTY_REPAIR_TARGETS: RepairTargets = {
  stocksNeedingRefresh: 0,
  missingListedMarketFundamentals: 0,
  failedFundamentalsDates: 0,
  failedFundamentalsCodes: 0,
};

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
    return 'rounded-xl border border-border/70 bg-background/60 p-4';
  }
  return 'rounded-xl border border-yellow-500/30 bg-yellow-500/10 p-4';
}

function buildSnapshotItems(dbStats: MarketStatsResponse, dbValidation: MarketValidationResponse): SnapshotItem[] {
  const fundamentals = dbValidation.fundamentals;
  const marginEmptySkippedCount = dbValidation.margin.emptySkippedCount ?? 0;
  const fundamentalsEmptySkippedCount = fundamentals.emptySkippedCount ?? 0;
  const issuerAliasCoveredCount = fundamentals.issuerAliasCoveredCount ?? 0;
  return [
    { label: 'Validation Status', value: dbValidation.status.toUpperCase() },
    { label: 'Time-Series Source', value: dbStats.timeSeriesSource },
    { label: 'Initialized', value: dbStats.initialized ? 'Yes' : 'No' },
    { label: 'Last Sync', value: formatTimestamp(dbStats.lastSync) },
    { label: 'Stock Data Latest', value: dbStats.stockData.dateRange?.max ?? 'n/a' },
    { label: 'TOPIX Latest', value: dbStats.topix.dateRange?.max ?? 'n/a' },
    { label: 'Indices Latest', value: dbStats.indices.dateRange?.max ?? 'n/a' },
    { label: 'Margin Latest', value: dbStats.margin.dateRange?.max ?? 'n/a' },
    { label: 'Margin Stocks', value: dbStats.margin.uniqueStockCount },
    { label: 'Margin Orphans', value: dbValidation.margin.orphanCount },
    { label: 'Missing Stock Dates', value: dbValidation.stockData.missingDatesCount },
    { label: 'Failed Sync Dates', value: dbValidation.failedDatesCount },
    { label: 'Stocks Needing Refresh', value: dbValidation.stocksNeedingRefreshCount ?? 0 },
    { label: 'Missing Listed-Market Fundamentals', value: fundamentals.missingListedMarketStocksCount },
    { label: 'Unsupported/Empty Fundamentals', value: fundamentalsEmptySkippedCount },
    { label: 'Preferred Alias Covered', value: issuerAliasCoveredCount },
    { label: 'Unsupported/Empty Margin Codes', value: marginEmptySkippedCount },
    { label: 'Readiness Issues', value: dbValidation.integrityIssuesCount ?? 0 },
  ];
}

function buildValidationDiagnostics(dbValidation: MarketValidationResponse): ValidationDiagnostic[] {
  const diagnostics: ValidationDiagnostic[] = [];
  const fundamentals = dbValidation.fundamentals;
  const margin = dbValidation.margin;
  const missingListedMarketStocks = fundamentals.missingListedMarketStocks ?? [];
  const fundamentalsEmptySkippedCount = fundamentals.emptySkippedCount ?? 0;
  const fundamentalsEmptySkippedCodes = fundamentals.emptySkippedCodes ?? [];
  const marginEmptySkippedCount = margin.emptySkippedCount ?? 0;
  const marginEmptySkippedCodes = margin.emptySkippedCodes ?? [];
  const issuerAliasCoveredCount = fundamentals.issuerAliasCoveredCount ?? 0;

  if (missingListedMarketStocks.length > 0) {
    diagnostics.push({
      label: 'Missing Listed-Market Fundamentals',
      value: fundamentals.missingListedMarketStocksCount,
      helpText: 'Repair sync will retry these listed-market issuers.',
      sampleCodes: missingListedMarketStocks,
    });
  }

  if (fundamentalsEmptySkippedCount > 0) {
    diagnostics.push({
      label: 'Unsupported/Empty Fundamentals',
      value: fundamentalsEmptySkippedCount,
      helpText: 'Suppressed until a newer disclosure frontier is available.',
      sampleCodes: fundamentalsEmptySkippedCodes,
    });
  }

  if (marginEmptySkippedCount > 0) {
    diagnostics.push({
      label: 'Unsupported/Empty Margin Codes',
      value: marginEmptySkippedCount,
      helpText: 'Suppressed until a newer margin frontier is available.',
      sampleCodes: marginEmptySkippedCodes,
    });
  }

  if (issuerAliasCoveredCount > 0) {
    diagnostics.push({
      label: 'Preferred Alias Covered',
      value: issuerAliasCoveredCount,
      helpText: 'Preferred-share listed codes already covered by parent issuer statements.',
    });
  }

  return diagnostics;
}

function resolveRepairTargets(dbValidation: MarketValidationResponse | undefined): RepairTargets {
  if (!dbValidation) {
    return EMPTY_REPAIR_TARGETS;
  }

  const fundamentals = dbValidation.fundamentals;

  return {
    stocksNeedingRefresh: dbValidation.stocksNeedingRefreshCount ?? 0,
    missingListedMarketFundamentals: fundamentals.missingListedMarketStocksCount ?? 0,
    failedFundamentalsDates: fundamentals.failedDatesCount ?? 0,
    failedFundamentalsCodes: fundamentals.failedCodesCount ?? 0,
  };
}

function hasRepairTargets(targets: RepairTargets): boolean {
  return (
    targets.stocksNeedingRefresh > 0 ||
    targets.missingListedMarketFundamentals > 0 ||
    targets.failedFundamentalsDates > 0 ||
    targets.failedFundamentalsCodes > 0
  );
}

function sumRepairTargets(targets: RepairTargets): number {
  return (
    targets.stocksNeedingRefresh +
    targets.missingListedMarketFundamentals +
    targets.failedFundamentalsDates +
    targets.failedFundamentalsCodes
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
  const validationDiagnostics = buildValidationDiagnostics(dbValidation);

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
        {snapshotItems.map((item) => (
          <div key={item.label} className="rounded-xl border border-border/70 bg-background/60 p-3">
            <p className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">{item.label}</p>
            <p className="mt-2 text-sm font-semibold text-foreground">{item.value}</p>
          </div>
        ))}
      </div>

      {validationDiagnostics.length > 0 ? (
        <div className="rounded-xl border border-border/70 bg-background/60 p-4">
          <p className="font-medium">Coverage Diagnostics</p>
          <div className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-2">
            {validationDiagnostics.map((diagnostic) => (
              <div key={diagnostic.label} className="rounded-xl border border-border/70 bg-card/80 p-3">
                <p className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">{diagnostic.label}</p>
                <p className="mt-2 text-lg font-semibold text-foreground">{diagnostic.value}</p>
                <p className="mt-2 text-xs text-muted-foreground">{diagnostic.helpText}</p>
                {diagnostic.sampleCodes && diagnostic.sampleCodes.length > 0 ? (
                  <p className="mt-2 text-xs text-muted-foreground">
                    Sample codes: {diagnostic.sampleCodes.join(', ')}
                  </p>
                ) : null}
              </div>
            ))}
          </div>
        </div>
      ) : null}

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
        <div className="rounded-xl bg-red-500/10 p-3 text-sm text-red-500">
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

function getRefreshCodesValidationError(codes: string[]): string | null {
  if (codes.length === 0) {
    return 'Enter at least one 4-digit stock code (comma/space/newline separated).';
  }
  if (codes.length > 50) {
    return 'Maximum 50 stock codes are allowed.';
  }
  return null;
}

function RefreshResultTable({ result }: { result: MarketRefreshResponse }) {
  return (
    <div className="space-y-3 text-sm">
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
        <div className="rounded-xl border border-border/70 bg-background/60 p-3">
          <span className="text-xs uppercase tracking-[0.18em] text-muted-foreground">Total Stocks</span>
          <p className="mt-2 text-lg font-semibold">{result.totalStocks}</p>
        </div>
        <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 p-3">
          <span className="text-xs uppercase tracking-[0.18em] text-emerald-700">Success</span>
          <p className="mt-2 text-lg font-semibold text-emerald-700">{result.successCount}</p>
        </div>
        <div className="rounded-xl border border-red-500/20 bg-red-500/10 p-3">
          <span className="text-xs uppercase tracking-[0.18em] text-red-700">Failed</span>
          <p className="mt-2 text-lg font-semibold text-red-700">{result.failedCount}</p>
        </div>
        <div className="rounded-xl border border-border/70 bg-background/60 p-3">
          <span className="text-xs uppercase tracking-[0.18em] text-muted-foreground">API Calls</span>
          <p className="mt-2 text-lg font-semibold">{result.totalApiCalls}</p>
        </div>
        <div className="rounded-xl border border-border/70 bg-background/60 p-3">
          <span className="text-xs uppercase tracking-[0.18em] text-muted-foreground">Records Stored</span>
          <p className="mt-2 text-lg font-semibold">{result.totalRecordsStored}</p>
        </div>
        <div className="rounded-xl border border-border/70 bg-background/60 p-3">
          <span className="text-xs uppercase tracking-[0.18em] text-muted-foreground">Updated</span>
          <p className="mt-2 text-sm font-semibold">{formatTimestamp(result.lastUpdated)}</p>
        </div>
      </div>

      <div className="max-h-56 overflow-auto rounded-xl border border-border/70">
        <table className="w-full text-xs">
          <thead className="sticky top-0 bg-muted/40">
            <tr>
              <th className="px-2 py-2 text-left">Code</th>
              <th className="px-2 py-2 text-left">Status</th>
              <th className="px-2 py-2 text-right">Fetched</th>
              <th className="px-2 py-2 text-right">Stored</th>
              <th className="px-2 py-2 text-left">Error</th>
            </tr>
          </thead>
          <tbody>
            {result.results.map((item) => (
              <tr key={item.code} className="border-t border-border/70">
                <td className="px-2 py-2 font-medium">{item.code}</td>
                <td className={`px-2 py-2 ${item.success ? 'text-green-500' : 'text-red-500'}`}>
                  {item.success ? 'ok' : 'failed'}
                </td>
                <td className="px-2 py-2 text-right">{item.recordsFetched}</td>
                <td className="px-2 py-2 text-right">{item.recordsStored}</td>
                <td className="px-2 py-2">{item.error ?? '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

interface DatabaseSyncSectionProps {
  syncMode: SyncMode;
  onSyncModeChange: (mode: SyncMode) => void;
  enforceBulkForStockData: boolean;
  onEnforceBulkChange: (checked: boolean) => void;
  isRunning: boolean;
  isStarting: boolean;
  onStartSync: () => void;
  errorMessage: string | null;
  className?: string;
}

function DatabaseSyncSection({
  syncMode,
  onSyncModeChange,
  enforceBulkForStockData,
  onEnforceBulkChange,
  isRunning,
  isStarting,
  onStartSync,
  errorMessage,
  className,
}: DatabaseSyncSectionProps) {
  return (
    <Card className={cn('border-border/70 bg-card/90 shadow-sm', className)}>
      <CardHeader className="pb-4">
        <span className="inline-flex w-fit rounded-full border border-primary/20 bg-primary/10 px-2.5 py-1 text-[11px] font-medium uppercase tracking-[0.18em] text-primary">
          Primary Action
        </span>
        <div className="flex items-center gap-2">
          <Database className="h-5 w-5" />
          <CardTitle className="text-xl">Database Sync</CardTitle>
        </div>
        <CardDescription>
          Synchronize J-Quants market data into the local DuckDB source of truth. Use incremental to resume interrupted
          syncs.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <SyncModeSelect value={syncMode} onChange={onSyncModeChange} disabled={isRunning || isStarting} />
        <div className="flex items-center justify-between gap-4 rounded-xl border border-border/70 bg-background/60 p-4">
          <div className="space-y-1">
            <Label htmlFor="enforce-stock-bulk">Enforce BULK for stock_data</Label>
            <p className="text-xs text-muted-foreground">
              When enabled, stock_data sync fails if BULK is unavailable and will not fall back to REST.
            </p>
          </div>
          <Switch
            id="enforce-stock-bulk"
            checked={enforceBulkForStockData}
            onCheckedChange={onEnforceBulkChange}
            disabled={isRunning || isStarting}
          />
        </div>

        <SyncActionButton isRunning={isRunning} isStarting={isStarting} onClick={onStartSync} />

        {errorMessage && <div className="rounded-xl bg-red-500/10 p-3 text-sm text-red-500">{errorMessage}</div>}
      </CardContent>
    </Card>
  );
}

interface WarningRecoverySectionProps {
  repairTargets: RepairTargets;
  isValidationLoading: boolean;
  isRunning: boolean;
  isStarting: boolean;
  onRepairWarnings: () => void;
  className?: string;
}

function WarningRecoverySection({
  repairTargets,
  isValidationLoading,
  isRunning,
  isStarting,
  onRepairWarnings,
  className,
}: WarningRecoverySectionProps) {
  const canRepair = hasRepairTargets(repairTargets);
  const repairSignals = sumRepairTargets(repairTargets);

  return (
    <Card className={cn('border-border/70 bg-card/90 shadow-sm', className)}>
      <CardHeader className="pb-4">
        <span className="inline-flex w-fit rounded-full border border-amber-500/20 bg-amber-500/10 px-2.5 py-1 text-[11px] font-medium uppercase tracking-[0.18em] text-amber-700">
          Maintenance
        </span>
        <div className="flex items-center gap-2">
          <Wrench className="h-5 w-5" />
          <CardTitle className="text-xl">Warning Recovery</CardTitle>
        </div>
        <CardDescription>
          Resolve DuckDB snapshot warnings in one async job instead of running one-off repairs stock by stock.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-2 gap-3">
          <div className="rounded-xl border border-border/70 bg-background/60 p-3">
            <p className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Stocks needing refresh</p>
            <p className="mt-2 text-lg font-semibold">{repairTargets.stocksNeedingRefresh}</p>
          </div>
          <div className="rounded-xl border border-border/70 bg-background/60 p-3">
            <p className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
              Missing listed-market fundamentals
            </p>
            <p className="mt-2 text-lg font-semibold">{repairTargets.missingListedMarketFundamentals}</p>
          </div>
          <div className="rounded-xl border border-border/70 bg-background/60 p-3">
            <p className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Failed fundamentals dates</p>
            <p className="mt-2 text-lg font-semibold">{repairTargets.failedFundamentalsDates}</p>
          </div>
          <div className="rounded-xl border border-border/70 bg-background/60 p-3">
            <p className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Failed fundamentals codes</p>
            <p className="mt-2 text-lg font-semibold">{repairTargets.failedFundamentalsCodes}</p>
          </div>
        </div>
        <p className="text-xs text-muted-foreground">
          Runs `repair` sync mode to bulk-refresh adjustment-affected stock series and backfill listed-market fundamentals.
        </p>
        <div className="flex items-center justify-between rounded-xl border border-border/70 bg-background/60 p-3">
          <div>
            <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground">Repair signals</p>
            <p className="mt-1 text-sm font-semibold">{repairSignals}</p>
          </div>
          {!canRepair && !isValidationLoading ? (
            <span className="rounded-full border border-border/70 px-3 py-1 text-xs text-muted-foreground">
              No repairs needed
            </span>
          ) : null}
        </div>
        <Button onClick={onRepairWarnings} disabled={isRunning || isStarting || !canRepair} className="w-full">
          {isStarting || isRunning ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Repair Job in Progress...
            </>
          ) : canRepair ? (
            'Repair Warnings'
          ) : (
            'No Repairs Needed'
          )}
        </Button>
      </CardContent>
    </Card>
  );
}

interface ManualStockRefreshSectionProps {
  refreshCodesInput: string;
  refreshInputError: string | null;
  refreshErrorMessage: string | null;
  refreshResult: MarketRefreshResponse | null;
  isRefreshing: boolean;
  onRefreshCodesInputChange: (value: string) => void;
  onRefreshStocks: () => void;
  className?: string;
}

function ManualStockRefreshSection({
  refreshCodesInput,
  refreshInputError,
  refreshErrorMessage,
  refreshResult,
  isRefreshing,
  onRefreshCodesInputChange,
  onRefreshStocks,
  className,
}: ManualStockRefreshSectionProps) {
  return (
    <Card className={cn('border-border/70 bg-card/90 shadow-sm', className)}>
      <CardHeader className="pb-4">
        <span className="inline-flex w-fit rounded-full border border-border/70 bg-background/60 px-2.5 py-1 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
          Targeted Repair
        </span>
        <div className="flex items-center gap-2">
          <RotateCcw className="h-5 w-5" />
          <CardTitle className="text-xl">Stock Refresh (Manual)</CardTitle>
        </div>
        <CardDescription>
          Re-fetch specific DuckDB stock series when you need a one-off repair outside the bulk warning flow.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end">
          <div className="flex-1 space-y-2">
            <Label htmlFor="manual-stock-refresh-input">Stock codes</Label>
            <Input
              id="manual-stock-refresh-input"
              placeholder="e.g. 7203, 6758, 9984"
              value={refreshCodesInput}
              onChange={(e) => onRefreshCodesInputChange(e.target.value)}
              disabled={isRefreshing}
            />
            <p className="text-xs text-muted-foreground">
              Accepts comma, space, or newline separated 4-digit codes, up to 50 at once.
            </p>
          </div>
          <Button onClick={onRefreshStocks} disabled={isRefreshing} className="w-full lg:w-auto">
            {isRefreshing ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Refreshing...
              </>
            ) : (
              'Refresh Stocks'
            )}
          </Button>
        </div>

        {refreshInputError && (
          <div className="rounded-xl bg-red-500/10 p-3 text-sm text-red-500">{refreshInputError}</div>
        )}
        {refreshErrorMessage && (
          <div className="rounded-xl bg-red-500/10 p-3 text-sm text-red-500">{refreshErrorMessage}</div>
        )}
        {refreshResult && <RefreshResultTable result={refreshResult} />}
      </CardContent>
    </Card>
  );
}

interface OverviewMetricCardProps {
  label: string;
  value: string;
  tone: StatusTone;
  description: string;
}

function OverviewMetricCard({ label, value, tone, description }: OverviewMetricCardProps) {
  return (
    <div className={cn('rounded-2xl border p-4 shadow-sm backdrop-blur-sm', getToneClasses(tone))}>
      <p className="text-[11px] font-medium uppercase tracking-[0.22em] opacity-80">{label}</p>
      <p className="mt-3 text-xl font-semibold leading-tight">{value}</p>
      <p className="mt-2 text-xs opacity-80">{description}</p>
    </div>
  );
}

function EmptyJobMonitorCard() {
  return (
    <Card className="border-border/70 bg-card/90 shadow-sm">
      <CardHeader className="pb-3">
        <div className="flex items-center gap-2">
          <Activity className="h-5 w-5" />
          <CardTitle className="text-xl">Job Monitor</CardTitle>
        </div>
        <CardDescription>
          Start a sync or repair job to inspect progress, fetch strategy details, and cancellation.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="rounded-xl border border-dashed border-border/80 bg-background/50 p-4 text-sm text-muted-foreground">
          No active sync job. The live monitor appears here as soon as a job is running or a recent result is restored.
        </div>
      </CardContent>
    </Card>
  );
}

function SectionLabel({ eyebrow, title, description }: { eyebrow: string; title: string; description: string }) {
  return (
    <div className="space-y-1">
      <p className="text-[11px] font-medium uppercase tracking-[0.22em] text-muted-foreground">{eyebrow}</p>
      <h2 className="text-xl font-semibold tracking-tight text-foreground">{title}</h2>
      <p className="text-sm text-muted-foreground">{description}</p>
    </div>
  );
}

interface MarketDbHeroProps {
  dbStats: MarketStatsResponse | undefined;
  dbValidation: MarketValidationResponse | undefined;
  isValidationLoading: boolean;
  currentJob: SyncJobResponse | null;
  repairSignalCount: number;
}

function MarketDbHero({
  dbStats,
  dbValidation,
  isValidationLoading,
  currentJob,
  repairSignalCount,
}: MarketDbHeroProps) {
  return (
    <section className="overflow-hidden rounded-3xl border border-border/70 bg-gradient-to-br from-primary/10 via-background to-amber-500/10 p-6 shadow-sm">
      <div className="flex flex-col gap-6 xl:flex-row xl:items-start xl:justify-between">
        <div className="max-w-3xl space-y-4">
          <span className="inline-flex w-fit rounded-full border border-primary/20 bg-primary/10 px-3 py-1 text-[11px] font-medium uppercase tracking-[0.22em] text-primary">
            Local Data Plane
          </span>
          <div className="space-y-3">
            <h1 className="text-3xl font-semibold tracking-tight text-foreground sm:text-4xl">Market DB</h1>
            <p className="max-w-2xl text-sm leading-6 text-muted-foreground sm:text-base">
              Sync, inspect, and repair the local DuckDB market snapshot. This page is focused on market data
              operations, not general application settings.
            </p>
          </div>
          <div className="flex flex-wrap gap-2 text-xs">
            <span className="rounded-full border border-border/70 bg-background/60 px-3 py-1 text-muted-foreground">
              Source: {dbStats?.timeSeriesSource ?? 'duckdb-parquet'}
            </span>
            <span className="rounded-full border border-border/70 bg-background/60 px-3 py-1 text-muted-foreground">
              Margin orphans: {dbValidation?.margin.orphanCount ?? 'n/a'}
            </span>
            <span className="rounded-full border border-border/70 bg-background/60 px-3 py-1 text-muted-foreground">
              Repair signals: {repairSignalCount}
            </span>
          </div>
        </div>

        <div className="grid gap-3 sm:grid-cols-2 xl:w-[32rem]">
          <OverviewMetricCard
            label="Validation"
            value={dbValidation ? dbValidation.status.toUpperCase() : isValidationLoading ? 'LOADING' : 'UNKNOWN'}
            tone={getValidationTone(dbValidation?.status)}
            description={dbValidation?.recommendations?.[0] ?? 'Health state from /api/db/validate'}
          />
          <OverviewMetricCard
            label="Last Sync"
            value={formatTimestamp(dbStats?.lastSync)}
            tone="neutral"
            description={`Initialized: ${dbStats?.initialized ? 'Yes' : 'No'}`}
          />
          <OverviewMetricCard
            label="Stock Latest"
            value={dbStats?.stockData.dateRange?.max ?? 'n/a'}
            tone="neutral"
            description={`TOPIX latest: ${dbStats?.topix.dateRange?.max ?? 'n/a'}`}
          />
          <OverviewMetricCard
            label="Active Job"
            value={formatSyncJobLabel(currentJob)}
            tone={getJobTone(currentJob?.status)}
            description={currentJob ? `Mode: ${currentJob.mode}` : 'No running sync job'}
          />
        </div>
      </div>
    </section>
  );
}

interface MarketDbHealthColumnProps {
  isStatsLoading: boolean;
  isValidationLoading: boolean;
  statsError: Error | null;
  validationError: Error | null;
  dbStats: MarketStatsResponse | undefined;
  dbValidation: MarketValidationResponse | undefined;
  currentJob: SyncJobResponse | null;
  syncFetchDetails: ReturnType<typeof useSyncFetchDetails>['data'];
  isPolling: boolean;
  onCancel: () => void;
  isCancelling: boolean;
}

function MarketDbHealthColumn({
  isStatsLoading,
  isValidationLoading,
  statsError,
  validationError,
  dbStats,
  dbValidation,
  currentJob,
  syncFetchDetails,
  isPolling,
  onCancel,
  isCancelling,
}: MarketDbHealthColumnProps) {
  return (
    <div className="space-y-6 xl:sticky xl:top-6 xl:self-start">
      <SectionLabel
        eyebrow="Health"
        title="Snapshot and jobs"
        description="The right side stays focused on read-only inspection: current coverage, validation notes, and live job progress."
      />

      <Card className="border-border/70 bg-card/90 shadow-sm">
        <CardHeader>
          <div className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            <CardTitle className="text-xl">DuckDB Snapshot</CardTitle>
          </div>
          <CardDescription>
            Current local source-of-truth status from FastAPI (`/api/db/stats`, `/api/db/validate`).
          </CardDescription>
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

      {currentJob ? (
        <SyncStatusCard
          className="border-border/70 bg-card/90 shadow-sm"
          job={currentJob}
          fetchDetails={syncFetchDetails}
          isLoading={isPolling}
          onCancel={onCancel}
          isCancelling={isCancelling}
        />
      ) : (
        <EmptyJobMonitorCard />
      )}
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

  const isJobStatusRunning = isSyncJobRunning(jobStatus);
  const isActiveJobRunning = isSyncJobRunning(activeSyncJob);
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
  const startSyncErrorMessage = startSync.error?.message ?? null;
  const refreshErrorMessage = refreshStocks.error?.message ?? null;
  const currentJob = jobStatus ?? activeSyncJob ?? null;
  const repairSignalCount = sumRepairTargets(repairTargets);

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
    const validationError = getRefreshCodesValidationError(codes);

    if (validationError) {
      setRefreshInputError(validationError);
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
    <div className="mx-auto flex max-w-7xl flex-col gap-6 p-6">
      <MarketDbHero
        dbStats={dbStats}
        dbValidation={dbValidation}
        isValidationLoading={isValidationLoading}
        currentJob={currentJob}
        repairSignalCount={repairSignalCount}
      />

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.2fr)_minmax(340px,0.88fr)]">
        <div className="space-y-6">
          <SectionLabel
            eyebrow="Operations"
            title="Sync and repair"
            description="The left side is for actions that change the local market DB: full syncs, warning repair, and targeted stock refresh."
          />

          <div className="grid gap-6 lg:grid-cols-2">
            <DatabaseSyncSection
              className="h-full"
              syncMode={syncMode}
              onSyncModeChange={setSyncMode}
              enforceBulkForStockData={enforceBulkForStockData}
              onEnforceBulkChange={setEnforceBulkForStockData}
              isRunning={isRunning}
              isStarting={startSync.isPending}
              onStartSync={handleStartSync}
              errorMessage={startSyncErrorMessage}
            />

            <WarningRecoverySection
              className="h-full"
              repairTargets={repairTargets}
              isValidationLoading={isValidationLoading}
              isRunning={isRunning}
              isStarting={startSync.isPending}
              onRepairWarnings={handleRepairWarnings}
            />
          </div>

          <ManualStockRefreshSection
            refreshCodesInput={refreshCodesInput}
            refreshInputError={refreshInputError}
            refreshErrorMessage={refreshErrorMessage}
            refreshResult={refreshResult}
            isRefreshing={refreshStocks.isPending}
            onRefreshCodesInputChange={(value) => {
              setRefreshCodesInput(value);
              setRefreshInputError(null);
            }}
            onRefreshStocks={handleRefreshStocks}
          />
        </div>

        <MarketDbHealthColumn
          isStatsLoading={isStatsLoading}
          isValidationLoading={isValidationLoading}
          statsError={statsError}
          validationError={validationError}
          dbStats={dbStats}
          dbValidation={dbValidation}
          currentJob={currentJob}
          syncFetchDetails={syncFetchDetails}
          isPolling={isPolling}
          onCancel={handleCancel}
          isCancelling={cancelSync.isPending}
        />
      </div>
    </div>
  );
}
