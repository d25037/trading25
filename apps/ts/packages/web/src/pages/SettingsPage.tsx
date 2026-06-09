import { isActiveJobStatus } from '@trading25/api-clients/base/job-status';
import type {
  AdjustedMetricsMaterializeJobResponse,
  MarketRefreshResponse,
  MarketStatsResponse,
  MarketValidationResponse,
  StartSyncRequest,
  SyncJobResponse,
  SyncMode,
} from '@trading25/contracts/types/api-response-types';
import { Activity, Database, Loader2, RotateCcw, Wrench } from 'lucide-react';
import { useEffect, useState } from 'react';
import {
  CompactMetric,
  PageIntro,
  PageIntroMetaList,
  SectionEyebrow,
  SectionHeading,
} from '@/components/Layout/Workspace';
import { SyncModeSelect } from '@/components/Settings/SyncModeSelect';
import { SyncStatusCard } from '@/components/Settings/SyncStatusCard';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import {
  useActiveAdjustedMetricsMaterializeJob,
  useActiveSyncJob,
  useAdjustedMetricsMaterializeJobStatus,
  useCancelSync,
  useDbStats,
  useDbValidation,
  useRefreshStocks,
  useStartAdjustedMetricsMaterialize,
  useStartSync,
  useSyncFetchDetails,
  useSyncJobStatus,
  useSyncSSE,
} from '@/hooks/useDbSync';
import { ApiError } from '@/lib/api-client';
import { ACTIVE_SYNC_JOB_STORAGE_KEY, readStoredString, writeStoredString } from '@/lib/persistedState';
import { cn } from '@/lib/utils';
import { formatBytes, formatCount, formatOptionalTimestamp } from '@/utils/formatters';
import {
  hasRepairTargets,
  type RepairTargets,
  resolveRepairTargets,
  SnapshotStatus,
  sumRepairTargets,
} from './SettingsMarketDbPanels';
import { buildStorageHelpText, resolveSnapshotObservedAt } from './SettingsMarketDbSnapshot';

const RESET_CONFIRMATION_TOKEN = 'RESET';

function readPersistedActiveSyncJobId(): string | null {
  if (typeof window === 'undefined') {
    return null;
  }
  return readStoredString(window.localStorage, ACTIVE_SYNC_JOB_STORAGE_KEY);
}

function persistActiveSyncJobId(jobId: string | null): void {
  if (typeof window === 'undefined') {
    return;
  }
  writeStoredString(window.localStorage, ACTIVE_SYNC_JOB_STORAGE_KEY, jobId);
}

function buildStartSyncRequest(
  syncMode: SyncMode,
  enforceBulkForStockData: boolean,
  resetBeforeSync: boolean
): StartSyncRequest {
  const request: StartSyncRequest = { mode: syncMode, enforceBulkForStockData };
  if (syncMode === 'initial' && resetBeforeSync) {
    request.resetBeforeSync = true;
  }
  return request;
}

function useResetBeforeSyncGuard(
  syncMode: SyncMode,
  setResetBeforeSync: (value: boolean) => void,
  setResetConfirmOpen: (value: boolean) => void,
  setResetConfirmationText: (value: string) => void
): void {
  useEffect(() => {
    if (syncMode === 'initial') {
      return;
    }
    setResetBeforeSync(false);
    setResetConfirmOpen(false);
    setResetConfirmationText('');
  }, [syncMode, setResetBeforeSync, setResetConfirmOpen, setResetConfirmationText]);
}

type StatusTone = 'neutral' | 'accent' | 'success' | 'warning' | 'danger';
type SyncJobStatusShape = Pick<SyncJobResponse | AdjustedMetricsMaterializeJobResponse, 'status'> | null | undefined;

function getToneClasses(tone: StatusTone): string {
  switch (tone) {
    case 'accent':
      return 'border-primary/18 bg-primary/10 text-primary';
    case 'success':
      return 'border-emerald-500/18 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300';
    case 'warning':
      return 'border-amber-500/18 bg-amber-500/10 text-amber-700 dark:text-amber-300';
    case 'danger':
      return 'border-red-500/18 bg-red-500/10 text-red-700 dark:text-red-300';
    default:
      return 'border-border/70 bg-[var(--app-surface-muted)] text-foreground';
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

function getJobTone(
  status: SyncJobResponse['status'] | AdjustedMetricsMaterializeJobResponse['status'] | null | undefined
): StatusTone {
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

function isSyncJobRunning(job: SyncJobStatusShape): boolean {
  return isActiveJobStatus(job?.status);
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
          <p className="mt-2 text-sm font-semibold">{formatOptionalTimestamp(result.lastUpdated)}</p>
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
  resetBeforeSync: boolean;
  onResetBeforeSyncChange: (checked: boolean) => void;
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
  resetBeforeSync,
  onResetBeforeSyncChange,
  isRunning,
  isStarting,
  onStartSync,
  errorMessage,
  className,
}: DatabaseSyncSectionProps) {
  return (
    <Card className={cn('border-border/70 bg-[var(--app-surface)] shadow-none', className)}>
      <CardHeader className="pb-4">
        <SectionEyebrow>Primary Action</SectionEyebrow>
        <div className="mt-1 flex items-start gap-3">
          <div className="app-panel-muted flex h-10 w-10 items-center justify-center rounded-xl text-primary">
            <Database className="h-5 w-5" />
          </div>
          <div className="space-y-1">
            <CardTitle className="text-xl tracking-tight">Database Sync</CardTitle>
            <CardDescription>
              Synchronize J-Quants market data into the local DuckDB source of truth. Use incremental to resume
              interrupted syncs. Initial mode becomes destructive only if you enable reset below.
            </CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <SyncModeSelect value={syncMode} onChange={onSyncModeChange} disabled={isRunning || isStarting} />
        {syncMode === 'initial' ? (
          <div className="space-y-3 rounded-2xl border border-amber-500/30 bg-amber-500/10 p-4">
            <div className="flex items-center justify-between gap-4">
              <div className="space-y-1">
                <Label htmlFor="reset-before-sync">Reset market.duckdb + parquet first</Label>
                <p className="text-xs text-muted-foreground">
                  Deletes the current market snapshot before the initial sync rebuilds the local 10-year window.
                </p>
              </div>
              <Switch
                id="reset-before-sync"
                checked={resetBeforeSync}
                onCheckedChange={onResetBeforeSyncChange}
                disabled={isRunning || isStarting}
              />
            </div>
            <p className="text-xs text-muted-foreground">
              Existing datasets must be rebuilt after a reset. `portfolio.db` is not touched.
            </p>
          </div>
        ) : null}
        <div className="flex items-center justify-between gap-4 rounded-2xl border border-border/70 bg-[var(--app-surface-muted)] p-4">
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
    <Card className={cn('border-border/70 bg-[var(--app-surface)] shadow-none', className)}>
      <CardHeader className="pb-4">
        <SectionEyebrow>Maintenance</SectionEyebrow>
        <div className="mt-1 flex items-start gap-3">
          <div className="app-panel-muted flex h-10 w-10 items-center justify-center rounded-xl text-amber-700 dark:text-amber-300">
            <Wrench className="h-5 w-5" />
          </div>
          <div className="space-y-1">
            <CardTitle className="text-xl tracking-tight">Warning Recovery</CardTitle>
            <CardDescription>
              Resolve only the DuckDB snapshot warnings that `repair` sync can actually fix. Legacy stock-price
              adjustment drift requires initial sync with reset enabled; N225 options coverage gaps must be handled from
              Database Sync.
            </CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
          <div className="rounded-2xl border border-border/70 bg-[var(--app-surface-muted)] p-3">
            <p className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
              Missing listed-market fundamentals
            </p>
            <p className="mt-2 text-lg font-semibold">{repairTargets.missingListedMarketFundamentals}</p>
          </div>
          <div className="rounded-2xl border border-border/70 bg-[var(--app-surface-muted)] p-3">
            <p className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Failed fundamentals dates</p>
            <p className="mt-2 text-lg font-semibold">{repairTargets.failedFundamentalsDates}</p>
          </div>
          <div className="rounded-2xl border border-border/70 bg-[var(--app-surface-muted)] p-3">
            <p className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Failed fundamentals codes</p>
            <p className="mt-2 text-lg font-semibold">{repairTargets.failedFundamentalsCodes}</p>
          </div>
        </div>
        <p className="text-xs text-muted-foreground">
          Runs `repair` sync mode to backfill listed-market fundamentals and related non-price warnings. It does not
          rebuild legacy stock-price snapshots or ingest `options_225_data`; use Initial sync with reset enabled for
          legacy price snapshots, and use Database Sync with `incremental` for options gaps.
        </p>
        <div className="flex items-center justify-between rounded-2xl border border-border/70 bg-[var(--app-surface-muted)] p-3">
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

interface AdjustedMetricsMaterializeSectionProps {
  dbStats: MarketStatsResponse | undefined;
  currentJob: AdjustedMetricsMaterializeJobResponse | null;
  isSyncRunning: boolean;
  isMaterializing: boolean;
  isStarting: boolean;
  onStartMaterialize: () => void;
  errorMessage: string | null;
}

function AdjustedMetricsMaterializeSection({
  dbStats,
  currentJob,
  isSyncRunning,
  isMaterializing,
  isStarting,
  onStartMaterialize,
  errorMessage,
}: AdjustedMetricsMaterializeSectionProps) {
  const adjustedMetrics = dbStats?.adjustedMetrics;
  const disabled = isSyncRunning || isMaterializing || isStarting;
  const result = currentJob?.result;

  return (
    <Card className="border-border/70 bg-[var(--app-surface)] shadow-none">
      <CardHeader className="pb-4">
        <SectionEyebrow>Derived SoT</SectionEyebrow>
        <div className="mt-1 flex items-start gap-3">
          <div className="app-panel-muted flex h-10 w-10 items-center justify-center rounded-xl text-primary">
            <Activity className="h-5 w-5" />
          </div>
          <div className="space-y-1">
            <CardTitle className="text-xl tracking-tight">Adjusted Metrics</CardTitle>
            <CardDescription>
              Rebuild `statement_metrics_adjusted` and `daily_valuation` as a separate full materialization job.
            </CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-2 gap-3">
          <div className="rounded-2xl border border-border/70 bg-[var(--app-surface-muted)] p-3">
            <p className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Status</p>
            <p className="mt-2 text-lg font-semibold">{adjustedMetrics?.status?.toUpperCase() ?? 'UNKNOWN'}</p>
          </div>
          <div className="rounded-2xl border border-border/70 bg-[var(--app-surface-muted)] p-3">
            <p className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Coverage</p>
            <p className="mt-2 text-sm font-semibold">{adjustedMetrics?.dailyValuationLatestDate ?? 'n/a'}</p>
            <p className="mt-1 text-[11px] text-muted-foreground">
              Codes {formatCount(adjustedMetrics?.dailyValuationLatestCodeCount ?? 0)}
            </p>
            <p className="mt-1 text-[11px] text-muted-foreground">Basis {adjustedMetrics?.priceBasisDate ?? 'n/a'}</p>
          </div>
        </div>
        {currentJob ? (
          <div className={cn('rounded-xl border p-3 text-sm', getToneClasses(getJobTone(currentJob.status)))}>
            <div className="flex items-center justify-between gap-3">
              <span className="font-medium">{currentJob.status.toUpperCase()}</span>
              <span>{currentJob.progress?.message ?? 'Materialization job tracked separately from sync.'}</span>
            </div>
            {result ? (
              <p className="mt-2 text-xs">
                Statements {formatCount(result.statementRows)} / Daily valuation{' '}
                {formatCount(result.dailyValuationRows)} / Coverage {result.dailyValuationLatestDate ?? 'n/a'}
              </p>
            ) : null}
          </div>
        ) : (
          <p className="text-xs text-muted-foreground">
            Run this after sync when adjusted fundamentals or valuation semantics changed. It does not fetch J-Quants.
          </p>
        )}
        <Button onClick={onStartMaterialize} disabled={disabled} className="w-full">
          {isStarting || isMaterializing ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Materializing...
            </>
          ) : isSyncRunning ? (
            'Sync in Progress'
          ) : (
            'Materialize Adjusted Metrics'
          )}
        </Button>
        {errorMessage && <div className="rounded-xl bg-red-500/10 p-3 text-sm text-red-500">{errorMessage}</div>}
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
  isDisabled?: boolean;
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
  isDisabled = false,
  onRefreshCodesInputChange,
  onRefreshStocks,
  className,
}: ManualStockRefreshSectionProps) {
  return (
    <Card className={cn('border-border/70 bg-[var(--app-surface)] shadow-none', className)}>
      <CardHeader className="pb-4">
        <SectionEyebrow>Targeted Repair</SectionEyebrow>
        <div className="mt-1 flex items-start gap-3">
          <div className="app-panel-muted flex h-10 w-10 items-center justify-center rounded-xl text-primary">
            <RotateCcw className="h-5 w-5" />
          </div>
          <div className="space-y-1">
            <CardTitle className="text-xl tracking-tight">Stock Refresh (Manual)</CardTitle>
            <CardDescription>
              Re-fetch specific DuckDB stock series when you need a one-off repair outside the chart header flow.
            </CardDescription>
          </div>
        </div>
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
              disabled={isRefreshing || isDisabled}
            />
            <p className="text-xs text-muted-foreground">
              Accepts comma, space, or newline separated 4-digit codes, up to 50 at once.
            </p>
          </div>
          <Button onClick={onRefreshStocks} disabled={isRefreshing || isDisabled} className="w-full lg:w-auto">
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

interface ResetConfirmDialogProps {
  open: boolean;
  confirmationText: string;
  isStarting: boolean;
  onOpenChange: (open: boolean) => void;
  onConfirmationTextChange: (value: string) => void;
  onSubmit: () => void;
}

function ResetConfirmDialog({
  open,
  confirmationText,
  isStarting,
  onOpenChange,
  onConfirmationTextChange,
  onSubmit,
}: ResetConfirmDialogProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-xl">
        <DialogHeader>
          <DialogTitle>Reset market DB before initial sync?</DialogTitle>
          <DialogDescription>
            This deletes the current `market.duckdb` and `parquet/` snapshot before rebuilding a fresh local 10-year
            window from J-Quants.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-3 rounded-2xl border border-amber-500/30 bg-amber-500/10 p-4 text-sm">
          <p>Local history older than the rolling 10-year J-Quants window is lost.</p>
          <p>`datasets/*` built from the current market DB must be recreated.</p>
          <p>`portfolio.db`, watchlists, and jobs metadata are not deleted.</p>
        </div>

        <div className="space-y-2">
          <Label htmlFor="reset-confirmation-input">Type {RESET_CONFIRMATION_TOKEN} to continue</Label>
          <Input
            id="reset-confirmation-input"
            value={confirmationText}
            onChange={(event) => onConfirmationTextChange(event.target.value)}
            autoComplete="off"
            autoCapitalize="characters"
            spellCheck={false}
          />
        </div>

        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)} disabled={isStarting}>
            Cancel
          </Button>
          <Button
            type="button"
            variant="destructive"
            onClick={onSubmit}
            disabled={isStarting || confirmationText.trim().toUpperCase() !== RESET_CONFIRMATION_TOKEN}
          >
            {isStarting ? 'Starting...' : 'Reset and Start Sync'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function EmptyJobMonitorCard() {
  return (
    <Card className="border-border/70 bg-[var(--app-surface)] shadow-none">
      <CardHeader className="pb-3">
        <SectionEyebrow>Job Monitor</SectionEyebrow>
        <div className="mt-1 flex items-start gap-3">
          <div className="app-panel-muted flex h-10 w-10 items-center justify-center rounded-xl text-primary">
            <Activity className="h-5 w-5" />
          </div>
          <div className="space-y-1">
            <CardTitle className="text-xl tracking-tight">Live Sync Job</CardTitle>
            <CardDescription>
              Start a sync or repair job to inspect progress, fetch strategy details, and cancellation.
            </CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <div className="rounded-2xl border border-dashed border-border/80 bg-[var(--app-surface-muted)] p-4 text-sm text-muted-foreground">
          No active sync job. The live monitor appears here as soon as a job is running or a recent result is restored.
        </div>
      </CardContent>
    </Card>
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
  const storageTotalBytes = dbStats?.storage?.totalBytes ?? dbStats?.databaseSize ?? 0;
  const introMetaItems = [
    { label: 'Source', value: dbStats?.timeSeriesSource ?? 'duckdb-parquet' },
    { label: 'Storage', value: formatBytes(storageTotalBytes) },
    { label: 'Status Checked', value: resolveSnapshotObservedAt(dbStats, dbValidation) },
    { label: 'Repair Signals', value: repairSignalCount.toString() },
  ];

  return (
    <PageIntro
      eyebrow="Local Data Plane"
      title="Market DB"
      description="Sync, inspect, and repair the local DuckDB market snapshot without leaving the operational workspace."
      meta={<PageIntroMetaList items={introMetaItems} />}
      aside={
        <div className="grid gap-3 sm:grid-cols-2">
          <CompactMetric
            label="Validation"
            value={dbValidation ? dbValidation.status.toUpperCase() : isValidationLoading ? 'LOADING' : 'UNKNOWN'}
            tone={getValidationTone(dbValidation?.status)}
            detail={dbValidation?.recommendations?.[0] ?? 'Health state from /api/db/validate'}
          />
          <CompactMetric
            label="Last Sync"
            value={formatOptionalTimestamp(dbStats?.lastSync)}
            tone="neutral"
            detail={`Initialized: ${dbStats?.initialized ? 'Yes' : 'No'}`}
          />
          <CompactMetric
            label="Storage"
            value={formatBytes(storageTotalBytes)}
            tone="neutral"
            detail={dbStats ? buildStorageHelpText(dbStats) : 'DuckDB and Parquet footprint'}
          />
          <CompactMetric
            label="Active Job"
            value={formatSyncJobLabel(currentJob)}
            tone={getJobTone(currentJob?.status)}
            detail={currentJob ? `Mode: ${currentJob.mode}` : 'No running sync job'}
          />
        </div>
      }
    />
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
      <SectionHeading
        eyebrow="Health"
        title="Snapshot and jobs"
        description="The right side stays focused on read-only inspection: current coverage, validation notes, and live job progress."
      />

      <Card className="border-border/70 bg-[var(--app-surface)] shadow-none">
        <CardHeader>
          <SectionEyebrow>Snapshot</SectionEyebrow>
          <div className="mt-1 flex items-start gap-3">
            <div className="app-panel-muted flex h-10 w-10 items-center justify-center rounded-xl text-primary">
              <Database className="h-5 w-5" />
            </div>
            <div className="space-y-1">
              <CardTitle className="text-xl tracking-tight">DuckDB Snapshot</CardTitle>
              <CardDescription>
                Current local source-of-truth status from FastAPI (`/api/db/stats`, `/api/db/validate`).
              </CardDescription>
            </div>
          </div>
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
          className="border-border/70 bg-[var(--app-surface)] shadow-none"
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
  const [resetBeforeSync, setResetBeforeSync] = useState(false);
  const [resetConfirmOpen, setResetConfirmOpen] = useState(false);
  const [resetConfirmationText, setResetConfirmationText] = useState('');
  const [activeJobId, setActiveJobId] = useState<string | null>(readPersistedActiveSyncJobId);
  const [activeMaterializeJobId, setActiveMaterializeJobId] = useState<string | null>(null);
  const [refreshCodesInput, setRefreshCodesInput] = useState('');
  const [refreshInputError, setRefreshInputError] = useState<string | null>(null);
  const [refreshResult, setRefreshResult] = useState<MarketRefreshResponse | null>(null);

  const startSync = useStartSync();
  const { data: activeSyncJob } = useActiveSyncJob(activeJobId === null);
  const { data: activeMaterializeJob } = useActiveAdjustedMetricsMaterializeJob(activeMaterializeJobId === null);
  const syncSse = useSyncSSE(activeJobId);
  const {
    data: jobStatus,
    isLoading: isPolling,
    error: syncJobError,
  } = useSyncJobStatus(activeJobId, syncSse.isConnected);
  const { data: syncFetchDetails } = useSyncFetchDetails(activeJobId, syncSse.isConnected);
  const { data: materializeJobStatus } = useAdjustedMetricsMaterializeJobStatus(activeMaterializeJobId);
  const cancelSync = useCancelSync();
  const refreshStocks = useRefreshStocks();
  const startMaterialize = useStartAdjustedMetricsMaterialize();

  useEffect(() => {
    activeSyncJob?.jobId && setActiveJobId(activeSyncJob.jobId);
  }, [activeSyncJob?.jobId]);

  useEffect(() => {
    persistActiveSyncJobId(activeJobId);
  }, [activeJobId]);

  useEffect(() => {
    syncJobError instanceof ApiError && syncJobError.status === 404 && setActiveJobId(null);
  }, [syncJobError]);

  useResetBeforeSyncGuard(syncMode, setResetBeforeSync, setResetConfirmOpen, setResetConfirmationText);

  const isJobStatusRunning = isSyncJobRunning(jobStatus);
  const isActiveJobRunning = isSyncJobRunning(activeSyncJob);
  const isMaterializeJobRunning = isSyncJobRunning(materializeJobStatus);
  const isActiveMaterializeJobRunning = isSyncJobRunning(activeMaterializeJob);
  const isRunning = startSync.isPending || isJobStatusRunning || (!jobStatus && isActiveJobRunning);
  const isMaterializing =
    startMaterialize.isPending || isMaterializeJobRunning || (!materializeJobStatus && isActiveMaterializeJobRunning);
  const {
    data: dbStats,
    isLoading: isStatsLoading,
    error: statsError,
    refetch: refetchDbStats,
  } = useDbStats({ isSyncRunning: isRunning || isMaterializing });
  const {
    data: dbValidation,
    isLoading: isValidationLoading,
    error: validationError,
    refetch: refetchDbValidation,
  } = useDbValidation({ isSyncRunning: isRunning || isMaterializing });
  const repairTargets = resolveRepairTargets(dbValidation);
  const startSyncErrorMessage = startSync.error?.message ?? null;
  const startMaterializeErrorMessage = startMaterialize.error?.message ?? null;
  const refreshErrorMessage = refreshStocks.error?.message ?? null;
  const currentJob = jobStatus ?? activeSyncJob ?? null;
  const currentMaterializeJob = materializeJobStatus ?? activeMaterializeJob ?? null;
  const repairSignalCount = sumRepairTargets(repairTargets);

  useEffect(() => {
    if (!isRunning && !isMaterializing) {
      return;
    }
    void refetchDbStats();
    void refetchDbValidation();
  }, [isRunning, isMaterializing, refetchDbStats, refetchDbValidation]);

  const submitStartSync = () => {
    const request = buildStartSyncRequest(syncMode, enforceBulkForStockData, resetBeforeSync);
    startSync.mutate(request, {
      onSuccess: (data) => {
        setActiveJobId(data.jobId);
        setResetConfirmOpen(false);
        setResetConfirmationText('');
      },
    });
  };

  const handleStartSync = () => {
    if (syncMode === 'initial' && resetBeforeSync) {
      setResetConfirmationText('');
      setResetConfirmOpen(true);
      return;
    }
    submitStartSync();
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

  const handleStartMaterialize = () => {
    startMaterialize.mutate(undefined, {
      onSuccess: (data) => {
        setActiveMaterializeJobId(data.jobId);
      },
    });
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
          <SectionHeading
            eyebrow="Operations"
            title="Sync and repair"
            description="The left side is for actions that change the local market DB: full syncs, warning repair, and targeted stock refresh."
          />

          <div className="grid gap-6 lg:grid-cols-3">
            <DatabaseSyncSection
              className="h-full"
              syncMode={syncMode}
              onSyncModeChange={setSyncMode}
              enforceBulkForStockData={enforceBulkForStockData}
              onEnforceBulkChange={setEnforceBulkForStockData}
              resetBeforeSync={resetBeforeSync}
              onResetBeforeSyncChange={setResetBeforeSync}
              isRunning={isRunning || isMaterializing}
              isStarting={startSync.isPending}
              onStartSync={handleStartSync}
              errorMessage={startSyncErrorMessage}
            />

            <WarningRecoverySection
              className="h-full"
              repairTargets={repairTargets}
              isValidationLoading={isValidationLoading}
              isRunning={isRunning || isMaterializing}
              isStarting={startSync.isPending}
              onRepairWarnings={handleRepairWarnings}
            />

            <AdjustedMetricsMaterializeSection
              dbStats={dbStats}
              currentJob={currentMaterializeJob}
              isSyncRunning={isRunning}
              isMaterializing={isMaterializing}
              isStarting={startMaterialize.isPending}
              onStartMaterialize={handleStartMaterialize}
              errorMessage={startMaterializeErrorMessage}
            />
          </div>

          <ManualStockRefreshSection
            refreshCodesInput={refreshCodesInput}
            refreshInputError={refreshInputError}
            refreshErrorMessage={refreshErrorMessage}
            refreshResult={refreshResult}
            isRefreshing={refreshStocks.isPending}
            isDisabled={isRunning || isMaterializing}
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

      <ResetConfirmDialog
        open={resetConfirmOpen}
        onOpenChange={(open) => {
          setResetConfirmOpen(open);
          if (!open) {
            setResetConfirmationText('');
          }
        }}
        confirmationText={resetConfirmationText}
        isStarting={startSync.isPending}
        onConfirmationTextChange={setResetConfirmationText}
        onSubmit={submitStartSync}
      />
    </div>
  );
}
