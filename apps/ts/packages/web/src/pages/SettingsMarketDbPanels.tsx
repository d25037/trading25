import type { MarketStatsResponse, MarketValidationResponse } from '@trading25/contracts/types/api-response-types';
import { Loader2 } from 'lucide-react';
import { cn } from '@/lib/utils';
import { formatCount } from '@/utils/formatters';
import {
  buildValidationDiagnosticSections,
  hasRepairTargets,
  type RepairTargets,
  resolveRepairTargets,
  sumRepairTargets,
  type ValidationDiagnostic,
} from './SettingsMarketDbDiagnostics';
import {
  buildCoverageItems,
  buildDomainHealthItems,
  buildSnapshotSummaryItems,
  type StatusTone,
  type ValidationHealthStatus,
} from './SettingsMarketDbSnapshot';

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

function getHealthStatusTone(status: ValidationHealthStatus | undefined): StatusTone {
  switch (status) {
    case 'healthy':
      return 'success';
    case 'info':
      return 'accent';
    case 'warning':
      return 'warning';
    case 'error':
      return 'danger';
    default:
      return 'neutral';
  }
}

interface SnapshotStatusProps {
  isStatsLoading: boolean;
  isValidationLoading: boolean;
  statsError: Error | null;
  validationError: Error | null;
  dbStats: MarketStatsResponse | undefined;
  dbValidation: MarketValidationResponse | undefined;
}

interface ValidationDiagnosticListProps {
  diagnostics: ValidationDiagnostic[];
  emptyMessage: string;
}

export { hasRepairTargets, type RepairTargets, resolveRepairTargets, sumRepairTargets };

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

function ValidationDiagnosticList({ diagnostics, emptyMessage }: ValidationDiagnosticListProps) {
  if (diagnostics.length <= 0) {
    return <p className="text-sm text-muted-foreground">{emptyMessage}</p>;
  }

  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
      {diagnostics.map((diagnostic) => (
        <div key={diagnostic.label} className="rounded-xl border border-border/70 bg-card/80 p-3">
          <p className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">{diagnostic.label}</p>
          <p className="mt-2 text-lg font-semibold text-foreground">{formatCount(diagnostic.value)}</p>
          <p className="mt-2 text-xs text-muted-foreground">{diagnostic.helpText}</p>
          {diagnostic.sampleItems && diagnostic.sampleItems.length > 0 ? (
            <p className="mt-2 text-xs text-muted-foreground">
              {diagnostic.sampleLabel ?? 'Sample'}: {diagnostic.sampleItems.join(', ')}
            </p>
          ) : null}
          {diagnostic.sampleHint ? <p className="mt-2 text-xs text-muted-foreground">{diagnostic.sampleHint}</p> : null}
        </div>
      ))}
    </div>
  );
}

function SnapshotDetails({
  dbStats,
  dbValidation,
}: {
  dbStats: MarketStatsResponse | undefined;
  dbValidation: MarketValidationResponse | undefined;
}) {
  const recommendations = dbValidation?.recommendations ?? [];
  const summaryItems = buildSnapshotSummaryItems(dbStats, dbValidation);
  const domainHealthItems = buildDomainHealthItems(dbValidation);
  const coverageItems = dbStats ? buildCoverageItems(dbStats, dbValidation) : [];
  const validationDiagnostics = dbValidation
    ? buildValidationDiagnosticSections(dbValidation)
    : {
        warningDiagnostics: [],
        informationalDiagnostics: [],
      };

  return (
    <div className="space-y-4">
      {summaryItems.length > 0 ? (
        <div className="space-y-3 rounded-xl border border-border/70 bg-background/60 p-4">
          <div className="space-y-1">
            <p className="font-medium">Snapshot Summary</p>
            <p className="text-xs text-muted-foreground">FastAPI response summary for the current DuckDB data plane.</p>
          </div>
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            {summaryItems.map((item) => (
              <div
                key={item.label}
                className={cn(
                  'rounded-xl border p-3',
                  item.tone ? getToneClasses(item.tone) : 'border-border/70 bg-card/80'
                )}
              >
                <p className="text-[11px] uppercase tracking-[0.18em] opacity-80">{item.label}</p>
                <p className="mt-2 text-sm font-semibold">{item.value}</p>
                <p className="mt-2 text-xs opacity-80">{item.helpText}</p>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {domainHealthItems.length > 0 ? (
        <div className="rounded-xl border border-border/70 bg-background/60 p-4">
          <p className="font-medium">Domain Health</p>
          <div className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-2">
            {domainHealthItems.map((item) => (
              <div
                key={item.label}
                className={cn('rounded-xl border p-3', getToneClasses(getHealthStatusTone(item.status)))}
              >
                <p className="text-[11px] uppercase tracking-[0.18em] opacity-80">{item.label}</p>
                <p className="mt-2 text-sm font-semibold">{item.status.toUpperCase()}</p>
                <p className="mt-2 text-xs opacity-80">{item.helpText}</p>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {coverageItems.length > 0 ? (
        <div className="rounded-xl border border-border/70 bg-background/60 p-4">
          <p className="font-medium">Data Coverage</p>
          <div className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-3">
            {coverageItems.map((item) => (
              <div key={item.label} className="rounded-xl border border-border/70 bg-card/80 p-3">
                <p className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">{item.label}</p>
                <p className="mt-2 text-lg font-semibold text-foreground">{item.value}</p>
                <div className="mt-3 space-y-1 text-xs text-muted-foreground">
                  {item.meta.map((metaItem) => (
                    <p key={metaItem}>{metaItem}</p>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {dbValidation ? (
        <div className="rounded-xl border border-border/70 bg-background/60 p-4">
          <p className="font-medium">Validation Diagnostics</p>
          <div className="mt-3 space-y-4">
            <div className="space-y-3">
              <p className="text-xs font-medium uppercase tracking-[0.18em] text-amber-700">Actionable Warnings</p>
              <ValidationDiagnosticList
                diagnostics={validationDiagnostics.warningDiagnostics}
                emptyMessage="No actionable validation diagnostics."
              />
            </div>

            <div className="space-y-3">
              <p className="text-xs font-medium uppercase tracking-[0.18em] text-muted-foreground">
                Informational Diagnostics
              </p>
              <ValidationDiagnosticList
                diagnostics={validationDiagnostics.informationalDiagnostics}
                emptyMessage="No additional informational diagnostics."
              />
            </div>
          </div>
        </div>
      ) : null}

      {dbValidation && recommendations.length > 0 ? (
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

export function SnapshotStatus({
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

  const errorMessages = [statsError?.message, validationError?.message].filter(
    (message): message is string => typeof message === 'string' && message.length > 0
  );

  return (
    <>
      {errorMessages.map((message) => (
        <div key={message} className="rounded-xl bg-red-500/10 p-3 text-sm text-red-500">
          {message}
        </div>
      ))}

      {dbStats || dbValidation ? <SnapshotDetails dbStats={dbStats} dbValidation={dbValidation} /> : null}
    </>
  );
}
