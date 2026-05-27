import type { MarketStatsResponse, MarketValidationResponse } from '@trading25/contracts/types/api-response-types';
import { Loader2 } from 'lucide-react';
import { cn } from '@/lib/utils';
import {
  formatBytes,
  formatCount,
  formatOptionalDateRange,
  formatOptionalTimestamp,
  formatRatioPercentage,
} from '@/utils/formatters';

const EMPTY_OPTIONS_225_STATS = {
  count: 0,
  dateCount: 0,
  dateRange: null,
} as const;
const EMPTY_OPTIONS_225_VALIDATION = {
  count: 0,
  dateCount: 0,
  dateRange: null,
  missingTopixCoverageDatesCount: 0,
  missingTopixCoverageDates: [],
  missingUnderlyingPriceDatesCount: 0,
  missingUnderlyingPriceDates: [],
  conflictingUnderlyingPriceDatesCount: 0,
  conflictingUnderlyingPriceDates: [],
} as const;

type StatusTone = 'neutral' | 'accent' | 'success' | 'warning' | 'danger';
type ValidationHealthStatus = 'healthy' | 'info' | 'warning' | 'error';

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

interface SnapshotSummaryItem {
  label: string;
  value: string;
  helpText: string;
  tone?: StatusTone;
}

interface SnapshotCoverageItem {
  label: string;
  value: string;
  meta: string[];
}

interface ValidationDiagnostic {
  label: string;
  value: number;
  helpText: string;
  sampleItems?: string[];
  sampleLabel?: string;
  sampleHint?: string;
}

interface ValidationDiagnosticSections {
  warningDiagnostics: ValidationDiagnostic[];
  informationalDiagnostics: ValidationDiagnostic[];
}

interface Options225CoverageDisplay {
  value: string;
  status: string;
}

type Options225CoverageKind = 'missing' | 'pending' | 'stale' | 'partial' | 'in_sync';

interface DomainHealthItem {
  label: string;
  status: ValidationHealthStatus;
  helpText: string;
}

interface ValidationDiagnosticListProps {
  diagnostics: ValidationDiagnostic[];
  emptyMessage: string;
}

export interface RepairTargets {
  missingListedMarketFundamentals: number;
  failedFundamentalsDates: number;
  failedFundamentalsCodes: number;
}

const EMPTY_REPAIR_TARGETS: RepairTargets = {
  missingListedMarketFundamentals: 0,
  failedFundamentalsDates: 0,
  failedFundamentalsCodes: 0,
};

function normalizeHealthStatus(
  value: string | undefined,
  fallback: ValidationHealthStatus = 'healthy'
): ValidationHealthStatus {
  if (value === 'healthy' || value === 'info' || value === 'warning' || value === 'error') {
    return value;
  }
  return fallback;
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
    return 'rounded-xl border border-border/70 bg-background/60 p-4';
  }
  return 'rounded-xl border border-yellow-500/30 bg-yellow-500/10 p-4';
}

function isDateBefore(lhs: string | null | undefined, rhs: string | null | undefined): boolean {
  if (!lhs || !rhs) {
    return false;
  }
  return lhs < rhs;
}

function resolveOptions225CoverageKind(params: {
  initialized?: boolean;
  topixCount: number;
  optionsCount: number;
  topixLatest: string | null;
  optionsLatest: string | null;
  missingCoverageCount: number;
  coverageStatus?: MarketValidationResponse['options225']['coverageStatus'];
}): Options225CoverageKind {
  if (params.coverageStatus) {
    return params.coverageStatus;
  }
  if (params.optionsCount <= 0 && params.topixCount > 0 && params.initialized !== false) {
    return 'missing';
  }
  if (isDateBefore(params.optionsLatest, params.topixLatest)) {
    return 'stale';
  }
  if (params.missingCoverageCount > 0) {
    return 'partial';
  }
  return 'in_sync';
}

function buildOptions225CoverageDisplay(
  stats: MarketStatsResponse['options225'],
  topix: MarketStatsResponse['topix'],
  validation?: MarketValidationResponse['options225']
): Options225CoverageDisplay {
  const topixLatest = topix.dateRange?.max ?? null;
  const optionsLatest = stats.dateRange?.max ?? null;
  const missingCoverageCount = validation?.missingTopixCoverageDatesCount ?? 0;
  const coverageKind = resolveOptions225CoverageKind({
    topixCount: topix.count,
    optionsCount: stats.count,
    topixLatest,
    optionsLatest,
    missingCoverageCount,
    coverageStatus: validation?.coverageStatus,
  });

  switch (coverageKind) {
    case 'missing':
      return {
        value: 'Not ingested',
        status: `Status: No local options chain yet (TOPIX latest ${topixLatest ?? 'n/a'})`,
      };
    case 'stale':
      return {
        value: `${optionsLatest ?? 'n/a'} (stale)`,
        status: `Status: Behind TOPIX latest ${topixLatest ?? 'n/a'}`,
      };
    case 'pending':
      return {
        value: `${optionsLatest ?? 'n/a'} (pending)`,
        status: `Status: Awaiting normal N225 options publication for ${formatCount(missingCoverageCount)} TOPIX date`,
      };
    case 'partial':
      return {
        value: `${optionsLatest ?? 'n/a'} (partial)`,
        status: `Status: Missing local coverage for ${formatCount(missingCoverageCount)} TOPIX dates`,
      };
    default:
      return {
        value: optionsLatest ?? 'n/a',
        status: 'Status: In sync with local TOPIX coverage',
      };
  }
}

function formatCategoryBreakdown(byCategory: Record<string, number>): string {
  const entries = Object.entries(byCategory).sort((left, right) => right[1] - left[1]);
  if (entries.length === 0) {
    return 'n/a';
  }
  const preview = entries
    .slice(0, 3)
    .map(([category, count]) => `${category} ${formatCount(count)}`)
    .join(', ');
  return entries.length > 3 ? `${preview}, ...` : preview;
}

export function resolveSnapshotObservedAt(
  dbStats: MarketStatsResponse | undefined,
  dbValidation: MarketValidationResponse | undefined
): string {
  const candidates = [dbStats?.lastUpdated, dbValidation?.lastUpdated].filter(
    (value): value is string => typeof value === 'string' && value.length > 0
  );
  if (candidates.length === 0) {
    return 'n/a';
  }

  const sorted = [...candidates].sort((left, right) => {
    const leftTime = new Date(left).getTime();
    const rightTime = new Date(right).getTime();
    if (Number.isNaN(leftTime) || Number.isNaN(rightTime)) {
      return left.localeCompare(right);
    }
    return rightTime - leftTime;
  });
  return formatOptionalTimestamp(sorted[0]);
}

function buildSnapshotSummaryItems(
  dbStats: MarketStatsResponse | undefined,
  dbValidation: MarketValidationResponse | undefined
): SnapshotSummaryItem[] {
  const items: SnapshotSummaryItem[] = [];

  if (dbValidation) {
    items.push({
      label: 'Validation',
      value: dbValidation.status.toUpperCase(),
      helpText: dbValidation.recommendations?.[0] ?? 'Health state from /api/db/validate',
      tone: getValidationTone(dbValidation.status),
    });
    items.push({
      label: 'Last Stock Refresh',
      value: formatOptionalTimestamp(dbValidation.lastStocksRefresh),
      helpText: `Status checked: ${resolveSnapshotObservedAt(dbStats, dbValidation)}`,
    });
  }

  if (dbStats) {
    items.push({
      label: 'Last Sync',
      value: formatOptionalTimestamp(dbStats.lastSync),
      helpText: `Initialized: ${dbStats.initialized ? 'Yes' : 'No'}`,
    });
    items.push({
      label: 'Local Storage',
      value: formatBytes(dbStats.storage?.totalBytes ?? dbStats.databaseSize),
      helpText: `DuckDB ${formatBytes(dbStats.storage?.duckdbBytes ?? dbStats.databaseSize)} / Parquet ${formatBytes(dbStats.storage?.parquetBytes ?? 0)}`,
    });
  }

  return items;
}

function buildDomainHealthItems(dbValidation: MarketValidationResponse | undefined): DomainHealthItem[] {
  if (!dbValidation) {
    return [];
  }

  const domains = dbValidation.healthDomains;
  return [
    {
      label: 'Core Daily',
      status: normalizeHealthStatus(domains?.coreDailyStatus, dbValidation.status),
      helpText: 'TOPIX, stock_data, indices, fundamentals, margin, and backtest readiness.',
    },
    {
      label: 'Derivatives',
      status: normalizeHealthStatus(domains?.derivativesStatus),
      helpText: 'Local N225 options coverage against TOPIX trading dates.',
    },
    {
      label: 'Intraday',
      status: normalizeHealthStatus(domains?.intradayStatus),
      helpText: 'Minute bars freshness, separated from daily snapshot health.',
    },
    {
      label: 'Source Quality',
      status: normalizeHealthStatus(domains?.sourceQualityStatus),
      helpText: 'Known source-data diagnostics such as historical UnderPx gaps or split inventory.',
    },
  ];
}

function buildCoverageItems(
  dbStats: MarketStatsResponse,
  dbValidation?: MarketValidationResponse
): SnapshotCoverageItem[] {
  const fundamentalsCoverage = dbStats.fundamentals.listedMarketCoverage;
  const options225 = dbStats.options225 ?? EMPTY_OPTIONS_225_STATS;
  const optionsDisplay = buildOptions225CoverageDisplay(options225, dbStats.topix, dbValidation?.options225);
  return [
    {
      label: 'Stock Data',
      value: dbStats.stockData.dateRange?.max ?? 'n/a',
      meta: [
        `Range: ${formatOptionalDateRange(dbStats.stockData.dateRange)}`,
        `Rows: ${formatCount(dbStats.stockData.count)}`,
        `Trading dates: ${formatCount(dbStats.stockData.dateCount)}`,
        `Average stocks/day: ${formatCount(Math.round(dbStats.stockData.averageStocksPerDay ?? 0))}`,
      ],
    },
    {
      label: 'TOPIX',
      value: dbStats.topix.dateRange?.max ?? 'n/a',
      meta: [`Range: ${formatOptionalDateRange(dbStats.topix.dateRange)}`, `Rows: ${formatCount(dbStats.topix.count)}`],
    },
    {
      label: 'Indices',
      value: dbStats.indices.dateRange?.max ?? 'n/a',
      meta: [
        `Range: ${formatOptionalDateRange(dbStats.indices.dateRange)}`,
        `Rows: ${formatCount(dbStats.indices.dataCount)}`,
        `Master entries: ${formatCount(dbStats.indices.masterCount)}`,
        `Categories: ${formatCategoryBreakdown(dbStats.indices.byCategory)}`,
      ],
    },
    {
      label: 'N225 Options',
      value: optionsDisplay.value,
      meta: [
        optionsDisplay.status,
        `Range: ${formatOptionalDateRange(options225.dateRange)}`,
        `Rows: ${formatCount(options225.count)}`,
        `Trading dates: ${formatCount(options225.dateCount)}`,
      ],
    },
    {
      label: 'Margin',
      value: dbStats.margin.dateRange?.max ?? 'n/a',
      meta: [
        `Range: ${formatOptionalDateRange(dbStats.margin.dateRange)}`,
        `Rows: ${formatCount(dbStats.margin.count)}`,
        `Stocks: ${formatCount(dbStats.margin.uniqueStockCount)}`,
        `Dates: ${formatCount(dbStats.margin.dateCount)}`,
      ],
    },
    {
      label: 'Fundamentals',
      value: dbStats.fundamentals.latestDisclosedDate ?? 'n/a',
      meta: [
        `Statements: ${formatCount(dbStats.fundamentals.count)}`,
        `Covered stocks: ${formatCount(fundamentalsCoverage.coveredStocks)} / ${formatCount(fundamentalsCoverage.listedMarketStocks)} (${formatRatioPercentage(fundamentalsCoverage.coverageRatio, { fallback: '0.0%' })})`,
        `Alias covered: ${formatCount(fundamentalsCoverage.issuerAliasCoveredCount)}`,
        `Deferred/empty: ${formatCount(fundamentalsCoverage.emptySkippedCount)}`,
      ],
    },
  ];
}

function buildSampleHint(
  sampleWindow:
    | {
        returnedCount: number;
        totalCount: number;
        truncated: boolean;
      }
    | null
    | undefined
): string | undefined {
  if (!sampleWindow || sampleWindow.returnedCount <= 0) {
    return undefined;
  }
  if (sampleWindow.truncated) {
    return `Showing ${formatCount(sampleWindow.returnedCount)} of ${formatCount(sampleWindow.totalCount)}.`;
  }
  return `Showing ${formatCount(sampleWindow.returnedCount)}.`;
}

function appendValidationDiagnostic(
  diagnostics: ValidationDiagnostic[],
  value: number | null | undefined,
  diagnostic: Omit<ValidationDiagnostic, 'value'>
): void {
  const normalizedValue = value ?? 0;

  if (normalizedValue <= 0) {
    return;
  }

  diagnostics.push({
    value: normalizedValue,
    ...diagnostic,
  });
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

function buildOptions225CoverageWarning(dbValidation: MarketValidationResponse): ValidationDiagnostic | null {
  const options225 = dbValidation.options225 ?? EMPTY_OPTIONS_225_VALIDATION;
  const sampleWindows = dbValidation.sampleWindows;
  const topixCount = dbValidation.topix?.count ?? 0;
  const topixLatest = dbValidation.topix?.dateRange?.max;
  const optionsLatest = options225.dateRange?.max;
  const missingCoverageCount = options225.missingTopixCoverageDatesCount ?? 0;
  const coverageKind = resolveOptions225CoverageKind({
    initialized: dbValidation.initialized,
    topixCount,
    optionsCount: options225.count ?? 0,
    topixLatest: topixLatest ?? null,
    optionsLatest: optionsLatest ?? null,
    missingCoverageCount,
    coverageStatus: options225.coverageStatus,
  });

  switch (coverageKind) {
    case 'missing':
      return {
        label: 'N225 Options Missing Locally',
        value: 1,
        helpText: `No local N225 options chain is stored yet. Run Database Sync with \`incremental\` to ingest \`options_225_data\` through ${topixLatest ?? 'the latest TOPIX date'}.`,
      };
    case 'stale':
      return {
        label: 'N225 Options Stale',
        value: 1,
        helpText: `Local N225 options data stops at ${optionsLatest ?? 'n/a'} while TOPIX is synced through ${topixLatest ?? 'n/a'}. Run Database Sync with \`incremental\` to refresh \`options_225_data\`.`,
      };
    case 'pending':
      return {
        label: 'N225 Options Pending',
        value: missingCoverageCount,
        helpText: `Local N225 options data is within the ${formatCount(options225.allowedTopixLagDates ?? 1)} TOPIX-date pending window. This is usually a publication timing gap; the next incremental sync should fill it after the source updates.`,
        sampleItems: options225.missingTopixCoverageDates,
        sampleLabel: 'Pending dates',
        sampleHint: buildSampleHint(sampleWindows?.options225MissingTopixCoverageDates),
      };
    case 'partial':
      return {
        label: 'N225 Options Partial Coverage',
        value: missingCoverageCount,
        helpText:
          'Local N225 options latest date matches TOPIX, but historical TOPIX dates are still missing from `options_225_data`. Run Database Sync with `incremental` to backfill local history.',
        sampleItems: options225.missingTopixCoverageDates,
        sampleLabel: 'Sample dates',
        sampleHint: buildSampleHint(sampleWindows?.options225MissingTopixCoverageDates),
      };
    default:
      return null;
  }
}

function buildValidationDiagnosticSections(dbValidation: MarketValidationResponse): ValidationDiagnosticSections {
  const warningDiagnostics: ValidationDiagnostic[] = [];
  const informationalDiagnostics: ValidationDiagnostic[] = [];
  const fundamentals = dbValidation.fundamentals;
  const margin = dbValidation.margin;
  const sampleWindows = dbValidation.sampleWindows;
  const options225 = dbValidation.options225 ?? EMPTY_OPTIONS_225_VALIDATION;

  const options225CoverageWarning = buildOptions225CoverageWarning(dbValidation);
  if (options225CoverageWarning) {
    if (options225.coverageStatus === 'pending') {
      informationalDiagnostics.push(options225CoverageWarning);
    } else {
      warningDiagnostics.push(options225CoverageWarning);
    }
  }

  appendValidationDiagnostic(warningDiagnostics, dbValidation.stockData.missingDatesCount, {
    label: 'Missing Stock Dates',
    helpText: 'Trading dates present in TOPIX but missing from stock_data.',
    sampleItems: dbValidation.stockData.missingDates,
    sampleLabel: 'Sample dates',
    sampleHint: buildSampleHint(sampleWindows?.stockDataMissingDates),
  });

  appendValidationDiagnostic(warningDiagnostics, dbValidation.failedDatesCount, {
    label: 'Failed Sync Dates',
    helpText: 'These dates failed during sync and still need a retry.',
    sampleItems: dbValidation.failedDates,
    sampleLabel: 'Sample dates',
    sampleHint: buildSampleHint(sampleWindows?.failedDates),
  });

  appendValidationDiagnostic(informationalDiagnostics, dbValidation.adjustmentEventsCount, {
    label: 'Adjustment Events',
    helpText: 'Recent split or reverse-split events tracked from stock_data.',
    sampleItems: (dbValidation.adjustmentEvents ?? []).map(
      (event) => `${event.code} ${event.date} (${event.eventType})`
    ),
    sampleLabel: 'Sample events',
    sampleHint: buildSampleHint(sampleWindows?.adjustmentEvents),
  });

  appendValidationDiagnostic(warningDiagnostics, margin.orphanCount, {
    label: 'Margin Orphans',
    helpText: 'margin_data contains codes that are missing from stocks metadata.',
  });

  appendValidationDiagnostic(informationalDiagnostics, options225.missingUnderlyingPriceDatesCount, {
    label: 'N225 UnderPx Missing Dates',
    helpText: 'These option dates exist locally but every contract is missing UnderPx.',
    sampleItems: options225.missingUnderlyingPriceDates,
    sampleLabel: 'Sample dates',
    sampleHint: buildSampleHint(sampleWindows?.options225MissingUnderlyingPriceDates),
  });

  appendValidationDiagnostic(informationalDiagnostics, options225.conflictingUnderlyingPriceDatesCount, {
    label: 'N225 UnderPx Conflicts',
    helpText: 'Multiple distinct UnderPx values were stored for the same trade date.',
    sampleItems: options225.conflictingUnderlyingPriceDates,
    sampleLabel: 'Sample dates',
    sampleHint: buildSampleHint(sampleWindows?.options225ConflictingUnderlyingPriceDates),
  });

  appendValidationDiagnostic(warningDiagnostics, dbValidation.integrityIssuesCount, {
    label: 'Readiness Issues',
    helpText: 'Chart or backtest readiness checks are currently failing.',
    sampleItems: (dbValidation.integrityIssues ?? []).map((issue) => `${issue.code} (${formatCount(issue.count)})`),
    sampleLabel: 'Issue codes',
  });

  appendValidationDiagnostic(warningDiagnostics, fundamentals.missingListedMarketStocksCount, {
    label: 'Missing Listed-Market Fundamentals',
    helpText: 'Repair sync will retry these listed-market issuers.',
    sampleItems: fundamentals.missingListedMarketStocks,
    sampleLabel: 'Sample codes',
    sampleHint: buildSampleHint(sampleWindows?.missingListedMarketStocks),
  });

  appendValidationDiagnostic(informationalDiagnostics, fundamentals.emptySkippedCount, {
    label: 'Unsupported/Empty Fundamentals',
    helpText: 'Suppressed until a newer disclosure frontier is available.',
    sampleItems: fundamentals.emptySkippedCodes,
    sampleLabel: 'Sample codes',
    sampleHint: buildSampleHint(sampleWindows?.fundamentalsEmptySkippedCodes),
  });

  appendValidationDiagnostic(informationalDiagnostics, fundamentals.issuerAliasCoveredCount, {
    label: 'Preferred Alias Covered',
    helpText: 'Preferred-share listed codes already covered by parent issuer statements.',
  });

  appendValidationDiagnostic(informationalDiagnostics, margin.emptySkippedCount, {
    label: 'Unsupported/Empty Margin Codes',
    helpText: 'Suppressed until a newer margin frontier is available.',
    sampleItems: margin.emptySkippedCodes,
    sampleLabel: 'Sample codes',
    sampleHint: buildSampleHint(sampleWindows?.marginEmptySkippedCodes),
  });

  return {
    warningDiagnostics,
    informationalDiagnostics,
  };
}

export function resolveRepairTargets(dbValidation: MarketValidationResponse | undefined): RepairTargets {
  if (!dbValidation) {
    return EMPTY_REPAIR_TARGETS;
  }

  const fundamentals = dbValidation.fundamentals;

  return {
    missingListedMarketFundamentals: fundamentals.missingListedMarketStocksCount ?? 0,
    failedFundamentalsDates: fundamentals.failedDatesCount ?? 0,
    failedFundamentalsCodes: fundamentals.failedCodesCount ?? 0,
  };
}

export function hasRepairTargets(targets: RepairTargets): boolean {
  return (
    targets.missingListedMarketFundamentals > 0 ||
    targets.failedFundamentalsDates > 0 ||
    targets.failedFundamentalsCodes > 0
  );
}

export function sumRepairTargets(targets: RepairTargets): number {
  return targets.missingListedMarketFundamentals + targets.failedFundamentalsDates + targets.failedFundamentalsCodes;
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
