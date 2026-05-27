import type { MarketStatsResponse, MarketValidationResponse } from '@trading25/contracts/types/api-response-types';
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

export type StatusTone = 'neutral' | 'accent' | 'success' | 'warning' | 'danger';
export type ValidationHealthStatus = 'healthy' | 'info' | 'warning' | 'error';
export type Options225CoverageKind = 'missing' | 'pending' | 'stale' | 'partial' | 'in_sync';

export interface SnapshotSummaryItem {
  label: string;
  value: string;
  helpText: string;
  tone?: StatusTone;
}

export interface SnapshotCoverageItem {
  label: string;
  value: string;
  meta: string[];
}

export interface DomainHealthItem {
  label: string;
  status: ValidationHealthStatus;
  helpText: string;
}

interface Options225CoverageDisplay {
  value: string;
  status: string;
}

function normalizeHealthStatus(
  value: string | undefined,
  fallback: ValidationHealthStatus = 'healthy'
): ValidationHealthStatus {
  if (value === 'healthy' || value === 'info' || value === 'warning' || value === 'error') {
    return value;
  }
  return fallback;
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

function isDateBefore(lhs: string | null | undefined, rhs: string | null | undefined): boolean {
  if (!lhs || !rhs) {
    return false;
  }
  return lhs < rhs;
}

export function resolveOptions225CoverageKind(params: {
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

export function buildSnapshotSummaryItems(
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

export function buildDomainHealthItems(dbValidation: MarketValidationResponse | undefined): DomainHealthItem[] {
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

export function buildCoverageItems(
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
