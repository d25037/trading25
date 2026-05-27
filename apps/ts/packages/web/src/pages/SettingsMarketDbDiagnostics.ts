import type { MarketValidationResponse } from '@trading25/contracts/types/api-response-types';
import { formatCount } from '@/utils/formatters';
import { resolveOptions225CoverageKind } from './SettingsMarketDbSnapshot';

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

export interface ValidationDiagnostic {
  label: string;
  value: number;
  helpText: string;
  sampleItems?: string[];
  sampleLabel?: string;
  sampleHint?: string;
}

export interface ValidationDiagnosticSections {
  warningDiagnostics: ValidationDiagnostic[];
  informationalDiagnostics: ValidationDiagnostic[];
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

export function buildValidationDiagnosticSections(
  dbValidation: MarketValidationResponse
): ValidationDiagnosticSections {
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
