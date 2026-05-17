import type { ApiFundamentalDataPoint } from '@trading25/contracts/types/api-types';
import { useMemo, useState } from 'react';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import {
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER,
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY,
  FUNDAMENTALS_HISTORY_METRIC_DEFINITIONS,
  type FundamentalsHistoryMetricId,
  normalizeFundamentalsHistoryMetricOrder,
  normalizeFundamentalsHistoryMetricVisibility,
} from '@/constants/fundamentalsHistoryMetrics';
import { useFundamentals } from '@/hooks/useFundamentals';
import { cn } from '@/lib/utils';
import { getFundamentalColor } from '@/utils/color-schemes';
import { formatFundamentalValue } from '@/utils/formatters';
import { hasActualFinancialData, isFiscalYear, isQuarterPeriod } from '@/utils/fundamental-analysis';

interface FundamentalsHistoryPanelProps {
  symbol: string | null;
  enabled?: boolean;
  metricOrder?: FundamentalsHistoryMetricId[];
  metricVisibility?: Record<FundamentalsHistoryMetricId, boolean>;
}

interface ForecastEpsFields {
  forecastEps?: number | null;
  adjustedForecastEps?: number | null;
  revisedForecastEps?: number | null;
  revisedForecastSource?: string | null;
}

interface ForecastDividendFields {
  forecastDividendFy?: number | null;
  adjustedForecastDividendFy?: number | null;
}

type HistoryMode = 'fyOnly5' | 'fyAndQuarter10';
type EpsYoYDeltaByPeriodKey = Map<string, number>;

const HISTORY_MODE_OPTIONS: Array<{ mode: HistoryMode; label: string }> = [
  { mode: 'fyOnly5', label: 'FYのみ5期' },
  { mode: 'fyAndQuarter10', label: 'FY+xQ 10回分' },
];

const EMPTY_MESSAGE_BY_MODE: Record<HistoryMode, string> = {
  fyOnly5: '過去のFYデータがありません',
  fyAndQuarter10: '過去のFY/xQデータがありません',
};

/**
 * Format date string (YYYY-MM-DD) to FY label (YYYY/M期).
 */
function formatFyLabel(date: string): string {
  const parts = date.split('-');
  const year = Number.parseInt(parts[0] ?? '', 10);
  const month = Number.parseInt(parts[1] ?? '', 10);
  return `${year}/${month}期`;
}

function formatPeriodLabel(date: string, periodType: string): string {
  const baseLabel = formatFyLabel(date);
  return isFiscalYear(periodType) ? baseLabel : `${baseLabel} (${periodType})`;
}

function compareByDateAndDisclosedDateDesc(
  a: { date: string; disclosedDate: string },
  b: { date: string; disclosedDate: string }
): number {
  const dateComparison = b.date.localeCompare(a.date);
  if (dateComparison !== 0) return dateComparison;
  return b.disclosedDate.localeCompare(a.disclosedDate);
}

function getPriorYearFiscalMonthKey(period: Pick<ApiFundamentalDataPoint, 'date' | 'periodType'>): string | null {
  const [yearPart, monthPart] = period.date.split('-');
  const year = Number.parseInt(yearPart ?? '', 10);
  if (!Number.isFinite(year) || !monthPart) return null;
  return `${period.periodType}:${year - 1}-${monthPart}`;
}

function getFiscalMonthKey(period: Pick<ApiFundamentalDataPoint, 'date' | 'periodType'>): string | null {
  const [yearPart, monthPart] = period.date.split('-');
  const year = Number.parseInt(yearPart ?? '', 10);
  if (!Number.isFinite(year) || !monthPart) return null;
  return `${period.periodType}:${year}-${monthPart}`;
}

function getPeriodComparisonKey(period: Pick<ApiFundamentalDataPoint, 'date' | 'periodType'>): string {
  return `${period.periodType}:${period.date}`;
}

function getPeriodEps(period: Pick<ApiFundamentalDataPoint, 'adjustedEps' | 'eps'>): number | null {
  return period.adjustedEps ?? period.eps ?? null;
}

function getPeriodMetric(
  period: ApiFundamentalDataPoint,
  metricId: 'operatingProfit' | 'operatingMargin'
): number | null {
  return period[metricId] ?? null;
}

function formatSignedEpsDelta(delta: number): string {
  if (delta === 0) return '±0';
  const sign = delta > 0 ? '＋' : '-';
  return `${sign}${formatFundamentalValue(Math.abs(delta), 'yen')}`;
}

function formatSignedPercentDelta(delta: number): string {
  if (Math.abs(delta) < 0.05) return '±0.0%';
  const sign = delta > 0 ? '＋' : '-';
  return `${sign}${Math.abs(delta).toFixed(1)}%`;
}

function formatSignedPointDelta(delta: number): string {
  if (Math.abs(delta) < 0.05) return '±0.0pt';
  const sign = delta > 0 ? '＋' : '-';
  return `${sign}${Math.abs(delta).toFixed(1)}pt`;
}

function getDeltaColor(delta: number): string {
  if (Math.abs(delta) < 0.05) return 'text-muted-foreground';
  if (delta > 0) return 'text-green-500';
  if (delta < 0) return 'text-red-500';
  return 'text-muted-foreground';
}

function getOperatingMarginColor(value: number | null): string {
  if (value == null) return 'text-muted-foreground';
  if (value < 0) return 'text-red-500';
  if (value >= 10) return 'text-green-500';
  if (value >= 5) return 'text-yellow-500';
  return 'text-foreground';
}

function buildEpsYoYDeltaByPeriodKey(periods: ApiFundamentalDataPoint[]): EpsYoYDeltaByPeriodKey {
  const latestPeriodByFiscalMonthKey = new Map<string, ApiFundamentalDataPoint>();
  const sortedPeriods = [...periods].sort(compareByDateAndDisclosedDateDesc);

  for (const period of sortedPeriods) {
    const key = getFiscalMonthKey(period);
    if (key != null && !latestPeriodByFiscalMonthKey.has(key)) {
      latestPeriodByFiscalMonthKey.set(key, period);
    }
  }

  const deltaByKey: EpsYoYDeltaByPeriodKey = new Map();
  for (const period of sortedPeriods) {
    const currentEps = getPeriodEps(period);
    const priorYearFiscalMonthKey = getPriorYearFiscalMonthKey(period);
    if (currentEps == null || priorYearFiscalMonthKey == null) continue;

    const priorPeriod = latestPeriodByFiscalMonthKey.get(priorYearFiscalMonthKey);
    const priorEps = priorPeriod ? getPeriodEps(priorPeriod) : null;
    if (priorEps == null) continue;

    deltaByKey.set(getPeriodComparisonKey(period), currentEps - priorEps);
  }

  return deltaByKey;
}

function buildLatestPeriodByFiscalMonthKey(periods: ApiFundamentalDataPoint[]): Map<string, ApiFundamentalDataPoint> {
  const latestPeriodByFiscalMonthKey = new Map<string, ApiFundamentalDataPoint>();
  const sortedPeriods = [...periods].sort(compareByDateAndDisclosedDateDesc);

  for (const period of sortedPeriods) {
    const key = getFiscalMonthKey(period);
    if (key != null && !latestPeriodByFiscalMonthKey.has(key)) {
      latestPeriodByFiscalMonthKey.set(key, period);
    }
  }

  return latestPeriodByFiscalMonthKey;
}

function calculateMetricYoYDelta(
  currentValue: number,
  priorValue: number,
  mode: 'percentChange' | 'pointDelta'
): number | null {
  if (mode === 'percentChange') {
    if (priorValue === 0) return null;
    return ((currentValue - priorValue) / Math.abs(priorValue)) * 100;
  }
  return currentValue - priorValue;
}

function buildMetricYoYDeltaByPeriodKey(
  periods: ApiFundamentalDataPoint[],
  metricId: 'operatingProfit' | 'operatingMargin',
  mode: 'percentChange' | 'pointDelta'
): EpsYoYDeltaByPeriodKey {
  const sortedPeriods = [...periods].sort(compareByDateAndDisclosedDateDesc);
  const latestPeriodByFiscalMonthKey = buildLatestPeriodByFiscalMonthKey(sortedPeriods);
  const deltaByKey: EpsYoYDeltaByPeriodKey = new Map();
  for (const period of sortedPeriods) {
    const currentValue = getPeriodMetric(period, metricId);
    const priorYearFiscalMonthKey = getPriorYearFiscalMonthKey(period);
    if (currentValue == null || priorYearFiscalMonthKey == null) continue;

    const priorPeriod = latestPeriodByFiscalMonthKey.get(priorYearFiscalMonthKey);
    const priorValue = priorPeriod ? getPeriodMetric(priorPeriod, metricId) : null;
    if (priorValue == null) continue;

    const delta = calculateMetricYoYDelta(currentValue, priorValue, mode);
    if (delta != null) {
      deltaByKey.set(getPeriodComparisonKey(period), delta);
    }
  }

  return deltaByKey;
}

function renderEps(period: ApiFundamentalDataPoint, epsYoYDelta?: number): React.ReactNode {
  const eps = getPeriodEps(period);
  const deltaBadge =
    epsYoYDelta != null ? (
      <span
        className={cn(
          'text-[11px] font-medium',
          epsYoYDelta > 0 ? 'text-green-500' : epsYoYDelta < 0 ? 'text-red-500' : 'text-muted-foreground'
        )}
        title="前年同期比"
      >
        （{formatSignedEpsDelta(epsYoYDelta)}）
      </span>
    ) : null;

  if (deltaBadge == null) return formatFundamentalValue(eps, 'yen');

  return (
    <span className="inline-flex items-baseline justify-end gap-1 whitespace-nowrap">
      <span>{formatFundamentalValue(eps, 'yen')}</span>
      {deltaBadge}
    </span>
  );
}

function renderForecastEps(fy: ForecastEpsFields): React.ReactNode {
  const { forecastEps, adjustedForecastEps, revisedForecastEps, revisedForecastSource } = fy;
  const displayForecastEps = adjustedForecastEps ?? forecastEps ?? null;

  const revisionBadge =
    revisedForecastEps != null ? (
      <span
        className={cn(
          'text-xs ml-1',
          displayForecastEps != null && revisedForecastEps > displayForecastEps ? 'text-green-500' : 'text-red-500'
        )}
      >
        ({revisedForecastSource}: {formatFundamentalValue(revisedForecastEps, 'yen')})
      </span>
    ) : null;

  if (displayForecastEps != null) {
    return (
      <span>
        {formatFundamentalValue(displayForecastEps, 'yen')}
        {revisionBadge}
      </span>
    );
  }

  if (revisedForecastEps != null) {
    return (
      <span className="text-xs text-muted-foreground">
        ({revisedForecastSource}: {formatFundamentalValue(revisedForecastEps, 'yen')})
      </span>
    );
  }

  return formatFundamentalValue(null, 'yen');
}

function renderForecastDividend(fy: ForecastDividendFields): React.ReactNode {
  const displayForecastDividend = fy.adjustedForecastDividendFy ?? fy.forecastDividendFy ?? null;
  return formatFundamentalValue(displayForecastDividend, 'yen');
}

function renderValueWithYoY(
  value: string,
  delta: number | undefined,
  formatDelta: (delta: number) => string
): React.ReactNode {
  if (delta == null) return value;

  return (
    <span className="inline-flex items-baseline justify-end gap-1 whitespace-nowrap">
      <span>{value}</span>
      <span className={cn('text-[11px] font-medium', getDeltaColor(delta))} title="前年同期比">
        （{formatDelta(delta)}）
      </span>
    </span>
  );
}

const FUNDAMENTALS_HISTORY_METRIC_LABEL_BY_ID = Object.fromEntries(
  FUNDAMENTALS_HISTORY_METRIC_DEFINITIONS.map((definition) => [definition.id, definition.label])
) as Record<FundamentalsHistoryMetricId, string>;

function resolveMetricCell(
  period: ApiFundamentalDataPoint,
  metricId: FundamentalsHistoryMetricId,
  epsYoYDelta?: number,
  operatingProfitYoYDelta?: number,
  operatingMarginYoYDelta?: number
): { className: string; content: React.ReactNode } {
  switch (metricId) {
    case 'eps':
      return {
        className: 'text-foreground',
        content: renderEps(period, epsYoYDelta),
      };
    case 'forecastEps':
      return {
        className: 'text-muted-foreground',
        content: renderForecastEps(period),
      };
    case 'netSales':
      return {
        className: 'text-foreground',
        content: formatFundamentalValue(period.netSales, 'millions'),
      };
    case 'operatingProfit': {
      const value = period.operatingProfit;
      const formattedValue = formatFundamentalValue(value, 'millions');
      return {
        className: getFundamentalColor(value, 'cashFlow'),
        content: renderValueWithYoY(formattedValue, operatingProfitYoYDelta, formatSignedPercentDelta),
      };
    }
    case 'forecastOperatingProfit':
      return {
        className: 'text-muted-foreground',
        content: formatFundamentalValue(period.forecastOperatingProfit ?? null, 'millions'),
      };
    case 'operatingMargin':
      return {
        className: getOperatingMarginColor(period.operatingMargin),
        content: renderValueWithYoY(
          formatFundamentalValue(period.operatingMargin, 'percent'),
          operatingMarginYoYDelta,
          formatSignedPointDelta
        ),
      };
    case 'bps':
      return {
        className: 'text-foreground',
        content: formatFundamentalValue(period.adjustedBps ?? period.bps, 'yen'),
      };
    case 'dividendPerShare':
      return {
        className: 'text-foreground',
        content: formatFundamentalValue(period.adjustedDividendFy ?? period.dividendFy ?? null, 'yen'),
      };
    case 'forecastDividendPerShare':
      return {
        className: 'text-muted-foreground',
        content: renderForecastDividend(period),
      };
    case 'payoutRatio':
      return {
        className: 'text-foreground',
        content: formatFundamentalValue(period.payoutRatio ?? null, 'percent'),
      };
    case 'forecastPayoutRatio':
      return {
        className: 'text-muted-foreground',
        content: formatFundamentalValue(period.forecastPayoutRatio ?? null, 'percent'),
      };
    case 'roe':
      return {
        className: 'text-foreground',
        content: formatFundamentalValue(period.roe, 'percent'),
      };
    case 'cashFlowOperating':
      return {
        className: getFundamentalColor(period.cashFlowOperating, 'cashFlow'),
        content: formatFundamentalValue(period.cashFlowOperating, 'millions'),
      };
    case 'cashFlowInvesting':
      return {
        className: getFundamentalColor(period.cashFlowInvesting, 'cashFlow'),
        content: formatFundamentalValue(period.cashFlowInvesting, 'millions'),
      };
    case 'cashFlowFinancing':
      return {
        className: getFundamentalColor(period.cashFlowFinancing, 'cashFlow'),
        content: formatFundamentalValue(period.cashFlowFinancing, 'millions'),
      };
  }
}

export function FundamentalsHistoryPanel({
  symbol,
  enabled = true,
  metricOrder = DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER,
  metricVisibility = DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY,
}: FundamentalsHistoryPanelProps) {
  const [historyMode, setHistoryMode] = useState<HistoryMode>('fyOnly5');
  const { data, isLoading, error } = useFundamentals(symbol, { enabled });

  const historyData = useMemo(() => {
    if (!data?.data) return [];
    const filtered = data.data.filter((d) => {
      if (!hasActualFinancialData(d)) return false;
      if (historyMode === 'fyOnly5') return isFiscalYear(d.periodType);
      return isFiscalYear(d.periodType) || isQuarterPeriod(d.periodType);
    });

    const limit = historyMode === 'fyOnly5' ? 5 : 10;
    return filtered.sort(compareByDateAndDisclosedDateDesc).slice(0, limit);
  }, [data, historyMode]);
  const epsYoYDeltaByPeriodKey = useMemo(() => {
    if (historyMode !== 'fyAndQuarter10' || !data?.data) return new Map<string, number>();
    return buildEpsYoYDeltaByPeriodKey(data.data.filter(hasActualFinancialData));
  }, [data, historyMode]);
  const operatingProfitYoYDeltaByPeriodKey = useMemo(() => {
    if (!data?.data) return new Map<string, number>();
    return buildMetricYoYDeltaByPeriodKey(data.data.filter(hasActualFinancialData), 'operatingProfit', 'percentChange');
  }, [data]);
  const operatingMarginYoYDeltaByPeriodKey = useMemo(() => {
    if (!data?.data) return new Map<string, number>();
    return buildMetricYoYDeltaByPeriodKey(data.data.filter(hasActualFinancialData), 'operatingMargin', 'pointDelta');
  }, [data]);
  const visibleMetricOrder = useMemo(() => {
    const normalizedOrder = normalizeFundamentalsHistoryMetricOrder(metricOrder);
    const normalizedVisibility = normalizeFundamentalsHistoryMetricVisibility(metricVisibility);
    return normalizedOrder.filter((metricId) => normalizedVisibility[metricId]);
  }, [metricOrder, metricVisibility]);

  if (!symbol) {
    return (
      <div className="h-full flex items-center justify-center">
        <p className="text-sm text-muted-foreground">銘柄を選択してください</p>
      </div>
    );
  }

  function normalizeError(err: unknown): Error | null {
    if (err instanceof Error) return err;
    if (err) return new Error('Failed to load fundamentals data');
    return null;
  }

  const normalizedError = normalizeError(error);

  return (
    <div className="h-full min-h-0 flex flex-col">
      <div className="mb-2 flex items-center gap-2 shrink-0">
        {HISTORY_MODE_OPTIONS.map((option) => (
          <button
            key={option.mode}
            type="button"
            onClick={() => setHistoryMode(option.mode)}
            className={cn(
              'px-2.5 py-1.5 text-xs font-medium rounded-md border transition-colors',
              historyMode === option.mode
                ? 'bg-primary text-primary-foreground border-primary'
                : 'bg-background/50 text-muted-foreground border-border/40 hover:bg-background/80 hover:text-foreground'
            )}
            aria-pressed={historyMode === option.mode}
          >
            {option.label}
          </button>
        ))}
      </div>

      <div className="flex-1 min-h-0">
        <DataStateWrapper
          isLoading={isLoading}
          error={normalizedError}
          isEmpty={historyData.length === 0 && !isLoading}
          emptyMessage={EMPTY_MESSAGE_BY_MODE[historyMode]}
          loadingMessage="Loading fundamentals history..."
          height="h-full"
        >
          <div className="h-full overflow-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border/30 text-xs text-muted-foreground">
                  <th className="text-left py-1.5 px-2.5 font-medium">期別</th>
                  {visibleMetricOrder.map((metricId) => (
                    <th key={metricId} className="text-right py-1.5 px-2.5 font-medium">
                      {FUNDAMENTALS_HISTORY_METRIC_LABEL_BY_ID[metricId]}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {visibleMetricOrder.length === 0 ? (
                  <tr>
                    <td colSpan={1} className="py-8 px-3 text-center text-sm text-muted-foreground">
                      表示する指標をサイドバーで選択してください
                    </td>
                  </tr>
                ) : (
                  historyData.map((period) => (
                    <tr
                      key={`${period.date}-${period.disclosedDate}-${period.periodType}`}
                      className="border-b border-border/20 hover:bg-background/40"
                    >
                      <td className="py-1.5 px-2.5 font-medium text-foreground">
                        <span className="block whitespace-nowrap">
                          {formatPeriodLabel(period.date, period.periodType)}
                        </span>
                        <span className="block text-xs font-normal text-muted-foreground">{period.disclosedDate}</span>
                      </td>
                      {visibleMetricOrder.map((metricId) => {
                        const cell = resolveMetricCell(
                          period,
                          metricId,
                          epsYoYDeltaByPeriodKey.get(getPeriodComparisonKey(period)),
                          operatingProfitYoYDeltaByPeriodKey.get(getPeriodComparisonKey(period)),
                          operatingMarginYoYDeltaByPeriodKey.get(getPeriodComparisonKey(period))
                        );
                        return (
                          <td key={metricId} className={cn('py-1.5 px-2.5 text-right', cell.className)}>
                            {cell.content}
                          </td>
                        );
                      })}
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </DataStateWrapper>
      </div>
    </div>
  );
}
