import type { ApiFundamentalDataPoint } from '@trading25/shared/types/api-types';
import { useMemo, useState } from 'react';
import {
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER,
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY,
  FUNDAMENTALS_HISTORY_METRIC_DEFINITIONS,
  type FundamentalsHistoryMetricId,
} from '@/constants/fundamentalsHistoryMetrics';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
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

const FUNDAMENTALS_HISTORY_METRIC_LABEL_BY_ID = Object.fromEntries(
  FUNDAMENTALS_HISTORY_METRIC_DEFINITIONS.map((definition) => [definition.id, definition.label])
) as Record<FundamentalsHistoryMetricId, string>;

function resolveMetricCell(
  period: ApiFundamentalDataPoint,
  metricId: FundamentalsHistoryMetricId
): { className: string; content: React.ReactNode } {
  switch (metricId) {
    case 'eps':
      return {
        className: 'text-foreground',
        content: formatFundamentalValue(period.adjustedEps ?? period.eps, 'yen'),
      };
    case 'forecastEps':
      return {
        className: 'text-muted-foreground',
        content: renderForecastEps(period),
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
  const visibleMetricOrder = useMemo(
    () => metricOrder.filter((metricId) => metricVisibility[metricId]),
    [metricOrder, metricVisibility]
  );

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
      <div className="mb-3 flex items-center gap-2 shrink-0">
        {HISTORY_MODE_OPTIONS.map((option) => (
          <button
            key={option.mode}
            type="button"
            onClick={() => setHistoryMode(option.mode)}
            className={cn(
              'px-3 py-1.5 text-xs font-medium rounded-md border transition-colors',
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
                  <th className="text-left py-2 px-3 font-medium">期別</th>
                  <th className="text-left py-2 px-3 font-medium">発表日</th>
                  {visibleMetricOrder.map((metricId) => (
                    <th key={metricId} className="text-right py-2 px-3 font-medium">
                      {FUNDAMENTALS_HISTORY_METRIC_LABEL_BY_ID[metricId]}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {visibleMetricOrder.length === 0 ? (
                  <tr>
                    <td colSpan={2} className="py-8 px-3 text-center text-sm text-muted-foreground">
                      表示する指標をサイドバーで選択してください
                    </td>
                  </tr>
                ) : (
                  historyData.map((period) => (
                    <tr
                      key={`${period.date}-${period.disclosedDate}-${period.periodType}`}
                      className="border-b border-border/20 hover:bg-background/40"
                    >
                      <td className="py-2.5 px-3 font-medium text-foreground">
                        {formatPeriodLabel(period.date, period.periodType)}
                      </td>
                      <td className="py-2.5 px-3 text-xs text-muted-foreground">{period.disclosedDate}</td>
                      {visibleMetricOrder.map((metricId) => {
                        const cell = resolveMetricCell(period, metricId);
                        return (
                          <td key={metricId} className={cn('py-2.5 px-3 text-right', cell.className)}>
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
