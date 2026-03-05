import type { ApiFundamentalDataPoint } from '@trading25/contracts/types/api-types';
import { useMemo } from 'react';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import {
  DEFAULT_FUNDAMENTAL_METRIC_ORDER,
  DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY,
  type FundamentalMetricId,
} from '@/constants/fundamentalMetrics';
import { useFundamentals } from '@/hooks/useFundamentals';
import { hasActualFinancialData, isFiscalYear } from '@/utils/fundamental-analysis';
import { FundamentalsSummaryCard } from './FundamentalsSummaryCard';

interface FundamentalsPanelProps {
  symbol: string | null;
  enabled?: boolean;
  tradingValuePeriod?: number;
  forecastEpsLookbackFyCount?: number;
  metricOrder?: FundamentalMetricId[];
  metricVisibility?: Record<FundamentalMetricId, boolean>;
}

type DailyValuationPoint = {
  per?: number | null;
  pbr?: number | null;
  close?: number | null;
};

type FundamentalsViewData = {
  data: ApiFundamentalDataPoint[];
  latestMetrics?: ApiFundamentalDataPoint | null;
  dailyValuation?: DailyValuationPoint[] | null;
  forecastEpsLookbackFyCount?: number | null;
};

function resolveChangeRate(
  actualValue: number | null | undefined,
  forecastValue: number | null | undefined
): number | null {
  if (actualValue == null || forecastValue == null || actualValue === 0) return null;
  return Math.round(((forecastValue - actualValue) / Math.abs(actualValue)) * 100 * 100) / 100;
}

function getSortedFiscalYearRows(rows: ApiFundamentalDataPoint[]): ApiFundamentalDataPoint[] {
  return [...rows]
    .filter((item) => isFiscalYear(item.periodType))
    .sort((a, b) => {
      if (a.date === b.date) return b.disclosedDate.localeCompare(a.disclosedDate);
      return b.date.localeCompare(a.date);
    });
}

function collectRecentFiscalYearActualEps(rows: ApiFundamentalDataPoint[], lookbackFyCount: number): number[] {
  const recentActuals: number[] = [];
  const seenPeriodEnds = new Set<string>();

  for (const row of rows) {
    if (seenPeriodEnds.has(row.date)) continue;
    const value = row.adjustedEps ?? row.eps;
    if (typeof value !== 'number' || !Number.isFinite(value)) continue;
    seenPeriodEnds.add(row.date);
    recentActuals.push(value);
    if (recentActuals.length >= lookbackFyCount) break;
  }

  return recentActuals;
}

function resolveForecastEpsAboveRecentFyActuals(
  rows: ApiFundamentalDataPoint[],
  forecastValue: number | null | undefined,
  lookbackFyCount: number
): boolean | null {
  if (forecastValue == null || !Number.isFinite(forecastValue)) return null;

  const recentActuals = collectRecentFiscalYearActualEps(getSortedFiscalYearRows(rows), lookbackFyCount);
  if (recentActuals.length < lookbackFyCount) return null;

  return forecastValue > Math.max(...recentActuals);
}

function findLatestFiscalYearWithActualData(rows: ApiFundamentalDataPoint[]): ApiFundamentalDataPoint | undefined {
  return rows.find((item) => isFiscalYear(item.periodType) && hasActualFinancialData(item));
}

function firstNonNull<T>(...values: Array<T | null | undefined>): T | null {
  for (const value of values) {
    if (value != null) {
      return value;
    }
  }
  return null;
}

function resolveForecastEpsFields(
  fyData: ApiFundamentalDataPoint,
  latestMetrics: ApiFundamentalDataPoint | null | undefined
) {
  const revisedForecastEps = firstNonNull(latestMetrics?.revisedForecastEps, fyData.revisedForecastEps);
  const revisedForecastSource = firstNonNull(latestMetrics?.revisedForecastSource, fyData.revisedForecastSource);
  const forecastEps = firstNonNull(revisedForecastEps, latestMetrics?.forecastEps);
  const adjustedForecastEps =
    revisedForecastEps != null ? null : firstNonNull(latestMetrics?.adjustedForecastEps, fyData.adjustedForecastEps);

  return {
    forecastEps,
    adjustedForecastEps,
    revisedForecastEps,
    revisedForecastSource,
  };
}

function resolveForecastAboveRecentFlag(
  fyData: ApiFundamentalDataPoint,
  latestMetrics: ApiFundamentalDataPoint | null | undefined
): boolean | null {
  return firstNonNull(
    latestMetrics?.forecastEpsAboveRecentFyActuals,
    latestMetrics?.forecastEpsAboveAllHistoricalActuals,
    fyData.forecastEpsAboveRecentFyActuals,
    fyData.forecastEpsAboveAllHistoricalActuals
  );
}

function mergeLatestMetrics(
  fyData: ApiFundamentalDataPoint,
  latestMetrics: ApiFundamentalDataPoint | null | undefined,
  forecastEpsLookbackFyCount: number
): ApiFundamentalDataPoint {
  const forecastEpsFields = resolveForecastEpsFields(fyData, latestMetrics);

  return {
    ...fyData,
    forecastEps: forecastEpsFields.forecastEps,
    adjustedForecastEps: forecastEpsFields.adjustedForecastEps,
    forecastEpsChangeRate: latestMetrics?.forecastEpsChangeRate ?? null,
    revisedForecastEps: forecastEpsFields.revisedForecastEps,
    revisedForecastSource: forecastEpsFields.revisedForecastSource,
    forecastDividendFy: latestMetrics?.forecastDividendFy ?? null,
    adjustedForecastDividendFy: latestMetrics?.adjustedForecastDividendFy ?? fyData.adjustedForecastDividendFy ?? null,
    forecastDividendFyChangeRate: latestMetrics?.forecastDividendFyChangeRate ?? null,
    payoutRatio: latestMetrics?.payoutRatio ?? fyData.payoutRatio ?? null,
    forecastPayoutRatio: latestMetrics?.forecastPayoutRatio ?? null,
    forecastPayoutRatioChangeRate: latestMetrics?.forecastPayoutRatioChangeRate ?? null,
    prevCashFlowOperating: latestMetrics?.prevCashFlowOperating ?? null,
    prevCashFlowInvesting: latestMetrics?.prevCashFlowInvesting ?? null,
    prevCashFlowFinancing: latestMetrics?.prevCashFlowFinancing ?? null,
    prevCashAndEquivalents: latestMetrics?.prevCashAndEquivalents ?? null,
    cfoToNetProfitRatio: latestMetrics?.cfoToNetProfitRatio ?? fyData.cfoToNetProfitRatio ?? null,
    tradingValueToMarketCapRatio:
      latestMetrics?.tradingValueToMarketCapRatio ?? fyData.tradingValueToMarketCapRatio ?? null,
    forecastEpsAboveRecentFyActuals: resolveForecastAboveRecentFlag(fyData, latestMetrics),
    forecastEpsLookbackFyCount,
  };
}

function applyForecastChangeRates(metrics: ApiFundamentalDataPoint): ApiFundamentalDataPoint {
  let nextMetrics = metrics;

  const displayActualEps = metrics.adjustedEps ?? metrics.eps ?? null;
  const displayForecastEps = metrics.revisedForecastEps ?? metrics.adjustedForecastEps ?? metrics.forecastEps ?? null;
  const epsChangeRate = resolveChangeRate(displayActualEps, displayForecastEps);
  if (epsChangeRate != null) {
    nextMetrics = { ...nextMetrics, forecastEpsChangeRate: epsChangeRate };
  }

  const displayActualDividend = metrics.adjustedDividendFy ?? metrics.dividendFy ?? null;
  const displayForecastDividend = metrics.adjustedForecastDividendFy ?? metrics.forecastDividendFy ?? null;
  const dividendChangeRate = resolveChangeRate(displayActualDividend, displayForecastDividend);
  if (dividendChangeRate != null) {
    nextMetrics = { ...nextMetrics, forecastDividendFyChangeRate: dividendChangeRate };
  }

  const payoutChangeRate = resolveChangeRate(metrics.payoutRatio ?? null, metrics.forecastPayoutRatio ?? null);
  if (payoutChangeRate != null) {
    nextMetrics = { ...nextMetrics, forecastPayoutRatioChangeRate: payoutChangeRate };
  }

  return nextMetrics;
}

function applyForecastFlagFallback(
  metrics: ApiFundamentalDataPoint,
  rows: ApiFundamentalDataPoint[]
): ApiFundamentalDataPoint {
  if (metrics.forecastEpsAboveRecentFyActuals != null) {
    return metrics;
  }

  const displayForecastEps = metrics.revisedForecastEps ?? metrics.adjustedForecastEps ?? metrics.forecastEps ?? null;
  return {
    ...metrics,
    forecastEpsAboveRecentFyActuals: resolveForecastEpsAboveRecentFyActuals(
      rows,
      displayForecastEps,
      metrics.forecastEpsLookbackFyCount ?? 3
    ),
  };
}

function applyLatestDailyValuation(
  metrics: ApiFundamentalDataPoint,
  dailyValuation: DailyValuationPoint[] | null | undefined
): ApiFundamentalDataPoint {
  const latestDaily = dailyValuation?.[dailyValuation.length - 1];
  if (!latestDaily) {
    return metrics;
  }

  return {
    ...metrics,
    per: latestDaily.per ?? null,
    pbr: latestDaily.pbr ?? null,
    stockPrice: latestDaily.close ?? null,
  };
}

function resolveLatestFyMetrics(data: FundamentalsViewData | undefined): ApiFundamentalDataPoint | undefined {
  if (!data?.data || data.data.length === 0) {
    return undefined;
  }

  const latestFyData = findLatestFiscalYearWithActualData(data.data);
  if (!latestFyData) {
    return undefined;
  }

  const lookbackCount = data.forecastEpsLookbackFyCount ?? 3;
  const merged = mergeLatestMetrics(latestFyData, data.latestMetrics, lookbackCount);
  const withRates = applyForecastChangeRates(merged);
  const withForecastFlag = applyForecastFlagFallback(withRates, data.data);
  return applyLatestDailyValuation(withForecastFlag, data.dailyValuation);
}

export function FundamentalsPanel({
  symbol,
  enabled = true,
  tradingValuePeriod = 15,
  forecastEpsLookbackFyCount = 3,
  metricOrder = DEFAULT_FUNDAMENTAL_METRIC_ORDER,
  metricVisibility = DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY,
}: FundamentalsPanelProps) {
  const { data, isLoading, error } = useFundamentals(symbol, {
    enabled,
    tradingValuePeriod,
    forecastEpsLookbackFyCount,
  });

  // Get the latest FY (full year) data with actual financial data for summary card
  // Then update PER/PBR/stockPrice with latest daily valuation for current prices
  // Also merge forecastEps and prevCashFlow* from latestMetrics (which has enhanced data)
  const latestFyMetrics = useMemo(() => resolveLatestFyMetrics(data), [data]);

  if (!symbol) {
    return (
      <div className="h-full flex items-center justify-center">
        <p className="text-sm text-muted-foreground">銘柄を選択してください</p>
      </div>
    );
  }

  const normalizedError = error instanceof Error ? error : error ? new Error('Failed to load fundamentals data') : null;

  return (
    <DataStateWrapper
      isLoading={isLoading}
      error={normalizedError}
      isEmpty={!data || data.data.length === 0}
      emptyMessage="No fundamentals data available"
      loadingMessage="Loading fundamentals data..."
      height="h-full"
    >
      {data && (
        <div className="h-full rounded-lg bg-background/30">
          <FundamentalsSummaryCard
            metrics={latestFyMetrics}
            tradingValuePeriod={data.tradingValuePeriod ?? tradingValuePeriod}
            metricOrder={metricOrder}
            metricVisibility={metricVisibility}
          />
        </div>
      )}
    </DataStateWrapper>
  );
}
