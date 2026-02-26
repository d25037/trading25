import type { ApiFundamentalDataPoint } from '@trading25/shared/types/api-types';
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
  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: data merging logic with multiple conditional fields
  const latestFyMetrics = useMemo(() => {
    if (!data?.data) return undefined;

    // Find latest FY with actual financial data (not forecast)
    const fyData = data.data.find((d) => isFiscalYear(d.periodType) && hasActualFinancialData(d));

    if (!fyData) return undefined;

    // Start with FY data and merge enhanced fields from latestMetrics
    const revisedForecastEps = data.latestMetrics?.revisedForecastEps ?? fyData.revisedForecastEps ?? null;
    const revisedForecastSource = data.latestMetrics?.revisedForecastSource ?? fyData.revisedForecastSource ?? null;
    const forecastEps = revisedForecastEps ?? data.latestMetrics?.forecastEps ?? null;
    const adjustedForecastEps =
      revisedForecastEps != null ? null : data.latestMetrics?.adjustedForecastEps ?? fyData.adjustedForecastEps ?? null;

    let result = {
      ...fyData,
      // Merge forecast and previous period data from latestMetrics (API enhances these)
      forecastEps,
      adjustedForecastEps,
      forecastEpsChangeRate: data.latestMetrics?.forecastEpsChangeRate ?? null,
      revisedForecastEps,
      revisedForecastSource,
      forecastDividendFy: data.latestMetrics?.forecastDividendFy ?? null,
      adjustedForecastDividendFy:
        data.latestMetrics?.adjustedForecastDividendFy ?? fyData.adjustedForecastDividendFy ?? null,
      forecastDividendFyChangeRate: data.latestMetrics?.forecastDividendFyChangeRate ?? null,
      payoutRatio: data.latestMetrics?.payoutRatio ?? fyData.payoutRatio ?? null,
      forecastPayoutRatio: data.latestMetrics?.forecastPayoutRatio ?? null,
      forecastPayoutRatioChangeRate: data.latestMetrics?.forecastPayoutRatioChangeRate ?? null,
      prevCashFlowOperating: data.latestMetrics?.prevCashFlowOperating ?? null,
      prevCashFlowInvesting: data.latestMetrics?.prevCashFlowInvesting ?? null,
      prevCashFlowFinancing: data.latestMetrics?.prevCashFlowFinancing ?? null,
      prevCashAndEquivalents: data.latestMetrics?.prevCashAndEquivalents ?? null,
      cfoToNetProfitRatio: data.latestMetrics?.cfoToNetProfitRatio ?? fyData.cfoToNetProfitRatio ?? null,
      tradingValueToMarketCapRatio:
        data.latestMetrics?.tradingValueToMarketCapRatio ?? fyData.tradingValueToMarketCapRatio ?? null,
      forecastEpsAboveRecentFyActuals:
        data.latestMetrics?.forecastEpsAboveRecentFyActuals ??
        data.latestMetrics?.forecastEpsAboveAllHistoricalActuals ??
        fyData.forecastEpsAboveRecentFyActuals ??
        fyData.forecastEpsAboveAllHistoricalActuals ??
        null,
      forecastEpsLookbackFyCount: data.forecastEpsLookbackFyCount ?? 3,
    };

    const displayActualEps = result.adjustedEps ?? result.eps ?? null;
    const displayForecastEps = result.revisedForecastEps ?? result.adjustedForecastEps ?? result.forecastEps ?? null;
    const epsChangeRate = resolveChangeRate(displayActualEps, displayForecastEps);
    if (epsChangeRate != null) {
      result = {
        ...result,
        forecastEpsChangeRate: epsChangeRate,
      };
    }

    const displayActualDividend = result.adjustedDividendFy ?? result.dividendFy ?? null;
    const displayForecastDividend =
      result.adjustedForecastDividendFy ?? result.forecastDividendFy ?? null;
    const dividendChangeRate = resolveChangeRate(displayActualDividend, displayForecastDividend);
    if (dividendChangeRate != null) {
      result = {
        ...result,
        forecastDividendFyChangeRate: dividendChangeRate,
      };
    }

    const payoutChangeRate = resolveChangeRate(result.payoutRatio ?? null, result.forecastPayoutRatio ?? null);
    if (payoutChangeRate != null) {
      result = {
        ...result,
        forecastPayoutRatioChangeRate: payoutChangeRate,
      };
    }

    result = {
      ...result,
      forecastEpsAboveRecentFyActuals:
        result.forecastEpsAboveRecentFyActuals ??
        resolveForecastEpsAboveRecentFyActuals(
          data.data,
          displayForecastEps,
          result.forecastEpsLookbackFyCount ?? 3
        ),
    };

    // Update with latest daily valuation (current stock price PER/PBR)
    const dailyValuation = data.dailyValuation;
    if (dailyValuation && dailyValuation.length > 0) {
      const latestDaily = dailyValuation[dailyValuation.length - 1];
      if (latestDaily) {
        result = {
          ...result,
          per: latestDaily.per,
          pbr: latestDaily.pbr,
          stockPrice: latestDaily.close,
        };
      }
    }

    return result;
  }, [data]);

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
