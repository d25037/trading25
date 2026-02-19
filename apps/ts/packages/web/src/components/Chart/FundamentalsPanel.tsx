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
  metricOrder?: FundamentalMetricId[];
  metricVisibility?: Record<FundamentalMetricId, boolean>;
}

export function FundamentalsPanel({
  symbol,
  enabled = true,
  tradingValuePeriod = 15,
  metricOrder = DEFAULT_FUNDAMENTAL_METRIC_ORDER,
  metricVisibility = DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY,
}: FundamentalsPanelProps) {
  const { data, isLoading, error } = useFundamentals(symbol, { enabled, tradingValuePeriod });

  // Get the latest FY (full year) data with actual financial data for summary card
  // Then update PER/PBR/stockPrice with latest daily valuation for current prices
  // Also merge forecastEps and prevCashFlow* from latestMetrics (which has enhanced data)
  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: data merging logic with multiple conditional fields
  const latestFyMetrics = useMemo(() => {
    if (!data?.data) return undefined;

    // Find latest FY with actual financial data (not forecast)
    const fyData = data.data.find((d) => isFiscalYear(d.periodType) && hasActualFinancialData(d));

    if (!fyData) return undefined;

    const resolveAdjustedChangeRate = (
      actualEps: number | null | undefined,
      forecastEps: number | null | undefined
    ): number | null => {
      if (actualEps == null || forecastEps == null || actualEps === 0) return null;
      return Math.round(((forecastEps - actualEps) / Math.abs(actualEps)) * 100 * 100) / 100;
    };

    // Start with FY data and merge enhanced fields from latestMetrics
    let result = {
      ...fyData,
      // Merge forecast and previous period data from latestMetrics (API enhances these)
      forecastEps: data.latestMetrics?.forecastEps ?? null,
      adjustedForecastEps: data.latestMetrics?.adjustedForecastEps ?? fyData.adjustedForecastEps ?? null,
      forecastEpsChangeRate: data.latestMetrics?.forecastEpsChangeRate ?? null,
      prevCashFlowOperating: data.latestMetrics?.prevCashFlowOperating ?? null,
      prevCashFlowInvesting: data.latestMetrics?.prevCashFlowInvesting ?? null,
      prevCashFlowFinancing: data.latestMetrics?.prevCashFlowFinancing ?? null,
      prevCashAndEquivalents: data.latestMetrics?.prevCashAndEquivalents ?? null,
      cfoToNetProfitRatio: data.latestMetrics?.cfoToNetProfitRatio ?? fyData.cfoToNetProfitRatio ?? null,
      tradingValueToMarketCapRatio:
        data.latestMetrics?.tradingValueToMarketCapRatio ?? fyData.tradingValueToMarketCapRatio ?? null,
    };

    const displayActualEps = result.adjustedEps ?? result.eps ?? null;
    const displayForecastEps = result.adjustedForecastEps ?? result.forecastEps ?? null;
    const adjustedChangeRate = resolveAdjustedChangeRate(displayActualEps, displayForecastEps);
    if (adjustedChangeRate != null) {
      result = {
        ...result,
        forecastEpsChangeRate: adjustedChangeRate,
      };
    }

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
