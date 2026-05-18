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

type FundamentalsViewData = {
  data: ApiFundamentalDataPoint[];
  latestMetrics?: ApiFundamentalDataPoint | null;
};

function findLatestFiscalYearWithActualData(rows: ApiFundamentalDataPoint[]): ApiFundamentalDataPoint | undefined {
  return rows.find((item) => isFiscalYear(item.periodType) && hasActualFinancialData(item));
}

function resolveLatestFyMetrics(data: FundamentalsViewData | undefined): ApiFundamentalDataPoint | undefined {
  return data?.latestMetrics ?? findLatestFiscalYearWithActualData(data?.data ?? []);
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

  // Financial metrics are backend-owned; frontend only selects the response row to display.
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
