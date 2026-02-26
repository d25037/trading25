import type { ApiFundamentalDataPoint } from '@trading25/shared/types/api-types';
import { Fragment } from 'react';
import {
  DEFAULT_FUNDAMENTAL_METRIC_ORDER,
  DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY,
  type FundamentalMetricId,
} from '@/constants/fundamentalMetrics';
import { cn } from '@/lib/utils';
import { type FundamentalColorScheme, getFundamentalColor } from '@/utils/color-schemes';
import { formatFundamentalValue } from '@/utils/formatters';

interface MetricCardProps {
  label: string;
  value: number | null;
  format: 'percent' | 'times' | 'yen' | 'millions';
  colorScheme?: FundamentalColorScheme;
  /** Secondary value to show in parentheses */
  prevValue?: number | null;
  decimals?: number;
}

function formatMetricValue(value: number | null, format: MetricCardProps['format'], decimals?: number): string {
  if (value === null || !Number.isFinite(value)) return '-';
  if (format === 'times' && decimals !== undefined) {
    return `${value.toLocaleString('en-US', {
      maximumFractionDigits: decimals,
      minimumFractionDigits: 0,
    })}x`;
  }
  return formatFundamentalValue(value, format);
}

function MetricCard({ label, value, format, colorScheme = 'neutral', prevValue, decimals }: MetricCardProps) {
  return (
    <div className="flex min-h-16 flex-col justify-center rounded-md bg-background/50 px-2 py-1.5">
      <span className="mb-0.5 text-center text-[10px] uppercase tracking-wide text-muted-foreground leading-tight">
        {label}
      </span>
      <span className={cn('text-center text-sm font-semibold leading-tight', getFundamentalColor(value, colorScheme))}>
        {formatMetricValue(value, format, decimals)}
      </span>
      {prevValue != null && (
        <span className="mt-0.5 text-center text-[10px] text-muted-foreground leading-tight">
          ({formatMetricValue(prevValue, format, decimals)})
        </span>
      )}
    </div>
  );
}

interface ForecastMetricCardProps {
  label: string;
  actualValue: number | null;
  forecastValue?: number | null;
  changeRate?: number | null;
  format: 'percent' | 'yen';
}

function ForecastMetricCard({ label, actualValue, forecastValue, changeRate, format }: ForecastMetricCardProps) {
  const formatChangeRate = (rate: number | null | undefined): string => {
    if (rate === null || rate === undefined) return '';
    const sign = rate >= 0 ? '+' : '';
    return `${sign}${rate.toFixed(1)}%`;
  };

  const getChangeRateColor = (rate: number | null | undefined): string => {
    if (rate === null || rate === undefined) return 'text-muted-foreground';
    if (rate > 0) return 'text-green-500';
    if (rate < 0) return 'text-red-500';
    return 'text-muted-foreground';
  };

  return (
    <div className="flex min-h-16 flex-col justify-center rounded-md bg-background/50 px-2 py-1.5">
      <span className="mb-0.5 text-center text-[10px] uppercase tracking-wide text-muted-foreground leading-tight">
        {label}
      </span>
      <span className="text-center text-sm font-semibold leading-tight text-foreground">
        {formatFundamentalValue(actualValue, format)}
      </span>
      {forecastValue != null && (
        <div className="mt-0.5 flex items-center justify-center gap-1">
          <span className="text-[10px] text-muted-foreground">予: {formatFundamentalValue(forecastValue, format)}</span>
          {changeRate != null && (
            <span className={cn('text-[10px] font-medium', getChangeRateColor(changeRate))}>
              ({formatChangeRate(changeRate)})
            </span>
          )}
        </div>
      )}
    </div>
  );
}

interface FundamentalsSummaryCardProps {
  metrics: ApiFundamentalDataPoint | undefined;
  tradingValuePeriod?: number;
  metricOrder?: FundamentalMetricId[];
  metricVisibility?: Record<FundamentalMetricId, boolean>;
}

function resolveForecastPer(stockPrice: number | null, forecastEps: number | null): number | null {
  if (stockPrice == null || forecastEps == null || forecastEps === 0) return null;
  const forecastPer = stockPrice / forecastEps;
  return Number.isFinite(forecastPer) ? forecastPer : null;
}

export function FundamentalsSummaryCard({
  metrics,
  tradingValuePeriod = 15,
  metricOrder = DEFAULT_FUNDAMENTAL_METRIC_ORDER,
  metricVisibility = DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY,
}: FundamentalsSummaryCardProps) {
  if (!metrics) {
    return (
      <div className="h-full flex items-center justify-center">
        <p className="text-sm text-muted-foreground">No data available</p>
      </div>
    );
  }

  const displayEps = metrics.adjustedEps ?? metrics.eps;
  const displayForecastEps = metrics.revisedForecastEps ?? metrics.adjustedForecastEps ?? metrics.forecastEps;
  const displayForecastPer = resolveForecastPer(metrics.stockPrice, displayForecastEps ?? null);
  const displayBps = metrics.adjustedBps ?? metrics.bps;
  const displayDividendFy = metrics.adjustedDividendFy ?? metrics.dividendFy ?? null;
  const displayForecastDividendFy =
    metrics.adjustedForecastDividendFy ?? metrics.forecastDividendFy ?? null;
  const forecastEpsAboveRecentFyActuals =
    metrics.forecastEpsAboveRecentFyActuals ?? metrics.forecastEpsAboveAllHistoricalActuals;
  const forecastEpsLookbackFyCount = metrics.forecastEpsLookbackFyCount ?? 3;
  const forecastEpsAboveRecentFyActualsLabel =
    forecastEpsAboveRecentFyActuals == null
      ? '-'
      : forecastEpsAboveRecentFyActuals
        ? 'true'
        : 'false';
  const visibleMetricOrder = metricOrder.filter((metricId) => metricVisibility[metricId]);

  const renderMetric = (metricId: FundamentalMetricId) => {
    switch (metricId) {
      case 'per':
        return (
          <MetricCard label="PER" value={metrics.per} format="times" colorScheme="per" prevValue={displayForecastPer} />
        );
      case 'pbr':
        return <MetricCard label="PBR" value={metrics.pbr} format="times" colorScheme="pbr" />;
      case 'roe':
        return <MetricCard label="ROE" value={metrics.roe} format="percent" colorScheme="roe" />;
      case 'roa':
        return <MetricCard label="ROA" value={metrics.roa} format="percent" colorScheme="roe" />;
      case 'eps':
        return (
          <ForecastMetricCard
            label="EPS"
            actualValue={displayEps}
            forecastValue={displayForecastEps}
            changeRate={metrics.forecastEpsChangeRate}
            format="yen"
          />
        );
      case 'bps':
        return <MetricCard label="BPS" value={displayBps} format="yen" />;
      case 'dividendPerShare':
        return (
          <ForecastMetricCard
            label="1株配当"
            actualValue={displayDividendFy}
            forecastValue={displayForecastDividendFy}
            changeRate={metrics.forecastDividendFyChangeRate}
            format="yen"
          />
        );
      case 'payoutRatio':
        return (
          <ForecastMetricCard
            label="配当性向"
            actualValue={metrics.payoutRatio ?? null}
            forecastValue={metrics.forecastPayoutRatio ?? null}
            changeRate={metrics.forecastPayoutRatioChangeRate}
            format="percent"
          />
        );
      case 'operatingMargin':
        return <MetricCard label="営業利益率" value={metrics.operatingMargin} format="percent" />;
      case 'netMargin':
        return <MetricCard label="純利益率" value={metrics.netMargin} format="percent" />;
      case 'cashFlowOperating':
        return (
          <MetricCard
            label="営業CF"
            value={metrics.cashFlowOperating}
            format="millions"
            colorScheme="cashFlow"
            prevValue={metrics.prevCashFlowOperating}
          />
        );
      case 'cashFlowInvesting':
        return (
          <MetricCard
            label="投資CF"
            value={metrics.cashFlowInvesting}
            format="millions"
            prevValue={metrics.prevCashFlowInvesting}
          />
        );
      case 'cashFlowFinancing':
        return (
          <MetricCard
            label="財務CF"
            value={metrics.cashFlowFinancing}
            format="millions"
            prevValue={metrics.prevCashFlowFinancing}
          />
        );
      case 'cashAndEquivalents':
        return (
          <MetricCard
            label="現金"
            value={metrics.cashAndEquivalents}
            format="millions"
            prevValue={metrics.prevCashAndEquivalents}
          />
        );
      case 'fcf':
        return <MetricCard label="FCF" value={metrics.fcf} format="millions" colorScheme="cashFlow" />;
      case 'fcfYield':
        return <MetricCard label="FCF利回り" value={metrics.fcfYield} format="percent" colorScheme="fcfYield" />;
      case 'fcfMargin':
        return <MetricCard label="FCFマージン" value={metrics.fcfMargin} format="percent" colorScheme="fcfMargin" />;
      case 'cfoYield':
        return <MetricCard label="CFO利回り" value={metrics.cfoYield} format="percent" colorScheme="cfoYield" />;
      case 'cfoMargin':
        return <MetricCard label="CFOマージン" value={metrics.cfoMargin} format="percent" colorScheme="cfoMargin" />;
      case 'cfoToNetProfitRatio':
        return <MetricCard label="営業CF/純利益" value={metrics.cfoToNetProfitRatio ?? null} format="times" />;
      case 'tradingValueToMarketCapRatio':
        return (
          <MetricCard
            label={`時価総額/${tradingValuePeriod}日売買代金`}
            value={metrics.tradingValueToMarketCapRatio ?? null}
            format="times"
            decimals={3}
          />
        );
    }
  };

  return (
    <div className="h-full min-h-0 flex flex-col">
      <div className="flex-1 min-h-0 overflow-y-auto">
        <div className="grid grid-cols-8 gap-1.5 p-2">
          {visibleMetricOrder.length > 0 ? (
            visibleMetricOrder.map((metricId) => <Fragment key={metricId}>{renderMetric(metricId)}</Fragment>)
          ) : (
            <div className="col-span-full py-8 text-center text-sm text-muted-foreground">
              表示する指標をサイドバーで選択してください
            </div>
          )}
        </div>
      </div>

      {/* Period info */}
      <div className="shrink-0 px-3 py-2 text-xs text-muted-foreground border-t border-border/30">
        <div className="flex justify-between">
          <span>
            {metrics.periodType} ({metrics.date})
          </span>
          <span>
            {metrics.isConsolidated ? '連結' : '単体'} / {metrics.accountingStandard || 'JGAAP'}
          </span>
        </div>
        <div className="mt-1">
          予想EPS &gt; 直近FY{forecastEpsLookbackFyCount}実績EPS:{' '}
          {forecastEpsAboveRecentFyActualsLabel}
        </div>
        {metrics.stockPrice && <div className="mt-1">株価 @ 開示日: {metrics.stockPrice.toLocaleString()}円</div>}
      </div>
    </div>
  );
}
