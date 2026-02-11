import type { ApiFundamentalDataPoint } from '@trading25/shared/types/api-types';
import { cn } from '@/lib/utils';
import { type FundamentalColorScheme, getFundamentalColor } from '@/utils/color-schemes';
import { formatFundamentalValue } from '@/utils/formatters';

interface MetricCardProps {
  label: string;
  value: number | null;
  format: 'percent' | 'times' | 'yen' | 'millions';
  colorScheme?: FundamentalColorScheme;
  /** Previous period value to show in parentheses */
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

interface EpsMetricCardProps {
  actualEps: number | null;
  forecastEps?: number | null;
  changeRate?: number | null;
}

function EpsMetricCard({ actualEps, forecastEps, changeRate }: EpsMetricCardProps) {
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
        EPS
      </span>
      <span className="text-center text-sm font-semibold leading-tight text-foreground">
        {formatFundamentalValue(actualEps, 'yen')}
      </span>
      {forecastEps != null && (
        <div className="mt-0.5 flex items-center justify-center gap-1">
          <span className="text-[10px] text-muted-foreground">予: {formatFundamentalValue(forecastEps, 'yen')}</span>
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
}

export function FundamentalsSummaryCard({ metrics, tradingValuePeriod = 15 }: FundamentalsSummaryCardProps) {
  if (!metrics) {
    return (
      <div className="h-full flex items-center justify-center">
        <p className="text-sm text-muted-foreground">No data available</p>
      </div>
    );
  }

  const displayEps = metrics.adjustedEps ?? metrics.eps;
  const displayForecastEps = metrics.adjustedForecastEps ?? metrics.forecastEps;
  const displayBps = metrics.adjustedBps ?? metrics.bps;

  return (
    <div className="h-full min-h-0 flex flex-col">
      <div className="flex-1 min-h-0 overflow-y-auto">
        <div className="grid grid-cols-8 gap-1.5 p-2">
          <MetricCard label="PER" value={metrics.per} format="times" colorScheme="per" />
          <MetricCard label="PBR" value={metrics.pbr} format="times" colorScheme="pbr" />
          <MetricCard label="ROE" value={metrics.roe} format="percent" colorScheme="roe" />
          <MetricCard label="ROA" value={metrics.roa} format="percent" colorScheme="roe" />

          <EpsMetricCard
            actualEps={displayEps}
            forecastEps={displayForecastEps}
            changeRate={metrics.forecastEpsChangeRate}
          />
          <MetricCard label="BPS" value={displayBps} format="yen" />
          <MetricCard label="営業利益率" value={metrics.operatingMargin} format="percent" />
          <MetricCard label="純利益率" value={metrics.netMargin} format="percent" />

          <MetricCard
            label="営業CF"
            value={metrics.cashFlowOperating}
            format="millions"
            colorScheme="cashFlow"
            prevValue={metrics.prevCashFlowOperating}
          />
          <MetricCard
            label="投資CF"
            value={metrics.cashFlowInvesting}
            format="millions"
            prevValue={metrics.prevCashFlowInvesting}
          />
          <MetricCard
            label="財務CF"
            value={metrics.cashFlowFinancing}
            format="millions"
            prevValue={metrics.prevCashFlowFinancing}
          />
          <MetricCard
            label="現金"
            value={metrics.cashAndEquivalents}
            format="millions"
            prevValue={metrics.prevCashAndEquivalents}
          />

          <MetricCard label="FCF" value={metrics.fcf} format="millions" colorScheme="cashFlow" />
          <MetricCard label="FCF利回り" value={metrics.fcfYield} format="percent" colorScheme="fcfYield" />
          <MetricCard label="FCFマージン" value={metrics.fcfMargin} format="percent" colorScheme="fcfMargin" />

          <MetricCard label="営業CF/純利益" value={metrics.cfoToNetProfitRatio ?? null} format="times" />
          <MetricCard
            label={`時価総額/${tradingValuePeriod}日売買代金`}
            value={metrics.tradingValueToMarketCapRatio ?? null}
            format="times"
            decimals={3}
          />
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
        {metrics.stockPrice && <div className="mt-1">株価 @ 開示日: {metrics.stockPrice.toLocaleString()}円</div>}
      </div>
    </div>
  );
}
