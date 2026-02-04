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
}

function MetricCard({ label, value, format, colorScheme = 'neutral', prevValue }: MetricCardProps) {
  return (
    <div className="flex flex-col items-center p-3 rounded-lg bg-background/50">
      <span className="text-xs text-muted-foreground uppercase tracking-wider mb-1">{label}</span>
      <span className={cn('text-xl font-bold', getFundamentalColor(value, colorScheme))}>
        {formatFundamentalValue(value, format)}
      </span>
      {prevValue != null && (
        <span className="text-xs text-muted-foreground mt-0.5">({formatFundamentalValue(prevValue, format)})</span>
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
    <div className="flex flex-col items-center p-3 rounded-lg bg-background/50">
      <span className="text-xs text-muted-foreground uppercase tracking-wider mb-1">EPS</span>
      <span className="text-xl font-bold text-foreground">{formatFundamentalValue(actualEps, 'yen')}</span>
      {forecastEps != null && (
        <div className="flex items-center gap-1 mt-0.5">
          <span className="text-xs text-muted-foreground">予: {formatFundamentalValue(forecastEps, 'yen')}</span>
          {changeRate != null && (
            <span className={cn('text-xs font-medium', getChangeRateColor(changeRate))}>
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
}

export function FundamentalsSummaryCard({ metrics }: FundamentalsSummaryCardProps) {
  if (!metrics) {
    return (
      <div className="h-full flex items-center justify-center">
        <p className="text-sm text-muted-foreground">No data available</p>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col">
      <div className="grid grid-cols-4 gap-2 p-2">
        {/* Row 1: Core Metrics */}
        <MetricCard label="PER" value={metrics.per} format="times" colorScheme="per" />
        <MetricCard label="PBR" value={metrics.pbr} format="times" colorScheme="pbr" />
        <MetricCard label="ROE" value={metrics.roe} format="percent" colorScheme="roe" />
        <MetricCard label="ROA" value={metrics.roa} format="percent" colorScheme="roe" />

        {/* Row 2: Per Share & Margins - EPS with forecast */}
        <EpsMetricCard
          actualEps={metrics.eps}
          forecastEps={metrics.forecastEps}
          changeRate={metrics.forecastEpsChangeRate}
        />
        <MetricCard label="BPS" value={metrics.bps} format="yen" />
        <MetricCard label="営業利益率" value={metrics.operatingMargin} format="percent" />
        <MetricCard label="純利益率" value={metrics.netMargin} format="percent" />

        {/* Row 3: Cash Flow with previous period values */}
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

        {/* Row 4: FCF Metrics */}
        <MetricCard label="FCF" value={metrics.fcf} format="millions" colorScheme="cashFlow" />
        <MetricCard label="FCF利回り" value={metrics.fcfYield} format="percent" colorScheme="fcfYield" />
        <MetricCard label="FCFマージン" value={metrics.fcfMargin} format="percent" colorScheme="fcfMargin" />
        <MetricCard label="" value={null} format="yen" />
      </div>

      {/* Period info */}
      <div className="px-3 py-2 text-xs text-muted-foreground border-t border-border/30">
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
