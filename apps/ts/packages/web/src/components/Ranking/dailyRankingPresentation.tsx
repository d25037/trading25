import { cn } from '@/lib/utils';
import { formatPriceJPY, formatTradingValue } from '@/utils/formatters';
import type { EquityRankingItem } from './EquityRankingTable';
import {
  type EvidenceColorTier,
  getCheapValuationPercentileTier,
  getForecastOperatingProfitGrowthTier,
  getForwardPerEvidenceTier,
  getFwdPerPbrValueCompositeTier,
  getLiquidityEvidenceTier,
  getPerEvidenceTier,
  getValuationSignal,
  type ValuationSignal,
} from './rankingEvidenceTiers';
import { type EquityRiskFlag, type EquityTechnicalFlag, formatRiskFlag, formatTechnicalFlag } from './rankingState';

export type DailyRankingMetricKey =
  | 'sectorStrengthScore'
  | 'currentPrice'
  | 'changePercentage'
  | 'sma5AboveCount5d'
  | 'per'
  | 'forwardPer'
  | 'forecastOperatingProfitGrowthRatio'
  | 'psr'
  | 'forwardPsr'
  | 'pbr'
  | 'valueCompositeScore'
  | 'liquidityResidualZ'
  | 'tradingValue';

export interface DailyRankingMetric {
  key: DailyRankingMetricKey;
  label: string;
  title?: string;
  format: (item: EquityRankingItem) => string;
  resolveClassName?: (item: EquityRankingItem) => string | undefined;
}

export function formatDailyRankingTradingValue(value: number | null | undefined): string {
  return value == null ? '-' : formatTradingValue(value);
}

function formatRatio(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return `${value.toFixed(2)}x`;
}

function formatChangePercentage(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(2)}%`;
}

function formatSignedNumber(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(2)}`;
}

function formatSectorStrengthScore(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return value.toFixed(2);
}

function formatSma5AboveCount(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return String(value);
}

function formatScore(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return value.toFixed(2);
}

function getSectorStrengthScoreClass(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return 'text-muted-foreground';
  if (value >= 0.8) return 'bg-green-50 text-green-700 dark:bg-green-950/40 dark:text-green-300';
  if (value <= 0.2) return 'bg-amber-50 text-amber-700 dark:bg-amber-950/40 dark:text-amber-300';
  return 'bg-[var(--app-surface-muted)] text-muted-foreground';
}

function getEvidenceTierClass(tier: EvidenceColorTier): string | undefined {
  if (tier === 'excellent') return 'text-green-600 dark:text-green-400';
  if (tier === 'good') return 'text-sky-600 dark:text-sky-400';
  if (tier === 'light_good') return 'text-cyan-600 dark:text-cyan-400';
  if (tier === 'bad') return 'text-yellow-600 dark:text-yellow-400';
  if (tier === 'very_bad') return 'text-red-600 dark:text-red-400';
  return undefined;
}

function getBadSidePercentileClass(percentile: number | null | undefined): string | undefined {
  if (percentile == null || !Number.isFinite(percentile)) return undefined;
  if (percentile > 0.9) return 'text-purple-700 dark:text-purple-300';
  if (percentile > 0.8) return 'text-red-600 dark:text-red-400';
  return undefined;
}

const POSITIVE_CHANGE_CLASS = 'text-green-600 dark:text-green-400';
const NEGATIVE_CHANGE_CLASS = 'text-red-600 dark:text-red-400';

export const DAILY_RANKING_VALUE_METRICS = [
  {
    key: 'sectorStrengthScore',
    label: 'Sector Strength',
    format: (item) => formatSectorStrengthScore(item.sectorStrengthScore),
    resolveClassName: (item) => getSectorStrengthScoreClass(item.sectorStrengthScore),
  },
  {
    key: 'currentPrice',
    label: '現在値',
    format: (item) => formatPriceJPY(item.currentPrice),
  },
  {
    key: 'changePercentage',
    label: '騰落率',
    format: (item) => formatChangePercentage(item.changePercentage),
    resolveClassName: (item) => ((item.changePercentage ?? 0) >= 0 ? POSITIVE_CHANGE_CLASS : NEGATIVE_CHANGE_CLASS),
  },
  {
    key: 'sma5AboveCount5d',
    label: 'SMA5 5D',
    format: (item) => formatSma5AboveCount(item.sma5AboveCount5d),
  },
  {
    key: 'per',
    label: 'PER',
    format: (item) => formatRatio(item.per),
    resolveClassName: (item) => getEvidenceTierClass(getPerEvidenceTier(item.perPercentile)),
  },
  {
    key: 'forwardPer',
    label: 'Fwd PER',
    format: (item) => formatRatio(item.forwardPer),
    resolveClassName: (item) => getEvidenceTierClass(getForwardPerEvidenceTier(item)),
  },
  {
    key: 'forecastOperatingProfitGrowthRatio',
    label: 'Fwd OP/OP',
    title: '来期予想営業利益 / 実績営業利益',
    format: (item) => formatRatio(item.forecastOperatingProfitGrowthRatio),
    resolveClassName: (item) =>
      getEvidenceTierClass(getForecastOperatingProfitGrowthTier(item.forecastOperatingProfitGrowthRatio)),
  },
  {
    key: 'psr',
    label: 'PSR',
    format: (item) => formatRatio(item.psr),
    resolveClassName: (item) => getBadSidePercentileClass(item.psrPercentile),
  },
  {
    key: 'forwardPsr',
    label: 'Fwd PSR',
    format: (item) => formatRatio(item.forwardPsr),
    resolveClassName: (item) => getBadSidePercentileClass(item.forwardPsrPercentile),
  },
  {
    key: 'pbr',
    label: 'PBR',
    format: (item) => formatRatio(item.pbr),
    resolveClassName: (item) => getEvidenceTierClass(getCheapValuationPercentileTier(item.pbrPercentile)),
  },
  {
    key: 'valueCompositeScore',
    label: 'Value Score',
    title: 'Fwd PER + PBR composite. Higher means lower forward PER and lower PBR versus Prime peers.',
    format: (item) => formatScore(item.valueCompositeScore),
    resolveClassName: (item) => getEvidenceTierClass(getFwdPerPbrValueCompositeTier(item.valueCompositeScore)),
  },
  {
    key: 'liquidityResidualZ',
    label: '流動性Z',
    format: (item) => formatSignedNumber(item.liquidityResidualZ),
    resolveClassName: (item) => getEvidenceTierClass(getLiquidityEvidenceTier(item)),
  },
  {
    key: 'tradingValue',
    label: '売買代金',
    format: (item) => formatDailyRankingTradingValue(item.tradingValue ?? item.tradingValueAverage),
  },
] as const satisfies readonly DailyRankingMetric[];

export function DailyRankingMetricValue({ item, metric }: { item: EquityRankingItem; metric: DailyRankingMetric }) {
  return <span className={metric.resolveClassName?.(item)}>{metric.format(item)}</span>;
}

export function SectorStrengthScoreChip({ value }: { value: number | null | undefined }) {
  return (
    <span
      title="Selected sector strength family score"
      className={cn(
        'inline-flex min-w-[3rem] justify-center rounded px-1.5 py-0.5 text-[10px] font-semibold tabular-nums',
        getSectorStrengthScoreClass(value)
      )}
    >
      {formatSectorStrengthScore(value)}
    </span>
  );
}

function formatLiquidityRegime(value: EquityRankingItem['liquidityRegime']): string {
  if (value === 'neutral_rerating') return 'Neutral Rerating';
  if (value === 'crowded_rerating') return 'Crowded Rerating';
  if (value === 'distribution_stress') return 'Stress';
  if (value === 'stale_liquidity') return 'Stale';
  if (value === 'neutral') return 'Neutral';
  return '-';
}

function getEvidenceTierChipClass(tier: EvidenceColorTier): string {
  if (tier === 'excellent') return 'bg-green-50 text-green-700 dark:bg-green-950/40 dark:text-green-300';
  if (tier === 'good') return 'bg-sky-50 text-sky-700 dark:bg-sky-950/40 dark:text-sky-300';
  if (tier === 'light_good') return 'bg-cyan-50 text-cyan-700 dark:bg-cyan-950/40 dark:text-cyan-300';
  if (tier === 'bad') return 'bg-yellow-50 text-yellow-800 dark:bg-yellow-950/40 dark:text-yellow-300';
  if (tier === 'very_bad') return 'bg-red-50 text-red-700 dark:bg-red-950/40 dark:text-red-300';
  return 'bg-[var(--app-surface-muted)] text-muted-foreground';
}

function getRiskFlagClass(value: EquityRiskFlag): string {
  if (value === 'overheat') return 'bg-purple-50 text-purple-700 dark:bg-purple-950/40 dark:text-purple-300';
  if (value === 'stale_rally_fade') return 'bg-red-50 text-red-700 dark:bg-red-950/40 dark:text-red-300';
  return 'bg-[var(--app-surface-muted)] text-muted-foreground';
}

function getTechnicalFlagClass(value: EquityTechnicalFlag): string {
  if (value === 'atr20_acceleration')
    return 'bg-emerald-50 text-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-300';
  if (value === 'momentum_20_60_top20') return 'bg-sky-50 text-sky-700 dark:bg-sky-950/40 dark:text-sky-300';
  return 'bg-[var(--app-surface-muted)] text-muted-foreground';
}

function formatValuationSignal(value: ValuationSignal): string {
  if (value === 'strong_value_confirmation') return 'Deep Value';
  if (value === 'medium_value_confirmation') return 'Undervalued';
  if (value === 'very_overvalued_warning') return 'Very Overvalued';
  if (value === 'overvalued_warning') return 'Overvalued';
  if (value === 'no_positive_earnings_valuation') return 'No Earnings';
  return value;
}

function getValuationSignalClass(value: ValuationSignal): string {
  if (value === 'strong_value_confirmation')
    return 'bg-green-50 text-green-700 dark:bg-green-950/40 dark:text-green-300';
  if (value === 'medium_value_confirmation') return 'bg-sky-50 text-sky-700 dark:bg-sky-950/40 dark:text-sky-300';
  if (value === 'very_overvalued_warning') return 'bg-red-50 text-red-700 dark:bg-red-950/40 dark:text-red-300';
  if (value === 'overvalued_warning') return 'bg-yellow-50 text-yellow-800 dark:bg-yellow-950/40 dark:text-yellow-300';
  if (value === 'no_positive_earnings_valuation')
    return 'bg-yellow-50 text-yellow-800 dark:bg-yellow-950/40 dark:text-yellow-300';
  return 'bg-[var(--app-surface-muted)] text-muted-foreground';
}

export function DailyRankingRegimeChip({ item }: { item: EquityRankingItem }) {
  return (
    <span
      className={cn(
        'inline-flex min-w-[4.5rem] justify-center whitespace-nowrap rounded px-1.5 py-0.5 text-[10px] font-semibold',
        getEvidenceTierChipClass(getLiquidityEvidenceTier(item))
      )}
    >
      {formatLiquidityRegime(item.liquidityRegime)}
    </span>
  );
}

export function DailyRankingSignalChips({ item }: { item: EquityRankingItem }) {
  const valuationSignal = getValuationSignal(item);
  const signals = [
    ...(valuationSignal
      ? [
          {
            key: valuationSignal,
            label: formatValuationSignal(valuationSignal),
            className: getValuationSignalClass(valuationSignal),
          },
        ]
      : []),
    ...(item.riskFlags ?? []).map((flag) => ({
      key: flag,
      label: formatRiskFlag(flag),
      className: getRiskFlagClass(flag),
    })),
    ...(item.technicalFlags ?? []).map((flag) => ({
      key: flag,
      label: formatTechnicalFlag(flag),
      className: getTechnicalFlagClass(flag),
    })),
  ];

  return (
    <div className="flex flex-wrap justify-center gap-1">
      {signals.map((signal) => (
        <span
          key={signal.key}
          className={cn(
            'inline-flex shrink-0 justify-center whitespace-nowrap rounded px-1.5 py-0.5 text-[10px] font-semibold leading-tight',
            signal.className
          )}
        >
          {signal.label}
        </span>
      ))}
    </div>
  );
}
