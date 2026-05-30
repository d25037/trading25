import type { RankingParams } from '@/types/ranking';

export type EquityRiskFlag = 'overheat' | 'stale_rally_fade';
export type EquityTechnicalFlag = 'atr20_acceleration' | 'momentum_20_60_top20';

export const RANKING_REGIME_STATE_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'neutral_rerating', label: 'Neutral Rerating - All' },
  { value: 'neutral_rerating_good', label: 'Neutral Rerating - Good' },
  { value: 'crowded_rerating', label: 'Crowded Rerating - All' },
  { value: 'crowded_rerating_good', label: 'Crowded Rerating - Good' },
  { value: 'distribution_stress', label: 'Stress' },
  { value: 'stale_liquidity', label: 'Stale' },
  { value: 'neutral', label: 'Neutral' },
] as const satisfies readonly { value: RankingParams['regimeState'] | 'all'; label: string }[];

export const RANKING_RISK_STATE_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'overheat', label: 'Overheat' },
  { value: 'stale_rally_fade', label: 'Rally Fade' },
] as const satisfies readonly { value: RankingParams['riskState'] | 'all'; label: string }[];

export const RANKING_TECHNICAL_STATE_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'atr20_acceleration', label: 'ATR20 Accel' },
  { value: 'momentum_20_60_top20', label: '20/60D Momentum' },
] as const satisfies readonly { value: RankingParams['technicalState'] | 'all'; label: string }[];

export function formatRiskFlag(value: EquityRiskFlag): string {
  if (value === 'overheat') return 'Overheat';
  if (value === 'stale_rally_fade') return 'Rally Fade';
  return value;
}

export function formatTechnicalFlag(value: EquityTechnicalFlag): string {
  if (value === 'atr20_acceleration') return 'ATR Accel';
  if (value === 'momentum_20_60_top20') return '20/60D Mom';
  return value;
}
