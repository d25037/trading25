import type { RankingParams } from '@/types/ranking';

export type EquityRiskFlag = 'overheat' | 'stale_rally_fade';

export const RANKING_LIQUIDITY_STATE_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'neutral_rerating', label: 'Neutral Rerating' },
  { value: 'crowded_rerating', label: 'Crowded Rerating' },
  { value: 'distribution_stress', label: 'Stress' },
  { value: 'stale_liquidity', label: 'Stale' },
  { value: 'neutral', label: 'Neutral' },
  { value: 'overheat', label: 'Overheat' },
  { value: 'stale_rally_fade', label: 'Rally Fade' },
] as const satisfies readonly { value: RankingParams['liquidityState'] | 'all'; label: string }[];

export function formatRiskFlag(value: EquityRiskFlag): string {
  if (value === 'overheat') return 'Overheat';
  if (value === 'stale_rally_fade') return 'Rally Fade';
  return value;
}
