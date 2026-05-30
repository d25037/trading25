import type { RankingParams } from '@/types/ranking';

export type EquityRiskFlag = 'overheat' | 'stale_rally_fade';
export type EquityTechnicalFlag = 'atr20_acceleration' | 'momentum_20_60_top20';
export type RankingPreset =
  | 'all'
  | 'momentum_value'
  | 'neutral_rerating'
  | 'neutral_rerating_good'
  | 'crowded_momentum'
  | 'crowded_rerating'
  | 'crowded_rerating_good'
  | 'overheat'
  | 'rally_fade'
  | 'custom';

type RankingFilterParams = Pick<RankingParams, 'liquidityState' | 'regimeState' | 'riskState' | 'technicalState'>;

export const RANKING_PRESET_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'momentum_value', label: 'Momentum Value' },
  { value: 'neutral_rerating_good', label: 'Neutral Good' },
  { value: 'neutral_rerating', label: 'Neutral All' },
  { value: 'crowded_momentum', label: 'Crowded Momentum' },
  { value: 'crowded_rerating_good', label: 'Crowded Good' },
  { value: 'crowded_rerating', label: 'Crowded All' },
  { value: 'overheat', label: 'Overheat' },
  { value: 'rally_fade', label: 'Rally Fade' },
  { value: 'custom', label: 'Custom' },
] as const satisfies readonly { value: RankingPreset; label: string }[];

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

const RANKING_PRESET_FILTERS = {
  all: {},
  momentum_value: {
    regimeState: 'neutral_rerating_good',
    technicalState: 'momentum_20_60_top20',
  },
  neutral_rerating: {
    regimeState: 'neutral_rerating',
  },
  neutral_rerating_good: {
    regimeState: 'neutral_rerating_good',
  },
  crowded_momentum: {
    regimeState: 'crowded_rerating',
    technicalState: 'momentum_20_60_top20',
  },
  crowded_rerating: {
    regimeState: 'crowded_rerating',
  },
  crowded_rerating_good: {
    regimeState: 'crowded_rerating_good',
  },
  overheat: {
    riskState: 'overheat',
  },
  rally_fade: {
    riskState: 'stale_rally_fade',
  },
} as const satisfies Record<Exclude<RankingPreset, 'custom'>, RankingFilterParams>;

export function applyRankingPreset(params: RankingParams, preset: RankingPreset): RankingParams {
  if (preset === 'custom') return params;

  return {
    ...params,
    liquidityState: undefined,
    regimeState: undefined,
    riskState: undefined,
    technicalState: undefined,
    ...RANKING_PRESET_FILTERS[preset],
  };
}

export function getRankingPreset(params: RankingParams): RankingPreset {
  for (const [preset, filters] of Object.entries(RANKING_PRESET_FILTERS)) {
    if (hasMatchingPresetFilters(params, filters)) {
      return preset as RankingPreset;
    }
  }
  return 'custom';
}

function hasMatchingPresetFilters(params: RankingParams, filters: RankingFilterParams): boolean {
  return (
    params.liquidityState === undefined &&
    params.regimeState === filters.regimeState &&
    params.riskState === filters.riskState &&
    params.technicalState === filters.technicalState
  );
}

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
