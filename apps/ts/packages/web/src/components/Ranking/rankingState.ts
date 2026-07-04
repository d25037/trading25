import type { DailyRankingTableFilters, RankingParams } from '@/types/ranking';

export type EquityRiskFlag = 'overheat' | 'stale_rally_fade';
export type EquityTechnicalFlag = 'atr20_acceleration' | 'momentum_20_60_top20';
export type RankingPreset =
  | 'all'
  | 'core_long'
  | 'earnings_priority'
  | 'aggressive_rerating'
  | 'overvalued_breakdown'
  | 'momentum_value'
  | 'neutral_rerating'
  | 'neutral_rerating_good'
  | 'crowded_momentum'
  | 'crowded_rerating'
  | 'crowded_rerating_good'
  | 'overheat'
  | 'custom';

type RankingFilterParams = Pick<
  RankingParams,
  'liquidityState' | 'regimeState' | 'fundamentalState' | 'riskState' | 'technicalState'
>;

export const RANKING_PRESET_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'core_long', label: 'Core Long' },
  { value: 'earnings_priority', label: 'Earnings Priority' },
  { value: 'aggressive_rerating', label: 'Aggressive Rerating' },
  { value: 'overvalued_breakdown', label: 'Overvalued Breakdown' },
  { value: 'momentum_value', label: 'Momentum Value' },
  { value: 'neutral_rerating', label: 'Neutral All' },
  { value: 'neutral_rerating_good', label: 'Neutral Good' },
  { value: 'crowded_momentum', label: 'Crowded Momentum' },
  { value: 'crowded_rerating', label: 'Crowded All' },
  { value: 'crowded_rerating_good', label: 'Crowded Good' },
  { value: 'overheat', label: 'Overheat' },
  { value: 'custom', label: 'Custom' },
] as const satisfies readonly { value: RankingPreset; label: string }[];

export const RANKING_PRESET_DESCRIPTIONS = {
  all: 'All: no named table filters.',
  core_long: 'Core Long: Neutral Rerating + Deep Value + ATR20 Accel + liquidity z -1..2.',
  earnings_priority: 'Earnings Priority: Core Long + Fwd OP/OP >= 1.2.',
  aggressive_rerating: 'Aggressive Rerating: Crowded Rerating + Deep Value + ATR20 Accel + liquidity z 1..2.',
  overvalued_breakdown: 'Overvalued Breakdown: Expensive OR + Sector Strength <= 0.4 + SMA5 Weak 0/1.',
  momentum_value: 'Momentum Value: Neutral Rerating + Deep Value + 20/60D Momentum confirmation + liquidity z -1..2.',
  neutral_rerating: 'Neutral All: neutral rerating regime with balanced sector strength.',
  neutral_rerating_good: 'Neutral Good: neutral rerating regime with Deep Value confirmation.',
  crowded_momentum: 'Crowded Momentum: Crowded All + 20/60D Momentum confirmation.',
  crowded_rerating: 'Crowded All: crowded rerating regime with balanced sector strength.',
  crowded_rerating_good: 'Crowded Good: crowded rerating regime with Value Confirmed fundamentals.',
  overheat: 'Overheat: 20D return overheat warning.',
  custom: 'Custom: current Ranking state does not exactly match a preset.',
} as const satisfies Record<RankingPreset, string>;

export const RANKING_REGIME_STATE_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'neutral_rerating', label: 'Neutral Rerating' },
  { value: 'crowded_rerating', label: 'Crowded Rerating' },
  { value: 'distribution_stress', label: 'Stress' },
  { value: 'neutral', label: 'Neutral' },
] as const satisfies readonly { value: RankingParams['regimeState'] | 'all'; label: string }[];

export const RANKING_RISK_STATE_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'overheat', label: 'Overheat' },
] as const satisfies readonly { value: RankingParams['riskState'] | 'all'; label: string }[];

export const RANKING_TECHNICAL_STATE_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'atr20_acceleration', label: 'ATR20 Accel' },
  { value: 'momentum_20_60_top20', label: '20/60D Momentum' },
] as const satisfies readonly { value: RankingParams['technicalState'] | 'all'; label: string }[];

type RankingPresetBundle = {
  tableFilters?: DailyRankingTableFilters;
};

const PRESET_TABLE_FILTER_KEYS = [
  'text',
  'market',
  'sector33Name',
  'watchlistId',
  'regimeState',
  'valuationSignal',
  'warningSignal',
  'riskState',
  'technicalState',
  'minChangePct',
  'maxChangePct',
  'minTradingValue',
  'maxTradingValue',
  'minMarketCap',
  'maxMarketCap',
  'minSma5AboveCount5d',
  'maxSma5AboveCount5d',
  'minPer',
  'maxPer',
  'minForwardPer',
  'maxForwardPer',
  'minForecastOperatingProfitGrowthRatio',
  'maxForecastOperatingProfitGrowthRatio',
  'minPsr',
  'maxPsr',
  'minForwardPsr',
  'maxForwardPsr',
  'minPbr',
  'maxPbr',
  'minLiquidityZ',
  'maxLiquidityZ',
  'minSectorScore',
  'maxSectorScore',
] as const satisfies readonly (keyof DailyRankingTableFilters)[];

const RANKING_PRESET_BUNDLES: Record<Exclude<RankingPreset, 'custom'>, RankingPresetBundle> = {
  all: {
    tableFilters: {},
  },
  core_long: {
    tableFilters: {
      regimeState: 'neutral_rerating',
      technicalState: 'atr20_acceleration',
      valuationSignal: 'deep_value',
      minLiquidityZ: -1,
      maxLiquidityZ: 2,
    },
  },
  earnings_priority: {
    tableFilters: {
      regimeState: 'neutral_rerating',
      technicalState: 'atr20_acceleration',
      valuationSignal: 'deep_value',
      minForecastOperatingProfitGrowthRatio: 1.2,
      minLiquidityZ: -1,
      maxLiquidityZ: 2,
    },
  },
  aggressive_rerating: {
    tableFilters: {
      regimeState: 'crowded_rerating',
      technicalState: 'atr20_acceleration',
      valuationSignal: 'deep_value',
      minLiquidityZ: 1,
      maxLiquidityZ: 2,
    },
  },
  overvalued_breakdown: {
    tableFilters: {
      valuationSignal: 'expensive_or',
      warningSignal: 'sma5_weak_0_1',
      maxSectorScore: 0.4,
    },
  },
  momentum_value: {
    tableFilters: {
      regimeState: 'neutral_rerating',
      technicalState: 'momentum_20_60_top20',
      valuationSignal: 'deep_value',
      minLiquidityZ: -1,
      maxLiquidityZ: 2,
    },
  },
  neutral_rerating: {
    tableFilters: {
      regimeState: 'neutral_rerating',
    },
  },
  neutral_rerating_good: {
    tableFilters: {
      regimeState: 'neutral_rerating',
      valuationSignal: 'deep_value',
    },
  },
  crowded_momentum: {
    tableFilters: {
      regimeState: 'crowded_rerating',
      technicalState: 'momentum_20_60_top20',
    },
  },
  crowded_rerating: {
    tableFilters: {
      regimeState: 'crowded_rerating',
    },
  },
  crowded_rerating_good: {
    tableFilters: {
      regimeState: 'crowded_rerating',
      valuationSignal: 'value_confirmed',
    },
  },
  overheat: {
    tableFilters: {
      warningSignal: 'overheat',
    },
  },
};

export function applyRankingPreset(
  params: RankingParams,
  tableFilters: DailyRankingTableFilters,
  preset: RankingPreset
): {
  rankingParams: RankingParams;
  rankingTableFilters: DailyRankingTableFilters;
} {
  if (preset === 'custom') return { rankingParams: params, rankingTableFilters: tableFilters };

  const clearedPresetParams: RankingFilterParams = {
    liquidityState: undefined,
    regimeState: undefined,
    fundamentalState: undefined,
    riskState: undefined,
    technicalState: undefined,
  };

  const bundle = RANKING_PRESET_BUNDLES[preset];

  return {
    rankingParams: {
      ...params,
      ...clearedPresetParams,
    },
    rankingTableFilters: {
      ...clearPresetTableFilters(tableFilters),
      ...bundle.tableFilters,
    },
  };
}

export function getRankingPreset(tableFilters: DailyRankingTableFilters = {}): RankingPreset {
  for (const [preset, bundle] of Object.entries(RANKING_PRESET_BUNDLES)) {
    if (hasMatchingPresetTableFilters(tableFilters, bundle.tableFilters ?? {})) {
      return preset as RankingPreset;
    }
  }
  return 'custom';
}

function hasMatchingPresetTableFilters(
  tableFilters: DailyRankingTableFilters,
  presetTableFilters: DailyRankingTableFilters
): boolean {
  return PRESET_TABLE_FILTER_KEYS.every((key) => tableFilters[key] === presetTableFilters[key]);
}

function clearPresetTableFilters(tableFilters: DailyRankingTableFilters): DailyRankingTableFilters {
  const nextFilters = { ...tableFilters };
  for (const key of PRESET_TABLE_FILTER_KEYS) {
    delete nextFilters[key];
  }
  return nextFilters;
}

export function getRankingPresetDescription(preset: RankingPreset): string {
  return RANKING_PRESET_DESCRIPTIONS[preset];
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
