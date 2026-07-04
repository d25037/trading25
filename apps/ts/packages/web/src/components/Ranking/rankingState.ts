import type { DailyRankingTableFilters, RankingParams } from '@/types/ranking';

export type EquityRiskFlag = 'overheat' | 'stale_rally_fade';
export type EquityTechnicalFlag = 'atr20_acceleration' | 'momentum_20_60_top20';

type RankingFilterParams = Pick<RankingParams, 'regimeState' | 'fundamentalState' | 'riskState' | 'technicalState'>;

interface RankingPresetDefinition {
  value: string;
  label: string;
  description: string;
  tableFilters?: DailyRankingTableFilters;
}

const RANKING_PRESET_DEFINITIONS = [
  {
    value: 'all',
    label: 'All',
    description: 'All: no named table filters.',
    tableFilters: {},
  },
  {
    value: 'core_long',
    label: 'Core Long',
    description: 'Core Long: Neutral Rerating + Deep Value + ATR20 Accel + liquidity z -1..2.',
    tableFilters: {
      regimeState: 'neutral_rerating',
      technicalState: 'atr20_acceleration',
      valuationSignal: 'deep_value',
      minLiquidityZ: -1,
      maxLiquidityZ: 2,
    },
  },
  {
    value: 'earnings_priority',
    label: 'Earnings Priority',
    description: 'Earnings Priority: Core Long + Fwd OP/OP >= 1.2.',
    tableFilters: {
      regimeState: 'neutral_rerating',
      technicalState: 'atr20_acceleration',
      valuationSignal: 'deep_value',
      minForecastOperatingProfitGrowthRatio: 1.2,
      minLiquidityZ: -1,
      maxLiquidityZ: 2,
    },
  },
  {
    value: 'aggressive_rerating',
    label: 'Aggressive Rerating',
    description: 'Aggressive Rerating: Crowded Rerating + Deep Value + ATR20 Accel + liquidity z 1..2.',
    tableFilters: {
      regimeState: 'crowded_rerating',
      technicalState: 'atr20_acceleration',
      valuationSignal: 'deep_value',
      minLiquidityZ: 1,
      maxLiquidityZ: 2,
    },
  },
  {
    value: 'overvalued_breakdown',
    label: 'Overvalued Breakdown',
    description: 'Overvalued Breakdown: Expensive OR + Sector Strength <= 0.4 + SMA5 Weak 0/1.',
    tableFilters: {
      valuationSignal: 'expensive_or',
      warningSignal: 'sma5_weak_0_1',
      maxSectorScore: 0.4,
    },
  },
  {
    value: 'momentum_value',
    label: 'Momentum Value',
    description: 'Momentum Value: Neutral Rerating + Deep Value + 20/60D Momentum confirmation + liquidity z -1..2.',
    tableFilters: {
      regimeState: 'neutral_rerating',
      technicalState: 'momentum_20_60_top20',
      valuationSignal: 'deep_value',
      minLiquidityZ: -1,
      maxLiquidityZ: 2,
    },
  },
  {
    value: 'neutral_rerating',
    label: 'Neutral All',
    description: 'Neutral All: neutral rerating regime with balanced sector strength.',
    tableFilters: {
      regimeState: 'neutral_rerating',
    },
  },
  {
    value: 'neutral_rerating_good',
    label: 'Neutral Good',
    description: 'Neutral Good: neutral rerating regime with Deep Value confirmation.',
    tableFilters: {
      regimeState: 'neutral_rerating',
      valuationSignal: 'deep_value',
    },
  },
  {
    value: 'crowded_momentum',
    label: 'Crowded Momentum',
    description: 'Crowded Momentum: Crowded All + 20/60D Momentum confirmation.',
    tableFilters: {
      regimeState: 'crowded_rerating',
      technicalState: 'momentum_20_60_top20',
    },
  },
  {
    value: 'crowded_rerating',
    label: 'Crowded All',
    description: 'Crowded All: crowded rerating regime with balanced sector strength.',
    tableFilters: {
      regimeState: 'crowded_rerating',
    },
  },
  {
    value: 'crowded_rerating_good',
    label: 'Crowded Good',
    description: 'Crowded Good: crowded rerating regime with Value Confirmed fundamentals.',
    tableFilters: {
      regimeState: 'crowded_rerating',
      valuationSignal: 'value_confirmed',
    },
  },
  {
    value: 'overheat',
    label: 'Overheat',
    description: 'Overheat: 20D return overheat warning.',
    tableFilters: {
      warningSignal: 'overheat',
    },
  },
  {
    value: 'custom',
    label: 'Custom',
    description: 'Custom: current Ranking state does not exactly match a preset.',
    tableFilters: undefined,
  },
] as const satisfies readonly RankingPresetDefinition[];

export type RankingPreset = (typeof RANKING_PRESET_DEFINITIONS)[number]['value'];

export const RANKING_PRESET_OPTIONS = RANKING_PRESET_DEFINITIONS.map(({ value, label }) => ({
  value,
  label,
})) as readonly { value: RankingPreset; label: string }[];

export const RANKING_PRESET_DESCRIPTIONS = Object.fromEntries(
  RANKING_PRESET_DEFINITIONS.map(({ value, description }) => [value, description])
) as Record<RankingPreset, string>;

export const RANKING_REGIME_STATE_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'neutral_rerating', label: 'Neutral Rerating' },
  { value: 'crowded_rerating', label: 'Crowded Rerating' },
  { value: 'distribution_stress', label: 'Stress' },
  { value: 'neutral', label: 'Neutral' },
] as const satisfies readonly { value: RankingParams['regimeState'] | 'all'; label: string }[];

export const RANKING_TECHNICAL_STATE_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'atr20_acceleration', label: 'ATR20 Accel' },
  { value: 'momentum_20_60_top20', label: '20/60D Momentum' },
] as const satisfies readonly { value: RankingParams['technicalState'] | 'all'; label: string }[];

const PRESET_TABLE_FILTER_KEYS = [
  'text',
  'market',
  'sector33Name',
  'watchlistId',
  'regimeState',
  'valuationSignal',
  'warningSignal',
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
    regimeState: undefined,
    fundamentalState: undefined,
    riskState: undefined,
    technicalState: undefined,
  };

  const definition = getPresetDefinition(preset);

  return {
    rankingParams: {
      ...params,
      ...clearedPresetParams,
    },
    rankingTableFilters: {
      ...clearPresetTableFilters(tableFilters),
      ...(definition.tableFilters ?? {}),
    },
  };
}

export function getRankingPreset(tableFilters: DailyRankingTableFilters = {}): RankingPreset {
  for (const definition of RANKING_PRESET_DEFINITIONS) {
    if (definition.value === 'custom') continue;
    if (hasMatchingPresetTableFilters(tableFilters, definition.tableFilters ?? {})) {
      return definition.value;
    }
  }
  return 'custom';
}

function getPresetDefinition(preset: RankingPreset) {
  return RANKING_PRESET_DEFINITIONS.find((definition) => definition.value === preset) ?? RANKING_PRESET_DEFINITIONS[0];
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
