/**
 * Ranking-related frontend route/filter params.
 */

export type RankingDailyView = 'stocks' | 'technicalEvents' | 'indices';
export type RankingTechnicalEventType = 'periodHigh' | 'periodLow';
export type RankingTechnicalState = 'atr20_acceleration' | 'momentum_20_60_top20';
export type RankingRiskState = 'overheat' | 'stale_rally_fade';
export type SectorStrengthFamily = 'balanced_sector_strength' | 'long_hybrid_leadership';
export type RankingRegimeState =
  | 'neutral_rerating'
  | 'crowded_rerating'
  | 'distribution_stress'
  | 'stale_liquidity'
  | 'neutral';
export type RankingLiquidityState =
  | 'neutral_rerating'
  | 'crowded_rerating'
  | 'distribution_stress'
  | 'stale_liquidity'
  | 'neutral'
  | 'overheat'
  | 'stale_rally_fade';
export type RankingSortField =
  | 'tradingValue'
  | 'changePercentage'
  | 'code'
  | 'currentPrice'
  | 'sma5AboveCount5d'
  | 'sectorStrengthScore'
  | 'per'
  | 'forwardPer'
  | 'forecastOperatingProfitGrowthRatio'
  | 'psr'
  | 'forwardPsr'
  | 'pbr'
  | 'marketCap'
  | 'liquidityResidualZ'
  | 'adv60ToFreeFloatPct';
export type RankingSortOrder = 'asc' | 'desc';
export type DailyRankingValuationSignalFilter =
  | 'deep_value'
  | 'value_confirmed'
  | 'undervalued'
  | 'expensive_or'
  | 'overvalued'
  | 'very_overvalued'
  | 'no_earnings';
export type DailyRankingWarningFilter = 'overheat' | 'sma5_weak_0_1' | 'sma5_below_streak_3';

export interface DailyRankingTableFilters {
  text?: string;
  market?: string;
  sector33Name?: string;
  watchlistId?: number;
  regimeState?: RankingRegimeState;
  valuationSignal?: DailyRankingValuationSignalFilter;
  warningSignal?: DailyRankingWarningFilter;
  riskState?: RankingRiskState;
  technicalState?: RankingTechnicalState;
  minChangePct?: number;
  maxChangePct?: number;
  minTradingValue?: number;
  maxTradingValue?: number;
  minMarketCap?: number;
  maxMarketCap?: number;
  minSma5AboveCount5d?: number;
  maxSma5AboveCount5d?: number;
  minPer?: number;
  maxPer?: number;
  minForwardPer?: number;
  maxForwardPer?: number;
  minForecastOperatingProfitGrowthRatio?: number;
  maxForecastOperatingProfitGrowthRatio?: number;
  minPsr?: number;
  maxPsr?: number;
  minForwardPsr?: number;
  maxForwardPsr?: number;
  minPbr?: number;
  maxPbr?: number;
  minLiquidityZ?: number;
  maxLiquidityZ?: number;
  minSectorScore?: number;
  maxSectorScore?: number;
}

export interface RankingParams {
  date?: string;
  limit?: number;
  markets?: string;
  lookbackDays?: number;
  periodDays?: number;
  technicalEventType?: RankingTechnicalEventType;
  sector33Name?: string;
  sector17Name?: string;
  includeValuation?: boolean;
  includeSectorStrength?: boolean;
  sectorStrengthFamily?: SectorStrengthFamily;
  sortBy?: RankingSortField;
  order?: RankingSortOrder;
  forwardEpsDisclosedWithinDays?: number;
  liquidityState?: RankingLiquidityState;
  regimeState?: RankingRegimeState;
  fundamentalState?: DailyRankingValuationSignalFilter;
  riskState?: RankingRiskState;
  technicalState?: RankingTechnicalState;
}
