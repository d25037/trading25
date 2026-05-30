/**
 * Ranking-related frontend route/filter params.
 */

export type RankingDailyView = 'stocks' | 'technicalEvents' | 'indices';
export type RankingTechnicalEventType = 'periodHigh' | 'periodLow';
export type RankingTechnicalState = 'atr20_acceleration' | 'momentum_20_60_top20';
export type RankingRiskState = 'overheat' | 'stale_rally_fade';
export type RankingRegimeState =
  | 'neutral_rerating'
  | 'neutral_rerating_good'
  | 'crowded_rerating'
  | 'crowded_rerating_good'
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
  | 'sectorStrengthScore'
  | 'per'
  | 'forwardPer'
  | 'forwardPOp'
  | 'pbr'
  | 'marketCap'
  | 'liquidityResidualZ'
  | 'adv60ToFreeFloatPct';
export type RankingSortOrder = 'asc' | 'desc';

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
  sortBy?: RankingSortField;
  order?: RankingSortOrder;
  forwardEpsDisclosedWithinDays?: number;
  liquidityState?: RankingLiquidityState;
  regimeState?: RankingRegimeState;
  riskState?: RankingRiskState;
  technicalState?: RankingTechnicalState;
}
