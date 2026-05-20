/**
 * Ranking-related types for frontend
 * Re-exports from @trading25/contracts and adds frontend-specific types
 */

export type {
  IndexPerformanceItem,
  MarketRankingResponse,
  RankingItem,
  Rankings,
  RankingType,
  SortOrder,
} from '@trading25/contracts/types/api-response-types';

export type RankingDailyView = 'stocks' | 'technicalEvents' | 'indices';
export type RankingTechnicalEventType = 'periodHigh' | 'periodLow';
export type RankingLiquidityState =
  | 'neutral_rerating'
  | 'crowded_rerating'
  | 'distribution_stress'
  | 'stale_liquidity'
  | 'neutral'
  | 'overheat';
export type RankingSortField =
  | 'tradingValue'
  | 'changePercentage'
  | 'code'
  | 'currentPrice'
  | 'per'
  | 'forwardPer'
  | 'forwardPOp'
  | 'pbr'
  | 'marketCap'
  | 'liquidityResidualZ'
  | 'adv60ToFreeFloatPct';
export type RankingSortOrder = 'asc' | 'desc';

// Frontend-specific types
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
  sortBy?: RankingSortField;
  order?: RankingSortOrder;
  forwardEpsDisclosedWithinDays?: number;
  liquidityState?: RankingLiquidityState;
}
