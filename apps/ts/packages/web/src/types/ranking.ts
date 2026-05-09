/**
 * Ranking-related types for frontend
 * Re-exports from @trading25/contracts and adds frontend-specific types
 */

export type {
  IndexPerformanceItem,
  MarketRankingResponse,
  RankingItem,
  Rankings,
  SortOrder,
  RankingType,
} from '@trading25/contracts/types/api-response-types';

export type RankingPageTab = 'ranking' | 'fundamentalRanking' | 'valueComposite';
export type RankingDailyView = 'stocks' | 'indices';

// Frontend-specific types
export interface RankingParams {
  date?: string;
  limit?: number;
  markets?: string;
  lookbackDays?: number;
  periodDays?: number;
  sector33Name?: string;
  sector17Name?: string;
  includeValuation?: boolean;
}
