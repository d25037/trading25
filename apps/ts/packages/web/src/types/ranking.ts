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
  Topix100PriceBucket,
  Topix100RankingItem,
  Topix100RankingResponse,
  Topix100VolumeBucket,
} from '@trading25/contracts/types/api-response-types';

export type RankingPageTab = 'ranking' | 'fundamentalRanking';
export type RankingDailyView = 'stocks' | 'indices' | 'topix100';
export type Topix100PriceBucketFilter = 'all' | 'q1' | 'q10' | 'q456';
export type Topix100VolumeBucketFilter = 'all' | 'high' | 'low';

// Frontend-specific types
export interface RankingParams {
  date?: string;
  limit?: number;
  markets?: string;
  lookbackDays?: number;
  periodDays?: number;
  topix100PriceBucket?: Topix100PriceBucketFilter;
  topix100VolumeBucket?: Topix100VolumeBucketFilter;
}
