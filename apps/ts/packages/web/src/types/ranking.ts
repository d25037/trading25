/**
 * Ranking-related types for frontend
 * Re-exports from @trading25/contracts and adds frontend-specific types
 */

import type {
  SortOrder,
  Topix100PriceSmaWindow,
  Topix100RankingMetric,
  Topix100StudyMode,
} from '@trading25/contracts/types/api-response-types';

export type {
  IndexPerformanceItem,
  MarketRankingResponse,
  RankingItem,
  Rankings,
  SortOrder,
  RankingType,
  Topix100PriceBucket,
  Topix100PriceSmaWindow,
  Topix100RankingItem,
  Topix100RankingResponse,
  Topix100StudyMode,
} from '@trading25/contracts/types/api-response-types';

export type { Topix100RankingMetric } from '@trading25/contracts/types/api-response-types';

export type RankingPageTab = 'ranking' | 'fundamentalRanking' | 'valueComposite';
export type RankingDailyView = 'stocks' | 'indices' | 'topix100';
export type Topix100PriceBucketFilter = 'all' | 'q1' | 'q10' | 'q234';
export type Topix100RankingSortKey =
  | 'rank'
  | 'code'
  | 'companyName'
  | 'metric'
  | 'longScore5d'
  | 'longScore5dRank'
  | 'intradayScore'
  | 'intradayLongRank'
  | 'intradayShortRank'
  | 'openToOpen5dReturn'
  | 'nextSessionIntradayReturn'
  | 'volumeSma5_20'
  | 'currentPrice'
  | 'sector33Name'
  | 'volume';

// Frontend-specific types
export interface RankingParams {
  date?: string;
  limit?: number;
  markets?: string;
  lookbackDays?: number;
  periodDays?: number;
  topix100StudyMode?: Topix100StudyMode;
  topix100Metric?: Topix100RankingMetric;
  topix100SmaWindow?: Topix100PriceSmaWindow;
  topix100PriceBucket?: Topix100PriceBucketFilter;
  topix100SortBy?: Topix100RankingSortKey;
  topix100SortOrder?: SortOrder;
}
