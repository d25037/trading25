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
  Topix100StreakMode,
  Topix100StudyMode,
  Topix100VolumeBucket,
} from '@trading25/contracts/types/api-response-types';

export type { Topix100RankingMetric } from '@trading25/contracts/types/api-response-types';

export type RankingPageTab = 'ranking' | 'fundamentalRanking';
export type RankingDailyView = 'stocks' | 'indices' | 'topix100';
export type Topix100PriceBucketFilter = 'all' | 'q1' | 'q10' | 'q234';
export type Topix100VolumeBucketFilter = 'all' | 'high' | 'low';
export type Topix100StreakModeFilter = 'all' | 'bullish' | 'bearish';
export type Topix100RankingSortKey =
  | 'rank'
  | 'code'
  | 'companyName'
  | 'metric'
  | 'volumeBucket'
  | 'streakShortMode'
  | 'streakLongMode'
  | 'longScore5d'
  | 'longScore5dRank'
  | 'intradayScore'
  | 'intradayLongRank'
  | 'intradayShortRank'
  | 'openToClose5dReturn'
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
  topix100VolumeBucket?: Topix100VolumeBucketFilter;
  topix100ShortMode?: Topix100StreakModeFilter;
  topix100LongMode?: Topix100StreakModeFilter;
  topix100SortBy?: Topix100RankingSortKey;
  topix100SortOrder?: SortOrder;
}
