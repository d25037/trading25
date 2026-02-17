/**
 * Screening-related types for frontend
 * Re-exports from @trading25/shared and adds frontend-specific types
 */

import type { BacktestMetric, ScreeningSortBy, SortOrder } from '@trading25/shared/types/api-response-types';

export type {
  BacktestMetric,
  MatchedStrategyItem,
  MarketScreeningResponse,
  ScreeningResultItem,
  ScreeningSortBy,
  ScreeningSummary,
  SortOrder,
} from '@trading25/shared/types/api-response-types';

// Frontend-specific types
export interface ScreeningParams {
  markets?: string;
  strategies?: string;
  recentDays?: number;
  date?: string;
  backtestMetric?: BacktestMetric;
  sortBy?: ScreeningSortBy;
  order?: SortOrder;
  limit?: number;
}
