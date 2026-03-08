/**
 * Screening-related types for frontend
 * Re-exports from @trading25/contracts and adds frontend-specific types
 */

import type { ScreeningMode, ScreeningSortBy, SortOrder } from '@trading25/contracts/types/api-response-types';

export type {
  MatchedStrategyItem,
  MarketScreeningResponse,
  ScreeningJobRequest,
  ScreeningJobResponse,
  ScreeningMode,
  ScreeningResultItem,
  ScreeningSortBy,
  ScreeningSummary,
  SortOrder,
} from '@trading25/contracts/types/api-response-types';

// Frontend-specific types
export interface ScreeningParams {
  mode?: ScreeningMode;
  markets?: string;
  strategies?: string;
  recentDays?: number;
  date?: string;
  sortBy?: ScreeningSortBy;
  order?: SortOrder;
  limit?: number;
}
