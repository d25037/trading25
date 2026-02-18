/**
 * Screening-related types for frontend
 * Re-exports from @trading25/shared and adds frontend-specific types
 */

import type { ScreeningSortBy, SortOrder } from '@trading25/shared/types/api-response-types';

export type {
  MatchedStrategyItem,
  MarketScreeningResponse,
  ScreeningJobRequest,
  ScreeningJobResponse,
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
  sortBy?: ScreeningSortBy;
  order?: SortOrder;
  limit?: number;
}
