/**
 * Screening-related types for frontend
 * Re-exports from @trading25/contracts and adds frontend-specific types
 */

import type { EntryDecidability, ScreeningSortBy, SortOrder } from '@trading25/contracts/types/api-response-types';

export type {
  EntryDecidability,
  MarketScreeningResponse,
  MatchedStrategyItem,
  ScreeningJobRequest,
  ScreeningJobResponse,
  ScreeningResultItem,
  ScreeningSortBy,
  ScreeningSummary,
  SortOrder,
} from '@trading25/contracts/types/api-response-types';

// Frontend-specific types
export interface ScreeningParams {
  entry_decidability?: EntryDecidability;
  markets?: string;
  strategies?: string;
  recentDays?: number;
  date?: string;
  sortBy?: ScreeningSortBy;
  order?: SortOrder;
  limit?: number;
}
