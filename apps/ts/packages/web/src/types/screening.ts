/**
 * Screening-related types for frontend
 * Re-exports from @trading25/shared and adds frontend-specific types
 */

import type { ScreeningSortBy, SortOrder } from '@trading25/shared/types/api-response-types';

export type {
  FuturePricePoint,
  FutureReturns,
  MarketScreeningResponse,
  RangeBreakDetails,
  ScreeningResultItem,
  ScreeningSortBy,
  ScreeningSummary,
  ScreeningType,
  SortOrder,
} from '@trading25/shared/types/api-response-types';

// Frontend-specific types
export interface ScreeningParams {
  markets?: string;
  rangeBreakFast?: boolean;
  rangeBreakSlow?: boolean;
  recentDays?: number;
  date?: string;
  minBreakPercentage?: number;
  minVolumeRatio?: number;
  sortBy?: ScreeningSortBy;
  order?: SortOrder;
  limit?: number;
}
