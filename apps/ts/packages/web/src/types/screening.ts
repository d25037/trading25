/**
 * Screening-related frontend route/filter params.
 */

import type { EntryDecidability, ScreeningSortBy, SortOrder } from '@trading25/contracts/types/api-response-types';

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
