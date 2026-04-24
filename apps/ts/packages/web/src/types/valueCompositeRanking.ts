/**
 * Value-composite ranking types for frontend.
 */

import type { ValueCompositeScoreMethod } from '@trading25/contracts/types/api-response-types';

export type {
  ValueCompositeScoreMethod,
  ValueCompositeRankingItem,
  ValueCompositeRankingResponse,
} from '@trading25/contracts/types/api-response-types';

export interface ValueCompositeRankingParams {
  date?: string;
  limit?: number;
  markets?: string;
  scoreMethod?: ValueCompositeScoreMethod;
}
