/**
 * Value-composite ranking types for frontend.
 */

import type {
  ValueCompositeForwardEpsMode,
  ValueCompositeScoreMethod,
} from '@trading25/contracts/types/api-response-types';

export type {
  ValueCompositeForwardEpsMode,
  ValueCompositeRankingItem,
  ValueCompositeRankingResponse,
  ValueCompositeScoreMethod,
} from '@trading25/contracts/types/api-response-types';

export interface ValueCompositeRankingParams {
  date?: string;
  limit?: number;
  markets?: string;
  scoreMethod?: ValueCompositeScoreMethod;
  forwardEpsMode?: ValueCompositeForwardEpsMode;
}
