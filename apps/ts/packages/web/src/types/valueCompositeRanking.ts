/**
 * Value-composite ranking types for frontend.
 */

import type {
  ValueCompositeForwardEpsMode,
  ValueCompositeProfileId,
  ValueCompositeScoreMethod,
} from '@trading25/contracts/types/api-response-types';

export type {
  ValueCompositeForwardEpsMode,
  ValueCompositeProfileId,
  ValueCompositeRankingItem,
  ValueCompositeRankingResponse,
  ValueCompositeScoreMethod,
} from '@trading25/contracts/types/api-response-types';

export interface ValueCompositeRankingParams {
  date?: string;
  limit?: number;
  markets?: string;
  profileId?: ValueCompositeProfileId;
  scoreMethod?: ValueCompositeScoreMethod;
  applyLiquidityFilter?: boolean;
  forwardEpsMode?: ValueCompositeForwardEpsMode;
}
