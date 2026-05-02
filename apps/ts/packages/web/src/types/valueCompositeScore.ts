import type { ValueCompositeScoreResponse } from '@trading25/contracts/types/api-response-types';

export type { ValueCompositeScoreResponse } from '@trading25/contracts/types/api-response-types';

export interface ValueCompositeScoreParams {
  symbol: string;
  date?: string;
  forwardEpsMode?: ValueCompositeScoreResponse['forwardEpsMode'];
}
