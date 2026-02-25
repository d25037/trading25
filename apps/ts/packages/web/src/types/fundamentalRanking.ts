/**
 * Fundamental ranking types for frontend
 * Re-exports from @trading25/shared and adds frontend-specific params
 */

export type {
  FundamentalRankingItem,
  FundamentalRankingSource,
  FundamentalRankings,
  MarketFundamentalRankingResponse,
} from '@trading25/shared/types/api-response-types';

export interface FundamentalRankingParams {
  limit?: number;
  markets?: string;
  forecastAboveAllActuals?: boolean;
}
